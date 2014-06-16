{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}

-- Remove read only binds from the code
module Language.K3.Transform.RemoveROBinds (
  transform
) where

import Control.Monad.Identity
import Data.Tree
import qualified Data.Map as Map
import Data.Map(Map)
import qualified Data.Set as Set
import Data.Set(Set)
import Data.Maybe(isJust, fromMaybe)
import Control.Arrow(second)
import Control.Monad.State

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Core.Constructor.Expression

isROBAnno :: Annotation Expression -> Bool
isROBAnno (EAnalysis(ReadOnlyBind _)) = True
isROBAnno _                                    = False

getROBval :: Maybe (Annotation Expression) -> [String]
getROBval (Just (EAnalysis(ReadOnlyBind x))) = x
getROBval _ = error "Not a ReadOnlyBind annotation"

flipList :: [(a, b)] -> [(b, a)]
flipList = map (\(a,b) -> (b,a))

-- Entry point for the module
-- Transform all functions and triggers: 
transform :: K3 Declaration -> K3 Declaration
transform ds = runIdentity $ mapTree wrap ds
  where
    wrap ch (details -> (DGlobal nm t (Just expr@(tag -> ELambda _)), _, annos)) = 
      return $ Node (DGlobal nm t (Just $ transformExpr $ annotateROBinds expr) :@: annos) ch
    wrap ch (details -> (DTrigger nm t expr, _, annos)) =
      return $ Node (DTrigger nm t (transformExpr $ annotateROBinds expr) :@: annos) ch
    wrap ch (Node x _) = return $ Node x ch

-- Stage 1
-- Annotate read-only binds in the tree
annotateROBinds :: K3 Expression -> K3 Expression
annotateROBinds e = snd $ runIdentity $ foldMapRebuildTree accumWrite Set.empty e
  where
    -- Accumulate the tags that have a write (assignment) bottom-up
    -- Remove ids as we encounter binds. 
    -- Lets, cases and lambdas don't support assign anyway so ignore them
    accumWrite :: [Set String] -> [K3 Expression] -> K3 Expression -> Identity (Set String, K3 Expression)
    accumWrite accs ch (details -> (t@(EAssign nm), _, annos)) =
      return (Set.insert nm (Set.unions accs), Node (t :@: annos) ch)

    -- unhandled. Just remove/capture a binding
    accumWrite accs ch (details -> (t@(EBindAs (BIndirection nm)), _, annos)) =
      return (Set.delete nm (accs!!1), Node (t :@: annos) ch)

    accumWrite accs ch (details -> (t@(EBindAs binder), _, annos)) = 
      let ids = Set.fromList $ case binder of
                                 BRecord idsMap -> map snd idsMap
                                 BTuple  ids'   -> ids'
                                 BIndirection _ -> error "unexpected"
      -- transmit the set of written ids, and annotate with the ones that aren't written to
          node = Node (t :@: annos @+ (EAnalysis $ ReadOnlyBind $ Set.toList $ Set.difference ids $ accs!!1)) ch
      in return (Set.difference (accs!!1) ids, node)

    accumWrite accs ch (details -> (t, _, annos)) = return (Set.unions accs, Node (t :@: annos) ch)


-- Stage 2: Go over the tree top down to send read-only binds to the statements where they're used.
-- Stage 3 (simultaneously with 2) transform the variable accesses and the binds themselves bottom-up

                   
-- The type we send down the tree.
--                 Variable (record label, record variable name)
type BindMap = Map String (String, String)

-- Transform the read-only binds to projections
-- When we send the message down the tree, lets/lambdas can capture both the bind id and the variable
-- we project from. Both need to be pruned.
transformExpr :: K3 Expression -> K3 Expression
transformExpr e = evalState computation 1
  where
    computation = foldMapIn1RebuildTree sendProjection ch1SendProjection handleNode Map.empty e

    -- For the first child, we always send the same set we already have, since the first child
    -- of a bind does not get the bind's assignment
    -- Lambda modifies its child's context
    sendProjection :: Monad m => BindMap -> K3 Expression -> K3 Expression -> m BindMap
    sendProjection acc _ (tag -> ELambda nm) = return $ Map.delete nm acc
    sendProjection acc _ _ = return acc

    replicateCh ch = replicate (length ch - 1)

    -- The post-child function requires a tuple, so let's duplicate it
    removeFromMap ids acc ch =
      let m = foldr Map.delete acc ids in
      return (m, replicateCh ch m)

    -- Send down a union of the current ro-set and the one found at this node (if any)
    -- Let, case and bind capture in the 2nd child's context only
    ch1SendProjection :: BindMap -> K3 Expression -> K3 Expression -> State Int (BindMap, [BindMap])
    ch1SendProjection acc _ (details -> (ELetIn nm, ch, _))  = removeFromMap [nm] acc ch
    ch1SendProjection acc _ (details -> (ECaseOf nm, ch, _)) = removeFromMap [nm] acc ch

    -- These binds are unhandled for this modification, and can only capture ids
    ch1SendProjection acc _ (details -> (EBindAs (BIndirection nm), ch, _)) = removeFromMap [nm] acc ch
    ch1SendProjection acc _ (details -> (EBindAs (BTuple ids), ch, _)) = removeFromMap ids acc ch

    ch1SendProjection acc _ (details -> (EBindAs (BRecord idNames), chs, annos)) | isJust (annos @~ isROBAnno) = do
      num <- get
      put $ num + 1
      let roIds = getROBval $ annos @~ isROBAnno 
          varId = "_rovar_"++show num  -- fresh variable name
          -- Create the data structure we want to transmit
          bindIds  = Map.fromList $ map (second (,varId)) $ flipList idNames
          roIds'   = Map.fromList $ map (\x -> (x, fromMaybe (error "Missing binds") $ Map.lookup x bindIds)) roIds
          -- bind captures some ids, and makes others read-only
          acc'     = (acc `Map.difference` bindIds) `Map.union` roIds'
      return (acc', replicateCh chs acc') -- send to all the other children

    ch1SendProjection acc _ (Node _ ch) = return (acc, replicateCh ch acc)

    -- Tranform variable accesses -> projections
    handleNode :: Monad m => BindMap -> [K3 Expression] -> K3 Expression -> m (K3 Expression)
    handleNode acc _  (tag -> EVariable nm) | nm `Map.member` acc = 
      let (proj, var) = fromMaybe (error "unexpected") $ nm `Map.lookup` acc
      in return $ immut $ project proj $ variable var

    -- We may need a let to replace our bind. 
    -- If some non-RO bindings remain, we need to keep them as well
    handleNode acc [ch1, ch2]
      (details -> (EBindAs (BRecord idNames), _, annos)) | isJust $ annos @~ isROBAnno = return addLet
      where
        roIds = getROBval $ annos @~ isROBAnno
        -- Lookup our saved newId
        newId = maybe (error "not found") snd $ Map.lookup (head roIds) acc
        addLet = immut $ letIn newId (immut ch1) $ remainingBinds newId

        remainingBinds nm = 
          let remain = Map.toList $ Map.difference (Map.fromList $ flipList idNames) $ Map.fromList $ map (\x -> (x,"")) roIds
          in if null remain then ch2
             else Node (EBindAs (BRecord remain) :@: annos) [variable nm, ch2]

    handleNode _ ch (Node x _) = return $ Node x ch
              

