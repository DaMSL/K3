{-# LANGUAGE ViewPatterns #-}

-- Remove read only binds from the code
module Language.K3.Transform.RemoveROBinds (
  addProfiling
) where

import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Transform.Common

import Language.K3.Core.Constructor.Expression
import qualified Language.K3.Core.Constructor.Type as T
import qualified Language.K3.Core.Constructor.Declaration as D

-- Loop over trigger/function
modifyExpression :: K3 Expression -> K3 Expression
modifyExpression p = mapIn1RebuildTree pre ch1F allChF p
  where 
    preF acc ch1 (details -> (Bind 
    ch1F = 
    allChF = 

setOfBinderIds :: Binder -> Set Identifier
setOfBinderIds (BRecord idNames) = Set.fromList $ map snd idNames

-- Transform the tree based on the read-only annotations
transformExpr :: K3 Expression -> K3 Expression
transformExpr e = runIdentity $ mapIn1RebuildTree sendProjection
  where
    -- For the first child, we always send the same set we already have, since the first child
    -- of a bind does not get the bind's assignment
    -- Lambda modifies its child's context
    sendProjection acc _ (tag -> ELambda id) = Map.delete id acc
    sendProjection acc _ _ = acc

    -- Send down a union of the current ro-set and the one found at this node (if any)
    -- Let, case and bind only capture in the 2nd child's context only
    ch1SendProjection acc _ (tag -> ELetIn id)  = Map.delete id acc
    ch1SendProjection acc _ (tag -> ECaseOf id) = Map.delete id acc

    -- These binds are unhandled, and can only capture ids
    ch1SendProjection acc _ (tag -> (EBindAs (BIndirection id))) = Map.delete id acc
    ch1SendProjection acc _ (tag -> (EBindAs (BTuple ids))) = foldr Map.delete acc ids

    -- bind captures some ids, and makes others read-only
    ch1SendProjection acc _ (details -> (EBindAs (BRecord idNames), chs, annos @~ AnalysisAnnotation(ReadOnlyBind(roIds)))) =
      let bindIds  = Map.fromList $ flipList idNames
          flipList = map (\(a,b) -> (b,a))
          acc'  = (acc `Map.difference` bindIds) `Map.union` roIds
      in (acc', replicate (length chs - 1) acc') -- send to all the other children

    ch1SendProjection acc _ _ = acc

    -- Modify variable accesses that match the ids we've sent
    handleNode acc _  (tag -> EVariable id) | id `Map.member` acc = id `Map.lookup` acc
    handleNode acc ch (details -> (EBindAs (BRecord idNames), chs, annos @~ AnalysisAnnotation(ReadOnlyBind(roIds)))) =

-- Annotate read-only binds in the tree
annotateROBinds :: K3 Expression -> K3 Expression
annotateROBinds e = runIdentity $ foldMapRebuildTree accumWrite Map.empty e
  where
    -- Accumulate the tags that have a write (assignment) bottom-up
    -- Remove ids as we encounter different binding structures
    accumWrite accs ch e@(tag -> EAssign id) = (Map.insert id E.unit (Map.unions accs), Node e ch)

    -- handle the case where we bind a variable
    accumWrite accs (ch@(tag -> EVariable):_) e@(tag -> EBindAs(BRecord idNames)) = 
      let idMap = Map.fromList $ map (second . toProjection) $ flipList idNames
          toProjection projId = E.project projID ch
          -- transmit the set of written ids, and annotate with the ones that aren't written to
      in (Map.difference (accs!!1) ids, Node e ch :@: AnalysisAnnotation $ ReadOnlyBind $ Map.difference idMap $ accs!!1)

    -- handle the case where we bind an expression -- we now need to use let

    accumWrite accs ch e = (Map.unions accs, Node e ch)



-- Get a a list of triggers and global functions
declMap :: K3 Declaration -> [String]
declMap ds = runIdentity $ foldTree add [] ds
  where
    add :: [String] -> K3 Declaration -> Identity [String]
    add acc (tag -> DGlobal id _ (Just e)) | isLambda e = return $ id:acc
    add acc (tag -> DTrigger id _ _) = return $ id:acc
    add acc _ = return $ acc

isLambda :: K3 Expression -> Bool
isLambda (tag -> ELambda _) = True
isLambda _                  = False

-- Wrap declarations with time expressions before and after
wrapDecls :: K3 Declaration -> K3 Declaration
wrapDecls ds = runIdentity $ mapTree wrap ds
  where
    wrap ch (details -> (DGlobal id t (Just expr), _, annos)) | isLambda expr = 
      return $ Node (DGlobal id t (Just $ wrapCode id expr) :@: annos) ch
    wrap ch (details -> (DTrigger id t expr, _, annos)) =
      return $ Node (DTrigger id t (wrapCode id expr) :@: annos) ch
    wrap ch (Node x _) = return $ Node x ch

-- Wrap a specific expression before and after
wrapCode :: String -> K3 Expression -> K3 Expression
wrapCode id expr =
  let tempVar = "__timeVar"
      gTimeVar = globalId id  -- global var for this declaration
  in
  letIn tempVar (immut $ binop OApp (variable "now") $ immut unit) $
    block 
      [expr,
      bindAs
        (immut $ variable gTimeVar)
        (BRecord [("time", "t"), ("count", "c")]) $
          block
            [assign "t" $
              binop OApp
                (binop OApp (variable "add_time") 
                  (variable "t")) $
                binop OApp
                  (binop OApp (variable "sub_time")
                    (binop OApp (variable "now") $ immut unit)) $
                  (variable tempVar),
              -- Increment the counter
              assign "c" $
                binop OAdd
                  (variable "c") $
                  immut $ constant $ CInt 1]]

-- Transform code to have profiling code bits added 
addProfiling :: K3 Declaration -> K3 Declaration
addProfiling p = appendGlobals $ wrapDecls p
  where
    appendGlobals decs@(details -> (DRole id, ds, annos)) = 
      -- Get a list of declarations, and add the variables
      let vars = map globalVar $ declMap decs
      in Node (DRole id :@: annos) (vars++ds)

    appendGlobals x = error $ "Expected role but found "++show x

