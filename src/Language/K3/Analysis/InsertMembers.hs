{-# LANGUAGE ViewPatterns #-}

-- | Insert annotation member data into the expression tree
module Language.K3.Analysis.InsertMembers (
    runAnalysis
)
where

import Control.Monad.Identity
import Control.Arrow ( (&&&), first )

import qualified Data.Map as Map
import Data.Map(Map)
import Data.Maybe
import Data.Tree
import Data.List(foldl')

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Analysis.Common

-- Map of collection annotations to attributes to properties
type AnnoMap = Map Identifier (Map Identifier [Annotation Declaration])
type GlobalMap = Map Identifier [Annotation Declaration]


runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog =
  let annos = collectAnnos prog
      map = annosToMap annos

      gMap = globalMap prog
      prog' = placeGlobalsInExprs gMap prog

  in placeAnnosInExprs map prog'

-- Collect all the collection annotations in the tree
collectAnnos :: K3 Declaration -> [Declaration]
collectAnnos prog = runIdentity $ foldTree addDecl [] prog
  where
    addDecl acc n@(tag -> x@(DAnnotation _ _ _)) = return $ x:acc
    addDecl acc _ = return acc

-- Convert the collection annotations to a map
annosToMap :: [Declaration] -> AnnoMap
annosToMap annos = foldr addAnno Map.empty annos
  where addAnno (DAnnotation i _ annMems) acc =
          let m = foldr addMember Map.empty annMems
          in Map.insert i m acc

        addMember (Lifted _ i _ _ annDecs) acc    = addMemberIdDecs i annDecs acc
        addMember (Attribute _ i _ _ annDecs) acc = addMemberIdDecs i annDecs acc
        addMemberIdDecs i annDecs map =
          case filter isValuable annDecs of
            []   -> map
            decs -> Map.insert i decs map
        isValuable (DProperty _ _) = True
        isValuable (DSyntax _)     = True
        isValuable _               = False

-- Insert the annotations in the expression tree
placeAnnosInExprs :: AnnoMap -> K3 Declaration -> K3 Declaration
placeAnnosInExprs annoMap prog = runIdentity $ mapProgram m_id m_id placeInExpr prog
  where
    m_id x = return x

    placeInExpr :: Monad m => K3 Expression -> m (K3 Expression)
    placeInExpr expr = modifyTree handleExpr expr

    handleExpr :: Monad m => K3 Expression -> m (K3 Expression)
    handleExpr n@(tag &&& children -> (EProject p, [ch])) =
      case ch @~ isEType of
        Nothing -> error "No type found on projection"
        Just (EType t@(tag &&& annotations -> (TCollection, tannos))) ->
          -- Find out which collection annoations we're using
          case filter isTAnnotation tannos of
            []     -> return n
            tAnnos ->
              let ids    = concatMap unwrapTAnno tAnnos -- get collection ids
                  props  = findProps ids p
                  props' = concatMap decToExpr props
              in return $ foldl' (@+) n props'
        Just _ -> return n
    handleExpr e = return e

    unwrapTAnno (TAnnotation i) = [i]
    unwrapTAnno _               = []


    findProps ids project =
      case catMaybes $ map (lookupId project) ids of
        [ps] -> ps
        []   -> []
        _    -> error $ "multiple definitions for same projection "++project++" found"

    lookupId project i = do
      innermap <- Map.lookup i annoMap
      props    <- Map.lookup project innermap
      return props

-- For each global in the tree, add an entry to the globals map.
globalMap :: K3 Declaration -> GlobalMap
globalMap prog = runIdentity $ foldTree addDecl Map.empty prog
  where
  addDecl acc ((tag &&& annotations) -> ((DGlobal n _ _), as) ) = return $ Map.insert n (filter isProperty as) acc
  addDecl acc _ = return acc

  isProperty (DProperty _ _) = True
  isProperty _                = False

placeGlobalsInExprs :: GlobalMap -> K3 Declaration -> K3 Declaration
placeGlobalsInExprs gMap prog = runIdentity $ mapProgram return return (visitE gMap) prog
  where
  -- Attempt to attach properties to an expression.
  -- Then recurse onto its children, shadowing globals along the way.
  visitE :: GlobalMap -> K3 Expression -> Identity (K3 Expression)
  visitE gs e = do
    let new_e  = attachProperties gs e
    let new_gs = tryBind gs e
    new_cs <- mapM (visitE new_gs) (children e)
    return $ replaceCh new_e new_cs

  -- Return the new globalMap, removing any shadowed identifiers
  tryBind :: GlobalMap -> K3 Expression -> GlobalMap
  tryBind gs (tag -> ELetIn i)                  = Map.delete i gs
  tryBind gs (tag -> ELambda i)                 = Map.delete i gs
  tryBind gs (tag -> EBindAs (BTuple ns))       = foldr (Map.delete) gs ns
  tryBind gs (tag -> EBindAs (BIndirection i))  = Map.delete i gs
  tryBind gs (tag -> EBindAs (BRecord tups))    = foldl (\acc -> \(_,b) -> Map.delete b acc) gs tups
  tryBind gs _ = gs

  -- Attach properties associated with this global identifier, if there are any.
  attachProperties :: GlobalMap -> K3 Expression -> K3 Expression
  attachProperties gs e@(tag -> EVariable i) = case Map.lookup i gs of
    Nothing -> e
    Just l -> foldl (\e ann -> foldl (@+) e $ decToExpr ann) e l
  attachProperties _ e = e

  isGlobal gs (tag -> EVariable i) = Map.member i gs
  isGlobal _ _ = False

-- Translate declaration annotations to expression annoations
decToExpr (DProperty i m) = [EProperty i m]
decToExpr (DSyntax s)     = [ESyntax s]
decToExpr _               = []
