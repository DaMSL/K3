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

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog =
  let annos = collectAnnos prog
      map = annosToMap annos
  in placeAnnosInExprs map prog

-- Collect all the collection annotations in the tree
collectAnnos :: K3 Declaration -> [Declaration]
collectAnnos prog = runIdentity $ foldTree addDecl [] prog
  where
    addDecl acc n@(tag -> x@(DDataAnnotation _ _ _)) = return $ x:acc
    addDecl acc _ = return acc

-- Convert the collection annotations to a map
annosToMap :: [Declaration] -> AnnoMap
annosToMap annos = foldr addAnno Map.empty annos
  where addAnno (DDataAnnotation i _ annMems) acc =
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

    unwrapTAnno (TAnnotation i) = [i]
    unwrapTAnno _               = []

    -- Translate declaration annotations to expression annoations
    decToExpr (DProperty i m) = [EProperty i m]
    decToExpr (DSyntax s)     = [ESyntax s]
    decToExpr _               = []

    findProps ids project =
      case catMaybes $ map (lookupId project) ids of
        [ps] -> ps
        []   -> []
        _    -> error $ "multiple definitions for same projection "++project++" found"

    lookupId project i = do
      innermap <- Map.lookup i annoMap
      props    <- Map.lookup project innermap
      return props

