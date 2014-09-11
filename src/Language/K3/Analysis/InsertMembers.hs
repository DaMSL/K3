{-# LANGUAGE ViewPatterns #-}

-- | Alias analysis for bind expressions.
--   This is used by the interpreter to ensure consistent modification
--   of alias variables following the interpretation of a bind expression.
module Language.K3.Analysis.Interpreter.BindAlias (
    labelBindAliases
  , labelBindAliasesExpr
)
where

import Control.Arrow ( (&&&), first )

import Data.Map
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

type AnnoMap = Map Identifier (Map Identifier [Annotation Declaration])

liftMemberAnnos :: K3 Declaration -> K3 Declaration
liftMemberAnnos prog =
  let annos = collectAnnos prog
      map = annosToMap annos
  in insertInExpr prog map

-- Collect all the collection annotations in the tree
collectAnnos :: K3 Declaration -> [DAnnotation]
collectAnnos prog = runIdentity $ foldTree addDecl [] prog
  where
    addDecl acc n@(tag -> DAnnotation _ _ _) = n::acc
    addDecl acc _ = acc

-- Convert the collection annotations to a map
annosToMap :: [DAnnotation] -> AnnoMap
annosToMap annos = foldr addAnno Map.empty annos
  where addAnno (DAnnotation i _ annMems) acc =
          let m = foldr addMember Map.empty annMems
          in Map.insert i m acc

        addMember (Lifted _ i _ _ annDecs) acc    = addMemberIdDecs i annDecs
        addMember (Attribute _ i _ _ annDecs) acc = addMemberIdDecs i annDecs
        addMemberIdDecs i annDecs =
          case filter isValuable annDecs of
            []   -> acc
            decs -> Map.insert i decs acc
        isValuable (DProperty _) = True
        isValuable (DSyntax _)   = True
        isValuable _ = False

placeAnnosInExprs :: K3 Declaration -> AnnoM (K3 Declaration)
placeAnnosInExprs annoMap prog = runIdentity $ mapProgram id id placeInExpr prog
  where
    placeInExpr expr = modifyTree handleExpr expr

    handleExpr n@(tag &&& children -> EProject p, [ch]) =
      case ch @~ isEType of
        Nothing -> error "No type found on projection"
        Just (Node (TCollection :@: annos) _) ->
          case annos @~ isTAnnotation of
            Nothing     -> n
            Just tAnnos ->
              let ids = mapTAnnos tAnnos
                  props = findProps ids p
              in fold @+ n $ concatMap decToExpr props
        Just _ -> n

    -- Return ids of Tannotations
    mapTAnnos annos = concatMap unwrapTAnno annos

    unwrapTAnno (TAnnotation i) = [i]
    unwrapTAnno _               = []

    decToExpr (DProperty i m) = [EProptery i m]
    decToExpr (DSyntax s)     = [ESyntax s]
    decToExpr _               = []

    findProps ids project =
      case catMaybes $ map (lookupId project) ids of
        [ps] -> ps
        []   -> []
         _   -> error $ "multiple definitions for same projection "++project++" found"

    lookupId project i = do
      innermap <- Map.lookup i annoMap
      props    <- Map.lookup project innermap
      return props

