{-# LANGUAGE ViewPatterns #-}

-- | Common tools for code generation.
module Language.K3.Codegen.Common (
    annotationComboId,
    annotationComboIdT,
    annotationComboIdE,
    annotationComboIdL,

    recordName
) where

import Language.K3.Core.Common

import Language.K3.Core.Annotation (K3, Annotation)
import Language.K3.Core.Expression (Expression, namedEAnnotations)
import Language.K3.Core.Literal (Literal, namedLAnnotations)
import Language.K3.Core.Type (Type, namedTAnnotations)

import Data.List

{- Annotation identifiers. -}

annotationComboId :: [Identifier] -> Identifier
annotationComboId = intercalate "_"

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (namedTAnnotations -> [])  = Nothing
annotationComboIdT (namedTAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (namedEAnnotations -> [])  = Nothing
annotationComboIdE (namedEAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdL :: [Annotation Literal] -> Maybe Identifier
annotationComboIdL (namedLAnnotations -> [])  = Nothing
annotationComboIdL (namedLAnnotations -> ids) = Just $ annotationComboId ids

-- Record Identifiers

recordName :: [(Identifier, K3 Type)] -> Identifier
recordName = undefined
