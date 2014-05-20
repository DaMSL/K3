{-# LANGUAGE ViewPatterns #-}

-- | Common tools for code generation.
module Language.K3.Codegen.Common (
    annotationComboId,
    annotationComboIdT,
    annotationComboIdE,
    annotationComboIdL,

    recordName,
    signature
) where

import Control.Arrow ((&&&))

import Language.K3.Core.Common

import Language.K3.Core.Annotation (K3, tag, children)
import Language.K3.Core.Expression (Expression, namedEAnnotations)
import Language.K3.Core.Literal (Literal, namedLAnnotations)
import Language.K3.Core.Type

import Data.List

{- Annotation identifiers. -}

annotationComboId :: [Identifier] -> Identifier
annotationComboId = ('_':) . intercalate "_"

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
recordName its = concat $ fst $ unzip its

signature :: Monad m => K3 Type -> m Identifier
signature (tag -> TBool)        = return "P"
signature (tag -> TByte)        = return "B"
signature (tag -> TInt)         = return "N"
signature (tag -> TReal)        = return "D"
signature (tag -> TString)      = return "S"
signature (tag -> TAddress)     = return "A"

signature (tag &&& children -> (TOption, [x])) = signature x >>= return . ("O" ++)

signature (tag &&& children -> (TTuple, ch)) =
  mapM signature ch >>= return . (("T" ++ (show $ length ch)) ++) . concat

signature (tag &&& children -> (TRecord ids, ch)) =
  mapM signature ch
  >>= return . intercalate "_" . map (\(x,y) -> (sanitize x) ++ "_" ++ y) . zip ids
  >>= return . (("R" ++ (show $ length ch)) ++)

signature (tag &&& children -> (TIndirection, [x])) = signature x >>= return . ("I" ++)
signature (tag &&& children -> (TCollection, [x]))  = signature x >>= return . ("C" ++)

signature (tag &&& children -> (TFunction, [a,r]))  = signature a >>= \s -> signature r >>= return . (("F" ++ s) ++)
signature (tag &&& children -> (TSink, [x]))        = signature x >>= return . ("K" ++)
signature (tag &&& children -> (TSource, [x]))      = signature x >>= return . ("U" ++)

signature (tag &&& children -> (TTrigger, [x])) = signature x >>= return . ("G" ++)

sanitize = id
