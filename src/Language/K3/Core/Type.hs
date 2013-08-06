{-# LANGUAGE TypeFamilies #-}

-- | Types in K3.
module Language.K3.Core.Type
( Type(..)
, TypeBuiltIn(..)
, Annotation(..)
) where

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Pretty

-- * Basic types

-- | Tags in the Type Tree. Every type can be qualified with a mutability
--   annotation.  This set of tags is a superset of those which can be parsed
--   by the type expression grammar in the K3 specification because it also
--   represents the (unparseable) types inferred by the type system.
data Type
    = TBool
    | TByte
    | TInt
    | TReal
    | TString
    | TOption
    | TIndirection
    | TTuple
    | TRecord [Identifier]
    | TCollection
    | TFunction
    | TAddress
    | TSource
    | TSink
    | TTrigger [Identifier]
    | TBuiltIn TypeBuiltIn
  deriving (Eq, Read, Show)
  
-- | The built-in type references.
data TypeBuiltIn
    = TSelf
    | TStructure
    | THorizon
    | TContent
  deriving (Eq, Read, Show)

-- | Annotations on types are the mutability qualifiers.
data instance Annotation Type
    = TMutable
    | TImmutable
    | TWitness
    | TSpan Span
    | TAnnotation Identifier
  deriving (Eq, Read, Show)

instance Pretty Type where
    prettyLines = (:[]) . show
