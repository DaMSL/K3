{-# LANGUAGE TypeFamilies #-}

-- | Types in K3.
module Language.K3.Core.Type (
    Identifier,
    Span(..),
    Type(..),
    Annotation(..)
) where

import Language.K3.Core.Annotation

-- | The types of K3 Identifiers.
type Identifier = String

-- | K3 source code spans (source name, line1, column1, line2, column2)
data Span = Span String Int Int Int Int deriving (Eq, Read, Show)

-- | Tags in the Type Tree. Every type can be qualified with a mutability annotation.
data Type
    = TBool
    | TByte
    | TInt
    | TFloat
    | TString
    | TOption
    | TIndirection
    | TTuple
    | TRecord [Identifier]
    | TCollection
    | TFunction
    | TSource
    | TSink
    | TTrigger [Identifier]
  deriving (Eq, Read, Show)

-- | Annotations on types are the mutability qualifiers.
instance Annotatable Type where
    data Annotation Type
        = TMutable
        | TImmutable
        | TWitness
        | TSpan Span
        | TAnnotation Identifier
      deriving (Eq, Read, Show)
