{-# LANGUAGE TypeFamilies #-}

-- | Types in K3.
module Language.K3.Core.Type (
    Identifier,
    Type(..),
    Annotation(..)
) where

import Language.K3.Core.Annotation

-- | The types of K3 Identifiers.
type Identifier = String

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
  deriving (Eq, Read, Show)

-- | Annotations on types are the mutability qualifiers.
instance Annotatable Type where
    data Annotation Type
        = TMutable
        | TImmutable
        | TWitness
      deriving (Eq, Read, Show)
