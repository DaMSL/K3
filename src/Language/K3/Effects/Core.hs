{-# LANGUAGE TypeFamilies #-}

-- | Data Representations for K3 Effects
module Language.K3.Effects.Core where

import Language.K3.Core.Annotation
import Language.K3.Core.Common

data EffectKind
    = FAtom
    | FSymbol
    | FFunction
  deriving (Eq, Read, Show)

data instance Annotation EffectKind = KAnnotation deriving (Eq, Read, Show)

data Provenance
    = FRecord Identifier
    | FTuple Integer
    | FIndirection
    | FLet
    -- The following can be roots
    | FLambdaVar
    | FTemporary
    | FGlobal
  deriving (Eq, Read, Show)

data Symbol = Symbol Identifier Provenance deriving (Eq, Read, Show)

data instance Annotation Symbol = SAnnotation deriving (Eq, Read, Show)

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    | FScope [K3 Symbol]
    | FVariable Identifier
    | FSubst
    | FLambda Identifier
    | FSeq
    | FSet   -- Set of effects, all of which are possible
    | FLoop                   -- a flattened loop
  deriving (Eq, Read, Show)

data instance Annotation Effect = FAnnotation
                                | FId Int
                                deriving (Eq, Read, Show)

