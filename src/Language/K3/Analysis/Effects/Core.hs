{-# LANGUAGE TypeFamilies #-}

-- | Data Representations for K3 Effects
module Language.K3.Analysis.Effects.Core where

import Language.K3.Core.Annotation
import Language.K3.Core.Common

data EffectKind
    = FAtom
    | FSymbol
    | FFunction
  deriving (Eq, Read, Show)

data instance Annotation EffectKind = KAnnotation deriving (Eq, Read, Show)

data Provenance
    = PRecord Identifier
    | PTuple Integer
    | PIndirection
    | PLet
    | PCase  -- not a reliable alias. Doesn't fully allow pulling out of effects
    -- A symbol can be 'applied' to produce effects and a new symbol
    | PLambda Identifier (Maybe (K3 Effect))
    | PApply
    -- Any of the children of PSet can occur
    | PSet
    -- The following can be roots
    | PVar
    | PTemporary
    | PGlobal
  deriving (Eq, Ord, Read, Show)

data Symbol = Symbol Identifier Provenance deriving (Eq, Ord, Read, Show)

data instance Annotation Symbol = SID Int deriving (Eq, Ord, Read, Show)

isSID :: Annotation Symbol -> Bool
isSID _ = True

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    | FScope [K3 Symbol]
    | FVariable Identifier
    | FApply (K3 Symbol) (K3 Symbol)
    | FSeq
    | FSet   -- Set of effects, all of which are possible
    | FLoop                   -- a flattened loop
  deriving (Eq, Ord, Read, Show)

data instance Annotation Effect = FID Int deriving (Eq, Ord, Read, Show)

isFID :: Annotation Effect -> Bool
isFID _ = True
