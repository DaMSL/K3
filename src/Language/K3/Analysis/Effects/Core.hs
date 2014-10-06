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

data TempType
    = TAlias    -- The temporary is an alias
    | TSub      -- The temporary is a subtype of the next element
    | TIndirect -- The temporary is an indirection
    | TUnbound  -- Unbound global
    | TTemp     -- The temp has no connection to any other element
    deriving (Eq, Ord, Read, Show)

data Provenance
    = PRecord Identifier
    | PTuple Integer
    | PIndirection
    | PLet
    | PCase
    -- A symbol can be 'applied' to produce effects and a new symbol
    | PLambda Identifier (K3 Effect)
    -- A symbol application only generates symbols
    | PApply
    -- Any of the children of PSet can occur
    | PSet
    -- The following can be roots
    | PVar
    | PTemporary TempType
    | PGlobal
  deriving (Eq, Ord, Read, Show)

data Symbol = Symbol Identifier Provenance deriving (Eq, Ord, Read, Show)

data instance Annotation Symbol = SID Int deriving (Eq, Ord, Read, Show)

isSID :: Annotation Symbol -> Bool
isSID _ = True

type ClosureInfo = ([K3 Symbol], [K3 Symbol], [K3 Symbol])

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    -- bound, read, written, applied within the scope
    | FScope [K3 Symbol] ClosureInfo
    -- An effect application only generates effects
    | FApply (K3 Symbol) (K3 Symbol)
    | FSeq
    | FSet   -- Set of effects, all of which are possible
    | FLoop  -- a flattened loop. can only happen in a foreign function
  deriving (Eq, Ord, Read, Show)

data instance Annotation Effect = FID Int deriving (Eq, Ord, Read, Show)

isFID :: Annotation Effect -> Bool
isFID _ = True
