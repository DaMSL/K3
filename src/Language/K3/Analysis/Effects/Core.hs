{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | Data Representations for K3 Effects
module Language.K3.Analysis.Effects.Core where

import Data.Tree
import Data.Map hiding (null)

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Utils.Pretty

data EffectKind
    = FAtom
    | FSymbol
    | FFunction
  deriving (Eq, Read, Show)

data instance Annotation EffectKind = KAnnotation deriving (Eq, Read, Show)

data TempType
    = TAlias      -- The temporary is an alias
    | TSub        -- The temporary is a subtype of the next element
    | TIndirect   -- The temporary is an indirection
    | TUnbound    -- Unbound global
    | TTemp       -- The temp has no connection to any other element
    | TSubstitute -- A projection requires a substitution
    deriving (Eq, Ord, Read, Show)

data Provenance
    = PRecord Identifier
    | PTuple Integer
    | PIndirection
    | PProject Identifier -- Created by projections
    | PLet
    | PCase
    | PScope [K3 Symbol] MaybeClosure
    -- A symbol can be 'applied' to produce effects and a new symbol
    | PLambda Identifier (K3 Effect)
    -- A symbol application only generates symbols
    | PApply
    -- Any of the children of PSet can occur
    | PSet
    | PChoice -- One of the cases must be chosen ie. they're exclusive
    -- The following can be roots
    | PVar
    | PTemporary TempType
    | PGlobal
  deriving (Eq, Ord, Read, Show)

data Symbol = Symbol Identifier Provenance
            | SymId Int
            deriving (Eq, Ord, Read, Show)

data instance Annotation Symbol = SID Int deriving (Eq, Ord, Read, Show)

isSID :: Annotation Symbol -> Bool
isSID _ = True

type ClosureInfo = ([K3 Symbol], [K3 Symbol], [K3 Symbol])
type MaybeClosure = Either (Map Identifier [K3 Symbol], Maybe(K3 Effect), Maybe(K3 Symbol)) ClosureInfo

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    -- bound, read, written, applied within the scope
    | FScope [K3 Symbol] MaybeClosure
    -- An effect application only generates effects
    | FApply (K3 Symbol) (K3 Symbol)
    | FIO
    | FSeq
    | FSet   -- Set of effects, all of which are possible
    | FLoop  -- a flattened loop. can only happen in a foreign function
    | FEffId Int
  deriving (Eq, Ord, Read, Show)

data instance Annotation Effect = FID Int deriving (Eq, Ord, Read, Show)

isFID :: Annotation Effect -> Bool
isFID _ = True


instance Pretty (K3 Symbol) where
  prettyLines (Node (Symbol i (PLambda j eff) :@: as) ch) =
    ["Symbol " ++ i ++ " PLambda " ++ j ++ " " ++ drawAnnotations as] ++
      (if null ch
        then terminalShift eff
        else nonTerminalShift eff ++ drawSubTrees ch)

  prettyLines (Node (tg :@: as) ch) = (show tg ++ drawAnnotations as) : drawSubTrees ch

instance Pretty (K3 Effect) where
  prettyLines (Node (FRead  sym :@: as) ch) =
    ["FRead " ++ drawAnnotations as] ++
      (if null ch
        then terminalShift sym
        else nonTerminalShift sym ++ drawSubTrees ch)

  prettyLines (Node (FWrite sym :@: as) ch) =
    ["FWrite " ++ drawAnnotations as] ++
      (if null ch
        then terminalShift sym
        else nonTerminalShift sym ++ drawSubTrees ch)

  prettyLines (Node (FScope syms (Right (rd,wr,app)) :@: as) ch) =
    ["FScope " ++ drawAnnotations as]
      ++ (concatMap (shift "+- " "|  " . prettyLines) syms)
      ++ (concatMap (shift "+R " "|  " . prettyLines) rd)
      ++ (concatMap (shift "+W " "|  " . prettyLines) wr)
      ++ (concatMap (shift "+A " "|  " . prettyLines) app)
      ++ drawSubTrees ch

  prettyLines (Node (FApply fSym argSym :@: as) ch) =
    ["FApply " ++ drawAnnotations as] ++
      (if null ch
        then nonTerminalShift fSym ++ terminalShift argSym
        else nonTerminalShift fSym ++ nonTerminalShift argSym ++ drawSubTrees ch)

  prettyLines (Node (tg :@: as) ch) = (show tg ++ drawAnnotations as) : drawSubTrees ch
