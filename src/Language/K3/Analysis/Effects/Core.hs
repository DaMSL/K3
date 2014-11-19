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

-- Temporaries represent missing symbols or return values
-- For now, we assume all return values are copied back (according to semantics)
data TempType
    = TDirect     -- The temporary leads directly to its child symbol
    | TSub        -- The temporary is a subtype of the next element
    | TIndirect   -- The temporary is an indirection
    | TTemp       -- The temp has no connection to any other element
    deriving (Eq, Ord, Read, Show)

data Provenance
    = PRecord Identifier
    | PTuple Integer
    | PIndirection
    | PProject Identifier -- Created by projections
    | PLet
    | PCase
    | PClosure
    -- A symbol can be 'applied' to produce effects and a new symbol
    -- 2nd field is closure
    | PLambda (K3 Effect)
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

-- Unless we mark with IsAlias, we assume separation semantics
data HasCopy = HasCopy | NoCopy
                 deriving (Eq, Ord, Read, Show)
data HasWriteback = HasWriteback | NoWriteback
                      deriving (Eq, Ord, Read, Show)

data Symbol = Symbol Identifier Provenance HasCopy HasWriteback
            | SymId Int
            deriving (Eq, Ord, Read, Show)

data instance Annotation Symbol = SID Int deriving (Eq, Ord, Read, Show)

isSID :: Annotation Symbol -> Bool
isSID _ = True

type ClosureInfo = ([K3 Symbol], [K3 Symbol], [K3 Symbol])

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    | FScope [K3 Symbol]
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
  prettyLines (Node (Symbol i (PLambda j cl eff) cp _ :@: as) ch) =
    ["Symbol " ++ i ++ " PLambda " ++ j ++ " " ++ show cp ++ " " ++ drawAnnotations as] ++
      (case (cl, ch) of
        ([], []) -> terminalShift eff
        ([], _)  -> nonTerminalShift eff ++ drawSubTrees ch
        (_,  []) -> drawSubTrees cl ++ terminalShift eff
        (_,  _ ) -> drawSubTrees cl ++ nonTerminalShift eff ++ drawSubTrees ch)

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

  prettyLines (Node (FScope syms :@: as) ch) =
    ["FScope " ++ drawAnnotations as]
      ++ (concatMap (shift "+- " "|  " . prettyLines) syms)
      ++ drawSubTrees ch

  prettyLines (Node (FApply fSym argSym :@: as) ch) =
    ["FApply " ++ drawAnnotations as] ++
      (if null ch
        then nonTerminalShift fSym ++ terminalShift argSym
        else nonTerminalShift fSym ++ nonTerminalShift argSym ++ drawSubTrees ch)

  prettyLines (Node (tg :@: as) ch) = (show tg ++ drawAnnotations as) : drawSubTrees ch
