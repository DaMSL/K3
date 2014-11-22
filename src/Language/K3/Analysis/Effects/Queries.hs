{-# LANGUAGE ViewPatterns #-}

-- | High-level effect queries.
module Language.K3.Analysis.Effects.Queries where

import Control.Applicative
import Control.Arrow
import Control.Monad.State

import Data.Traversable

import Control.Monad.Identity

import Language.K3.Core.Annotation

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects

type QueryS = EffectEnv
type QueryM = StateT QueryS Identity

runQueryM :: QueryM a -> QueryS -> (a, QueryS)
runQueryM = runState

evalQueryM :: QueryM a -> QueryS -> a
evalQueryM = evalState

getEnv :: QueryM EffectEnv
getEnv = get

eSM :: K3 Symbol -> QueryM (K3 Symbol)
eSM s = eS <$> getEnv <*> pure s

eEM :: K3 Effect -> QueryM (K3 Effect)
eEM e = eE <$> getEnv <*> pure e

(===) :: K3 Symbol -> K3 Symbol -> Bool
(===) = symEqual

isIsolated :: K3 Symbol -> QueryM Bool
isIsolated s = case s of
  (tag -> SymId _) -> eSM s >>= isIsolated
  (tag -> symHasCopy -> b) -> return b

isMoved :: K3 Symbol -> QueryM Bool
isMoved s = case s of
  (tag -> SymId _) -> eSM s >>= isMoved
  (tag -> symHasMove -> b) -> return b

isWrittenBack :: K3 Symbol -> QueryM Bool
isWrittenBack s = case s of
  (tag -> SymId _) -> eSM s >>= isWrittenBack
  (tag -> symHasWb -> b) -> return b

-- | Is this symbol a global?
--
-- This query is materialization-aware.
isGlobal :: K3 Symbol -> QueryM Bool
isGlobal s = case s of
  -- Expand symbols first.
  (tag -> SymId _) -> eSM s >>= isGlobal

  -- A global is a global.
  (tag -> symProv -> PGlobal) -> return True

  -- Partially evaluated applications aren't globals.
  (tag -> symProv -> PApply) -> return False

  -- Any symbol which is isolated from its ancestors cannot be a global.
  (tag -> symHasCopy -> True) -> return False

  -- Otherwise, a symbol is (conservatively) a global if /any/ of its ancestors are globals.
  (children -> ss) -> or <$> traverse isGlobal ss

-- | Is this symbol derived from a global?
--
-- This query is materialization-agnostic.
isDerivedGlobal :: K3 Symbol -> QueryM Bool
isDerivedGlobal s = case s of
  -- Expand symbols first.
  (tag -> SymId _) -> eSM s >>= isDerivedGlobal

  -- A global is derived from a global.
  (tag -> symProv -> PGlobal) -> return True

  -- A non-global symbol is derived from a global if any of its ancestors are derived from globals.
  (children -> ss) -> or <$> traverse isDerivedGlobal ss

-- | Is =s= derived from =t=?
--
-- This query is materialization-agnostic.
isDerivedIndirectlyFrom :: K3 Symbol -> K3 Symbol -> QueryM Bool
isDerivedIndirectlyFrom s t = case s of
  -- A symbol is derived from itself. This does not require either symbol to be expanded.
  _ | s === t -> return True

  -- Any other check requires source symbol alone to be expanded.
  (tag -> SymId _) -> eSM s >>= flip isDerivedIndirectlyFrom t

  -- =s= is derived from =t= if any ancestor of =s= is derived from =t=.
  (children -> ss) -> or <$> traverse (flip isDerivedIndirectlyFrom t) ss

-- | Is =s= derived /directly/ from =t=?
--
-- This query is materialization-aware
isDerivedDirectlyFrom :: K3 Symbol -> K3 Symbol -> QueryM Bool
isDerivedDirectlyFrom s t = case s of
  -- A symbol is derived directly from itself.
  _ | s === t -> return True

  -- Any other check requires the source symbol to be expanded.
  (tag -> SymId _) -> eSM s >>= flip isDerivedDirectlyFrom t

  -- If the given symbol is isolated, it is not derived directly from anything.
  (tag -> symHasCopy -> True) -> return False

  -- If the symbol is not isolated, it may be derived directly from any of its ancestors.
  (children -> ss) -> or <$> traverse (flip isDerivedDirectlyFrom t) ss

-- | Does the given effect perform a read on the given symbol?
--
-- This query is materialization-aware.
doesReadOn :: K3 Effect -> K3 Symbol -> QueryM Bool
doesReadOn e s = case e of
  -- Expand effect first.
  (tag -> FEffId _) -> eEM e >>= flip doesReadOn s

  -- A read effect on a symbol which is derived directly from the given symbol is a read on the
  -- given symbol.
  (tag -> FRead q) -> isDerivedDirectlyFrom q s

  -- A scope does a read on the given symbol if:
  --   - It does a read on the given symbol.
  --   - It needs to populate one of its bindings with the given symbol.
  --
  -- If the scope introduces a binding derived /with aliasing/ from the given symbol, no read
  -- registers here; one /may/ register later if the aliased symbol itself is read.
  (tag &&& children -> (FScope ss, es)) ->
    (||) <$> (or <$> traverse (flip doesReadOn s) es)
         <*> (or <$> traverse (\q -> (&&) <$> ((||) <$> isIsolated q <*> isMoved q)
                                          <*> (or <$> traverse (flip isDerivedDirectlyFrom s) (children q))) ss)

  -- Otherwise, the given effect performs a read on the given symbol if any of its constituent
  -- effects do.
  (children -> es) -> or <$> traverse (flip doesReadOn s) es

-- | Does the given effect perform a write on the given symbol?
--
-- This query is materialization-aware.
doesWriteOn :: K3 Effect -> K3 Symbol -> QueryM Bool
doesWriteOn e s = case e of
  -- Expand effect first.
  (tag -> FEffId _) -> eEM e >>= flip doesWriteOn s

  -- A write effect on a symbol which is derived directly from the given symbol is a write on the
  -- given symbol.
  (tag -> FWrite q) -> isDerivedDirectlyFrom q s

  -- A scope does a write on the given symbol if:
  --   - It does a write on the given symbol.
  --   - It does a write-back on any symobl derived through a copy from the given symbol.
  (tag &&& children -> (FScope ss, es)) ->
      (||) <$> (or <$> traverse (flip doesWriteOn s) es)
           <*> (or <$> traverse (\q -> (&&) <$> ((||) <$> isWrittenBack q <*> isMoved q)
                                            <*> (or <$> traverse (flip isDerivedDirectlyFrom s) (children q))) ss)

  -- Otherwise, the given effect performs a read on the given symbol if any of its constituent
  -- effects do.
  (children -> es) -> or <$> traverse (flip doesWriteOn s) es
