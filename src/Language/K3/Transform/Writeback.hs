{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Language.K3.Transform.Writeback where

import qualified GHC.Exts (IsList(..))

import Prelude hiding (any)
import Control.Arrow ((&&&))

import Data.Maybe
import Data.Foldable
import Data.Functor
import Data.Tree

import qualified Data.Set as S

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv, eE, eS, expandEffDeep, expandSymDeep)
import Language.K3.Analysis.Effects.Queries

import Language.K3.Transform.Hints

-- Orphan for IsList (S.Set a)
instance (Ord a) => GHC.Exts.IsList (S.Set a) where
    type Item (S.Set a) = a
    fromList = S.fromList
    toList = S.toList

pattern TAC t as cs = Node (t :@: as) cs

findScope :: EffectEnv -> K3 Effect -> K3 Effect
findScope env f@(tag . eE env -> FScope _) = f
findScope env (children . eE env -> fs) = findScope env (last fs)

symIDs :: EffectEnv -> S.Set (K3 Symbol) -> S.Set Identifier
symIDs env = S.map (symIdent . tag . eS env)

-- | Determine whether or not a given symbol has a read/write/superstructure conflict in the given
-- effect tree. To conflict, the symbol's superstructure must be read after the symbol was written
-- to, or the superstructure was written anywhere.
conflicts :: EffectEnv -> K3 Effect -> K3 Symbol -> Bool
conflicts env (expandEffDeep env -> f) (expandSymDeep env -> k)
    = conflictsWR f || any (\q -> evalQueryM (doesWriteOn f q) env) kk
  where
    kk = S.delete k $ anySuperStructure k

    conflictsWR (eE env -> tnc -> (FSeq, [first, second]))
        = conflictsWR first || conflictsWR second ||
          (evalQueryM (doesWriteOn first k) env && any (\s -> evalQueryM (doesReadOn second s) env) kk)
    conflictsWR (children . eE env -> cs) = any conflictsWR cs

writebackOpt :: EffectEnv -> K3 Declaration -> K3 Declaration
writebackOpt env (TAC (DGlobal i t me) as cs) = TAC (DGlobal  i t (writebackOptE env <$> me)) as cs
writebackOpt env (TAC (DTrigger i t e) as cs) = TAC (DTrigger i t (writebackOptE env e))      as cs
writebackOpt env (TAC (DRole n) as cs)        = TAC (DRole n) as $ map (writebackOpt env) cs
writebackOpt _ t = t

writebackOptE :: EffectEnv -> K3 Expression -> K3 Expression
writebackOptE env g@(TAC t@(EBindAs _) as cs) = TAC t (constructBindHint env g : as) $
                                                   map (writebackOptE env) cs
writebackOptE env g@(TAC t@(ECaseOf _) as cs) = TAC t (constructBindHint env g : as) $
                                                   map (writebackOptE env) cs
writebackOptE env (TAC t as cs)               = TAC t as $ map (writebackOptE env) cs

constructBindHint :: EffectEnv -> K3 Expression -> Annotation Expression
constructBindHint env g = EOpt $ BindHint (symIDs env refBound, [], symIDs env writeBound)
  where
    (eE env -> tnc -> (FScope bs, es)) = findScope env $ let (EEffect k) = fromJust $ g @~ isEEffect in k
    (writeBound, refBound) = if null es then ([], S.fromList bs)
                             else S.partition (conflicts env $ head es) $ S.fromList bs
