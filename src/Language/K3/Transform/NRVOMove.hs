{-# LANGUAGE PatternSynonyms #-}

module Language.K3.Transform.NRVOMove where

import Data.Functor
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects

pattern TAC t as cs = Node (t :@: as) cs

nrvoMoveOpt :: EffectEnv -> K3 Declaration -> K3 Declaration
nrvoMoveOpt = nrvoMoveOptD

nrvoMoveOptD :: EffectEnv -> K3 Declaration -> K3 Declaration
nrvoMoveOptD env (TAC (DGlobal i t me) as cs) = TAC (DGlobal i t (nrvoMoveOptE env <$> me)) as cs
nrvoMoveOptD env (TAC (DTrigger i t e) as cs) = TAC (DTrigger i t (nrvoMoveOptE env e)) as cs
nrvoMoveOptD env (TAC (DRole n) as cs) = TAC (DRole n) as (map (nrvoMoveOptD env) cs)
nrvoMoveOptD env t = t

nrvoMoveOptE :: EffectEnv -> K3 Expression -> K3 Expression
nrvoMoveOptE env (TAC (ELambda i) as cs) = TAC (ELambda i) as cs
nrvoMoveOptE env (TAC t as cs) = TAC t as (map (nrvoMoveOptE env) cs)
