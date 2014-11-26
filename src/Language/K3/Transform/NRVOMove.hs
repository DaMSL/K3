{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.NRVOMove where

import Control.Arrow

import Data.Functor
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects
import Language.K3.Analysis.Effects.Queries

import Language.K3.Transform.Hints

pattern TAC t as cs = Node (t :@: as) cs

-- Determine if we can perform move compensation for Named Return-Value Optimization (NRVO).
nrvoMoveOpt :: EffectEnv -> K3 Declaration -> K3 Declaration
nrvoMoveOpt = nrvoMoveOptD

nrvoMoveOptD :: EffectEnv -> K3 Declaration -> K3 Declaration
nrvoMoveOptD env (TAC (DGlobal i t me) as cs) = TAC (DGlobal i t (nrvoMoveOptE env <$> me)) as cs
nrvoMoveOptD env (TAC (DTrigger i t e) as cs) = TAC (DTrigger i t (nrvoMoveOptE env e)) as cs
nrvoMoveOptD env (TAC (DRole n) as cs) = TAC (DRole n) as (map (nrvoMoveOptD env) cs)
nrvoMoveOptD _ t = t

nrvoMoveOptE :: EffectEnv -> K3 Expression -> K3 Expression
nrvoMoveOptE env e@(TAC (EVariable v) as cs) = TAC (EVariable v) (as' ++ as) cs
  where
    ESymbol s = fromJust $ e@~ isESymbol
    as' = [EOpt GlobalHint | evalQueryM (isGlobal s) env]

nrvoMoveOptE env e@(TAC (ELambda i) as cs) = TAC (ELambda i) (a:as) (map (nrvoMoveOptE env) cs)
  where
    a = EOpt $ ReturnMoveHint nrvoMovePermitted
    nrvoMovePermitted = not (null rs) && (any (\q -> evalQueryM (isDerivedDirectlyFrom (head rs) q) env) bindings)
    ESymbol ((tag &&& children) . eS env
                 -> (Symbol { symProv = PLambda (eE env -> Node (FScope bindings :@: _) _) }, rs))
        = fromJust $ e @~ isESymbol

nrvoMoveOptE env (TAC t as cs) = TAC t as (map (nrvoMoveOptE env) cs)
