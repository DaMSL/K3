module Language.K3.Optimization where

import Data.List

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Optimization.Bind

import Language.K3.Analysis.Effects.InsertEffects

passes :: [K3 Declaration -> K3 Declaration]
passes = [bindOpt]

runOptimization :: K3 Declaration -> K3 Declaration
runOptimization d = foldl' (\a f -> runAnalysis (f a)) d passes
