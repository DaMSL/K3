module Language.K3.Optimization where

import Data.List

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Analysis.Effects.InsertEffects
import Language.K3.Transform.Writeback

passes :: [K3 Declaration -> K3 Declaration]
passes = [writebackOpt]

runOptimization :: K3 Declaration -> K3 Declaration
runOptimization d = foldl' (\a f -> runAnalysis (f a)) d passes
