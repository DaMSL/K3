{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Monad
import Control.Arrow (first, second)
import Data.List
import Debug.Trace

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Analysis.Properties
import Language.K3.Analysis.HMTypes.Inference
import qualified Language.K3.Analysis.CArgs as CArgs

import qualified Language.K3.Analysis.Provenance.Inference as Provenance
import qualified Language.K3.Analysis.SEffects.Inference   as SEffects

import Language.K3.Analysis.Effects.InsertEffects(EffectEnv)
import qualified Language.K3.Analysis.Effects.InsertEffects as Effects
import qualified Language.K3.Analysis.Effects.Purity        as Purity
import qualified Language.K3.Analysis.InsertMembers         as InsertMembers

import Language.K3.Transform.LambdaForms
import Language.K3.Transform.NRVOMove
import Language.K3.Transform.Simplification
import Language.K3.Transform.Writeback
import Language.K3.Transform.Common

import Language.K3.Utils.Pretty

type ProgramTransform = (K3 Declaration, Maybe EffectEnv) -> Either String (K3 Declaration, Maybe EffectEnv)

{- Transform constructors -}

type TrF  = K3 Declaration -> K3 Declaration
type TrE  = K3 Declaration -> Either String (K3 Declaration)
type TrEF = EffectEnv -> K3 Declaration -> K3 Declaration
type TrEE = EffectEnv -> K3 Declaration -> Either String (K3 Declaration)

-- Add an environment for functions that return only a program
transformF :: TrF -> ProgramTransform
transformF f (prog, menv) = Right (f prog, menv)

transformE :: TrE -> ProgramTransform
transformE f (prog, menv) = f prog >>= return . (, menv)

transformEDbg :: String -> TrE -> ProgramTransform
transformEDbg tg f (prog, menv) = mkTg "Before " prog (f prog) >>= \p -> mkTg "After " p (return (p, menv))
  where mkTg pfx p = trace (boxToString $ [pfx ++ tg] %$ prettyLines p)

transformEnvF :: TrEF -> ProgramTransform
transformEnvF _ (_, Nothing)     = Left "missing effect environment"
transformEnvF f (prog, Just env) = Right (f env prog, Just env)

transformEnvE :: TrEE -> ProgramTransform
transformEnvE _ (_, Nothing)     = Left "missing effect environment"
transformEnvE f (prog, Just env) = f env prog >>= return . (, Just env)

fixpointTransform :: ProgramTransform -> ProgramTransform
fixpointTransform f p = do
  np <- f p
  if fst np == fst p then return np
  else fixpointTransform f np

fixpointTransformI :: [ProgramTransform] -> ProgramTransform -> ProgramTransform
fixpointTransformI interF f p = do
  np <- f p
  if fst np == fst p then return np
  else foldM (flip ($)) np interF >>= fixpointTransformI interF f

fixpointF :: TrF -> ProgramTransform
fixpointF f = fixpointTransform $ transformF f

fixpointE :: TrE -> ProgramTransform
fixpointE f = fixpointTransform $ transformE f

fixpointEnvF :: TrEF -> ProgramTransform
fixpointEnvF f = fixpointTransform $ transformEnvF f

fixpointEnvE :: TrEE -> ProgramTransform
fixpointEnvE f = fixpointTransform $ transformEnvE f

-- Fixpoint constructors with intermediate transformations between rounds.
fixpointIF :: [ProgramTransform] -> TrF -> ProgramTransform
fixpointIF interF f = fixpointTransformI interF $ transformF f

fixpointIE :: [ProgramTransform] -> TrE-> ProgramTransform
fixpointIE interF f = fixpointTransformI interF $ transformE f

fixpointIEnvF :: [ProgramTransform] -> TrEF -> ProgramTransform
fixpointIEnvF interF f = fixpointTransformI interF $ transformEnvF f

fixpointIEnvE :: [ProgramTransform] -> TrEE -> ProgramTransform
fixpointIEnvE interF f = fixpointTransformI interF $ transformEnvE f

{- High-level passes -}
inferTypes :: ProgramTransform
inferTypes (prog, e) = liftM (, e) $ inferProgramTypes prog >>= translateProgramTypes

inferCEffects :: ProgramTransform
inferCEffects (prog, _) = return $ second Just $ Effects.runConsolidatedAnalysis prog

inferSEffects :: ProgramTransform
inferSEffects (prog, e) = do
  (p,  ppenv) <- Provenance.inferProgramProvenance prog
  (p', _)     <- SEffects.inferProgramEffects Nothing ppenv p
  return (p', e)

-- | Effect algorithm selection.
inferEffects :: ProgramTransform
inferEffects = inferSEffects

inferTypesAndEffects :: ProgramTransform
inferTypesAndEffects p = inferTypes p >>= inferEffects

inferFreshTypes :: ProgramTransform
inferFreshTypes = inferTypes . first stripTypeAnns

inferFreshEffects :: ProgramTransform
inferFreshEffects = inferEffects . first stripEffectAnns

inferFreshTypesAndEffects :: ProgramTransform
inferFreshTypesAndEffects = inferTypesAndEffects . first stripTypeAndEffectAnns

withTypecheck :: ProgramTransform -> ProgramTransform
withTypecheck f prog = inferTypes prog >>= f

withEffects :: ProgramTransform -> ProgramTransform
withEffects f prog = inferEffects prog >>= f

withTypeAndEffects :: ProgramTransform -> ProgramTransform
withTypeAndEffects f prog = f =<< inferEffects =<< inferTypes prog

withProperties :: ProgramTransform -> ProgramTransform
withProperties f p = transformE inferProgramUsageProperties p >>= f

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg f prog = liftM (first $ repairProgram msg) $ f prog

withPasses :: [ProgramTransform] -> ProgramTransform
withPasses passes prog = foldM (flip ($!)) prog passes

simplify :: ProgramTransform
simplify = fixpointTransform $ simplifyChain
  where simplifyChain p = withPasses simplifyPasses p
        simplifyPasses = intersperse inferFreshTypesAndEffects $
                           map (mkXform False) [ ("FC", foldProgramConstants)
                                               , ("BR", betaReductionOnProgram)
                                               , ("DCE", eliminateDeadProgramCode) ]
        mkXform asDebug (i,f) = withRepair i $ (if asDebug then transformEDbg i else transformE) f


simplifyWCSE :: ProgramTransform
simplifyWCSE p = simplify p >>= transformE commonProgramSubexprElim

-- TODO: remove whole-program fixpoint once we have the ability to
-- locally infer effects on expressions.
streamFusion :: ProgramTransform
streamFusion = withProperties $ \p -> transformE encodeTransformers p >>= fusionFixpoint
  where fusionFixpoint = fixpointIE fusionInterF fuseProgramFoldTransformers
        fusionInterF   = [inferFreshTypesAndEffects, transformE betaReductionOnProgram, inferFreshTypesAndEffects]

runPasses :: [ProgramTransform] -> K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runPasses passes d = withPasses passes (d, Nothing)

optPasses :: [ProgramTransform]
optPasses = map prepareOpt [ (simplify,         "opt-simplify-prefuse")
                           , (streamFusion,     "opt-fuse")
                           , (simplify,         "opt-simplify-final") ]
  where prepareOpt (f,i) = withPasses $ [inferFreshTypesAndEffects, withRepair i f]

cgPasses :: Int -> [ProgramTransform]
-- no moves
cgPasses 3 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF nrvoMoveOpt,
              writebackOptPT,
              transformEnvF (lambdaFormOpt noMovesTransConfig)
             ]

-- no refs
cgPasses 2 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF nrvoMoveOpt,
              writebackOptPT,
              transformEnvF (lambdaFormOpt noRefsTransConfig)
             ]

cgPasses 1 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF nrvoMoveOpt,
              writebackOptPT,
              transformEnvF (lambdaFormOpt defaultTransConfig)
             ]

cgPasses _ = [transformF InsertMembers.runAnalysis,
              transformF CArgs.runAnalysis
             ]

runOptPasses :: K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runOptPasses prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPasses :: K3 Declaration -> Int -> Either String (K3 Declaration)
runCGPasses prog lvl = liftM fst $ runPasses (cgPasses lvl) prog
