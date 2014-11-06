{-# LANGUAGE TupleSections #-}
-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Monad
import Control.Arrow (first, second)

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Analysis.Properties
import Language.K3.Analysis.HMTypes.Inference
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv)
import qualified Language.K3.Analysis.Effects.InsertEffects as Effects
import qualified Language.K3.Analysis.Effects.Purity        as Purity
import qualified Language.K3.Analysis.InsertMembers         as InsertMembers
import qualified Language.K3.Analysis.CArgs                 as CArgs

import Language.K3.Transform.LambdaForms
import Language.K3.Transform.NRVOMove
import Language.K3.Transform.Simplification
import Language.K3.Transform.Writeback
import Language.K3.Transform.Common

type ProgramTransform = (K3 Declaration, Maybe EffectEnv) -> Either String (K3 Declaration, Maybe EffectEnv)

isAnyETypeAnn :: Annotation Expression -> Bool
isAnyETypeAnn a = isETypeOrBound a || isEQType a

isAnyEEffectAnn :: Annotation Expression -> Bool
isAnyEEffectAnn a = isEEffect a || isESymbol a

isAnyETypeOrEffectAnn :: Annotation Expression -> Bool
isAnyETypeOrEffectAnn a = isAnyETypeAnn a || isAnyEEffectAnn a

{- Annotation cleanup methods -}
stripTypeAnns :: K3 Declaration -> K3 Declaration
stripTypeAnns = stripDeclAnnotations (const False) isAnyETypeAnn (const False)

stripEffectAnns :: K3 Declaration -> K3 Declaration
stripEffectAnns = stripDeclAnnotations (const False) isAnyEEffectAnn (const False)

-- | Effects-related metadata removal, including user-specified effect signatures.
stripAllEffectAnns :: K3 Declaration -> K3 Declaration
stripAllEffectAnns = stripDeclAnnotations isDSymbol isAnyEEffectAnn (const False)

-- | Single-pass composition of type and effect removal.
stripTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripTypeAndEffectAnns = stripDeclAnnotations (const False) isAnyETypeOrEffectAnn (const False)

-- | Single-pass variant removing all effect annotations.
stripAllTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripAllTypeAndEffectAnns = stripDeclAnnotations isDSymbol isAnyETypeOrEffectAnn (const False)

-- | Removes all properties from a program.
stripAllProperties :: K3 Declaration -> K3 Declaration
stripAllProperties = stripDeclAnnotations isDProperty isEProperty (const False)

{- High-level passes -}
inferTypes :: ProgramTransform
inferTypes (prog, e) = liftM (, e) $ inferProgramTypes prog >>= translateProgramTypes

inferEffects :: ProgramTransform
inferEffects (prog, _) = return $ second Just $ Effects.runConsolidatedAnalysis prog

inferTypesAndEffects :: ProgramTransform
inferTypesAndEffects p = inferTypes p >>= inferEffects

inferFreshTypes :: ProgramTransform
inferFreshTypes = inferTypes . first stripTypeAnns

inferFreshEffects :: ProgramTransform
inferFreshEffects = inferEffects . first stripEffectAnns

inferFreshTypesAndEffects :: ProgramTransform
inferFreshTypesAndEffects = inferTypesAndEffects . first stripTypeAndEffectAnns

withTypecheck :: ProgramTransform -> ProgramTransform
withTypecheck transformF prog = inferTypes prog >>= transformF

withEffects :: ProgramTransform -> ProgramTransform
withEffects transformF prog = inferEffects prog >>= transformF

withTypeAndEffects :: ProgramTransform -> ProgramTransform
withTypeAndEffects transformF prog = transformF =<< inferEffects =<< inferTypes prog

{-
wrapTypecheck :: ProgramTransform -> ProgramTransform
wrapTypecheck transformF prog = inferFreshTypes =<< withTypecheck transformF prog

wrapEffects :: ProgramTransform -> ProgramTransform
wrapEffects transformF prog = inferFreshEffects =<< withEffects transformF prog

wrapTypeAndEffects :: ProgramTransform -> ProgramTransform
wrapTypeAndEffects transformF prog =
  inferFreshTypesAndEffects =<< withTypeAndEffects transformF prog
-}

-- Add an environment for functions that return only a program

addEitherEnv :: (K3 Declaration -> K3 Declaration) -> ProgramTransform
addEitherEnv f (prog, env) = Right (f prog, env)

addEnv :: (K3 Declaration -> Either String (K3 Declaration)) -> ProgramTransform
addEnv f (prog, env) = case f prog of
                         Right x -> Right (x, env)
                         Left s  -> Left s

addEnvRet :: (EffectEnv -> K3 Declaration -> K3 Declaration) -> ProgramTransform
addEnvRet f (prog, menv) = maybe (Left "missing effect environment") (\env -> Right (f env prog, Just env)) menv

withProperties :: ProgramTransform -> ProgramTransform
withProperties transformF p = addEnv inferProgramUsageProperties p >>= transformF

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg transformF prog = liftM (first $ repairProgram msg) $ transformF prog

withPasses :: [ProgramTransform] -> ProgramTransform
withPasses passes prog = foldM (flip ($!)) prog passes

simplify :: ProgramTransform
simplify (prog, env) = do
  prog1 <- foldProgramConstants prog
  let prog2 = betaReductionOnProgram prog1
  prog3 <- eliminateDeadProgramCode prog2
  return (prog3, env)

simplifyWCSE :: ProgramTransform
simplifyWCSE p = simplify p >>= addEnv commonProgramSubexprElim

streamFusion :: ProgramTransform
streamFusion = withProperties $ \p -> addEnv inferFusableProgramApplies p >>= addEnv fuseProgramTransformers

runPasses :: [ProgramTransform] -> K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runPasses passes d = withPasses passes (d, Nothing)

effectPasses :: [ProgramTransform]
effectPasses = [addEnvRet Purity.runPurity]

optPasses :: [ProgramTransform]
optPasses = map prepareOpt [ (simplify,         "opt-simplify-prefuse")
                           , (streamFusion,     "opt-fuse")
                           , (simplify,         "opt-simplify-final") ]
  where prepareOpt (f,i) =
          withPasses $ [inferFreshTypesAndEffects] ++ effectPasses ++ [withRepair i f]

cgPasses :: Int -> [ProgramTransform]
-- no moves
cgPasses 3 = [inferFreshEffects,
              addEnvRet Purity.runPurity,
              addEitherEnv CArgs.runAnalysis,
              addEnvRet writebackOpt,
              addEnvRet (lambdaFormOptD noMovesTransConfig),
              addEnvRet nrvoMoveOpt
             ]
-- no refs
cgPasses 2 = [inferFreshEffects,
              addEnvRet Purity.runPurity,
              addEitherEnv CArgs.runAnalysis,
              addEnvRet writebackOpt,
              addEnvRet (lambdaFormOptD noRefsTransConfig),
              addEnvRet nrvoMoveOpt
             ]
cgPasses 1 = [inferFreshEffects,
              addEnvRet Purity.runPurity,
              addEitherEnv CArgs.runAnalysis,
              addEnvRet writebackOpt,
              addEnvRet (lambdaFormOptD defaultTransConfig),
              addEnvRet nrvoMoveOpt
             ]
cgPasses _ = [addEitherEnv InsertMembers.runAnalysis,
              addEitherEnv CArgs.runAnalysis
             ]

runOptPasses :: K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runOptPasses prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPasses :: K3 Declaration -> Int -> Either String (K3 Declaration)
runCGPasses prog lvl = liftM fst $ runPasses (cgPasses lvl) prog
