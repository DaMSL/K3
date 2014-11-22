{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Monad
import Control.Arrow (first, second)
import Data.Functor.Identity

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Analysis.Properties
import Language.K3.Analysis.HMTypes.Inference
import Language.K3.Analysis.Effects.Core
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

resetEffectAnns :: K3 Declaration -> K3 Declaration
resetEffectAnns p = runIdentity $ mapMaybeAnnotation resetF idF idF p
  where idF = return . Just
        resetF (DSymbol s@(tag -> SymId _)) = return $
          case s @~ isSDeclared of
            Just (SDeclared s') -> Just $ DSymbol s'
            _ -> Nothing
        resetF a = return $ Just a

stripEffectAnns :: K3 Declaration -> K3 Declaration
stripEffectAnns p = resetEffectAnns $
  stripDeclAnnotations (const False) isAnyEEffectAnn (const False) p

-- | Effects-related metadata removal, including user-specified effect signatures.
stripAllEffectAnns :: K3 Declaration -> K3 Declaration
stripAllEffectAnns = stripDeclAnnotations isDSymbol isAnyEEffectAnn (const False)

-- | Single-pass composition of type and effect removal.
stripTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripTypeAndEffectAnns p = resetEffectAnns $
  stripDeclAnnotations (const False) isAnyETypeOrEffectAnn (const False) p

-- | Single-pass variant removing all effect annotations.
stripAllTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripAllTypeAndEffectAnns = stripDeclAnnotations isDSymbol isAnyETypeOrEffectAnn (const False)

-- | Removes all properties from a program.
stripAllProperties :: K3 Declaration -> K3 Declaration
stripAllProperties = stripDeclAnnotations isDProperty isEProperty (const False)

{- Transform constructors -}
-- Add an environment for functions that return only a program
transformF :: (K3 Declaration -> K3 Declaration) -> ProgramTransform
transformF f (prog, menv) = Right (f prog, menv)

transformE :: (K3 Declaration -> Either String (K3 Declaration)) -> ProgramTransform
transformE f (prog, menv) = f prog >>= return . (, menv)

transformEnvF :: (EffectEnv -> K3 Declaration -> K3 Declaration) -> ProgramTransform
transformEnvF f (prog, menv) = maybe (Left "missing effect environment") (\env -> Right (f env prog, Just env)) menv

transformEnvE :: (EffectEnv -> K3 Declaration -> Either String (K3 Declaration)) -> ProgramTransform
transformEnvE f (prog, menv) = maybe (Left "missing effect environment") (\env -> f env prog >>= return . (, menv)) menv

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
simplify p = transformE foldProgramConstants p
              >>= transformEnvF betaReductionOnProgram
              -- >>= transformEnvE eliminateDeadProgramCode

simplifyWCSE :: ProgramTransform
simplifyWCSE p = simplify p >>= transformEnvE commonProgramSubexprElim

streamFusion :: ProgramTransform
streamFusion = withProperties $ \p ->
  transformEnvE encodeTransformers p >>= transformEnvE fuseProgramFoldTransformers

runPasses :: [ProgramTransform] -> K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runPasses passes d = withPasses passes (d, Nothing)

effectPasses :: [ProgramTransform]
effectPasses = [transformEnvF Purity.runPurity]

optPasses :: [ProgramTransform]
optPasses = map prepareOpt [ (simplify,         "opt-simplify-prefuse") ]
                           --, (streamFusion,     "opt-fuse") ]
                           --, (simplify,         "opt-simplify-final") ]
  where prepareOpt (f,i) =
          withPasses $ [inferFreshTypesAndEffects] ++ effectPasses ++ [withRepair i f]

cgPasses :: Int -> [ProgramTransform]
-- no moves
cgPasses 3 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF writebackOpt,
              transformEnvF (lambdaFormOpt noMovesTransConfig),
              transformEnvF nrvoMoveOpt
             ]

-- no refs
cgPasses 2 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF writebackOpt,
              transformEnvF (lambdaFormOpt noRefsTransConfig),
              transformEnvF nrvoMoveOpt
             ]

cgPasses 1 = [inferFreshEffects,
              transformEnvF Purity.runPurity,
              transformF    CArgs.runAnalysis,
              transformEnvF writebackOpt,
              transformEnvF (lambdaFormOpt defaultTransConfig),
              transformEnvF nrvoMoveOpt
             ]

cgPasses _ = [transformF InsertMembers.runAnalysis,
              transformF CArgs.runAnalysis
             ]

runOptPasses :: K3 Declaration -> Either String (K3 Declaration, Maybe EffectEnv)
runOptPasses prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPasses :: K3 Declaration -> Int -> Either String (K3 Declaration)
runCGPasses prog lvl = liftM fst $ runPasses (cgPasses lvl) prog
