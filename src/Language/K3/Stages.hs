-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Monad

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Analysis.Properties
import Language.K3.Analysis.HMTypes.Inference
import qualified Language.K3.Analysis.Effects.InsertEffects as Effects

import Language.K3.Transform.Simplification
import Language.K3.Transform.Writeback
import Language.K3.Transform.LambdaForms

type ProgramTransform = K3 Declaration -> Either String (K3 Declaration)

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
inferTypes prog = translateProgramTypes =<< inferProgramTypes prog

inferEffects :: ProgramTransform
inferEffects prog = return $ Effects.runAnalysis prog

withTypecheck :: ProgramTransform -> ProgramTransform
withTypecheck transformF prog = transformF =<< inferTypes prog

wrapTypecheck :: ProgramTransform -> ProgramTransform
wrapTypecheck transformF prog = inferTypes . stripTypeAnns =<< withTypecheck transformF prog

withEffects :: ProgramTransform -> ProgramTransform
withEffects transformF prog = transformF =<< inferEffects prog

wrapEffects :: ProgramTransform -> ProgramTransform
wrapEffects transformF prog = inferEffects . stripEffectAnns =<< withEffects transformF prog

withTypeAndEffects :: ProgramTransform -> ProgramTransform
withTypeAndEffects transformF prog = transformF =<< inferEffects =<< inferTypes prog

wrapTypeAndEffects :: ProgramTransform -> ProgramTransform
wrapTypeAndEffects transformF prog =
  inferEffectsAndTypes . stripTypeAndEffectAnns =<< withTypeAndEffects transformF prog
  where inferEffectsAndTypes p = inferEffects =<< inferTypes p

withProperties :: ProgramTransform -> ProgramTransform
withProperties transformF prog = transformF =<< inferProgramUsageProperties prog

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg transformF prog = return . repairProgram msg =<< transformF prog

simplify :: ProgramTransform
simplify prog = do
  prog1 <- foldProgramConstants prog
  prog2 <- return $ betaReductionOnProgram prog1
  eliminateDeadProgramCode prog2

simplifyWCSE :: ProgramTransform
simplifyWCSE prog = commonProgramSubexprElim =<< simplify prog

streamFusion :: ProgramTransform
streamFusion = withProperties $ \p -> fuseProgramTransformers =<< inferFusableProgramApplies p

runPasses :: [ProgramTransform] -> K3 Declaration -> Either String (K3 Declaration)
runPasses passes prog = foldM (flip ($!)) prog passes

optPasses :: [ProgramTransform]
optPasses = map (\(f,i) -> wrapTypecheck $ withRepair i f)
              [ (simplify,     "opt-simplify-prefuse")
              , (streamFusion, "opt-fuse")
              , (simplify,     "opt-simplify-final") ]

cgPasses :: [ProgramTransform]
cgPasses = [return . writebackOpt, return . lambdaFormOptD]

runOptPasses :: K3 Declaration -> Either String (K3 Declaration)
runOptPasses prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPasses :: K3 Declaration -> Either String (K3 Declaration)
runCGPasses prog = runPasses (map (\f -> (\p -> f p >>= inferEffects)) cgPasses) prog
