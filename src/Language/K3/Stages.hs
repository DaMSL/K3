-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Monad

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Analysis.HMTypes.Inference
import Language.K3.Analysis.Effects.InsertEffects

import Language.K3.Transform.Simplification
import Language.K3.Transform.Writeback
import Language.K3.Transform.LambdaForms

type ProgramTransform = K3 Declaration -> Either String (K3 Declaration)

isAnyETypeAnn :: Annotation Expression -> Bool
isAnyETypeAnn a = isETypeOrBound a || isEQType a

isAnyEEffectAnn :: Annotation Expression -> Bool
isAnyEEffectAnn a = isEEffect a || isESymbol a

stripTypeAnns :: K3 Declaration -> K3 Declaration
stripTypeAnns = stripDeclAnnotations (const False) isAnyETypeAnn (const False)

stripEffectAnns :: K3 Declaration -> K3 Declaration
stripEffectAnns = stripDeclAnnotations (const False) isAnyEEffectAnn (const False)

-- | Single-pass composition of type and effect removal.
stripTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripTypeAndEffectAnns = stripDeclAnnotations (const False) (\a -> isAnyETypeAnn a || isAnyEEffectAnn a) (const False)

withTypecheck :: ProgramTransform -> ProgramTransform
withTypecheck transformF prog = transformF =<< inferProgramTypes prog

wrapTypecheck :: ProgramTransform -> ProgramTransform
wrapTypecheck transformF prog = inferProgramTypes . stripTypeAnns =<< withTypecheck transformF prog

withEffects :: ProgramTransform -> ProgramTransform
withEffects transformF prog = transformF =<< (Right $ runAnalysis prog)

wrapEffects :: ProgramTransform -> ProgramTransform
wrapEffects transformF prog = return . runAnalysis . stripEffectAnns =<< withEffects transformF prog

withTypeAndEffects :: ProgramTransform -> ProgramTransform
withTypeAndEffects transformF prog = transformF =<< return . runAnalysis =<< inferProgramTypes prog

wrapTypeAndEffects :: ProgramTransform -> ProgramTransform
wrapTypeAndEffects transformF prog = do
  prog1 <- inferProgramTypes prog
  let prog2 = runAnalysis prog1
  prog3 <- return . stripTypeAndEffectAnns =<< transformF prog2
  return . runAnalysis =<< inferProgramTypes prog3

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg transformF prog = return . repairProgram msg =<< transformF prog

simplify :: ProgramTransform
simplify prog = do
  prog1 <- foldProgramConstants prog
  let prog2 = betaReductionOnProgram prog1
  eliminateDeadProgramCode prog2

simplifyWCSE :: ProgramTransform
simplifyWCSE prog = commonProgramSubexprElim =<< simplify prog

streamFusion :: ProgramTransform
streamFusion prog = fuseProgramTransformers =<< inferFusableProgramApplies prog

runPasses :: [ProgramTransform] -> K3 Declaration -> Either String (K3 Declaration)
runPasses passes prog = foldM (flip ($!)) prog passes

optPasses :: [ProgramTransform]
optPasses = map (\(f,i) -> wrapTypeAndEffects $ withRepair i f)
              [ (simplifyWCSE, "opt-simplify-cse")
              , (streamFusion, "opt-fuse") ]

cgPasses :: [ProgramTransform]
cgPasses = [return . writebackOpt, return . lambdaFormOptD]

runOptPasses :: K3 Declaration -> Either String (K3 Declaration)
runOptPasses prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPasses :: K3 Declaration -> Either String (K3 Declaration)
runCGPasses prog = runPasses (map (\f -> (\p -> f p >>= return . runAnalysis)) cgPasses) prog
