{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

import Language.K3.Core.Annotation (K3, tag, (@~))
import Language.K3.Core.Declaration (Declaration(..))
import Language.K3.Core.Expression

import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Analysis.Effects.InsertEffects
import Language.K3.Analysis.Effects.Common
import Language.K3.Optimization
import Language.K3.Optimization.Bind

testTriggerBody :: K3 Expression
testTriggerBody = E.lambda "x" $ E.letIn "y" (E.variable "x") (E.tuple [])

testTrigger :: K3 Declaration
testTrigger = D.trigger "testTrigger" (T.option T.int) testTriggerBody

main :: IO ()
main = do
  let d = runAnalysis testTrigger
  let q =  runOptimization d
  print q
