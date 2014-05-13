-- | Module to handle 'typecheck' mode in the driver.
module Language.K3.Driver.Typecheck where

import Data.Map (Map)
import Data.Sequence (Seq)
import qualified Data.Foldable as Foldable
import qualified Data.Map      as Map
import qualified Data.Sequence as Seq

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type

import Language.K3.TypeSystem hiding ( typecheck )
import Language.K3.TypeSystem.Error

import Language.K3.Utils.Pretty


withTypecheckedProgram :: (K3 Declaration -> Map UID (K3 Type, K3 Type) -> IO ())
                       -> K3 Declaration -> IO ()
withTypecheckedProgram action prog =
    let (errs, result, typedP) = typecheckProgram prog in
    if Seq.null errs then 
      maybe inferenceError (action typedP) $ tcExprBounds result
    else putStrLn $ prettyTCErrors typedP errs

  where inferenceError = putStrLn $ "Could not infer types for program."

typecheck :: K3 Declaration -> IO ()
typecheck p = withTypecheckedProgram (\tp tm -> putStrLn $ prettyTCResult tp tm) p

prettyTCResult :: K3 Declaration -> Map UID (K3 Type, K3 Type) -> String
prettyTCResult p bounds = unlines $
  ["Successfully typed AST:"]
    %$ prettyLines p
    %$ indent 2 (prettyExprBounds bounds)

prettyTCErrors :: K3 Declaration -> Seq TypeError -> String
prettyTCErrors p errs = unlines $
  ["Errors while typechecking AST:"]
    %$ indent 2 (prettyLines p)
    %$ indent 2 (["Errors were:"] %$ indent 2
          (vconcats $ map prettyLines $ Foldable.toList errs))

prettyExprBounds :: Map UID (K3 Type, K3 Type) -> [String]
prettyExprBounds bounds =
     ["Inferred expression bounds:"]
     %$ indent 2 (vconcats $ flip map (Map.toList bounds) $
        \(u,(lb,ub)) ->
          let n = take 10 $ show u ++ repeat ' ' in
          [n ++ " ≥ "] %+ prettyLines lb %$ [n ++ " ≤ "] %+ prettyLines ub)
