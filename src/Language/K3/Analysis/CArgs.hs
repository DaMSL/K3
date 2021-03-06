{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module Language.K3.Analysis.CArgs (runAnalysis, eCArgs, isECArgs, isErrorFn) where

import Control.Monad.Identity
import Control.Arrow ((&&&))
import Data.Maybe (isJust)
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Utils

-- Top Level. Propagate existing CArgs through the AST.
-- Assuming that all property annotations have already been attached to uses
-- of annotation members, global functions or local bindings. (See Properties.hs)

runAnalysis :: K3 Declaration -> K3 Declaration
runAnalysis prog =
  runIdentity $ mapProgram return return (modifyTree propagateCArgs) Nothing prog

-- Propagate the existing CArgs properties attached to the tree
-- TODO Aliases
propagateCArgs :: K3 Expression -> Identity (K3 Expression)
propagateCArgs e@(tag &&& children -> (EOperate OApp, (l:_)))
  | isJust $ l @~ isECArgs = return $ e @+ makeCArgs ((eCArgs l) - 1)
  | otherwise    = return e
propagateCArgs e = return e

-- Create a CArgs property with the specified int
makeCArgs :: Int -> Annotation Expression
makeCArgs n = EProperty $ Right ("CArgs", Just lit)
  where lit = Node ((LInt n) :@: []) []

-- Look for a CArgs property specifying the number of arguments expected by the backend implementation of a declared function.
-- Functions without a CArgs property default to 1.
eCArgs :: K3 Expression -> Int
eCArgs (annotations -> anns) =
	case filter isECArgs anns of
    ((EProperty (cargELit -> Just literal)):_) -> extractN literal
    _ -> 1
  where
    cargELit (Left  ("CArgs", Just literal)) = Just literal
    cargELit (Right ("CArgs", Just literal)) = Just literal
    cargELit _ = Nothing

    extractN (tag -> LInt n) = n
    extractN _ = error "Invalid Literal for CArgs Property. Specify an Int."

isECArgs :: Annotation Expression -> Bool
isECArgs (EProperty (ePropertyV -> ("CArgs", Just _))) = True
isECArgs _ = False

isErrorFn :: Annotation Expression -> Bool
isErrorFn (EProperty (ePropertyName -> "ErrorFn")) = True
isErrorFn _ = False
