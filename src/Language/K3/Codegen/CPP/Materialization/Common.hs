{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Materialization.Common where

import Control.Arrow

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression

rollLambdaChain :: K3 Expression -> ([(Identifier, K3 Expression)], K3 Expression)
rollLambdaChain e@(tag &&& children -> (ELambda i, [f])) = let (ies, b) = rollLambdaChain f in ((i, e):ies, b)
rollLambdaChain e = ([], e)

rollAppChain :: K3 Expression -> (K3 Expression, [K3 Expression])
rollAppChain e@(tag &&& children -> (EOperate OApp, [f, x])) = let (f', xs) = rollAppChain f in (f', xs ++ [e])
rollAppChain e = (e, [])
