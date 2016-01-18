{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Materialization.Common where

import Control.Arrow
import Data.Hashable

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

rollLambdaChain :: K3 Expression -> ([(Identifier, K3 Expression)], K3 Expression)
rollLambdaChain e@(tag &&& children -> (ELambda i, [f])) = let (ies, b) = rollLambdaChain f in ((i, e):ies, b)
rollLambdaChain e = ([], e)

rollAppChain :: K3 Expression -> (K3 Expression, [K3 Expression])
rollAppChain e@(tag &&& children -> (EOperate OApp, [f, x])) = let (f', xs) = rollAppChain f in (f', xs ++ [e])
rollAppChain e = (e, [])

anon :: Int
anon = hashJunctureName "!"

anonS :: Identifier
anonS = "!"

hashJunctureName :: Identifier -> Int
hashJunctureName = hash

isNonScalarType :: K3 Type -> Bool
isNonScalarType t = case t of
  (tag -> TString) -> True
  (tag &&& children -> (TTuple, cs)) -> any isNonScalarType cs
  (tag &&& children -> (TRecord _, cs)) -> any isNonScalarType cs
  (tag -> TCollection) -> True
  _ -> False
