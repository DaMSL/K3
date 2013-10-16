{-# LANGUAGE TupleSections #-}

module Language.K3.TypeSystem.Closure.BinOp
( binOpType
) where

import Control.Applicative
import Data.Monoid

import Language.K3.TypeSystem.Data

-- |An operation for determining how binary operations should be typed.  This
--  function models the @BinOpType@ function from the specification.  When
--  @BinOpType@ is undefined, this function returns @Nothing@.
binOpType :: BinaryOperator -> ShallowType -> ShallowType
          -> Maybe (TypeOrVar, ConstraintSet) 
binOpType op t1 t2 =
  case (op,t1,t2) of
    (BinOpAdd, SString, SString) -> Just (CLeft SString, mempty)
    _ | op `elem` arithOp ->
          (,mempty) . CLeft <$> promotedType
    _ | op `elem` compOp ->
          (,mempty) . CLeft <$> comparisonType
    (BinOpSequence, _, _) -> Just (CLeft t2, mempty)
    (BinOpApply, SFunction a1 a2, _) ->
      Just (CRight a2, csSing $ constraint t2 a1)
    -- TODO: update as per new spec re: triggers
    (BinOpSend, STrigger a, _) ->
      Just (CLeft $ STuple [], csSing $ constraint t2 a)
    _ -> Nothing
  where
    arithOp = [BinOpAdd,BinOpSubtract,BinOpMultiply,BinOpDivide]
    compOp = [BinOpEquals,BinOpLess,BinOpGreater,BinOpLessEq,BinOpGreaterEq]
    promotedType = case (t1,t2) of
      (SInt, SInt) -> Just SInt
      (SReal, SInt) -> Just SReal
      (SInt, SReal) -> Just SReal
      (SReal, SReal) -> Just SReal
      _ -> Nothing
    comparisonType = case (t1,t2) of
      (SInt, SInt) -> Just SBool
      (SReal, SInt) -> Just SBool
      (SInt, SReal) -> Just SBool
      (SReal, SReal) -> Just SBool
      (SString, SString) -> Just SBool
      _ -> Nothing
