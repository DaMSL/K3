{-# LANGUAGE TupleSections #-}

{-|
  This module contains general type manipulation utilities.
-}
module Language.K3.TypeSystem.Utils
( typeOfOp
, recordOf
) where

import Control.Monad
import Data.Map as Map
import Data.Set as Set

import Language.K3.Core.Expression
import Language.K3.TypeSystem.Data

-- |Translates from expression-level operators to types.
typeOfOp :: Operator -> AnyOperator
typeOfOp op = case op of
  OAdd -> SomeBinaryOperator BinOpAdd
  OSub -> SomeBinaryOperator BinOpSubtract
  OMul -> SomeBinaryOperator BinOpMultiply
  ODiv -> SomeBinaryOperator BinOpDivide
  ONeg -> error "No unary operators in spec yet!" -- TODO
  OEqu -> SomeBinaryOperator BinOpEquals
  ONeq -> error "No not-equals in spec yet!" -- TODO
  OLth -> SomeBinaryOperator BinOpLess
  OLeq -> SomeBinaryOperator BinOpLessEq
  OGth -> SomeBinaryOperator BinOpGreater
  OGeq -> SomeBinaryOperator BinOpGreaterEq
  OAnd -> error "No and in spec yet!" -- TODO
  OOr -> error "No or in spec yet!" -- TODO
  ONot -> error "No unary operators in spec yet!" -- TODO
  OSeq -> SomeBinaryOperator BinOpSequence
  OApp -> SomeBinaryOperator BinOpApply
  OSnd -> SomeBinaryOperator BinOpSend

-- |Concatenates a set of concrete record types.  @Nothing@ is produced if
--  any of the types are not records (or top) or if the record types overlap.
recordOf :: [ShallowType] -> Maybe ShallowType
recordOf = foldM concatRecs (SRecord Map.empty) . Prelude.filter (/=STop)
  where
    concatRecs :: ShallowType -> ShallowType -> Maybe ShallowType
    concatRecs t1 t2 =
      case (t1,t2) of
        (SRecord m1, SRecord m2) ->
          if Set.null $ Set.fromList (Map.keys m1) `Set.intersection`
                        Set.fromList (Map.keys m2)
            then Just $ SRecord $ m1 `Map.union` m2
            else Nothing
        _ -> Nothing
