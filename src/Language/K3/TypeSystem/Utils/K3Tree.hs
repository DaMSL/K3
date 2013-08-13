{-# LANGUAGE TupleSections, TemplateHaskell #-}

module Language.K3.TypeSystem.Utils.K3Tree
( spanOf

, assert0Children
, assert1Child
, assert2Children
, assert3Children
, assert4Children
, assert5Children
, assert6Children
, assert7Children
, assert8Children
) where

import Control.Applicative
import Data.Maybe
import Language.Haskell.TH

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type as K3T
import Language.K3.Core.Utils
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Utils.TemplateHaskell

-- * Span-related routines

class SpanOfTree a where
  unSpan :: Annotation a -> Maybe Span
  spanError :: K3 a -> InternalTypeError
instance SpanOfTree Expression where
  unSpan tree = case tree of { ESpan s -> Just s; _ -> Nothing }
  spanError = InvalidSpansInExpression
instance SpanOfTree K3T.Type where
  unSpan tree = case tree of { TSpan s -> Just s; _ -> Nothing }
  spanError = InvalidSpansInTypeExpression
instance SpanOfTree Declaration where
  unSpan tree = case tree of { DSpan s -> Just s }
  spanError = InvalidSpansInDeclaration

-- |Retrieves the span from the provided expression.  If no such span exists,
--  an error is produced.
spanOf :: (Monad m, TypeErrorI m, SpanOfTree a) => K3 a -> m Span
spanOf tree =
  let spans = mapMaybe unSpan $ annotations tree in
  if length spans /= 1
    then internalTypeError $ spanError tree
    else return $ head spans

-- * Generated routines

class ErrorForWrongChildren a where
  childCountError :: K3 a -> InternalTypeError
instance ErrorForWrongChildren Expression where
  childCountError = InvalidExpressionChildCount
instance ErrorForWrongChildren K3T.Type where
  childCountError = InvalidTypeExpressionChildCount
instance ErrorForWrongChildren Declaration where
  childCountError = InvalidDeclarationChildCount
$(
  let f = mkAssertChildren
            (\tpn tt -> ForallT [PlainTV tpn] [] <$>
                        [t| ( Monad m, TypeErrorI m
                            , ErrorForWrongChildren $(varT tpn))
                           => K3 $(varT tpn) -> m $(tt) |])
            (\treeExp -> [| internalTypeError $ childCountError $(treeExp) |])
            (\tupleExp -> [| return $(tupleExp) |])
  in
  concat <$> mapM f [0::Int .. 8]
 )
