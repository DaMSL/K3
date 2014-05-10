{-# LANGUAGE TupleSections, TemplateHaskell #-}

module Language.K3.TypeSystem.Utils.K3Tree
( uidOf
, uidOfAnnMem

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

-- * UID-related routines

class UidOfTree a where
  unUid :: Annotation a -> Maybe UID
  uidError :: K3 a -> InternalTypeError
instance UidOfTree Expression where
  unUid tree = case tree of { EUID s -> Just s; _ -> Nothing }
  uidError = InvalidUIDsInExpression
instance UidOfTree K3T.Type where
  unUid tree = case tree of { TUID s -> Just s; _ -> Nothing }
  uidError = InvalidUIDsInTypeExpression
instance UidOfTree Declaration where
  unUid tree = case tree of { DUID s -> Just s; _ -> Nothing }
  uidError = InvalidUIDsInDeclaration

-- |Retrieves the span from the provided expression.  If no such span exists,
--  an error is produced.
uidOf :: (Monad m, TypeErrorI m, UidOfTree a) => K3 a -> m UID
uidOf tree =
  let uids = mapMaybe unUid $ annotations tree in
  if length uids /= 1
    then internalTypeError $ uidError tree
    else return $ head uids

uidOfAnnMem :: (Monad m, TypeErrorI m) => AnnMemDecl -> m UID
uidOfAnnMem mem = case mem of
    Lifted _ _ _ _ anns    -> extractUID anns
    Attribute _ _ _ _ anns -> extractUID anns
    MAnnotation _ _ anns   -> extractUID anns
  where 
    extractUID anns =  
      let uids = mapMaybe (\a -> case a of { DUID u -> Just u; _ -> Nothing }) anns in
      if length uids == 1 then return $ head uids
                          else internalTypeError $ InvalidUIDsInAnnMemDecl mem


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
