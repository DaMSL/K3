{-# LANGUAGE ViewPatterns #-}

{-|
  This module defines the type constraint closure operation.
-}
module Language.K3.TypeSystem.Closure
(
) where

import Data.Monoid

import Language.K3.Core.Annotation
import Language.K3.Core.Constructor.Type
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Type.Data

-- |An operation for determining how binary operations should be typed.  This
--  function models the @BinOpType@ function from the specification.  When
--  @BinOpType@ is undefined, this function returns @Nothing@.
binOpType :: Operator -> K3 Type -> K3 Type -> Maybe (K3 Type, ConstraintSet)
binOpType op t1 t2 =
  case (op,t1,t2) of
    (_, tag -> TInt, tag -> TInt) -> Just (int, mempty)
    -- TODO