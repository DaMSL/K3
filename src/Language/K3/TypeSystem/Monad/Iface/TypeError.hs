{-|
  A module defining a monad in which general typing errors can be raised.
-}
module Language.K3.TypeSystem.Monad.Iface.TypeError
( TypeErrorI(..)
) where

import Language.K3.TypeSystem.Error

-- |A monad in which general typing errors can be raised.  A minimal complete
--  implementation consists of just @typeError@.
class TypeErrorI m where
  internalTypeError :: InternalTypeError -> m a
  typeError :: TypeError -> m a
  internalTypeError = typeError . InternalError
