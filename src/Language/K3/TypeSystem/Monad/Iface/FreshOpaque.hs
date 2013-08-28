{-# LANGUAGE DataKinds #-}
{-|
  This module contains a typeclass which serves as an interface declarations for
  monads which can generate fresh opaque variables.
-}

module Language.K3.TypeSystem.Monad.Iface.FreshOpaque
(  FreshOpaqueI(..)
) where

import Control.Monad.Trans
import Control.Monad.Trans.Either
import Control.Monad.Trans.Maybe

import Language.K3.TypeSystem.Data

-- |A class defining the behavior of monads which can generate fresh variables.
class (Monad m, Functor m) => FreshOpaqueI m where
  freshOVar :: OpaqueOrigin -> m OpaqueVar

instance (FreshOpaqueI m, Monad m) => FreshOpaqueI (MaybeT m) where
  freshOVar = lift . freshOVar

instance (FreshOpaqueI m, Monad m) => FreshOpaqueI (EitherT e m) where
  freshOVar = lift . freshOVar
