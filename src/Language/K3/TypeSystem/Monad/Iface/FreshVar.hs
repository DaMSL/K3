{-# LANGUAGE DataKinds #-}
{-|
  This module contains a typeclass which serves as an interface declarations for
  monads which can generate fresh variables.
-}

module Language.K3.TypeSystem.Monad.Iface.FreshVar
(  FreshVarI(..)
) where

import Control.Monad.Trans
import Control.Monad.Trans.Either
import Control.Monad.Trans.Maybe

import Language.K3.TypeSystem.Data

-- |A class defining the behavior of monads which can generate fresh variables.
class (Monad m, Functor m) => FreshVarI m where
  freshQVar :: TVarOrigin QualifiedTVar -> m (TVar QualifiedTVar)
  freshUVar :: TVarOrigin UnqualifiedTVar -> m (TVar UnqualifiedTVar)

instance (FreshVarI m, Monad m) => FreshVarI (MaybeT m) where
  freshQVar = lift . freshQVar
  freshUVar = lift . freshUVar

instance (FreshVarI m, Monad m) => FreshVarI (EitherT e m) where
  freshQVar = lift . freshQVar
  freshUVar = lift . freshUVar
