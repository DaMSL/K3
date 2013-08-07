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

class (Monad m, Functor m) => FreshVarI m where
  freshVar :: TVarOrigin a -> m (TVar a)

instance (FreshVarI m, Monad m) => FreshVarI (MaybeT m) where
  freshVar = lift . freshVar

instance (FreshVarI m, Monad m) => FreshVarI (EitherT e m) where
  freshVar = lift . freshVar
  