{-|
  This module contains a typeclass which serves as an interface declarations for
  monads which can generate fresh variables.
-}

module Language.K3.TypeSystem.Monad.Iface.FreshVar
(  FreshVarI(..)
) where

import Language.K3.TypeSystem.Data

class FreshVarI m where
  freshQVar :: m QVar
  freshUVar :: m UVar
