{-# LANGUAGE DataKinds #-}

{-|
  Utility functions relevant to typechecking.
-}

module Language.K3.TypeSystem.Monad.Utils
( freshTypecheckingQVar
, freshTypecheckingUVar
, envRequire
, envRequireM
) where

import Control.Applicative
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError

-- * Utilities

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingQVar :: (FreshVarI m) => Span -> m (TVar QualifiedTVar)
freshTypecheckingQVar s = freshQVar =<< return (TVarSourceOrigin s)

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingUVar :: (FreshVarI m) => Span -> m (TVar UnqualifiedTVar)
freshTypecheckingUVar s = freshUVar =<< return (TVarSourceOrigin s)

-- |Retrieves an entry from an environment, producing an error if it cannot be
--  found.
envRequire :: (Ord a, Applicative m, Monad m, TypeErrorI m)
           => TypeError -> a -> Map a b -> m b
envRequire e = envRequireM $ return e

-- |Retrieves an entry from an environment, producing an error if it cannot be
--  found.
envRequireM :: (Ord a, Applicative m, Monad m, TypeErrorI m)
            => m TypeError -> a -> Map a b -> m b
envRequireM e k m = fromMaybe <$> (typeError =<< e)
                              <*> return (Map.lookup k m)
