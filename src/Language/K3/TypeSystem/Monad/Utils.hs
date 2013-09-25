{-# LANGUAGE DataKinds #-}

{-|
  Utility functions relevant to typechecking.
-}

module Language.K3.TypeSystem.Monad.Utils
( freshTypecheckingQVar
, freshTypecheckingUVar
, envRequire
, envRequireM
, aEnvRequireAnn
, pEnvRequire
) where

import Control.Applicative
import Data.Map (Map)
import qualified Data.Map as Map

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError

-- * Utilities

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingQVar :: (FreshVarI m) => UID -> m (TVar QualifiedTVar)
freshTypecheckingQVar u = freshQVar =<< return (TVarSourceOrigin u)

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingUVar :: (FreshVarI m) => UID -> m (TVar UnqualifiedTVar)
freshTypecheckingUVar u = freshUVar =<< return (TVarSourceOrigin u)

-- |Retrieves an entry from an environment, producing an error if it cannot be
--  found.
envRequire :: (Ord a, Applicative m, Monad m, TypeErrorI m)
           => TypeError -> a -> Map a b -> m b
envRequire e = envRequireM $ return e

-- |Retrieves an entry from an environment, producing an error if it cannot be
--  found.
envRequireM :: (Ord a, Applicative m, Monad m, TypeErrorI m)
            => m TypeError -> a -> Map a b -> m b
envRequireM e k m = maybe (typeError =<< e) return $ Map.lookup k m

aEnvRequireAnn :: (Applicative m, Monad m, TypeErrorI m)
               => UID -> TEnvId -> TAliasEnv -> m NormalAnnType
aEnvRequireAnn u i aEnv = do
  entry <- envRequire (UnboundTypeEnvironmentIdentifier u i) i aEnv
  case entry of
    AnnAlias annType -> return annType
    QuantAlias _ -> typeError $ NonAnnotationAlias u i

pEnvRequire :: (Applicative m, Monad m, TypeErrorI m)
            => TEnvId -> TParamEnv -> m UVar
pEnvRequire i p = do
  envRequire (InternalError $ MissingTypeParameter p i) i p
