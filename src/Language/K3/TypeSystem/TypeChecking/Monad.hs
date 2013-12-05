{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, GeneralizedNewtypeDeriving, DataKinds, TupleSections, TemplateHaskell, ScopedTypeVariables, MultiParamTypeClasses, UndecidableInstances, FlexibleContexts #-}

{-|
  A module defining the computational environment in which typechecking
  occurs.
-}

module Language.K3.TypeSystem.TypeChecking.Monad
( TypecheckContext(..)
, typecheckingContext
, TypecheckM
, runTypecheckM
, typecheckError
, gatherParallelErrors
) where

import Control.Applicative
import Control.Monad.RWS
import Control.Monad.Trans.Either
import Data.Either
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeAttribution
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |The state of the typechecking monads.
data TypecheckState
  = TypecheckState
    { nextVarId :: Int
    }

-- |The context for the typechecking monads.
data TypecheckContext
  = TypecheckContext
    { globalREnv :: TGlobalQuantEnv
    }

-- |The accumulator used to gather type ascriptions.
type TypecheckLog = (Map UID AnyTVar, ConstraintSet)

-- |Describes a monad in which typechecking can occur.
newtype TypecheckM a
 = TypecheckM
      { unTypecheckM ::
          EitherT (Seq TypeError)
            (RWS TypecheckContext TypecheckLog TypecheckState) a }
  deriving ( Monad, Functor, Applicative, MonadState TypecheckState
           , MonadReader TypecheckContext, MonadWriter TypecheckLog )

-- |Retrieves the typechecking context for a typechecking monad.
typecheckingContext :: (MonadReader TypecheckContext m)
                    => m TypecheckContext
typecheckingContext = ask

-- |Evaluates a typechecking computation.
runTypecheckM :: TGlobalQuantEnv -> Int -> TypecheckM a
              -> ( Either (Seq TypeError) (a, Int) 
                 , (Map UID AnyTVar, ConstraintSet) )
runTypecheckM rEnv firstVarId x =
  let initState = TypecheckState { nextVarId = firstVarId } in
  let initContext = TypecheckContext { globalREnv = rEnv } in
  let (eVal,finalState,tmap) =
        runRWS (runEitherT (unTypecheckM x)) initContext initState in
  (,tmap) $ case eVal of
    Left err -> Left err
    Right v -> Right (v,nextVarId finalState)
  
getNextVarId :: TypecheckM Int
getNextVarId = do
  s <- get
  put $ s { nextVarId = nextVarId s + 1 }
  return $ nextVarId s

instance FreshVarI TypecheckM where
  freshQVar = freshVar QTVar
  freshUVar = freshVar UTVar

instance FreshOpaqueI TypecheckM where
  freshOVar origin = OpaqueVar origin . OpaqueID <$> getNextVarId
  
freshVar :: (TVarID -> TVarOrigin q -> TVar q) -> TVarOrigin q
         -> TypecheckM (TVar q)
freshVar cnstr origin = cnstr <$> (TVarBasicID <$> getNextVarId) <*> pure origin
  
instance TypeErrorI TypecheckM where
  typeError = typecheckError

instance TypeVarAttrI TypecheckM where
  attributeVar u a =
    TypecheckM $ lift $ tell (Map.singleton u a, csEmpty)
  attributeConstraints cs =
    TypecheckM $ lift $ tell (Map.empty, cs)

typecheckError :: TypeError -> TypecheckM a
typecheckError err = TypecheckM $ hoistEither $ Left $ Seq.singleton err

gatherParallelErrors :: [TypecheckM a] -> TypecheckM [a]
gatherParallelErrors ops = TypecheckM $ EitherT $ do
  executed <- mapM (runEitherT . unTypecheckM) ops
  let (errs,vals) = partitionEithers executed
  return $ if null errs
            then Right vals
            else Left $ foldl (Seq.><) Seq.empty errs
