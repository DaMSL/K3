{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, GeneralizedNewtypeDeriving, DataKinds, TupleSections, TemplateHaskell, ScopedTypeVariables #-}

{-|
  A module defining the computational environment in which typechecking
  occurs.
-}

module Language.K3.TypeSystem.TypeChecking.Monad
( DeclTypecheckM
, ExprTypecheckM
, runDeclTypecheckM
, transExprToDeclTypecheckM
, typecheckError
, gatherParallelErrors
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Trans.Writer
import Data.Either
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Data.TypesAndConstraints
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeAttribution
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |The state of the typechecking monad.
data TypecheckState
  = TypecheckState
    { nextVarId :: Int
    }

-- |A type for abstract typechecking environments.  This newtype represents the
--  common behaviors for typechecking independent of the specific typechecking
--  context.
newtype AbstractTypecheckT m a
  = AbstractTypecheckT
      { unAbstractTypecheckT ::
          EitherT (Seq TypeError) ((StateT TypecheckState) m) a }
  deriving ( Monad, Functor, Applicative, MonadState TypecheckState )

instance MonadTrans AbstractTypecheckT where
  lift = AbstractTypecheckT . lift . lift

-- |A type for expression typechecking environments.
newtype ExprTypecheckM a
  = ExprTypecheckM
      { unExprTypecheckM ::
          AbstractTypecheckT (Writer (Map UID AnyTVar, ConstraintSet)) a }
  deriving ( Monad, Functor, Applicative, MonadState TypecheckState
           , FreshVarI, FreshOpaqueI, TypeErrorI, TypecheckErrorable )

-- |A type for declaration typechecking environments.
newtype DeclTypecheckM a
  = DeclTypecheckM
      { unDeclTypecheckM ::
          AbstractTypecheckT (Writer (Map UID (AnyTVar, ConstraintSet))) a }
  deriving ( Monad, Functor, Applicative, MonadState TypecheckState
           , FreshVarI, FreshOpaqueI, TypeErrorI, TypecheckErrorable )
  
-- |Evaluates a declaration typechecking computation.
runDeclTypecheckM :: Int -> DeclTypecheckM a
                  -> ( Either (Seq TypeError) (a, Int) 
                     , Map UID (AnyTVar, ConstraintSet) )
runDeclTypecheckM firstVarId x =
  let initState = TypecheckState { nextVarId = firstVarId } in
  let ((eVal,finalState),tmap) =
        runWriter (runStateT (runEitherT $
          unAbstractTypecheckT $ unDeclTypecheckM x) initState) in
  ( (,nextVarId finalState) <$> eVal, tmap )
  
transExprToDeclTypecheckM :: ExprTypecheckM a -> DeclTypecheckM a
transExprToDeclTypecheckM =
  DeclTypecheckM . AbstractTypecheckT . mapE .
    unAbstractTypecheckT . unExprTypecheckM
  where
    mapE = mapEitherT $ mapStateT $ mapWriter mapV
    mapV (x, (m,cs)) = (x, Map.map (,cs) m)

getNextVarId :: (Monad m) => AbstractTypecheckT m Int
getNextVarId = do
  s <- get
  put $ s { nextVarId = nextVarId s + 1 }
  return $ nextVarId s

instance (Monad m) => FreshVarI (AbstractTypecheckT m) where
  freshQVar = freshVar QTVar
  freshUVar = freshVar UTVar

instance (Monad m) => FreshOpaqueI (AbstractTypecheckT m) where
  freshOVar origin = OpaqueVar origin . OpaqueID <$> getNextVarId
  
freshVar :: (Monad m)
         => (Int -> TVarOrigin q -> TVar q) -> TVarOrigin q
         -> AbstractTypecheckT m (TVar q)
freshVar cnstr origin = cnstr <$> getNextVarId <*> pure origin
  
instance (Monad m) => TypeErrorI (AbstractTypecheckT m) where
  typeError = typecheckError

instance TypeVarAttrI ExprTypecheckM where
  attributeExprVar u a =
    ExprTypecheckM $ lift $ tell (Map.singleton u a, csEmpty)
  attributeExprConstraints cs =
    ExprTypecheckM $ lift $ tell (Map.empty, cs)

instance TypeAttrI DeclTypecheckM where
  attributeExprType u a cs =
    DeclTypecheckM $ lift $ tell $ Map.singleton u (a, cs)

-- |Generates an error for a typechecking operation.
class (TypeErrorI m) => TypecheckErrorable m where
  typecheckError :: TypeError -> m a
  gatherParallelErrors :: [m a] -> m [a]

instance (Monad m) => TypecheckErrorable (AbstractTypecheckT m) where
  typecheckError = AbstractTypecheckT . EitherT . return . Left . Seq.singleton
  gatherParallelErrors ops = AbstractTypecheckT $ EitherT $ do
    executed <- mapM (runEitherT . unAbstractTypecheckT) ops
    let (errs,vals) = partitionEithers executed
    return $ if null errs
              then Right vals
              else Left $ foldl (Seq.><) Seq.empty errs
