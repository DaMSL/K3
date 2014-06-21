{-# LANGUAGE GeneralizedNewtypeDeriving, TupleSections #-}

{-|
  A module defining the type decision monad.
-}
module Language.K3.TypeSystem.TypeDecision.Monad
( TypeDecideM
, runDecideM
, nextStub
, decisionError
, gatherParallelErrors
) where

import Control.Applicative
import Control.Monad.State
import Control.Monad.Trans.Either
import Data.Either
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.TypeDecision.Data

-- |A monad in which type decision takes place.
newtype TypeDecideM a
  = TypeDecideM { unTypeDecideM ::
                    EitherT (Seq TypeError) (State TypeDecideState) a }
  deriving (Monad, Applicative, Functor, MonadState TypeDecideState)

-- |Runs a type decision computation.  Note that this operation assumes that the
--  default stub ID is zero; if the result may contain stubs, it should not be
--  merged directly into another @TypeDecideM@ without careful consideration.
runDecideM :: Int -> TypeDecideM a -> Either (Seq TypeError) (a, Int)
runDecideM firstVarId (TypeDecideM x) =
  let (ans,st) =
        runState (runEitherT x)
          TypeDecideState
            { nextStubId = 0
            , nextVarId = firstVarId }
  in (,nextVarId st) <$> ans

-- |A type describing the state of the type decision monad.
data TypeDecideState
  = TypeDecideState
    { nextStubId :: Int
    , nextVarId :: Int
    }

getNextVarId :: TypeDecideM Int
getNextVarId = do
  s <- get
  put $ s { nextVarId = nextVarId s + 1 }
  return $ nextVarId s

instance FreshOpaqueI TypeDecideM where
  freshOVar origin = OpaqueVar origin . OpaqueID <$> getNextVarId

instance FreshVarI TypeDecideM where
  freshQVar origin = QTVar <$> (TVarBasicID <$> getNextVarId) <*> return origin
  freshUVar origin = UTVar <$> (TVarBasicID <$> getNextVarId) <*> return origin

instance TypeErrorI TypeDecideM where
  typeError = decisionError


-- |A routine to fetch the next stub from a type decision monad.
nextStub :: TypeDecideM Stub
nextStub = do
  s <- get
  put $ s { nextStubId = nextStubId s + 1 }
  return $ Stub $ nextStubId s

-- |Expresses in a @TypeDecideM@ that an error has occurred.
decisionError :: TypeError -> TypeDecideM a
decisionError = TypeDecideM . EitherT . return . Left . Seq.singleton

-- |An operation to execute multiple independent typechecking operations in
--  parallel.  If any operation fails, then the errors from *all* failed
--  operations are collected in the result.  Otherwise, the results of the
--  operations are reported as in @mapM@.
gatherParallelErrors :: [TypeDecideM a] -> TypeDecideM [a]
gatherParallelErrors ops = TypeDecideM $ EitherT $ do
  executed <- mapM (runEitherT . unTypeDecideM) ops
  let (errs,vals) = partitionEithers executed
  return $ if null errs
            then Right vals
            else Left $ foldl (Seq.><) Seq.empty errs
