{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, GeneralizedNewtypeDeriving, DataKinds, TupleSections, TemplateHaskell #-}

{-|
  A module defining the computational environment in which typechecking
  occurs.
-}

module Language.K3.TypeSystem.TypeChecking.Monad
( TypecheckM
, runTypecheckM
, typecheckError
, gatherParallelErrors
) where

import Control.Applicative
import Control.Monad.State
import Control.Monad.Trans.Either
import Data.Either
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Language.K3.TypeSystem.Data.TypesAndConstraints
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |The state of the typechecking monad.
data TypecheckState
  = TypecheckState
    { nextVarId :: Int
    }

-- |A type alias for typechecking environments.
newtype TypecheckM a
  = TypecheckM
      { unTypecheckM ::
          EitherT (Seq TypeError) (State TypecheckState) a }
  deriving (Monad, Functor, Applicative, MonadState TypecheckState)
  
-- |Evaluates a typechecking computation.
runTypecheckM :: Int -> TypecheckM a -> Either (Seq TypeError) (a, Int)
runTypecheckM firstVarId x =
  let (ans, st) =
        runState (runEitherT $ unTypecheckM x)
          TypecheckState { nextVarId = firstVarId }
  in (,nextVarId st) <$> ans

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
  
freshVar :: (Int -> TVarOrigin q -> TVar q) -> TVarOrigin q
         -> TypecheckM (TVar q)
freshVar cnstr origin = do
  varId <- getNextVarId
  let ret = cnstr varId origin
  {-
  case origin of
    TVarPolyinstantiationOrigin v _ ->
      _debug $ boxToString $
        ["Polyinstantiated "] %+ prettyLines v %+ [" as "] %+ prettyLines ret
    _ -> return ()
  -}
  return ret
  
instance TypeErrorI TypecheckM where
  typeError = typecheckError

-- |Generates an error for a typechecking operation.
typecheckError :: TypeError -> TypecheckM a
typecheckError = TypecheckM . EitherT . return . Left . Seq.singleton

-- |An operation to execute multiple independent typechecking operations in
--  parallel.  If any operation fails, then the errors from *all* failed
--  operations are collected in the result.  Otherwise, the results of the
--  operations are reported as in @mapM@.
gatherParallelErrors :: [TypecheckM a] -> TypecheckM [a]
gatherParallelErrors ops = TypecheckM $ EitherT $ do
  executed <- mapM (runEitherT . unTypecheckM) ops
  let (errs,vals) = partitionEithers executed
  return $ if null errs
            then Right vals
            else Left $ foldl (Seq.><) Seq.empty errs
    
