{-|
  A module defining the type decision monad.
-}
module Language.K3.TypeSystem.TypeDecision.Monad
( TypeDecideM
, runDecideM
, nextStub
, decisionError
) where

import Control.Monad.State

import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.TypeDecision.Data

-- |A monad in which type decision takes place.
type TypeDecideM a = StateT TypeDecideState (Either TypeError) a

-- |Runs a type decision computation.  Note that this operation assumes that the
--  default stub ID is zero; if the result may contain stubs, it should not be
--  merged directly into another @TypeDecideM@ without careful consideration.
runDecideM :: TypeDecideM a -> Either TypeError a
runDecideM x = evalStateT x TypeDecideState{ nextStubId = 0 }

-- |A type describing the state of the type decision monad.
data TypeDecideState
  = TypeDecideState
    { nextStubId :: Int
    }

-- |A routine to fetch the next stub from a type decision monad.
nextStub :: TypeDecideM Int
nextStub = do
  s <- get
  put $ s { nextStubId = nextStubId s + 1 }
  return $ nextStubId s

-- |Expresses in a @TypeDecideM@ that an error has occurred.
decisionError :: TypeError -> TypeDecideM a
decisionError = lift . Left
