{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | The K3 Interpreter
module Language.K3.Interpreter (
    Interpretation,
    InterpretationError,

    IEnvironment, ILog
) where

import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans.Either
import Control.Monad.Writer

import Language.K3.Core.Annotation
import Language.K3.Core.Type
import Language.K3.Core.Expression

-- | K3 Values
data Value
    = VBool Bool
    | VInt Int
    | VFloat Float
    | VString String
    | VOption Value
    | VTuple [Value]
    | VRecord [(Identifier, Value)]
    | VCollection
    | VIndirection Value
    | VFunction Identifier (K3 Expression)
  deriving (Read, Show)

-- | Interpretation event log.
type ILog = [String]

-- | Interpretation Environment.
type IEnvironment = [(Identifier, Value)]

-- | Errors encountered during interpretation.
data InterpretationError
    = UnknownVariable Identifier 
  deriving (Eq, Read, Show)

-- | The Interpretation Monad. Computes a result (valid/error), with the final state and an event log.
type Interpretation = EitherT InterpretationError (StateT IEnvironment (WriterT ILog Identity))

-- | Run an interpretation to get a value or error, resulting environment and event log.
runInterpretation :: IEnvironment -> Interpretation a -> ((Either InterpretationError a, IEnvironment), ILog)
runInterpretation e = runIdentity . runWriterT . flip runStateT e . runEitherT

-- | Raise an error inside an interpretation. The error will be captured alongside the event log
-- till date, and the current state.
throwE :: InterpretationError -> Interpretation a
throwE = left
