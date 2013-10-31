-- | Imperative code generation for K3.
-- This module provides functions which perform the first stage tree transformation to the
-- imperative embedding. Stringification to a target language must be done subsequently.
module Language.K3.Codegen.Imperative (
    ImperativeE,
    ImperativeS,

    ImperativeM,
    runImperativeM
) where

import Control.Monad.State
import Control.Monad.Trans.Either

type ImperativeE = ()
type ImperativeS = ()

type ImperativeM a = EitherT ImperativeE (State ImperativeS) a

runImperativeM :: ImperativeM a -> ImperativeS -> (Either ImperativeE a, ImperativeS)
runImperativeM m s = flip runState s $ runEitherT m
