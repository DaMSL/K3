-- | Imperative code generation for K3.
-- This module provides functions which perform the first stage tree transformation to the
-- imperative embedding. Stringification to a target language must be done subsequently.
module Language.K3.Codegen.Imperative (
    -- * Transformation Types
    ImperativeE,
    ImperativeS,
    ImperativeM,

    -- * Transformation Actions
    runImperativeM,

    -- * Tree Forms
    declaration,
    expression
) where

import Control.Monad.State
import Control.Monad.Trans.Either

import Data.Maybe
import Data.Functor
import Data.Tree

import Language.K3.Core.Annotation

import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Codegen.Common

type ImperativeE = ()
type ImperativeS = ()

type ImperativeM a = EitherT ImperativeE (State ImperativeS) a

runImperativeM :: ImperativeM a -> ImperativeS -> (Either ImperativeE a, ImperativeS)
runImperativeM m s = flip runState s $ runEitherT m

declaration :: K3 Declaration -> ImperativeM (K3 Declaration)
declaration (Node (DGlobal i t (Just e) :@: as) cs) = do
    me' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DGlobal i t (Just me') :@: as) cs'
declaration (Node (DTrigger i t e :@: as) cs) = do
    ne' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DGlobal i (T.function t T.unit) (Just ne') :@: as) cs'
declaration (Node t cs) = Node t <$> mapM declaration cs

expression :: K3 Expression -> ImperativeM (K3 Expression)
expression (Node t cs) = Node t <$> mapM expression cs
