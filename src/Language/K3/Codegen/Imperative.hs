{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

-- | Imperative code generation for K3.
-- This module provides functions which perform the first stage tree transformation to the
-- imperative embedding. Stringification to a target language must be done subsequently.
module Language.K3.Codegen.Imperative (
    -- * Transformation Types
    ImperativeE,
    ImperativeS(..),
    ImperativeM,

    -- * Transformation Actions
    runImperativeM,
    defaultImperativeS,

    -- * Tree Forms
    declaration,
    expression
) where

import Control.Arrow ((&&&))
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

type ImperativeE = ()

data ImperativeS = ImperativeS {
        globals :: [Identifier],
        patchables :: [Identifier],
        showables  :: [(Identifier, K3 Type)],
        mutables :: [Identifier]
    }

type ImperativeM a = EitherT ImperativeE (State ImperativeS) a

runImperativeM :: ImperativeM a -> ImperativeS -> (Either ImperativeE a, ImperativeS)
runImperativeM m s = flip runState s $ runEitherT m

defaultImperativeS :: ImperativeS
defaultImperativeS = ImperativeS { globals = [], patchables = [], mutables = [], showables = []}

withMutable :: Identifier -> ImperativeM a -> ImperativeM a
withMutable i m = do
    oldS <- get
    put $ oldS { mutables = i : mutables oldS }
    result <- m
    newS <- get
    case mutables newS of
        (_:xs) -> put (newS { mutables = xs })
        [] -> left ()
    return result

addGlobal :: Identifier -> ImperativeM ()
addGlobal i = modify $ \s -> s { globals = i : globals s }

addPatchable :: Identifier -> ImperativeM ()
addPatchable i = modify $ \s -> s { patchables  = i : patchables s }

-- | Add a new showable variable
addShowable :: Identifier -> K3 Type -> ImperativeM ()
addShowable i t = modify $ \s -> s { showables = (i,t) : showables s }

isCachedMutable :: Identifier -> ImperativeM Bool
isCachedMutable i = elem i . mutables <$> get

declaration :: K3 Declaration -> ImperativeM (K3 Declaration)
declaration (Node t@(DGlobal i y Nothing :@: _) cs) = do
    addGlobal i
    when (tag y `notElem` [TFunction, TSource]) $ (addPatchable i >> addShowable i y)
    Node t <$> mapM declaration cs
declaration (Node (DGlobal i t (Just e) :@: as) cs) = do
    addGlobal i
    when (tag t `notElem` [TFunction, TSource]) $ (addPatchable i >> addShowable i t)
    me' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DGlobal i t (Just me') :@: as) cs'
declaration (Node (DTrigger i t e :@: as) cs) = do
    addGlobal i
    ne' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DTrigger i t ne' :@: as) cs'
declaration (Node t cs) = Node t <$> mapM declaration cs

expression :: K3 Expression -> ImperativeM (K3 Expression)
expression e@(tag &&& children -> (ELetIn _, [v@(tag -> EVariable i), _])) = do
    let isMutable = isJust $ v @~ (\case {EMutable -> True; _ -> False})
    let modifier = if isMutable then withMutable i else id
    let (Node t cs) = e in modifier $ Node t <$> mapM expression cs

expression e@(tag -> EVariable i) = do
    b <- isCachedMutable i
    return $ if b then e @+ EMutable else e

expression (Node t cs) = Node t <$> mapM expression cs
