{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TypeFamilies #-}

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

type SetFlag = Bool
type IsBuiltIn = Bool

data ImperativeS = ImperativeS {
        globals :: [(Identifier, (K3 Type, IsBuiltIn))],
        triggers :: [(Identifier, K3 Type)],
        patchables :: [(Identifier, (K3 Type, SetFlag))],
        showables  :: [(Identifier, K3 Type)],
        mutables :: [Identifier]
    }

type ImperativeM a = EitherT ImperativeE (State ImperativeS) a

runImperativeM :: ImperativeM a -> ImperativeS -> (Either ImperativeE a, ImperativeS)
runImperativeM m s = flip runState s $ runEitherT m

defaultImperativeS :: ImperativeS
defaultImperativeS = ImperativeS { globals = [], patchables = [], mutables = [], showables = [], triggers = []}

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

addGlobal :: Identifier -> K3 Type -> IsBuiltIn -> ImperativeM ()
addGlobal i t b = modify $ \s -> s { globals = (i, (t, b)) : globals s }

addTrigger :: Identifier -> K3 Type -> ImperativeM ()
addTrigger i t = modify $ \s -> s { triggers = (i, t) : triggers s }

addPatchable :: Identifier -> K3 Type -> Bool -> ImperativeM ()
addPatchable i t setFlag = modify $ \s -> s { patchables  = (i, (t, setFlag)) : patchables s }

-- | Add a new showable variable
addShowable :: Identifier -> K3 Type -> ImperativeM ()
addShowable i t = modify $ \s -> s { showables = (i,t) : showables s }

isCachedMutable :: Identifier -> ImperativeM Bool
isCachedMutable i = elem i . mutables <$> get

isFunction :: K3 Type -> Bool
isFunction (tag -> TFunction) = True
isFunction (tag -> TForall _) = True
isFunction (tag -> TSource) = True
isFunction (tag -> TSink) = True
isFunction _ = False

declaration :: K3 Declaration -> ImperativeM (K3 Declaration)
declaration (Node t@(DGlobal i y Nothing :@: as) cs) = do
    let isF = isFunction y
    let isP = isJust $ as @~ (\case { DProperty (dPropertyV -> ("Pinned", Nothing)) -> True; _ -> False })
    addGlobal i y isF
    unless isF (addPatchable i y False >> unless isP (addShowable i y))
    Node t <$> mapM declaration cs

declaration (Node (DGlobal i t (Just e) :@: as) cs) = do
    addGlobal i t False
    (if tag t == TSink then addTrigger i (head $ children t) else return ())
    let isP = isJust $ as @~ (\case { DProperty (dPropertyV -> ("Pinned", Nothing)) -> True; _ -> False })
    unless (isFunction t || isTid i t) (addPatchable i t True >> unless isP (addShowable i t))
    me' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DGlobal i t (Just me') :@: as) cs'
    where isTid n (tag -> TInt) | drop (length n - 4) n == "_tid" = True
          isTid _ _ = False

declaration (Node (DTrigger i t e :@: as) cs) = do
    addGlobal i t False
    addTrigger i t
    ne' <- expression e
    cs' <- mapM declaration cs
    return $ Node (DTrigger i t ne' :@: as) cs'

declaration t@(tag -> DDataAnnotation _ _ amds) = do
    forM_ amds $ \case
        Lifted Provides j u _ _ -> addGlobal j u False
        _ -> return ()
    let (Node t' cs') = t in Node t' <$> mapM declaration cs'

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
