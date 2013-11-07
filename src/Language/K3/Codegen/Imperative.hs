{-# LANGUAGE ViewPatterns #-}
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

import Data.Functor
import Data.Tree

import Language.K3.Core.Annotation

import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

type ImperativeE = ()
type ImperativeS = ()

type ImperativeM a = EitherT ImperativeE (State ImperativeS) a

runImperativeM :: ImperativeM a -> ImperativeS -> (Either ImperativeE a, ImperativeS)
runImperativeM m s = flip runState s $ runEitherT m

declaration :: K3 Declaration -> ImperativeM (K3 Declaration)
declaration = undefined

-- | Given a list of annotations corresponding to a collection declaration, construct the name of
-- the composite which will fulfill the annotation.
constructComposite :: [Annotation Expression] -> K3 Type
constructComposite = undefined

-- | Given a list of record labels, construct the name of the composite which will represent the
-- record type.
constructRecord :: [Identifier] -> K3 Type
constructRecord = undefined

-- Rewrite function application. If the function to be applied is a global function (referenced
-- through a variable, it remains as-is. However if it is an anonymous function, it is rewritten to
-- a let-in.
expression :: K3 Expression -> ImperativeM (K3 Expression)

-- Constant expressions remain largely unchanged; empty collection declarations need to have their
-- corresponding composite type generated based on their declared annotations.
expression e@(Node (EConstant c :@: as) _) = case c of
    CEmpty _ -> return $ e @+ (EImplementationType $ constructComposite as)
    _ -> return e

-- Function application must be rewritten to a let-in, if the function to be applied is an anonymous
-- lambda, since it can be inlined. Global function applications remain unchanged.
expression (Node (EOperate OApp :@: as) [f, x]) = case f of
    (tag -> EVariable _) -> Node (EOperate OApp :@: as) <$> mapM expression [f, x]
    Node (ELambda i :@: as') [b]  -> Node (ELetIn i :@: (EImmutable:as')) <$> mapM expression [x, b]
expression (Node (EOperate op :@: as) cs) = Node (EOperate op :@: as) <$> mapM expression cs

-- Catch-all case for remaining forms.
expression (Node t cs) = Node t <$> mapM expression cs
