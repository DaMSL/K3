{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE FlexibleContexts #-}

-- Common utilities in writing program transformations.
module Language.K3.Transform.Common where

import Control.Monad.Identity
import Control.Monad.State
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Expression as EC

-- | Substitute all occurrences of a variable with an expression in the specified target expression.
substituteImmutBinding :: Identifier -> K3 Expression -> K3 Expression -> K3 Expression
substituteImmutBinding i iExpr expr =
    runIdentity $ biFoldMapTree pruneSubs rebuild [(i, iExpr)] EC.unit expr
  where
    pruneSubs subs (tag -> ELambda j) = return $ pruneBinding subs [j] [True]
    pruneSubs subs (tag -> ELetIn  j) = return $ pruneBinding subs [j] [False, True]
    pruneSubs subs (tag -> EBindAs b) = return $ pruneBinding subs (bindingVariables b) [False, True]
    pruneSubs subs (tag -> ECaseOf j) = return $ pruneBinding subs [j] [False, True, False]
    pruneSubs subs n = return $ (subs, replicate (length $ children n) subs)

    pruneBinding subs ids oldOrNew =
      let newSubs = foldl removeAssoc subs ids
      in (subs, map (\useNew -> if useNew then newSubs else subs) oldOrNew)

    rebuild subs _  n@(tag -> EVariable j) = return $ maybe n id $ lookup j subs
    rebuild _    _  n@(tag -> EConstant _) = return $ n
    rebuild _    ch   (Node t _)           = return $ Node t ch


-- Renumber the uuids in a program
renumberUids :: K3 Declaration -> K3 Declaration
renumberUids p = evalState run 1
  where
    run = do
      -- First modify declaration annotations
      ds  <- modifyTree (\n -> replace isDUID (DUID . UID) n) p
      ds' <- mapExpression replaceAll ds
      return ds'

    replaceAll d = modifyTree (\n -> replace isEUID (EUID . UID) n) d

    replace matcher constructor n = do
      i <- get
      put (i+1)
      return $ (stripAnnot matcher n) @+ constructor i

    stripAnnot :: Eq (Annotation a) => (Annotation a -> Bool) -> K3 a -> K3 a
    stripAnnot f t = maybe t (t @-) $ t @~ f

-- Add missing spans in a program tree
addSpans :: String -> K3 Declaration -> K3 Declaration
addSpans spanName p = runIdentity $ do
  ds  <- modifyTree (return . add isDSpan (DSpan $ GeneratedSpan spanName)) p
  mapExpression addExpr ds
  where
    addExpr n = modifyTree addSpanQual n

    addSpanQual n = return $
                      add isESpan (ESpan $ GeneratedSpan spanName) n
                      -- add isEQualified EImmutable n

    -- Don't add span if we already have it
    add matcher anno n = maybe (n @+ anno) (const n) (n @~ matcher)

-- Clean up code generation with renumbering uids and adding spans
cleanGeneration :: String -> K3 Declaration -> K3 Declaration
cleanGeneration spanName = (addSpans spanName) . renumberUids
