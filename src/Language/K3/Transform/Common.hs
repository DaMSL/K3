{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- Common utilities in writing program transformations.
module Language.K3.Transform.Common where

import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression

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
