{-# LANGUAGE ViewPatterns #-}

-- Common utilities in writing program transformations.
module Language.K3.Transform.Common where

import Control.Arrow
import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import qualified Language.K3.Core.Constructor.Expression as EC

-- | Map a function over all expressions in the program tree.
mapExpression :: (Monad m)
              => (K3 Expression -> m (K3 Expression))
              -> K3 Declaration
              -> m (K3 Declaration)
mapExpression exprF prog = mapTree rebuildDecl prog
  where
    rebuildDecl ch (tag &&& annotations -> (DGlobal i t eOpt, anns)) = do
      neOpt <- rebuildInitializer eOpt
      return $ Node (DGlobal i t neOpt :@: anns) ch

    rebuildDecl ch (tag &&& annotations -> (DTrigger i t e, anns)) = do
      ne <- exprF e
      return $ Node (DTrigger i t ne :@: anns) ch

    rebuildDecl ch (tag &&& annotations -> (DAnnotation i tVars mems, anns)) = do
      nMems <- mapM rebuildAnnMem mems
      return $ Node (DAnnotation i tVars nMems :@: anns) ch

    rebuildDecl ch (Node t _) = return $ Node t ch

    rebuildAnnMem (Lifted    p n t eOpt anns) =
      rebuildInitializer eOpt >>= \neOpt -> return $ Lifted    p n t neOpt anns
    
    rebuildAnnMem (Attribute p n t eOpt anns) =
      rebuildInitializer eOpt >>= \neOpt -> return $ Attribute p n t neOpt anns
    
    rebuildAnnMem (MAnnotation p n anns) = return $ MAnnotation p n anns
    
    rebuildInitializer eOpt = maybe (return Nothing) (\e -> exprF e >>= return . Just) eOpt

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
