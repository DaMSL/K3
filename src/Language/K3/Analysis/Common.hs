{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- Common utilities in writing program transformations.
module Language.K3.Analysis.Common where

import Control.Arrow
import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

-- | Fold a declaration and expression reducer and accumulator over the given program.
foldProgram :: (Monad m)
            => (a -> K3 Declaration -> m (a, K3 Declaration))
            -> (a -> AnnMemDecl     -> m (a, AnnMemDecl))
            -> (a -> K3 Expression  -> m (a, K3 Expression))
            -> a -> K3 Declaration
            -> m (a, K3 Declaration)
foldProgram declF annMemF exprF a prog = foldRebuildTree rebuildDecl a prog
  where
    rebuildDecl acc ch (tag &&& annotations -> (DGlobal i t eOpt, anns)) = do
      (acc2, neOpt) <- rebuildInitializer acc eOpt
      declF acc2 $ Node (DGlobal i t neOpt :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DTrigger i t e, anns)) = do
      (acc2, ne) <- exprF acc e
      declF acc2 $ Node (DTrigger i t ne :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DDataAnnotation i sVars tVars mems, anns)) = do
      (acc2, nMems) <- foldM rebuildAnnMem (acc, []) mems
      declF acc2 $ Node (DDataAnnotation i sVars tVars nMems :@: anns) ch

    rebuildDecl acc ch (Node t _) = declF acc $ Node t ch

    rebuildAnnMem (acc, memAcc) (Lifted p n t eOpt anns) =
      rebuildMem acc memAcc eOpt $ \neOpt -> Lifted p n t neOpt anns

    rebuildAnnMem (acc, memAcc) (Attribute p n t eOpt anns) =
      rebuildMem acc memAcc eOpt $ \neOpt -> Attribute p n t neOpt anns

    rebuildAnnMem (acc, memAcc) (MAnnotation p n anns) = do
      (acc2, nMem) <- annMemF acc $ MAnnotation p n anns
      return (acc2, memAcc ++ [nMem])

    rebuildMem acc memAcc eOpt rebuildF = do
      (acc2, neOpt) <- rebuildInitializer acc eOpt
      (acc3, nMem)  <- annMemF acc2 $ rebuildF neOpt
      return (acc3, memAcc ++ [nMem])

    rebuildInitializer acc eOpt =
      maybe (return (acc, Nothing)) (\e -> exprF acc e >>= return . fmap Just) eOpt


-- | Fold a declaration, expression and annotation member transformer over the given program.
mapProgram :: (Monad m)
            => (K3 Declaration -> m (K3 Declaration))
            -> (AnnMemDecl     -> m AnnMemDecl)
            -> (K3 Expression  -> m (K3 Expression))
            -> K3 Declaration
            -> m (K3 Declaration)
mapProgram declF annMemF exprF prog =
    foldProgram (wrap declF) (wrap annMemF) (wrap exprF) () prog >>= return . snd
  where wrap f _ x = f x >>= return . ((), )


-- | Fold a function and accumulator over all expressions in the given program.
foldExpression :: (Monad m)
                => (a -> K3 Expression -> m (a, K3 Expression)) -> a -> K3 Declaration
                -> m (a, K3 Declaration)
foldExpression exprF a prog = foldProgram returnPair returnPair exprF a prog
  where returnPair x y = return (x,y)

-- | Map a function over all expressions in the program tree.
mapExpression :: (Monad m)
              => (K3 Expression -> m (K3 Expression)) -> K3 Declaration -> m (K3 Declaration)
mapExpression exprF prog = foldProgram returnPair returnPair wrapExprF () prog >>= return . snd
  where returnPair a b = return (a,b)
        wrapExprF a e = exprF e >>= return . (a,)

