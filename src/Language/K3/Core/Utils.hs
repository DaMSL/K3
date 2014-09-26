{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

{-|
  Contains utilities for manipulating the core K3 data structures.
-}
module Language.K3.Core.Utils
( check0Children
, check1Child
, check2Children
, check3Children
, check4Children
, check5Children
, check6Children
, check7Children
, check8Children
, prependToRole

, mapTree
, modifyTree
, foldMapTree
, foldTree
, biFoldTree
, biFoldMapTree
, foldRebuildTree
, foldMapRebuildTree
, mapIn1RebuildTree
, foldIn1RebuildTree
, foldMapIn1RebuildTree

, foldProgram
, mapProgram
, foldExpression
, mapExpression

, freeVariables
, bindingVariables
, stripAnnotations
, compareWithoutAnnotations
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.Functor.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.Haskell.TH hiding ( Type )

-- * Generated routines

-- Defines check0Children through check8Children.  These routines accept a
-- K3 tree and verify that it has a given number of children.  When it does, the
-- result is Just a tuple with that many elements.  When it does not, the result
-- is Nothing.
$(
  let mkCheckChildren :: Int -> Q [Dec]
      mkCheckChildren n = do
        let fname = mkName $ "check" ++ show n ++ "Child" ++
                                (if n /= 1 then "ren" else "")
        ename <- newName "tree"
        elnames <- mapM (newName . ("el" ++) . show) [1::Int .. n]
        typN <- newName "a"
        let typ = varT typN
        let tupTyp = foldl appT (tupleT n) $ replicate n $ [t|K3 $(typ)|]
        ftype <- [t| K3 $(typ) -> Maybe $(tupTyp) |]
        let ftype' = ForallT [PlainTV typN] [] ftype
        let signature = sigD fname $ return ftype'
        let badMatch = match wildP (normalB [| Nothing |]) []
        let goodMatch = match (listP $ map varP elnames) (normalB $
                          appE ([|return|]) $ tupE $ map varE elnames) []
        let bodyExp = caseE ([|subForest $(varE ename)|]) [goodMatch, badMatch]
        let cl = clause [varP ename] (normalB bodyExp) []
        let impl = funD fname [cl]
        sequence [signature,impl]
  in
  concat <$> mapM mkCheckChildren [0::Int .. 8]
 )

-- Prepend declarations to the beginning of a role
prependToRole :: K3 Declaration -> [K3 Declaration] -> K3 Declaration
prependToRole (Node r@(DRole _ :@: _) sub) ds = Node r (ds++sub)
prependToRole _ _ = error "Expected a role"


-- | Transform a tree by mapping a function over every tree node. The function
--   is provided transformed children for every new node built.
mapTree :: (Monad m) => ([Tree b] -> Tree a -> m (Tree b)) -> Tree a -> m (Tree b)
mapTree f n@(Node _ []) = f [] n
mapTree f n@(Node _ ch) = mapM (mapTree f) ch >>= flip f n

-- | Transform a tree by mapping a function over every tree node.
--   The children of a node are pre-transformed recursively
modifyTree :: (Monad m) => (Tree a -> m (Tree a)) -> Tree a -> m (Tree a)
modifyTree f n@(Node _ []) = f n
modifyTree f   (Node x ch) = do
   ch' <- mapM (modifyTree f) ch
   f (Node x ch')

-- | Map an accumulator over a tree, recurring independently over each child.
--   The result is produced by transforming independent subresults in bottom-up fashion.
foldMapTree :: (Monad m) => ([b] -> Tree a -> m b) -> b -> Tree a -> m b
foldMapTree f x n@(Node _ []) = f [x] n
foldMapTree f x n@(Node _ ch) = mapM (foldMapTree f x) ch >>= flip f n

-- | Fold over a tree, threading the accumulator between children.
foldTree :: (Monad m) => (b -> Tree a -> m b) -> b -> Tree a -> m b
foldTree f x n@(Node _ []) = f x n
foldTree f x n@(Node _ ch) = foldM (foldTree f) x ch >>= flip f n

-- | Joint top-down and bottom-up traversal of a tree.
--   This variant threads an accumulator across all siblings, and thus all
--   nodes in the prefix of the tree to every node.
biFoldTree :: (Monad m)
           => (td -> Tree a -> m (td, [td]))
           -> (td -> bu -> Tree a -> m bu)
           -> td -> bu -> Tree a -> m bu
biFoldTree tdF buF tdAcc buAcc n@(Node _ []) = tdF tdAcc n >>= \(td,_) -> buF td buAcc n
biFoldTree tdF buF tdAcc buAcc n@(Node _ ch) = do
  (ntd, cntd) <- tdF tdAcc n
  if (length cntd) /= (length ch)
    then fail "Invalid top-down accumulation in biFoldTree"
    else do
      nbu <- foldM (\nbuAcc (ctd, c) -> biFoldTree tdF buF ctd nbuAcc c) buAcc $ zip cntd ch
      buF ntd nbu n

-- | Join top-down and bottom-up traversal of a tree.
--   This variant threads a bottom-up accumulator independently between siblings.
--   Thus there is no sideways information passing (except for top-down accumulation).
--   tdF: takes the top-down accumulator and node. Returns a top-down value for post-processing
--        at the same node, and messages for each child
biFoldMapTree :: (Monad m)
              => (td -> Tree a -> m (td, [td]))
              -> (td -> [bu] -> Tree a -> m bu)
              -> td -> bu -> Tree a -> m bu
biFoldMapTree tdF buF tdAcc buAcc n@(Node _ []) = tdF tdAcc n >>= \(td,_) -> buF td [buAcc] n
biFoldMapTree tdF buF tdAcc buAcc n@(Node _ ch) = do
  (ntd, cntd) <- tdF tdAcc n
  if (length cntd) /= (length ch)
    then fail "Invalid top-down accumulation in biFoldMapTree"
    else do
      nbu <- mapM (\(ctd,c) -> biFoldMapTree tdF buF ctd buAcc c) $ zip cntd ch
      buF ntd nbu n

-- | Rebuild a tree with an accumulator and transformed children at every node.
foldRebuildTree :: (Monad m)
                => (b -> [Tree a] -> Tree a -> m (b, Tree a))
                -> b -> Tree a -> m (b, Tree a)
foldRebuildTree f x n@(Node _ []) = f x [] n
foldRebuildTree f x n@(Node _ ch) = foldM rebuild (x,[]) ch >>= uncurry (\a b -> f a b n)
  where rebuild (acc, chAcc) c =
          foldRebuildTree f acc c >>= (\(nAcc, nc) -> return (nAcc, chAcc++[nc]))

-- | Rebuild a tree with independent accumulators and transformed children at every node.
foldMapRebuildTree :: (Monad m)
                   => ([b] -> [Tree a] -> Tree a -> m (b, Tree a))
                   -> b -> Tree a -> m (b, Tree a)
foldMapRebuildTree f x n@(Node _ []) = f [x] [] n
foldMapRebuildTree f x n@(Node _ ch) =
    mapM (foldMapRebuildTree f x) ch >>= (\(a, b) -> f a b n) . unzip

-- | Rebuild a tree with explicit pre and post transformers applied to the first
--   child of every tree node. This is useful for stateful monads that modify
--   environments based on the type of the first child.
mapIn1RebuildTree :: (Monad m)
                  => (Tree a -> Tree a -> m ())
                  -> (Tree a -> Tree a -> m [m ()])
                  -> ([Tree a] -> Tree a -> m (Tree a))
                  -> Tree a -> m (Tree a)
mapIn1RebuildTree _ _ allChF n@(Node _ []) = allChF [] n
mapIn1RebuildTree preCh1F postCh1F allChF n@(Node _ ch) = do
    preCh1F (head ch) n
    nc1 <- rcr $ head ch
    restm <- postCh1F nc1 n
    if (length restm) /= (length $ tail ch)
      then fail "Invalid mapIn1RebuildTree sequencing"
      else do
        nRestCh <- mapM (\(m, c) -> m >> rcr c) $ zip restm $ tail ch
        allChF (nc1:nRestCh) n

  where rcr = mapIn1RebuildTree preCh1F postCh1F allChF

-- | Tree accumulation and reconstruction, with a priviliged first child accumulation.
--   This function is useful for manipulating ASTs subject to bindings introduced by the first
--   child, for example with let-ins, bind-as and case-of.
--   This function takes a pre- and post-first child traversal accumulator transformation function.
--   The post-first-child transformation additionally returns siblings to which the accumulator
--   should be propagated, for example case-of should not propagate bindings to the None branch.
--   The traversal also takes a merge function to combine the running accumulator
--   passed through siblings with those that skip accumulator propagation.
--   Finally, the traversal takes a post-order accumulator and children transformation function.
foldIn1RebuildTree :: (Monad m)
                   => (b -> Tree a -> Tree a -> m b)
                   -> (b -> Tree a -> Tree a -> m (b, [Bool]))
                   -> (b -> b -> m b)
                   -> (b -> [Tree a] -> Tree a -> m (b, Tree a))
                   -> b -> Tree a -> m (b, Tree a)
foldIn1RebuildTree _ _ _ allChF acc n@(Node _ []) = allChF acc [] n
foldIn1RebuildTree preCh1F postCh1F mergeF allChF acc n@(Node _ ch) = do
    nAcc                 <- preCh1F acc (head ch) n
    (nAcc2, nc1)         <- rcr nAcc $ head ch
    (nAcc3, useInitAccs) <- postCh1F nAcc2 nc1 n
    if (length useInitAccs) /= (length $ tail ch)
      then fail "Invalid foldIn1RebuildTree accumulation"
      else do
        (nAcc4, nch) <- foldM rebuild (nAcc3, [nc1]) $ zip useInitAccs $ tail ch
        allChF nAcc4 nch n

  where rcr = foldIn1RebuildTree preCh1F postCh1F mergeF allChF

        rebuild (rAcc, chAcc) (True, c) = do
          (nrAcc, nc) <- rcr rAcc c
          return (nrAcc, chAcc++[nc])

        rebuild (rAcc, chAcc) (False, c) = do
          (cAcc, nc) <- rcr acc c
          nrAcc      <- mergeF rAcc cAcc
          return (nrAcc, chAcc++[nc])

-- | A mapping variant of foldIn1RebuildTree that threads a top-down accumulator
--   while reconstructing the tree.
-- preCh1F:  The pre-child function takes the top-down accumulator, the first child, and the node.
--           It returns a new accumulator
-- postCh1F: The post-child function takes the pre's accumulator, the processed first child, and the node.
--           It returns an accumulator, and a list of accumulators to be sent down while recursing
--           over the other children.
-- allChF:   The all-child function takes the post child's single accumulator, the processed children,
--           and the node, and returns a new tree.
foldMapIn1RebuildTree :: (Monad m)
                  => (b -> Tree a -> Tree a -> m b)
                  -> (b -> Tree a -> Tree a -> m (b, [b]))
                  -> (b -> [Tree a] -> Tree a -> m (Tree a))
                  -> b -> Tree a -> m (Tree a)
foldMapIn1RebuildTree _ _ allChF tdAcc n@(Node _ []) = allChF tdAcc [] n
foldMapIn1RebuildTree preCh1F postCh1F allChF tdAcc n@(Node _ ch) = do
    nCh1Acc        <- preCh1F tdAcc (head ch) n
    nc1            <- rcr nCh1Acc $ head ch
    (nAcc, chAccs) <- postCh1F nCh1Acc nc1 n
    if length chAccs /= (length $ tail ch)
      then fail "Invalid foldMapIn1RebuildTree accumulation"
      else do
        nRestCh <- zipWithM rcr chAccs $ tail ch
        allChF nAcc (nc1:nRestCh) n

  where rcr = foldMapIn1RebuildTree preCh1F postCh1F allChF

-- | Fold a declaration and expression reducer and accumulator over the given program.
foldProgram :: (Monad m)
            => (a -> K3 Declaration -> m (a, K3 Declaration))
            -> (a -> AnnMemDecl     -> m (a, AnnMemDecl))
            -> (a -> K3 Expression  -> m (a, K3 Expression))
            -> Maybe (a -> K3 Type  -> m (a, K3 Type))
            -> a -> K3 Declaration
            -> m (a, K3 Declaration)
foldProgram declF annMemF exprF typeFOpt a prog = foldRebuildTree rebuildDecl a prog
  where
    rebuildDecl acc ch (tag &&& annotations -> (DGlobal i t eOpt, anns)) = do
      (acc2, nt)    <- onType acc t
      (acc3, neOpt) <- rebuildInitializer acc2 eOpt
      declF acc3 $ Node (DGlobal i nt neOpt :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DTrigger i t e, anns)) = do
      (acc2, nt) <- onType acc t
      (acc3, ne) <- exprF acc2 e
      declF acc3 $ Node (DTrigger i nt ne :@: anns) ch

    rebuildDecl acc ch (tag &&& annotations -> (DDataAnnotation i tVars mems, anns)) = do
      (acc2, nMems) <- foldM rebuildAnnMem (acc, []) mems
      declF acc2 $ Node (DDataAnnotation i tVars nMems :@: anns) ch

    rebuildDecl acc ch (Node t _) = declF acc $ Node t ch

    rebuildAnnMem (acc, memAcc) (Lifted p n t eOpt anns) =
      rebuildMem acc memAcc t eOpt $ \(nt, neOpt) -> Lifted p n nt neOpt anns

    rebuildAnnMem (acc, memAcc) (Attribute p n t eOpt anns) =
      rebuildMem acc memAcc t eOpt $ \(nt, neOpt) -> Attribute p n nt neOpt anns

    rebuildAnnMem (acc, memAcc) (MAnnotation p n anns) = do
      (acc2, nMem) <- annMemF acc $ MAnnotation p n anns
      return (acc2, memAcc ++ [nMem])

    rebuildMem acc memAcc t eOpt rebuildF = do
      (acc2, nt) <- onType acc t
      (acc3, neOpt) <- rebuildInitializer acc2 eOpt
      (acc4, nMem)  <- annMemF acc3 $ rebuildF (nt, neOpt)
      return (acc4, memAcc ++ [nMem])

    rebuildInitializer acc eOpt =
      maybe (return (acc, Nothing)) (\e -> exprF acc e >>= return . fmap Just) eOpt

    onType acc t = maybe (return (acc,t)) (\f -> f acc t) typeFOpt


-- | Fold a declaration, expression and annotation member transformer over the given program.
mapProgram :: (Monad m)
            => (K3 Declaration -> m (K3 Declaration))
            -> (AnnMemDecl     -> m AnnMemDecl)
            -> (K3 Expression  -> m (K3 Expression))
            -> Maybe (K3 Type  -> m (K3 Type))
            -> K3 Declaration
            -> m (K3 Declaration)
mapProgram declF annMemF exprF typeFOpt prog =
    foldProgram (wrap declF) (wrap annMemF) (wrap exprF) (maybe Nothing (Just . wrap) $ typeFOpt) () prog >>= return . snd
  where wrap f _ x = f x >>= return . ((), )


-- | Fold a function and accumulator over all expressions in the given program.
foldExpression :: (Monad m)
                => (a -> K3 Expression -> m (a, K3 Expression)) -> a -> K3 Declaration
                -> m (a, K3 Declaration)
foldExpression exprF a prog = foldProgram returnPair returnPair exprF Nothing a prog
  where returnPair x y = return (x,y)

-- | Map a function over all expressions in the program tree.
mapExpression :: (Monad m)
              => (K3 Expression -> m (K3 Expression)) -> K3 Declaration -> m (K3 Declaration)
mapExpression exprF prog = foldProgram returnPair returnPair wrapExprF Nothing () prog >>= return . snd
  where returnPair a b = return (a,b)
        wrapExprF a e = exprF e >>= return . (a,)


{- Expression utilities -}

-- | Retrieves all free variables in an expression.
freeVariables :: K3 Expression -> [Identifier]
freeVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EVariable n) = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ filter (`notElem` bindingVariables b) $ concat chAcc
    extractVariable chAcc (tag -> ELetIn i)    = return $ filter (/= i) $ concat chAcc
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Retrieves all variables introduced by a binder
bindingVariables :: Binder -> [Identifier]
bindingVariables (BIndirection i) = [i]
bindingVariables (BTuple is)      = is
bindingVariables (BRecord ivs)    = snd (unzip ivs)

-- | Strips all annotations from an expression.
stripAnnotations :: K3 Expression -> K3 Expression
stripAnnotations = runIdentity . mapTree strip
  where strip ch n = return $ Node (tag n :@: []) ch

-- | Compares two expressions for identical AST structures while ignoring annotations
--   (such as UIDs, spans, etc.)
compareWithoutAnnotations :: K3 Expression -> K3 Expression -> Bool
compareWithoutAnnotations e1 e2 = stripAnnotations e1 == stripAnnotations e2
