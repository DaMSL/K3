{-# LANGUAGE LambdaCase #-}
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
, biFoldRebuildTree
, biFoldMapRebuildTree
, mapIn1RebuildTree
, foldIn1RebuildTree
, foldMapIn1RebuildTree

, foldProgramWithDecl
, foldProgram
, mapProgramWithDecl
, mapProgram
, foldExpression
, mapExpression

, mapMaybeAnnotation
, mapAnnotation
, mapExprAnnotation

, foldReturnExpression
, foldMapReturnExpression
, mapReturnExpression

, defaultExpression
, freeVariables
, bindingVariables
, modifiedVariables
, compareWithoutAnnotations

, stripDeclAnnotations
, stripExprAnnotations
, stripTypeAnnotations
, stripAllDeclAnnotations
, stripAllExprAnnotations
, stripAllTypeAnnotations

, repairProgram
, onProgramUID
, maxProgramUID
, minProgramUID

, stripComments
, stripTypeAnns
, resetEffectAnns
, stripEffectAnns
, stripAllEffectAnns
, stripTypeAndEffectAnns
, stripAllTypeAndEffectAnns
, stripAllProperties

) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.Functor.Identity
import Data.List
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Expression as EC

import Language.K3.Analysis.Effects.Core

import Language.K3.Utils.Pretty hiding ( wrap )

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

-- | Joint top-down and bottom-up traversal of a tree with both
--   threaded accumulation and tree reconstruction.
biFoldRebuildTree :: (Monad m)
                  => (td -> Tree a -> m (td, [td]))
                  -> (td -> bu -> [Tree a] -> Tree a -> m (bu, Tree a))
                  -> td -> bu -> Tree a -> m (bu, Tree a)
biFoldRebuildTree tdF buF tdAcc buAcc n@(Node _ []) = tdF tdAcc n >>= \(td,_) -> buF td buAcc [] n
biFoldRebuildTree tdF buF tdAcc buAcc n@(Node _ ch) = do
  (ntd, cntd) <- tdF tdAcc n
  if (length cntd) /= (length ch)
    then fail "Invalid top-down accumulation in biFoldRebuildTree"
    else do
      (nbu, nch) <- foldM rcrWAccumCh (buAcc, []) $ zip cntd ch
      buF ntd nbu nch n

  where
    rcrWAccumCh (nbuAcc, chAcc) (ctd, c) = do
      (rAcc, nc) <- biFoldRebuildTree tdF buF ctd nbuAcc c
      return (rAcc, chAcc ++ [nc])

-- | Joint top-down and bottom-up traversal of a tree with both
--   independent accumulation and tree reconstruction.
biFoldMapRebuildTree :: (Monad m)
                     => (td -> Tree a -> m (td, [td]))
                     -> (td -> [bu] -> [Tree a] -> Tree a -> m (bu, Tree a))
                     -> td -> bu -> Tree a -> m (bu, Tree a)
biFoldMapRebuildTree tdF buF tdAcc buAcc n@(Node _ []) = tdF tdAcc n >>= \(td,_) -> buF td [buAcc] [] n
biFoldMapRebuildTree tdF buF tdAcc buAcc n@(Node _ ch) = do
  (ntd, cntd) <- tdF tdAcc n
  if (length cntd) /= (length ch)
    then fail "Invalid top-down accumulation in biFoldMapRebuildTree"
    else do
      let rcr (ctd, c) = biFoldMapRebuildTree tdF buF ctd buAcc c
      (nbu, nch) <- mapM rcr (zip cntd ch) >>= return . unzip
      buF ntd nbu nch n

-- | Rebuild a tree with explicit pre and post transformers applied to the first
--   child of every tree node. This is useful for stateful monads that modify
--   environments based on the type of the first child.
mapIn1RebuildTree :: (Monad m)
                  => (Tree a -> Tree a -> m ())
                  -> (Tree b -> Tree a -> m [m ()])
                  -> ([Tree b] -> Tree a -> m (Tree b))
                  -> Tree a -> m (Tree b)
mapIn1RebuildTree _ _ allChF n@(Node _ []) = allChF [] n
mapIn1RebuildTree preCh1F postCh1F allChF n@(Node _ ch) = do
    preCh1F (head ch) n
    nc1 <- rcr $ head ch
    restm <- postCh1F nc1 n
    -- Allow for a final action before the call to allChF
    let len = length $ tail ch
        goodLengths = [len, len + 1]
    if length restm `notElem` goodLengths
      then fail "Invalid mapIn1RebuildTree sequencing"
      else do
        nRestCh <- mapM (\(m, c) -> m >> rcr c) $ zip restm $ tail ch
        case drop len restm of
          []  -> allChF (nc1:nRestCh) n
          [m] -> m >> allChF (nc1:nRestCh) n
          _   -> error "unexpected"


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
                   => (c -> Tree a -> Tree a -> m c)
                   -> (c -> Tree b -> Tree a -> m (c, [Bool]))
                   -> (c -> c -> m c)
                   -> (c -> [Tree b] -> Tree a -> m (c, Tree b))
                   -> c -> Tree a -> m (c, Tree b)
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
                  => (c -> Tree a -> Tree a -> m c)
                  -> (c -> Tree b -> Tree a -> m (c, [c]))
                  -> (c -> [Tree b] -> Tree a -> m (Tree b))
                  -> c -> Tree a -> m (Tree b)
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
foldProgramWithDecl :: (Monad m)
                    => (a -> K3 Declaration -> m (a, K3 Declaration))
                    -> (a -> K3 Declaration -> AnnMemDecl     -> m (a, AnnMemDecl))
                    -> (a -> K3 Declaration -> K3 Expression  -> m (a, K3 Expression))
                    -> Maybe (a -> K3 Declaration -> K3 Type  -> m (a, K3 Type))
                    -> a -> K3 Declaration
                    -> m (a, K3 Declaration)
foldProgramWithDecl declF annMemF exprF typeFOpt a prog = foldRebuildTree rebuildDecl a prog
  where
    rebuildDecl acc ch d@(tag &&& annotations -> (DGlobal i t eOpt, anns)) = do
      (acc2, nt)    <- onType acc d t
      (acc3, neOpt) <- rebuildInitializer acc2 d eOpt
      declF acc3 $ Node (DGlobal i nt neOpt :@: anns) ch

    rebuildDecl acc ch d@(tag &&& annotations -> (DTrigger i t e, anns)) = do
      (acc2, nt) <- onType acc d t
      (acc3, ne) <- exprF acc2 d e
      declF acc3 $ Node (DTrigger i nt ne :@: anns) ch

    rebuildDecl acc ch d@(tag &&& annotations -> (DDataAnnotation i tVars mems, anns)) = do
      (acc2, nMems) <- foldM (rebuildAnnMem d) (acc, []) mems
      declF acc2 $ Node (DDataAnnotation i tVars nMems :@: anns) ch

    rebuildDecl acc ch (Node t _) = declF acc $ Node t ch

    rebuildAnnMem d (acc, memAcc) (Lifted p n t eOpt anns) =
      rebuildMem d acc memAcc t eOpt $ \(nt, neOpt) -> Lifted p n nt neOpt anns

    rebuildAnnMem d (acc, memAcc) (Attribute p n t eOpt anns) =
      rebuildMem d acc memAcc t eOpt $ \(nt, neOpt) -> Attribute p n nt neOpt anns

    rebuildAnnMem d (acc, memAcc) (MAnnotation p n anns) = do
      (acc2, nMem) <- annMemF acc d $ MAnnotation p n anns
      return (acc2, memAcc ++ [nMem])

    rebuildMem d acc memAcc t eOpt rebuildF = do
      (acc2, nt) <- onType acc d t
      (acc3, neOpt) <- rebuildInitializer acc2 d eOpt
      (acc4, nMem)  <- annMemF acc3 d $ rebuildF (nt, neOpt)
      return (acc4, memAcc ++ [nMem])

    rebuildInitializer acc d eOpt =
      maybe (return (acc, Nothing)) (\e -> exprF acc d e >>= return . fmap Just) eOpt

    onType acc d t = maybe (return (acc,t)) (\f -> f acc d t) typeFOpt

-- | Variant of the foldProgramWithDecl function, ignoring the parent declaration.
foldProgram :: (Monad m)
            => (a -> K3 Declaration -> m (a, K3 Declaration))
            -> (a -> AnnMemDecl     -> m (a, AnnMemDecl))
            -> (a -> K3 Expression  -> m (a, K3 Expression))
            -> Maybe (a -> K3 Type  -> m (a, K3 Type))
            -> a -> K3 Declaration
            -> m (a, K3 Declaration)
foldProgram declF annMemF exprF typeFOpt a prog =
  foldProgramWithDecl declF (ignore2 annMemF) (ignore2 exprF) (ignore2Opt typeFOpt) a prog
  where ignore2 f = \x _ y -> f x y
        ignore2Opt fOpt = maybe Nothing (\f -> Just $ \x _ y -> f x y) fOpt

-- | Fold a declaration, expression and annotation member transformer over the given program.
--   This variant uses transformer functions that require the containing declaration.
mapProgramWithDecl :: (Monad m)
                   => (K3 Declaration -> m (K3 Declaration))
                   -> (K3 Declaration -> AnnMemDecl     -> m AnnMemDecl)
                   -> (K3 Declaration -> K3 Expression  -> m (K3 Expression))
                   -> Maybe (K3 Declaration -> K3 Type  -> m (K3 Type))
                   -> K3 Declaration
                   -> m (K3 Declaration)
mapProgramWithDecl declF annMemF exprF typeFOpt prog = do
    (_, r) <- foldProgramWithDecl (wrap declF) (wrap2 annMemF) (wrap2 exprF) (maybe Nothing (Just . wrap2) $ typeFOpt) () prog
    return r
  where wrap  f _ x   = f x >>= return . ((), )
        wrap2 f _ d x = f d x >>= return . ((), )

-- | Fold a declaration, expression and annotation member transformer over the given program.
mapProgram :: (Monad m)
            => (K3 Declaration -> m (K3 Declaration))
            -> (AnnMemDecl     -> m AnnMemDecl)
            -> (K3 Expression  -> m (K3 Expression))
            -> Maybe (K3 Type  -> m (K3 Type))
            -> K3 Declaration
            -> m (K3 Declaration)
mapProgram declF annMemF exprF typeFOpt prog = do
    (_, r) <- foldProgram (wrap declF) (wrap annMemF) (wrap exprF) (maybe Nothing (Just . wrap) $ typeFOpt) () prog
    return r
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

-- | Map a function over all program annotations, filtering null returns.
mapMaybeAnnotation :: (Applicative m, Monad m)
                   => (Annotation Declaration -> m (Maybe (Annotation Declaration)))
                   -> (Annotation Expression  -> m (Maybe (Annotation Expression)))
                   -> (Annotation Type        -> m (Maybe (Annotation Type)))
                   -> K3 Declaration
                   -> m (K3 Declaration)
mapMaybeAnnotation declF exprF typeF = mapProgram onDecl onMem onExpr (Just onType)
  where onDecl d = nodeF declF d
        onMem (Lifted    p n t eOpt anns) = memF (Lifted    p n) t eOpt anns
        onMem (Attribute p n t eOpt anns) = memF (Attribute p n) t eOpt anns
        onMem (MAnnotation p n anns)      = mapM declF anns >>= \nanns -> return $ MAnnotation p n $ catMaybes nanns

        memF ctor t eOpt anns = ctor <$> onType t
                                     <*> maybe (return Nothing) (\e -> onExpr e >>= return . Just) eOpt
                                     <*> (mapM declF anns >>= return . catMaybes)

        onExpr e = modifyTree (nodeF exprF) e
        onType t = modifyTree (nodeF typeF) t
        nodeF f (Node (tg :@: anns) ch) = mapM f anns >>= \nanns -> return $ Node (tg :@: catMaybes nanns) ch

mapAnnotation :: (Applicative m, Monad m)
              => (Annotation Declaration -> m (Annotation Declaration))
              -> (Annotation Expression  -> m (Annotation Expression))
              -> (Annotation Type        -> m (Annotation Type))
              -> K3 Declaration
              -> m (K3 Declaration)
mapAnnotation declF exprF typeF = mapMaybeAnnotation (wrap declF) (wrap exprF) (wrap typeF)
  where wrap f a = f a >>= return . Just

mapExprAnnotation :: (Monad m)
                  => (Annotation Expression  -> m (Annotation Expression))
                  -> (Annotation Type        -> m (Annotation Type))
                  -> K3 Expression
                  -> m (K3 Expression)
mapExprAnnotation exprF typeF = modifyTree (onNode chainType)
  where chainType (EType t) = modifyTree (onNode typeF) t >>= exprF . EType
        chainType a = exprF a
        onNode f (Node (tg :@: anns) ch) = mapM f anns >>= \nanns -> return $ Node (tg :@: nanns) ch


-- | Fold a function and accumulator over all return expressions in the program.
--
--   This function accepts a top-down aggregator, a bottom-up aggregator for return expressions
--   and a bottom-up aggregator for non-return expressions.
--   The top-down aggregator is applied to all expressions (e.g., to track lambda shadowing).
--
--   Return expressions are those expressions defining the return value of an arbitrary expression.
--   For example, the body of a let-in is the return expression, not the binding expression.
--   Each return expression is visited: consider an expression returning a tuple. Both the tuple
--   constructor and individual tuple fields are return expressions, and they will all be visited.
foldReturnExpression :: (Monad m)
                     => (a -> K3 Expression -> m (a, [a]))
                     -> (a -> b -> K3 Expression -> m (b, K3 Expression))
                     -> (a -> b -> K3 Expression -> m (b, K3 Expression))
                     -> a -> b -> K3 Expression
                     -> m (b, K3 Expression)
foldReturnExpression tdF onReturnF onNonReturnF tdAcc buAcc expr =
  biFoldRebuildTree (skipChildrenForReturns tdF) skipOrApply (False, tdAcc) buAcc expr

  where skipOrApply (skip, tdAcc') buAcc' ch e =
          (if skip then onNonReturnF else onReturnF) tdAcc' buAcc' (replaceCh e ch)

-- | Variant of the above with independent rather than serial bottom-up accumulations.
foldMapReturnExpression :: (Monad m)
                        => (a -> K3 Expression -> m (a, [a]))
                        -> (a -> [b] -> K3 Expression -> m (b, K3 Expression))
                        -> (a -> [b] -> K3 Expression -> m (b, K3 Expression))
                        -> a -> b -> K3 Expression
                        -> m (b, K3 Expression)
foldMapReturnExpression tdF onReturnF onNonReturnF tdAcc buAcc expr =
  biFoldMapRebuildTree (skipChildrenForReturns tdF) skipOrApply (False, tdAcc) buAcc expr

  where skipOrApply (skip, tdAcc') buAcc' ch e =
          (if skip then onNonReturnF else onReturnF) tdAcc' buAcc' (replaceCh e ch)

skipChildrenForReturns :: (Monad m)
                       => (a -> K3 Expression -> m (a, [a])) -> (Bool, a) -> K3 Expression
                       -> m ((Bool, a), [(Bool, a)])
skipChildrenForReturns tdF (skip, tdAcc) e =
  let chSkip = replicate (length $ children e) True
  in do { (nTd, chTd) <- tdF tdAcc e;
          if skip then return ((True, nTd), zip chSkip chTd)
          else skipChildren e >>= return . ((False, nTd),) . flip zip chTd }

  where
    skipChildren :: (Monad m) => K3 Expression -> m [Bool]
    skipChildren (tag -> EOperate OApp) = return [False, True]
    skipChildren (tag -> EOperate OSnd) = return [True, False]
    skipChildren (tag -> EOperate OSeq) = return [True, False]

    skipChildren (tag -> ELetIn  _)   = return [True, False]
    skipChildren (tag -> EBindAs _)   = return [True, False]
    skipChildren (tag -> ECaseOf _)   = return [True, False, False]
    skipChildren (tag -> EIfThenElse) = return [True, False, False]

    skipChildren e' = return $ replicate (length $ children e') False


-- | Map a function over all return expressions.
--   See definition of foldReturnExpression for more information.
mapReturnExpression :: (Monad m)
                    => (K3 Expression -> m (K3 Expression))
                    -> (K3 Expression -> m (K3 Expression))
                    -> K3 Expression -> m (K3 Expression)
mapReturnExpression onReturnF nonReturnF expr =
  foldReturnExpression tdF wrapRetF wrapNonRetF () () expr >>= return . snd
  where tdF tdAcc e = return (tdAcc, replicate (length $ children e) tdAcc)
        wrapRetF    _ a e = onReturnF  e >>= return . (a,)
        wrapNonRetF _ a e = nonReturnF e >>= return . (a,)


{- Expression utilities -}

defaultExpression :: K3 Type -> Either String (K3 Expression)
defaultExpression typ = mapTree mkExpr typ
  where mkExpr _ t@(tag -> TBool)   = withQualifier t $ EC.constant $ CBool False
        mkExpr _ t@(tag -> TByte)   = withQualifier t $ EC.constant $ CByte 0
        mkExpr _ t@(tag -> TInt)    = withQualifier t $ EC.constant $ CInt  0
        mkExpr _ t@(tag -> TReal)   = withQualifier t $ EC.constant $ CReal 0.0
        mkExpr _ t@(tag -> TNumber) = withQualifier t $ EC.constant $ CInt  0
        mkExpr _ t@(tag -> TString) = withQualifier t $ EC.constant $ CString ""

        mkExpr [e] t@(tag -> TOption) = let nm = case e @~ isEQualified of
                                                   Just EMutable -> NoneMut
                                                   _ -> NoneImmut
                                        in withQualifier t $ EC.constant $ CNone nm

        mkExpr [e] t@(tag -> TIndirection) = withQualifier t $ EC.indirect e
        mkExpr ch t@(tag -> TTuple)        = withQualifier t $ EC.tuple ch
        mkExpr ch t@(tag -> TRecord ids)   = withQualifier t $ EC.record $ zip ids ch

        mkExpr _ t@(tag -> TCollection) = withQualifier t $
          foldl (@+) (EC.empty $ head $ children t) $ extractTCAnns $ annotations t

        mkExpr _ (tag -> TFunction) = Left "Cannot create a default expression for a function"

        mkExpr _ t@(tag -> TAddress) = withQualifier t $
          EC.address (EC.constant $ CString "127.0.0.1") (EC.constant $ CInt 40000)

        mkExpr _ t = Left $ boxToString $ ["Cannot create a default expression for: "] %+ prettyLines t

        extractTCAnns as = concatMap extract as
          where extract (TAnnotation i) = [EAnnotation i]
                extract _ = []

        withQualifier t e = case t @~ isTQualified of
                             Just TMutable -> return $ EC.mut e
                             Just TImmutable -> return $ EC.immut e
                             _ -> return $ e

-- | Retrieves all free variables in an expression.
freeVariables :: K3 Expression -> [Identifier]
freeVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EVariable n) = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> EAssign i)   = return $ concat chAcc ++ [i]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Retrieves all variables introduced by a binder
bindingVariables :: Binder -> [Identifier]
bindingVariables (BIndirection i) = [i]
bindingVariables (BTuple is)      = is
bindingVariables (BRecord ivs)    = snd (unzip ivs)

-- | Retrieves all variables modified in an expression.
modifiedVariables :: K3 Expression -> [Identifier]
modifiedVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EAssign n)   = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ (chAcc !! 0) ++ (filter (`notElem` bindingVariables b) $ chAcc !! 1)
    extractVariable chAcc (tag -> ELetIn i)    = return $ (chAcc !! 0) ++ (filter (/= i) $ chAcc !! 1)
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Compares two expressions for identical AST structures while ignoring annotations
--   (such as UIDs, spans, etc.)
compareWithoutAnnotations :: K3 Expression -> K3 Expression -> Bool
compareWithoutAnnotations e1 e2 = stripAllExprAnnotations e1 == stripAllExprAnnotations e2


{- Annotation cleaning -}

-- | Strips all annotations from a declaration (including in any contained types and expressions)
stripDeclAnnotations :: (Annotation Declaration -> Bool)
                     -> (Annotation Expression -> Bool)
                     -> (Annotation Type -> Bool)
                     -> K3 Declaration -> K3 Declaration
stripDeclAnnotations dStripF eStripF tStripF d =
    runIdentity $ mapProgram stripDeclF stripMemF stripExprF (Just stripTypeF) d
  where
    stripDeclF (Node (tg :@: anns) ch) =  return $ Node (tg :@: (filter (not . dStripF) anns)) ch

    stripMemF (Lifted    p n t eOpt anns) = return $ Lifted      p n t eOpt $ stripDAnns anns
    stripMemF (Attribute p n t eOpt anns) = return $ Attribute   p n t eOpt $ stripDAnns anns
    stripMemF (MAnnotation p n anns)      = return $ MAnnotation p n $ stripDAnns anns

    stripExprF e = return $ stripExprAnnotations eStripF tStripF e
    stripTypeF t = return $ stripTypeAnnotations tStripF t

    stripDAnns anns = filter (not . dStripF) anns

-- | Strips all annotations from an expression given expression and type annotation filtering functions.
stripExprAnnotations :: (Annotation Expression -> Bool) -> (Annotation Type -> Bool)
                     -> K3 Expression -> K3 Expression
stripExprAnnotations eStripF tStripF e = runIdentity $ mapTree strip e
  where
    strip ch n@(tag -> EConstant (CEmpty t)) =
      let nct = stripTypeAnnotations tStripF t
      in return $ Node (EConstant (CEmpty nct) :@: stripEAnns n) ch

    strip ch n = return $ Node (tag n :@: stripEAnns n) ch

    stripEAnns n = filter (not . eStripF) $ annotations n

-- | Strips all annotations from a type given a type annotation filtering function.
stripTypeAnnotations :: (Annotation Type -> Bool) -> K3 Type -> K3 Type
stripTypeAnnotations tStripF t = runIdentity $ mapTree strip t
  where strip ch n = return $ Node (tag n :@: (filter (not . tStripF) $ annotations n)) ch

-- | Strips all annotations from a declaration deeply.
stripAllDeclAnnotations :: K3 Declaration -> K3 Declaration
stripAllDeclAnnotations = stripDeclAnnotations (const True) (const True) (const True)

-- | Strips all annotations from an expression deeply.
stripAllExprAnnotations :: K3 Expression -> K3 Expression
stripAllExprAnnotations = stripExprAnnotations (const True) (const True)

-- | Strips all annotations from a type deeply.
stripAllTypeAnnotations :: K3 Type -> K3 Type
stripAllTypeAnnotations = stripTypeAnnotations (const True)


{-| Tree repair utilities -}

-- | Ensures every node has a valid UID and Span.
--   This currently does not handle literals.

repairProgram :: String -> K3 Declaration -> K3 Declaration
repairProgram repairMsg p =
    let maxUid = (\case { UID i -> i }) $ maxProgramUID p
    in snd $ runIdentity $ foldProgram repairDecl repairMem repairExpr (Just repairType) (maxUid + 1) p

  where repairDecl uid n = validateD uid (children n) n
        repairExpr uid n = foldRebuildTree validateE uid n
        repairType uid n = foldRebuildTree validateT uid n

        repairMem uid (Lifted      pol n t eOpt anns) = rebuildMem uid anns $ Lifted      pol n t eOpt
        repairMem uid (Attribute   pol n t eOpt anns) = rebuildMem uid anns $ Attribute   pol n t eOpt
        repairMem uid (MAnnotation pol n anns)        = rebuildMem uid anns $ MAnnotation pol n

        validateD uid ch n = ensureUIDSpan uid DUID isDUID DSpan isDSpan ch n
        validateE uid ch n = ensureUIDSpan uid EUID isEUID ESpan isESpan ch n
        validateT uid ch n = ensureUIDSpan uid TUID isTUID TSpan isTSpan ch n

        rebuildMem uid anns ctor = return $ (\(nuid, nanns) -> (nuid, ctor nanns)) $ validateMem uid anns

        validateMem uid anns =
          let (nuid, extraAnns) =
                (\spa -> maybe (uid+1, [DUID $ UID uid]++spa) (const (uid, spa)) $ find isDUID anns)
                  $ maybe ([DSpan $ GeneratedSpan repairMsg]) (const []) $ find isDSpan anns
          in (nuid, anns ++ extraAnns)

        ensureUIDSpan uid uCtor uT sCtor sT ch (Node tg _) =
          return $ ensureUID uid uCtor uT $ snd $ ensureSpan sCtor sT $ Node tg ch

        ensureSpan    ctor t n = addAnn () () (ctor $ GeneratedSpan repairMsg) t n
        ensureUID uid ctor t n = addAnn (uid+1) uid (ctor $ UID uid) t n

        addAnn rUsed rNotUsed a t n = maybe (rUsed, n @+ a) (const (rNotUsed, n)) (n @~ t)

onProgramUID :: (UID -> UID -> UID) -> UID -> K3 Declaration -> (UID, K3 Declaration)
onProgramUID uidF z d = runIdentity $ foldProgram onDecl onMem onExpr (Just onType) z d
  where onDecl a n = return $ (dUID a n, n)
        onExpr a n = foldTree (\a' n' -> return $ eUID a' n') a n >>= return . (,n)
        onType a n = foldTree (\a' n' -> return $ tUID a' n') a n >>= return . (,n)

        onMem a (Lifted    p n t eOpt anns) = return $ (dMemUID a anns, Lifted      p n t eOpt $ anns)
        onMem a (Attribute p n t eOpt anns) = return $ (dMemUID a anns, Attribute   p n t eOpt $ anns)
        onMem a (MAnnotation p n anns)      = return $ (dMemUID a anns, MAnnotation p n        $ anns)

        dUID a n = maybe a (\case {DUID b -> uidF a b; _ -> a}) $ n @~ isDUID
        eUID a n = maybe a (\case {EUID b -> uidF a b; _ -> a}) $ n @~ isEUID
        tUID a n = maybe a (\case {TUID b -> uidF a b; _ -> a}) $ n @~ isTUID

        dMemUID a anns = maybe a (\case {DUID b -> uidF a b; _ -> a}) $ find isDUID anns

maxProgramUID :: K3 Declaration -> UID
maxProgramUID d = fst $ onProgramUID maxUID (UID (minBound :: Int)) d
  where maxUID (UID a) (UID b) = UID $ max a b

minProgramUID :: K3 Declaration -> UID
minProgramUID d = fst $ onProgramUID minUID (UID (maxBound :: Int)) d
  where minUID (UID a) (UID b) = UID $ min a b

{- Annotation removal -}
stripComments :: K3 Declaration -> K3 Declaration
stripComments = stripDeclAnnotations isDSyntax isESyntax (const False)

stripTypeAnns :: K3 Declaration -> K3 Declaration
stripTypeAnns = stripDeclAnnotations (const False) isAnyETypeAnn (const False)

resetEffectAnns :: K3 Declaration -> K3 Declaration
resetEffectAnns p = runIdentity $ mapMaybeAnnotation resetF idF idF p
  where idF = return . Just
        resetF (DSymbol s@(tag -> SymId _)) = return $
          case s @~ isSDeclared of
            Just (SDeclared s') -> Just $ DSymbol s'
            _ -> Nothing
        resetF a = return $ Just a

stripEffectAnns :: K3 Declaration -> K3 Declaration
stripEffectAnns p = resetEffectAnns $
  stripDeclAnnotations (const False) isAnyEEffectAnn (const False) p

-- | Effects-related metadata removal, including user-specified effect signatures.
stripAllEffectAnns :: K3 Declaration -> K3 Declaration
stripAllEffectAnns = stripDeclAnnotations isDSymbol isAnyEEffectAnn (const False)

-- | Single-pass composition of type and effect removal.
stripTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripTypeAndEffectAnns p = resetEffectAnns $
  stripDeclAnnotations (const False) isAnyETypeOrEffectAnn (const False) p

-- | Single-pass variant removing all effect annotations.
stripAllTypeAndEffectAnns :: K3 Declaration -> K3 Declaration
stripAllTypeAndEffectAnns = stripDeclAnnotations isDSymbol isAnyETypeOrEffectAnn (const False)

-- | Removes all properties from a program.
stripAllProperties :: K3 Declaration -> K3 Declaration
stripAllProperties = stripDeclAnnotations isDProperty isEProperty (const False)
