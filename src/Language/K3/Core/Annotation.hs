{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

-- | The K3 Annotation System.
module Language.K3.Core.Annotation (
    -- * Basic Infrastructure
    Annotation,
    AContainer(..),
    AConstruct(..),

    -- * K3 Instantiations
    (:@:)(..),
    K3,

    children,
    details,

    mapTree,
    modifyTree,
    foldMapTree,
    foldTree,
    
    biFoldTree,
    biFoldMapTree,

    foldRebuildTree,
    foldMapRebuildTree,
    mapIn1RebuildTree,
    foldIn1RebuildTree,
    foldMapIn1RebuildTree
) where

import Control.Monad
import Data.List (delete, find)
import Data.Tree

-- | Every tag type defines the set of annotations that can be associated with that tag.
data family Annotation t :: *

-- | An annotation container is a generic container which supports a standard set of operations that
-- one might want to perform on a set of annotations. There is, however, nothing annotation-specific
-- about the container's interface.
class AContainer c where
    -- | The sub-component of an annotated value that is actually annotatable.
    type IElement c :: *

    -- | The annotation addition operator. Adds the given annotation to an annotated value.
    (@+) :: Eq (IElement c) => c -> IElement c -> c

    -- | The annotation subtraction operator. Removes the given annotation from an annotated value.
    (@-) :: Eq (IElement c) => c -> IElement c -> c

    -- | The annotation matching operator. Returns the first annotation satisfying the given
    -- predicate.
    (@~) :: c -> (IElement c -> Bool) -> Maybe (IElement c)

infixl 3 @+
infixl 3 @-

-- | An annotation construct is a data structure where each member contains a tag, and a container
-- of the corresponding tag type's annotations. 
class AConstruct c where
    -- | The type of the tag.
    type ITag c :: *

    -- | The type of the container.
    type IContainer c :: *

    -- | Retrieve the tag from the construct.
    tag :: c -> ITag c

    -- | Retrieve the annotation container from the construct.
    annotations :: c -> IContainer c

    -- | Replace the annotation container within the construct.
    (@<-) :: c -> IContainer c -> c

-- | A maybe-type is a container of a single annotation.
instance AContainer (Maybe a) where
    type IElement (Maybe a) = a

    -- | Adding an annotation replaces the existing annotation, if there was one.
    _ @+ v' = Just v'

    -- | Removing an annotation only occurs if the exact annotation was already present.
    Just v @- v'
        | v == v' = Nothing
        | otherwise = Just v
    Nothing @- _ = Nothing

    -- | Matching an annotation only occurs if there was an annotation to begin with.
    Just v @~ p
        | p v = Just v
        | otherwise = Nothing
    Nothing @~ _ = Nothing

-- | A list is a container of annotations, quite trivially.
instance AContainer [a] where
    type IElement [a] = a

    vs @+ v = vs++[v]
    vs @- v = delete v vs
    vs @~ p = find p vs

-- | A tree can act as a proxy to the container at its root.
instance AContainer a => AContainer (Tree a) where
    type IElement (Tree a) = IElement a

    Node a cs @+ v = Node (a @+ v) cs
    Node a cs @- v = Node (a @- v) cs
    Node a _ @~ f = a @~ f

-- | A convenience form for attachment, structurally equivalent to tupling.
data a :@: b = a :@: b deriving (Eq, Ord, Read, Show)

-- | A pair can act as a proxy to the container it contains as its second element.
instance AContainer a => AContainer (b :@: a) where
    type IElement (b :@: a) = IElement a

    (b :@: a) @+ v = b :@: (a @+ v)
    (b :@: a) @- v = b :@: (a @- v)
    (_ :@: a) @~ f = a @~ f

-- | A pair can also act as a construct, where the first element is the tag.
instance AConstruct (b :@: a) where
    type ITag (b :@: a) = b
    type IContainer (b :@: a) = a

    tag (b :@: _) = b

    annotations (_ :@: a) = a

    (b :@: _) @<- a' = b :@: a'

-- | A tree can act as a proxy to the construct at its root.
instance AConstruct a => AConstruct (Tree a) where
    type ITag (Tree a) = ITag a
    type IContainer (Tree a) = IContainer a

    tag (Node a _) = tag a

    annotations (Node a _) = annotations a

    (Node a cs) @<- a' = Node (a @<- a') cs

-- | A K3 source tree contains a tag, along with a list of annotations defined for trees with that
-- tag.
type K3 a = Tree (a :@: [Annotation a])

{- Tree utilities -}

instance (Ord a, Eq (Annotation a), Ord (Annotation a)) => Ord (K3 a) where
  compare a b = compare (flatten a) (flatten b)


-- | Subtree extraction
children :: Tree a -> Forest a
children = subForest

-- | Get all elements: tag, children, annotations
details :: K3 a -> (a, [K3 a], [Annotation a])
details (Node (tg :@: anns) ch) = (tg, ch, anns)


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
--   child of every rree node. This is useful for stateful monads that modify
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
-- preCh1F:  The pre-child function takes the top-down accumulator, the first child, and the node
--           returns a new accumulator
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
    if (length chAccs) /= (length $ tail ch)
      then fail "Invalid foldMapIn1RebuildTree accumulation"
      else do
        nRestCh <- mapM (uncurry rcr) $ zip chAccs $ tail ch
        allChF nAcc (nc1:nRestCh) n

  where rcr = foldMapIn1RebuildTree preCh1F postCh1F allChF
