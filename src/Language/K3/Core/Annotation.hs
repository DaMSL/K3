{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

-- | The K3 Annotation System.
module Language.K3.Core.Annotation (
    -- * Basic Infrastructure
    Annotatable(..),
    Annotated(..),

    -- * K3 Instantiations
    (:@:)(..),
    K3
) where

import Data.List (delete)
import Data.Tree

-- | Values of a given type are 'Annotatable' if they can specify exactly what types of data they
-- can be annotated with.
class Annotatable a where
    data Annotation a :: *

-- | A value is 'Annotated' if it contains an 'Annotatable' element inside it, which it can specify
-- as its 'Kernel', and can explain how to add/remove annotations relating to that kernel.
class Annotated t where
    -- | The sub-component of an annotated value that is actually annotatable.
    type Kernel t :: *

    -- | The annotation addition operator. Adds the given annotation to an annotated value.
    (@+) :: Annotatable (Kernel t) => t -> Annotation (Kernel t) -> t

    -- | The annotation subtraction operator. Removes the given annotation from an annotated value.
    (@-) :: Annotatable (Kernel t) => t -> Annotation (Kernel t) -> t

infixl 3 @+
infixl 3 @-

-- | A convenience form for attachment, structurally equivalent to tupling.
data a :@: b = a :@: b deriving (Eq, Read, Show)

-- | One standard way of attaching annotations to an annotatable is to pair it with a list of
-- annotations. We can then define how to add and remove annotations to that list.
instance (Annotatable a, Eq (Annotation a)) => Annotated (a :@: [Annotation a]) where
    -- | Proxy the annotatability to the first element of the tuple.
    type Kernel (a :@: [Annotation a]) = a

    -- | We prepend the new annotation to the list; we don't care about duplicates right now.
    (v :@: as) @+ a = v :@: (a:as)

    -- | Deleting an annotation from a list requires equality on annotations, hence the constraint.
    (v :@: as) @- a = v :@: (delete a as)

-- | If we wish to impose the restriction that at most one annotation is attached to a particular
-- annotatable, we can use a @Maybe@ type instead.
instance (Annotatable a, Eq (Annotation a)) => Annotated (a :@: Maybe (Annotation a)) where
    -- | Proxy the annotatability to the first element of the tuple.
    type Kernel (a :@: Maybe (Annotation a)) = a

    -- | Adding an annotation simply puts it in the Maybe cell, regardless of what was there before.
    (v :@: _) @+ a = v :@: (Just a)

    -- | Removing an annotation only has an effect if the annotation to be removed was the one there
    -- to begin with.
    (v :@: Nothing) @- a = v :@: Nothing
    (v :@: Just a') @- a
        | a == a' = v :@: Nothing
        | otherwise = v :@: (Just a')

-- | If we have a tree with annotatated elements at each node, we can say that the tree itself is
-- annotated, where it acts as a proxy to the annotated element at its root.
instance Annotated a => Annotated (Tree a) where
    type Kernel (Tree a) = Kernel a
    (Node v cs) @+ a = Node (v @+ a) cs
    (Node v cs) @- a = Node (v @- a) cs

-- | A K3 source tree contains a tag, along with a list of annotations defined for trees with that
-- tag.
type K3 a = Tree (a :@: [Annotation a])
