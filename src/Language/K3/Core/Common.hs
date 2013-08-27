-- | Primitive Definitions for Compiler-Wide Terms.
module Language.K3.Core.Common (
    Identifier,
    Span(..),
    UID(..),

    addAssoc,
    removeAssoc,
    replaceAssoc,
    modifyAssoc
) where

import Language.K3.Pretty

-- | Identifiers are used everywhere.
type Identifier = String

-- | Spans are locations in the program source.
data Span = Span String Int Int Int Int deriving (Eq, Ord, Read, Show)

-- | Unique identifiers for AST nodes.
data UID = UID Int deriving (Eq, Ord, Read, Show)

instance Pretty UID where
  prettyLines (UID n) = [show n]

-- | Associative lists
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

modifyAssoc :: Eq a => [(a,b)] -> a -> (Maybe b -> (c,b)) -> (c, [(a,b)])
modifyAssoc l k f = (r, replaceAssoc l k nv)
  where (r, nv) = f $ lookup k l
