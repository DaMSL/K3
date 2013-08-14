-- | Primitive Definitions for Compiler-Wide Terms.
module Language.K3.Core.Common (
    Identifier,
    Span(..),
    UID(..),
) where

-- | Identifiers are used everywhere.
type Identifier = String

-- | Spans are locations in the program source.
data Span = Span String Int Int Int Int deriving (Eq, Ord, Read, Show)

-- | Unique identifiers for AST nodes.
data UID = UID Int deriving (Eq, Ord, Read, Show)