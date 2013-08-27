-- | Haskell code generation annotations.
--   These are attached to expressions.
module Language.K3.Core.Annotation.Codegen where

import Language.K3.Core.Common

data EmbeddingAnnotation
    = IOLoad
    | IOStore
    | IOAction 
    | IOStructure PStructure
    deriving (Eq, Read, Show)

data PQualifier 
    = PImmutable
    | PMutable
    deriving (Eq, Read, Show)

data PStructure
    = PLeaf     PQualifier
    | PSingle   PQualifier  PStructure 
    | PComplex  PQualifier [PStructure]
    deriving (Eq, Read, Show)
