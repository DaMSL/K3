{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

-- | Haskell code generation annotations.
--   These are attached to expressions.
module Language.K3.Core.Annotation.Codegen where

import Control.DeepSeq
import Data.Typeable
import GHC.Generics (Generic)

data EmbeddingAnnotation
    = IOLoad
    | IOStore
    | IOAction
    | IOStructure PStructure
    deriving (Eq, Ord, Read, Show, Typeable, Generic)

data PQualifier
    = PImmutable
    | PMutable
    deriving (Eq, Ord, Read, Show, Typeable, Generic)

data PStructure
    = PLeaf     PQualifier
    | PSingle   PQualifier  PStructure
    | PComplex  PQualifier [PStructure]
    deriving (Eq, Ord, Read, Show, Typeable, Generic)

instance NFData EmbeddingAnnotation
instance NFData PQualifier
instance NFData PStructure
