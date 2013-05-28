{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}

-- | Top-Level Declarations in K3.
module Language.K3.Core.Declaration (
    Declaration(..),
    Annotation(..)
) where

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

-- | Top-Level Declarations
data Declaration
    = DGlobal Identifier (K3 Type) (Maybe (K3 Expression))
    | DBind Identifier Identifier
    | DSelector Identifier
    | DRole
  deriving (Eq, Read, Show)

-- | Annotations on Declarations.
instance Annotatable Declaration where
    data Annotation Declaration
