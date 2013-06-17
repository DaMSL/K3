{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}

-- | Top-Level Declarations in K3.
module Language.K3.Core.Declaration (
    Declaration(..),
    Annotation(..),

    -- * User defined Annotations
    Polarity(..),
    AnnMemDecl(..)
) where

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

-- | Top-Level Declarations
data Declaration
    = DGlobal     Identifier (K3 Type) (Maybe (K3 Expression))
    | DRole       Identifier
    | DAnnotation Identifier [AnnMemDecl]
  deriving (Eq, Read, Show)

-- | Annotation declaration members
data AnnMemDecl
    = Method      Identifier Polarity                [(Identifier, K3 Type)]
                  (K3 Type)  (Maybe (K3 Expression))
    
    | Lifted      Identifier Polarity
                  (K3 Type) (Maybe (K3 Expression))
    
    | Attribute   Identifier Polarity
                  (K3 Type) (Maybe (K3 Expression))
    
    | MAnnotation Identifier Polarity
  deriving (Eq, Read, Show)  

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Read, Show)

-- | Annotations on Declarations.
data instance Annotation Declaration
  = DSpan Span
  deriving (Eq, Read, Show)

-- | TODO: pretty printing of declaration tree
