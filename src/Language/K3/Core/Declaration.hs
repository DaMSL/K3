{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Top-Level Declarations in K3.
module Language.K3.Core.Declaration (
    Declaration(..),
    Annotation(..),

    -- * User defined Annotations
    Polarity(..),
    AnnMemDecl(..)
) where

import Data.Functor
import Data.Maybe
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Pretty

-- | Top-Level Declarations
data Declaration
    = DGlobal     Identifier (K3 Type) (Maybe (K3 Expression))
    | DTrigger    Identifier (K3 Type) (K3 Expression)
        -- ^Trigger declaration.  Type is argument type of trigger.  Expression
        --  must be a function taking that argument type and returning unit.
    | DRole       Identifier
    | DAnnotation Identifier [AnnMemDecl]
  deriving (Eq, Read, Show)

-- | Annotation declaration members
data AnnMemDecl
    = Lifted      Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  Span
    
    | Attribute   Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  Span
    
    | MAnnotation Polarity Identifier Span
  deriving (Eq, Read, Show)  

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Read, Show)

-- | Annotations on Declarations.
data instance Annotation Declaration
  = DSpan Span
  | DUID UID
  deriving (Eq, Read, Show)

instance Pretty (K3 Declaration) where
    prettyLines (Node (DGlobal i t me :@: as) ds) =
        ["DGlobal " ++ i ++ drawAnnotations as, "|"]
        ++ case (me, ds) of
            (Nothing, []) -> terminalShift t
            (Just e, []) ->  nonTerminalShift t ++ ["|"] ++ terminalShift e
            (Nothing, xs) -> drawSubTrees ds
            (Just e, xs) -> nonTerminalShift t ++ ["|"] ++ drawSubTrees ds
    prettyLines (Node (DRole i :@: as) ds) =
        ["DRole " ++ i ++ " :@: " ++ show as, "|"]
        ++ drawSubTrees ds
    prettyLines x = lines $ show x
