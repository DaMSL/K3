{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Literals (i.e., constant values) in K3.
module Language.K3.Core.Literal where

import Data.Typeable
import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Utils.Pretty

-- | Literal variants include all builtin data types.
data Literal
    = LBool       Bool
    | LInt        Int
    | LByte       Word8
    | LReal       Double
    | LString     String
    | LNone       NoneMutability
    | LSome
    | LIndirect
    | LTuple
    | LRecord     [Identifier]
    | LEmpty      (K3 Type)
    | LCollection (K3 Type)
    | LAddress
  deriving (Eq, Read, Show, Typeable)

-- | Annotations on literals.
data instance Annotation Literal
    = LSpan Span
    | LUID UID
    | LMutable
    | LImmutable
    | LAnnotation Identifier
    | LSyntax SyntaxAnnotation
    | LType (K3 Type)
  deriving (Eq, Read, Show)

instance Pretty (K3 Literal) where
    prettyLines (Node (LTuple :@: as) []) = ["LUnit" ++ drawAnnotations as]
    
    prettyLines (Node (LEmpty t :@: as) []) =
        ["LEmpty" ++ drawAnnotations as, "|"] ++ prettyLines t

    prettyLines (Node (LCollection t :@: as) []) =
        ["LCollection" ++ drawAnnotations as, "|"] ++ prettyLines t
    
    prettyLines (Node (t :@: as) es) = (show t ++ drawAnnotations as) : drawSubTrees es

{- Literal annotation predicates -}

isLSpan :: Annotation Literal -> Bool
isLSpan (LSpan _) = True
isLSpan _         = False

isLQualified :: Annotation Literal -> Bool
isLQualified LImmutable = True
isLQualified LMutable   = True
isLQualified _          = False

isLUID :: Annotation Literal -> Bool
isLUID (LUID _) = True
isLUID _        = False

isLAnnotation :: Annotation Literal -> Bool
isLAnnotation (LAnnotation _) = True
isLAnnotation _               = False

namedLAnnotations :: [Annotation Literal] -> [Identifier]
namedLAnnotations anns = map extractId $ filter isLAnnotation anns
  where extractId (LAnnotation n) = n
        extractId _ = error "Invalid named annotation"

