{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ViewPatterns #-}

-- | Expressions in K3.
module Language.K3.Core.Expression where

import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Pretty

-- | Expression tags. Every expression can be qualified with a mutability annotation.
data Expression
    = EConstant   Constant
    | EVariable   Identifier
    | ESome
    | EIndirect
    | ETuple
    | ERecord     [Identifier]
    | ELambda     Identifier
    | EOperate    Operator
    | EProject    Identifier
    | ELetIn      Identifier
    | EAssign     Identifier
    | ECaseOf     Identifier
    | EBindAs     Binder
    | EIfThenElse
    | EAddress
    | ESelf
  deriving (Eq, Read, Show)

-- | Constant expression values.
data Constant
    = CBool    Bool
    | CInt     Int
    | CByte    Word8
    | CReal    Double
    | CString  String
    | CNone    NoneMutability
    | CEmpty   (K3 Type)
  deriving (Eq, Read, Show)
  
-- |Mutability modes for @CNone@.  These are kept distinct from the expression
--  annotations because e.g. @mut (mut None mut, mut None mut)@ must have a
--  place to put each @mut@ without overlapping.
data NoneMutability
    = NoneMut
    | NoneImmut
  deriving (Eq, Read, Show)

-- | Operators (unary and binary).
data Operator
    = OAdd
    | OSub
    | OMul
    | ODiv
    | ONeg
    | OEqu
    | ONeq
    | OLth
    | OLeq
    | OGth
    | OGeq
    | OAnd
    | OOr
    | ONot
    | OSeq
    | OApp
    | OSnd
  deriving (Eq, Read, Show)

-- | Binding Forms.
data Binder
    = BIndirection Identifier
    | BTuple [Identifier]
    | BRecord [(Identifier, Identifier)]
  deriving (Eq, Read, Show)

-- | Annotations on expressions are mutability qualifiers.
data instance Annotation Expression
    = EMutable
    | EImmutable
    | ELexicalName Identifier
    | EType (K3 Type)
    | ESpan Span
    | EUID UID
    | EAnnotation Identifier
  deriving (Eq, Read, Show)

instance Pretty (K3 Expression) where
    prettyLines (Node (ETuple :@: as) []) = ["EUnit" ++ drawAnnotations as]
    prettyLines (Node (t :@: as) es) = (show t ++ drawAnnotations as) : drawSubTrees es

{- Expression annotation predicates -}

isESpan :: Annotation Expression -> Bool
isESpan (ESpan _) = True
isESpan _         = False

isEQualified :: Annotation Expression -> Bool
isEQualified EImmutable = True
isEQualified EMutable   = True
isEQualified _          = False

isEUID :: Annotation Expression -> Bool
isEUID (EUID _) = True
isEUID _        = False

isEAnnotation :: Annotation Expression -> Bool
isEAnnotation (EAnnotation _) = True
isEAnnotation _               = False

{- Expression utilities -}

-- | Retrieves all free variables in an expression. 
freeVariables :: K3 Expression -> [Identifier]
freeVariables e = foldMapTree extractVariable [] e
  where 
    extractVariable chAcc (tag -> EVariable n) = concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = filter (/= n) $ concat chAcc
    extractVariable chAcc _ = concat chAcc
