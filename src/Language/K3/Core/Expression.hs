{-# LANGUAGE TypeFamilies #-}

-- | Expressions in K3.
module Language.K3.Core.Expression where

import Language.K3.Core.Annotation
import Language.K3.Core.Type

-- | Expression tags. Every expression can be qualified with a mutability annotation.
data Expression
    = EConstant Constant
    | EVariable Identifier
    | EOption
    | EIndirection
    | ETuple
    | ERecord [Identifier]
    | EEmpty
    | ELambda Identifier
    | EOperator Operator
    | EProjection Identifier
    | ELet Identifier
    | EAssign Identifier
    | ECase Identifier
    | EBind Binder
    | EIfThenElse
  deriving (Eq, Read, Show)

-- | Constant expression values.
data Constant
    = CBool Bool
    | CInt Int
    | CByte Int
    | CFloat Float
    | CString String
  deriving (Eq, Read, Show)

-- | Binary Operators.
data Operator
    = OAdd
    | OSubtract
    | OMultiply
    | ODivide
    | OEqu
    | OLth
    | OLeq
    | OGth
    | OGeq
    | OSequence
    | OApply
    | OSend
  deriving (Eq, Read, Show)

-- | Binding Forms.
data Binder
    = BIndirection Identifier
    | BTuple [Identifier]
    | BRecord [(Identifier, Identifier)]
  deriving (Eq, Read, Show)

-- | Annotations on expressions are mutability qualifiers.
instance Annotatable Expression where
    data Annotation Expression
        = EMutable
        | EImmutable
      deriving (Eq, Read, Show)
