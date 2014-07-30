module Language.K3.Codegen.CPP.Core.Expression where

import Language.K3.Core.Common

import Language.K3.Codegen.CPP.Core.Type

data Expression
    = Binary Identifier Expression Expression
    | Call Expression Expression
    | Dereference Expression
    | Lambda [(Identifier, Expression)] [(Type, Identifier)] Type [Statement]
    | Literal LiteralExpression
    | Project Expression Expression
    | Qualified Identifier Identifier
    | Specialized [Type] Expression
    | Unary Identifier Expression
    | Variable Identifier
  deriving (Eq, Read, Show)

data LiteralExpression
    = LBool Bool
    | LInt Int
    | LDouble Double
    | LString String
  deriving (Eq, Read, Show)
