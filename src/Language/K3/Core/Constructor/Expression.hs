{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}

-- | Constructors for K3 Expressions. This module should almost certainly be imported qualified.
module Language.K3.Core.Constructor.Expression (
    immut,
    mut,
    constant,
    variable,
    some,
    indirect,
    tuple,
    unit,
    record,
    empty,
    lambda,
    lambdaMany,
    unop,
    binop,
    applyMany,
    block,
    send,
    project,
    letIn,
    assign,
    caseOf,
    bindAs,
    ifThenElse,
    address,
    self,

    Binder(..),
    Constant(..),
    Operator(..)
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

-- | Add mutability qualifier
mutable :: Bool -> K3 Expression -> K3 Expression
mutable b n =
  n @+ (if b then EMutable else EImmutable)

-- | Shortcuts to the above
mut :: K3 Expression -> K3 Expression
mut = mutable True

immut :: K3 Expression -> K3 Expression
immut = mutable False

-- | Create a constant expression.
constant :: Constant -> K3 Expression
constant c = Node (EConstant c :@: []) []

-- | Create a variable expression.
variable :: Identifier -> K3 Expression
variable v = Node (EVariable v :@: []) []

-- | Create an option value from a constituent expression.
some :: K3 Expression -> K3 Expression
some e = Node (ESome :@: []) [e]

-- | Create an indirection to a the result of an expression.
indirect :: K3 Expression -> K3 Expression
indirect e = Node (EIndirect :@: []) [e]

-- | Create a tuple from a list of members.
tuple :: [K3 Expression] -> K3 Expression
tuple = Node (ETuple :@: [])

-- | Unit value.
unit :: K3 Expression
unit = Node (ETuple :@: []) []

-- | Create a record from a list of field names and initializers.
record :: [(Identifier, K3 Expression)] -> K3 Expression
record vs = Node (ERecord ids :@: []) es where (ids, es) = unzip vs

-- | Create an empty collection.
empty :: K3 Type -> K3 Expression
empty t = Node (EConstant (CEmpty t) :@: []) []

-- | Create an anonymous function.
lambda :: Identifier -> K3 Expression -> K3 Expression
lambda x b = Node (ELambda x :@: []) [b]

-- | Create a multi-argument anonymous function.
lambdaMany :: [Identifier] -> K3 Expression -> K3 Expression
lambdaMany args b = foldr lambda b args

-- | Create an application of a unary operator.
unop :: Operator -> K3 Expression -> K3 Expression
unop op a = Node (EOperate op :@: []) [a]

-- | Create an application of a binary operator.
binop :: Operator -> K3 Expression -> K3 Expression -> K3 Expression
binop op a b = Node (EOperate op :@: []) [a, b]

-- | Create a multi-argument function application.
applyMany :: K3 Expression -> [K3 Expression] -> K3 Expression
applyMany f args = foldl (\curried_f arg -> binop OApp curried_f arg) f args

-- | Create a chain of sequential computation.
block :: [K3 Expression] -> K3 Expression
block []  = unit
block [x] = x
block exprs = foldl (\sq e -> binop OSeq sq e) (head exprs) (tail exprs)

send :: K3 Expression -> K3 Expression -> K3 Expression -> K3 Expression
send target addr arg = binop OSnd (tuple [qualifyE target, qualifyE addr]) arg
  where qualifyE e = if null $ filter isEQualified $ annotations e then e @+ EImmutable else e

-- | Project a field from a record.
project :: Identifier -> K3 Expression -> K3 Expression
project i r = Node (EProject i :@: []) [r]

-- | Create a let binding.
letIn :: Identifier -> K3 Expression -> K3 Expression -> K3 Expression
letIn i e b = Node (ELetIn i :@: []) [e, b]

-- | Create an assignment.
assign :: Identifier -> K3 Expression -> K3 Expression
assign i v = Node (EAssign i :@: []) [v]

-- | Create a case destruction.
caseOf :: K3 Expression -> Identifier -> K3 Expression -> K3 Expression -> K3 Expression
caseOf e x s n = Node (ECaseOf x :@: []) [e, s, n]

-- | Create a binding destruction.
bindAs :: K3 Expression -> Binder -> K3 Expression -> K3 Expression
bindAs e x b = Node (EBindAs x :@: []) [e, b]

-- | Create an if/then/else conditional expression.
ifThenElse :: K3 Expression -> K3 Expression -> K3 Expression -> K3 Expression
ifThenElse p t e = Node (EIfThenElse :@: []) [p, t, e]

-- | Create an address expression
address :: K3 Expression -> K3 Expression -> K3 Expression
address ip port = Node (EAddress :@: []) [ip, port]

-- | A self expression.
self :: K3 Expression
self = Node (ESelf :@: []) []

