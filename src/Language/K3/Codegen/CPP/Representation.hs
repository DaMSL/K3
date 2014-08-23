{-# LANGUAGE OverloadedStrings #-}

module Language.K3.Codegen.CPP.Representation where

import Data.String

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

class Stringifiable a where
    stringify :: a -> Doc

-- Pretty-Printing Utility Functions

commaSep :: [Doc] -> Doc
commaSep = fillSep . punctuate comma

data Name
    = Name Identifier
    | Qualified Identifier Name
    | Specialized [Type] Name
  deriving (Eq, Read, Show)

instance Stringifiable Name where
    stringify (Name i) = text i
    stringify (Qualified i n) = text i <> "::" <> stringify n
    stringify (Specialized ts n) = stringify n <> angles (commaSep $ map stringify ts)

data Primitive
    = PBool
    | PInt
    | PDouble
    | PString
  deriving (Eq, Read, Show)

instance Stringifiable Primitive where
    stringify PBool = "bool"
    stringify PInt = "int"
    stringify PDouble = "double"
    stringify PString = "string"

data Type
    = Named Name
    | Inferred
    | Parameter Identifier
    | Primitive Primitive
  deriving (Eq, Read, Show)

instance Stringifiable Type where
    stringify (Named n) = stringify n
    stringify Inferred = "auto"
    stringify (Parameter i) = fromString i
    stringify (Primitive p) = stringify p

data Literal
    = LBool Bool
    | LInt Int
    | LDouble Double
    | LString String
  deriving (Eq, Read, Show)

instance Stringifiable Literal where
    stringify (LBool b) = if b then "true" else "false"
    stringify (LInt i) = int i
    stringify (LDouble d) = double d
    stringify (LString s) = dquotes $ string s

data Expression
    = Binary Identifier Expression Expression
    | Call Expression [Expression]
    | Dereference Expression
    | Initialization Type Expression
    | Lambda [(Identifier, Expression)] [(Identifier, Type)] (Maybe Type) [Statement]
    | Literal Literal
    | Project Expression Identifier
    | Unary Identifier Expression
    | Variable Name
  deriving (Eq, Read, Show)

data Statement
    = Assignment (Maybe Type) Expression Expression
    | Block [Statement]
    | Ignored Expression
    | Return Expression
  deriving (Eq, Read, Show)

data Declaration
    = Class Name [Type] [Declaration] [Declaration] [Declaration]
    | Function Type Name [(Identifier, Type)] [Statement]
    | Global Statement
    | Templated [(Identifier, Maybe Type)] Declaration
  deriving (Eq, Read, Show)

