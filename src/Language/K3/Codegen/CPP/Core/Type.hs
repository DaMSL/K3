module Language.K3.Codegen.CPP.Core.Type where

import Language.K3.Core.Common

data Type
    = Named Identifier
    | Inferred
    | Parameter Identifier
    | Primitive PrimitiveType
    | Templated Type [Type]
    | Qualified Identifier Type
  deriving (Eq, Read, Show)

data PrimitiveType
    = PBool
    | PInt
    | PDouble
    | PString
  deriving (Eq, Read, Show)
