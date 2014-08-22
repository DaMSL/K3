module Language.K3.Codegen.CPP.Core.Declaration where

import Language.K3.Core.Common

import Language.K3.Codegen.CPP.Core.Expression ()
import Language.K3.Codegen.CPP.Core.Statement
import Language.K3.Codegen.CPP.Core.Type

data Declaration
    = Class Identifier [Type] [Declaration] [Declaration] [Declaration]
    | Function Type Identifier [(Type, Identifier)] [Statement]
    | Global Statement
    | Specialized [Type] Declaration
    | Templated Declaration
  deriving (Eq, Read, Show)
