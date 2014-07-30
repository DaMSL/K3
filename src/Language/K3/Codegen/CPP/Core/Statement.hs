module Language.K3.Codegen.CPP.Core.Statement where

import Language.K3.Codegen.CPP.Core.Expression

data Statement
    = Assignment Expression Expression
    | Block [Statement]
    | Ignored Expression
    | Return Expression
  deriving (Eq, Read, Show)
