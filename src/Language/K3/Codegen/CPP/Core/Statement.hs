module Language.K3.Codegen.CPP.Core.Statement where

import {-# SOURCE #-} Language.K3.Codegen.CPP.Core.Expression

data StatementP a
    = Assignment a a
    | Block [StatementP a]
    | Ignored a
    | Return a
  deriving (Eq, Read, Show)

type Statement = StatementP Expression
