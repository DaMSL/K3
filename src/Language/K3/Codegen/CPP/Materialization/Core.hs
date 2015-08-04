module Language.K3.Codegen.CPP.Materialization.Core where

import Language.K3.Core.Common

import Language.K3.Codegen.CPP.Materialization.Hints (Method(..))

data MExpr
  = MVar (Identifier, UID)
  | MAtom Method
  | MIfThenElse MPred
 deriving (Eq, Read, Show)

data MPred
  = MNot
  | MAnd
  | MOr
  | MOneOf MExpr [Method]
  | MBool Bool
 deriving (Eq, Read, Show)
