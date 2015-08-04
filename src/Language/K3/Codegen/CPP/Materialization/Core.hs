{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE StandaloneDeriving #-}

module Language.K3.Codegen.CPP.Materialization.Core where

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Codegen.CPP.Materialization.Hints (Method(..))

data MExpr
  = MVar Juncture
  | MAtom Method
  | MIfThenElse (K3 MPred)
 deriving (Eq, Read, Show)

data instance Annotation MExpr

deriving instance Eq (Annotation MExpr)
deriving instance Read (Annotation MExpr)
deriving instance Show (Annotation MExpr)

data MPred
  = MNot
  | MAnd
  | MOr
  | MOneOf (K3 MExpr) [Method]
  | MBool Bool
 deriving (Eq, Read, Show)

data instance Annotation MPred
deriving instance Eq (Annotation MPred)
deriving instance Read (Annotation MPred)
deriving instance Show (Annotation MPred)

newtype Juncture = Juncture (UID, Identifier) deriving (Eq, Ord, Read, Show)
