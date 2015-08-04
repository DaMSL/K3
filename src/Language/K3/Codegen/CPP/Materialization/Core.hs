{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE StandaloneDeriving #-}

module Language.K3.Codegen.CPP.Materialization.Core where

import Text.Printf

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Utils.Pretty

import Language.K3.Codegen.CPP.Materialization.Hints (Method(..))

data MExpr
  = MVar Juncture
  | MAtom Method
  | MIfThenElse (K3 MPred)
 deriving (Eq, Read, Show)

simpleShowE :: K3 MExpr -> String
simpleShowE m = case tag m of
  MVar (Juncture (u, i)) -> printf "%d/%s" (gUID u) i
  MAtom t -> show t
  MIfThenElse p ->
    let [t, e] = children m
    in printf "if %s then %s else %s" (simpleShowP p) (simpleShowE t) (simpleShowE e)

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

simpleShowP :: K3 MPred -> String
simpleShowP p = case (tag p, children p) of
  (MNot, [c]) -> printf "-!- %s" (simpleShowP c)
  (MAnd, [a, b]) -> printf "%s -&&- %s" (simpleShowP a) (simpleShowP b)
  (MOr, [a, b]) -> printf "%s -||- %s" (simpleShowP a) (simpleShowP b)
  (MOneOf m ms, _) -> printf "%s ∈ %s" (simpleShowE m) (show ms)
  (MBool b, _) -> show b

data instance Annotation MPred
deriving instance Eq (Annotation MPred)
deriving instance Read (Annotation MPred)
deriving instance Show (Annotation MPred)

newtype Juncture = Juncture { jLoc :: (UID, Identifier) } deriving (Eq, Ord, Read, Show)
