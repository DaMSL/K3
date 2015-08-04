module Language.K3.Codegen.CPP.Materialization.Constructors where

import Data.Tree

import Language.K3.Core.Annotation

import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Hints

-- * MExpr Constructors

mVar :: Juncture -> K3 MExpr
mVar j = Node (MVar j :@: []) []

mAtom :: Method -> K3 (MExpr)
mAtom m = Node (MAtom m :@: []) []

mITE :: K3 MPred -> K3 MExpr -> K3 MExpr -> K3 MExpr
mITE p t e = Node (MIfThenElse p :@: []) [t, e]

-- * MPred Constructors

mBool :: Bool -> K3 MPred
mBool b = Node (MBool b :@: []) []

mOneOf :: K3 MExpr -> [Method] -> K3 MPred
mOneOf m ms = Node (MOneOf m ms :@: []) []

(-&&-) :: K3 MPred -> K3 MPred -> K3 MPred
(-&&-) a b = Node (MAnd :@: []) [a, b]
