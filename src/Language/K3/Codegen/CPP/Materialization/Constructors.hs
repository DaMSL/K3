module Language.K3.Codegen.CPP.Materialization.Constructors where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Common
import Language.K3.Codegen.CPP.Materialization.Hints

-- * MExpr Constructors

mVar :: UID -> Int -> Direction -> K3 MExpr
mVar u i d  = Node (MVar (Juncture u i) d :@: []) []

mAtom :: Method -> K3 (MExpr)
mAtom m = Node (MAtom m :@: []) []

mITE :: K3 MPred -> K3 MExpr -> K3 MExpr -> K3 MExpr
mITE p t e = Node (MIfThenElse p :@: []) [t, e]

-- * MPred Constructors

mBool :: Bool -> K3 MPred
mBool b = Node (MBool b :@: []) []

mOneOf :: K3 MExpr -> [Method] -> K3 MPred
mOneOf m ms = Node (MOneOf m ms :@: []) []

mNot :: K3 MPred -> K3 MPred
mNot m = Node (MNot :@: []) [m]

(-&&-) :: K3 MPred -> K3 MPred -> K3 MPred
(-&&-) a b = Node (MAnd :@: []) [a, b]

mOr :: [K3 MPred] -> K3 MPred
mOr = Node (MOr :@: [])

(-||-) :: K3 MPred -> K3 MPred -> K3 MPred
(-||-) a b = Node (MOr :@: []) [a, b]

mAnd :: [K3 MPred] -> K3 MPred
mAnd = Node (MAnd :@: [])

(->>-) :: K3 MPred -> K3 MPred -> K3 MPred
(->>-) a b = (mNot a) -||- b

infixr 3 -&&-
infixr 2 -||-
