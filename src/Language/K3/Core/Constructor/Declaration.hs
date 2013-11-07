-- | Constructors for Declarations.
module Language.K3.Core.Constructor.Declaration (
    global,
    Language.K3.Core.Constructor.Declaration.trigger,
    endpoint,
    role,
    annotation,
    typeDef
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

-- | Create a global declaration.
global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3 Declaration
global i t me = Node (DGlobal i t me :@: []) []

-- | Create a trigger declaration.
trigger :: Identifier -> K3 Type -> K3 Expression -> K3 Declaration
trigger i t e = Node (DTrigger i t e :@: []) []

-- | Create an endpoint declaration.
endpoint :: Identifier -> K3 Type -> Maybe (K3 Expression) -> [K3 Declaration] -> K3 Declaration
endpoint i t eOpt = Node (DGlobal i t eOpt :@: [])

-- | Define a sequence of declarations to make up a role.
role :: Identifier -> [K3 Declaration] -> K3 Declaration
role i = Node (DRole i :@: [])

typeDef :: Identifier -> K3 Type -> K3 Declaration
typeDef i t = Node (DTypeDef i t :@: []) []
-- | Create a user-defined annotation.  Arguments are annotation name,
--   declared type parameters, and member declarations.
annotation :: Identifier -> [TypeVarDecl] -> [AnnMemDecl] -> K3 Declaration
annotation i vdecls members = Node (DAnnotation i vdecls members :@: []) []
