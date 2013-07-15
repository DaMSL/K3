-- | Constructors for Declarations.
module Language.K3.Core.Constructor.Declaration (
    global,
    endpoint,
    role,
    annotation
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Core.Constructor.Type

-- | Create a global declaration.
global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3 Declaration
global i t me = Node (DGlobal i t me :@: []) []

-- | Create an endpoint declaration.
endpoint :: Identifier -> K3 Type -> Maybe (K3 Expression) -> [K3 Declaration]
            -> K3 Declaration
endpoint i t eOpt subDecls = Node (DGlobal i t eOpt :@: []) subDecls

-- | Define a sequence of declarations to make up a role.
role :: Identifier -> [K3 Declaration] -> K3 Declaration
role i = Node (DRole i :@: [])

-- | Create a user-defined annotation
annotation :: Identifier -> [AnnMemDecl] -> K3 Declaration
annotation i members = Node (DAnnotation i members :@: []) []
