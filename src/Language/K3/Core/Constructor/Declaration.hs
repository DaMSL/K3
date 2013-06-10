-- | Constructors for Declarations.
module Language.K3.Core.Constructor.Declaration (
    global,
    endpoint,
    bind,
    selector,
    role,
    annotation
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Core.Constructor.Type

-- | Create a global declaration.
global :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3 Declaration
global i t me = Node (DGlobal i t me :@: []) []

-- | Create an endpoint declaration.
endpoint :: Identifier -> K3 Type -> (K3 Expression, K3 Expression, K3 Expression) -> K3 Declaration
endpoint i t (init_e, process_e, final_e) = Node (DGlobal i t Nothing :@: []) children
  where children = map (uncurry method) methods
        methods = [("Init", (init_e, unit)),
                   ("Process", (process_e, option t)),
                   ("Final", (final_e, unit))]
        method j (e, t) = global (i++j) (function unit t) (Just e)

-- | Create a binding between a producer and a consumer.
bind :: Identifier -> Identifier -> K3 Declaration
bind t f = Node (DBind t f :@: []) []

-- | Create a selector for a role.
selector :: Identifier -> K3 Declaration
selector s = Node (DSelector s :@: []) []

-- | Define a sequence of declarations to make up a role.
role :: Identifier -> [K3 Declaration] -> K3 Declaration
role i = Node (DRole i :@: [])

-- | Create a user-defined annotation
annotation :: Identifier -> [AnnMemDecl] -> K3 Declaration
annotation i members = Node (DAnnotation i members :@: []) []
