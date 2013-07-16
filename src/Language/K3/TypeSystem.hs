{-# LANGUAGE ViewPatterns #-}

module Language.K3.TypeSystem
(
) where

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.TypeSystem.Types
data TypeError = TODO_TypeError -- TODO: TypeError data type

-- |The top level of typechecking in K3.  This routine accepts a role
--  declaration and typechecks it.  Upon success, an updated version of the
--  declaration is returned; this version includes an annotation at each node
--  describing the type of that subtree.  These types are described with respect
--  to a global set of constraints, which is also returned.
typecheck :: K3 Declaration
          -> Either TypeError (ConstraintSet, K3 Declaration)
typecheck = undefined -- TODO
