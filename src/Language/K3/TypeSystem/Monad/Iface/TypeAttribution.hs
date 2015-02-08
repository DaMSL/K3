{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TupleSections, ScopedTypeVariables #-}

{-|
  A module defining an interface for type attribution, the process by which
  nodes in an AST are assigned constrained types.
-}

module Language.K3.TypeSystem.Monad.Iface.TypeAttribution
( TypeVarAttrI(..)
) where

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data

-- |The typeclass defining the interface of monads supporting type variable
--  attribution.  This allows UIDs to be matched with the type variables which
--  will represent them.  The relevant constraints are assumed to be available
--  in context.
class (Monad m) => TypeVarAttrI m where
  attributeVar :: UID -> AnyTVar -> m ()
  attributeConstraints :: ConstraintSet -> m ()
