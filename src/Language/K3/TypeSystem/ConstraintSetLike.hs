{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies, FlexibleInstances #-}

{-|
  A module used to generalize constraint sets.  This module is used by the
  environment decision procedure to allow constraint sets which have stubs in
  addition to constraints.
-}
module Language.K3.TypeSystem.ConstraintSetLike
( ConstraintSetLike(..)
, ConstraintSetLikePromotable(..)
) where

import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.ConstraintSet

class ConstraintSetLike e c | c -> e where
  empty :: c
  singleton :: e -> c
  add :: e -> c -> c
  csingleton :: Constraint -> c
  fromList :: [e] -> c
  toList :: c -> [e]
  isSubsetOf :: c -> c -> Bool
  union :: c -> c -> c
  unions :: [c] -> c
  query :: (Ord r) => c -> ConstraintSetQuery r -> [r]

instance ConstraintSetLike Constraint ConstraintSet where
  empty = csEmpty
  singleton = csSing
  csingleton = csSing
  add e c = c `union` singleton e
  fromList = csFromList
  toList = csToList
  isSubsetOf = csSubset
  union = csUnion
  unions = csUnions
  query = csQuery

class ConstraintSetLikePromotable c1 c2 where
  promote :: c1 -> c2

instance ConstraintSetLikePromotable c c where
  promote = id
