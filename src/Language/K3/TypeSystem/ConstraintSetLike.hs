{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies #-}

{-|
  A module used to generalize constriant sets.  This module is used by the
  environment decision procedure to allow constraint sets which have stubs in
  addition to constraints.
-}
module Language.K3.TypeSystem.ConstraintSetLike
( ConstraintSetLike(..)
, ConstraintSetLikePromotable(..)
) where

import Language.K3.TypeSystem.Data

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

class ConstraintSetLikePromotable c1 c2 where
  promote :: c1 -> c2

instance ConstraintSetLikePromotable ConstraintSet ConstraintSet where
  promote = id

  
{-
csEmpty :: ConstraintSet
csEmpty = ConstraintSet Set.empty

csSing :: Constraint -> ConstraintSet
csSing = ConstraintSet . Set.singleton

csFromList :: [Constraint] -> ConstraintSet
csFromList = ConstraintSet . Set.fromList

csToList :: ConstraintSet -> [Constraint]
csToList (ConstraintSet cs) = Set.toList cs

csSubset :: ConstraintSet -> ConstraintSet -> Bool
csSubset (ConstraintSet a) (ConstraintSet b) = Set.isSubsetOf a b

csUnion :: ConstraintSet -> ConstraintSet -> ConstraintSet
csUnion (ConstraintSet a) (ConstraintSet b) = ConstraintSet $ a `Set.union` b

csUnions :: [ConstraintSet] -> ConstraintSet
csUnions css = ConstraintSet $ Set.unions $ map (\(ConstraintSet s) -> s) css
-}