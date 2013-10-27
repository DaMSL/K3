{-# LANGUAGE GADTs, TemplateHaskell, LambdaCase, MultiParamTypeClasses #-}

{-|
  A module defining the behavior of constraint sets.  The actual constraint set
  data type is defined in @Language.K3.TypeSystem.Data.TypesAndConstraints@ to
  avoid a cyclic reference.
-}
module Language.K3.TypeSystem.Data.ConstraintSet
( csEmpty
, csNull
, csSing
, csInsert
, csFromList
, csToList
, csFromSet
, csToSet
, csSubset
, csUnion
, csUnions
, csQuery
, ConstraintSet(..)
, ConstraintSetQuery(..)
) where

import Data.Function
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.ConstraintSet.Queries
import Language.K3.Utils.IndexedSet
import Language.K3.Utils.Pretty

-- * Constraint set data structure definition.

$(createIndexedSet "ConstraintSet" "ConstraintSetQuery" [t|Constraint|]
    constraintSetQueryDescriptors)

instance Eq ConstraintSet where
  (==) = (==) `on` toSet
instance Ord ConstraintSet where
  compare = compare `on` toSet
instance Show ConstraintSet where
  show = show . toSet

instance Pretty ConstraintSet where
  prettyLines cs =
    -- Filter out self-constraints.
    let filteredCs = filter (not . silly) $ csToList cs in
    ["{ "] %+
    sequenceBoxes (max 1 $ maxWidth - 4) ", " (map prettyLines filteredCs) +%
    [" }"] +%
    ["*" | Set.size (toSet cs) /= length filteredCs]
    where
      silly :: Constraint -> Bool
      silly c = case c of
        IntermediateConstraint x y -> x == y
        QualifiedIntermediateConstraint x y -> x == y
        _ -> False

instance Monoid ConstraintSet where
  mempty = csEmpty
  mappend = csUnion
    
-- * Constraint set operations

csEmpty :: ConstraintSet
csEmpty = empty

csNull :: ConstraintSet -> Bool
csNull = Set.null . toSet

csSing :: Constraint -> ConstraintSet
csSing = singleton

csInsert :: Constraint -> ConstraintSet -> ConstraintSet
csInsert = insert

csFromList :: [Constraint] -> ConstraintSet
csFromList = fromSet . Set.fromList

csToList :: ConstraintSet -> [Constraint]
csToList = Set.toList . toSet

csFromSet :: Set Constraint -> ConstraintSet
csFromSet = fromSet

csToSet :: ConstraintSet -> Set Constraint
csToSet = toSet

csSubset :: ConstraintSet -> ConstraintSet -> Bool
csSubset = Set.isSubsetOf `on` toSet

csUnion :: ConstraintSet -> ConstraintSet -> ConstraintSet
csUnion = union

csUnions :: [ConstraintSet] -> ConstraintSet
csUnions = unions

-- |Performs a query against a constraint set.  The results are returned as a
--  list in no particular order.
csQuery :: (Ord r) => ConstraintSet -> ConstraintSetQuery r -> [r]
csQuery cs = Set.toList . query cs
