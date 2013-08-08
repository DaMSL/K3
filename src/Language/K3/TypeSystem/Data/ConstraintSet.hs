{-# LANGUAGE GADTs #-}

{-|
  A module defining the behavior of constraint sets.  The actual constraint set
  data type is defined in @Language.K3.TypeSystem.Data.TypesAndConstraints@ to
  avoid a cyclic reference.
-}
module Language.K3.TypeSystem.Data.ConstraintSet
( csEmpty
, csSing
, csFromList
, csToList
, csSubset
, csUnion
, csUnions
, csQuery
, ConstraintSetQuery(..)
) where

import Control.Monad
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data.TypesAndConstraints
import Language.K3.TypeSystem.Data.Utils

-- TODO: Implement a considerably more efficient underlying data structure for
-- constraint sets.

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

{-|
  Queries against the constraint set are managed via the ConstraintSetQuery
  data type.  This data type allows queries to be expressed in such a way that
  an efficient implementation of ConstraintSet can use a uniform policy for
  indexing the answers.
  
  The @QueryBoundingConstraintsByQVar@ and @QueryBoundingConstraintsByUVar@
  query forms find all constraints bounding the given variable.  This only
  includes immediate bounds; it does not include e.g. binary operation
  constraints.
-}
data ConstraintSetQuery r where
  QueryAllTypesLowerBoundingUVars ::
    ConstraintSetQuery (ShallowType,UVar)
  QueryAllTypesLowerBoundingTypes ::
    ConstraintSetQuery (ShallowType,ShallowType)
  QueryAllBinaryOperations ::
    ConstraintSetQuery (UVar,BinaryOperator,UVar,UVar)
  QueryAllQualOrVarLowerBoundingQualOrVar ::
    ConstraintSetQuery (QualOrVar, QualOrVar)
  QueryAllTypeOrVarLowerBoundingQVar ::
    ConstraintSetQuery (TypeOrVar, QVar)
  QueryAllQVarLowerBoundingQVar ::
    ConstraintSetQuery (QVar, QVar)
  QueryTypeOrVarByUVarLowerBound ::
    UVar -> ConstraintSetQuery TypeOrVar
  QueryTypeByUVarUpperBound ::
    UVar -> ConstraintSetQuery ShallowType
  QueryTypeByQVarUpperBound ::
    QVar -> ConstraintSetQuery ShallowType
  QueryQualOrVarByQualOrVarLowerBound ::
    QualOrVar -> ConstraintSetQuery QualOrVar
  QueryTypeOrVarByQVarLowerBound ::
    QVar -> ConstraintSetQuery TypeOrVar
  QueryTypeOrVarByQVarUpperBound ::
    QVar -> ConstraintSetQuery TypeOrVar
  QueryTQualSetByQVarUpperBound ::
    QVar -> ConstraintSetQuery (Set TQual)
  QueryBoundingConstraintsByUVar ::
    UVar -> ConstraintSetQuery Constraint
  QueryBoundingConstraintsByQVar ::
    QVar -> ConstraintSetQuery Constraint

-- TODO: this routine is a prime candidate for optimization once the
--       ConstraintSet type is fancier.
-- |Performs a query against a constraint set.  The results are returned as a
--  list in no particular order.
csQuery :: (Ord r) => ConstraintSet -> ConstraintSetQuery r -> [r]
csQuery (ConstraintSet csSet) query =
  let cs = Set.toList csSet in
  case query of
    QueryAllTypesLowerBoundingUVars -> do
      IntermediateConstraint (CLeft t) (CRight a) <- cs
      return (t,a)
    QueryAllTypesLowerBoundingTypes -> do
      IntermediateConstraint (CLeft t) (CLeft t') <- cs
      return (t,t')
    QueryAllBinaryOperations -> do
      BinaryOperatorConstraint a1 op a2 a3 <- cs
      return (a1,op,a2,a3)
    QueryAllQualOrVarLowerBoundingQualOrVar -> do
      QualifiedIntermediateConstraint qv1 qv2 <- cs
      return (qv1,qv2)
    QueryAllTypeOrVarLowerBoundingQVar -> do
      QualifiedLowerConstraint ta qa <- cs
      return (ta,qa)
    QueryAllQVarLowerBoundingQVar -> do
      QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) <- cs
      return (qa1, qa2)
    QueryTypeOrVarByUVarLowerBound a -> do
      IntermediateConstraint (CRight a') ta <- cs
      guard $ a == a'
      return ta
    QueryTypeByUVarUpperBound a -> do
      IntermediateConstraint (CLeft t) (CRight a') <- cs
      guard $ a == a'
      return t
    QueryTypeByQVarUpperBound qa -> do
      QualifiedLowerConstraint (CLeft t) qa' <- cs
      guard $ qa == qa'
      return t
    QueryQualOrVarByQualOrVarLowerBound qa -> do
      QualifiedIntermediateConstraint qa1 qa2 <- cs
      guard $ qa == qa1
      return qa2
    QueryTypeOrVarByQVarLowerBound qa -> do
      QualifiedUpperConstraint qa' ta <- cs
      guard $ qa == qa'
      return ta
    QueryTypeOrVarByQVarUpperBound qa -> do
      QualifiedLowerConstraint ta qa' <- cs
      guard $ qa == qa'
      return ta
    QueryTQualSetByQVarUpperBound qa -> do
      QualifiedIntermediateConstraint (CLeft qs) (CRight qa') <- cs
      guard $ qa == qa'
      return qs
    QueryBoundingConstraintsByUVar a ->
      (do
        c@(IntermediateConstraint _ (CRight a')) <- cs
        guard $ a == a'
        return c
      ) ++
      (do
        c@(IntermediateConstraint (CRight a') _) <- cs
        guard $ a == a'
        return c
      )
    QueryBoundingConstraintsByQVar qa ->
      (do
        c@(QualifiedLowerConstraint _ qa') <- cs
        guard $ qa == qa'
        return c
      ) ++
      (do
        c@(QualifiedUpperConstraint qa' _) <- cs
        guard $ qa == qa'
        return c
      ) ++
      (do
        c@(QualifiedIntermediateConstraint _ (CRight qa')) <- cs
        guard $ qa == qa'
        return c
      ) ++
      (do
        c@(QualifiedIntermediateConstraint (CRight qa') _) <- cs
        guard $ qa == qa'
        return c
      )