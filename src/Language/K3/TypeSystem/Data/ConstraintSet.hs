{-# LANGUAGE GADTs #-}

{-|
  A module defining the behavior of constraint sets.  The actual constraint set
  data type is defined in @Language.K3.TypeSystem.Data.TypesAndConstraints@ to
  avoid a cyclic reference.
-}
module Language.K3.TypeSystem.Data.ConstraintSet
( csEmpty
, csNull
, csSing
, csFromList
, csToList
, csFromSet
, csToSet
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

csNull :: ConstraintSet -> Bool
csNull (ConstraintSet cs) = Set.null cs

csSing :: Constraint -> ConstraintSet
csSing = ConstraintSet . Set.singleton

csFromList :: [Constraint] -> ConstraintSet
csFromList = ConstraintSet . Set.fromList

csToList :: ConstraintSet -> [Constraint]
csToList (ConstraintSet cs) = Set.toList cs

csFromSet :: Set Constraint -> ConstraintSet
csFromSet s = ConstraintSet s

csToSet :: ConstraintSet -> Set Constraint
csToSet (ConstraintSet s) = s

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
  QueryAllTypesLowerBoundingAnyVars ::
    ConstraintSetQuery (ShallowType,AnyTVar)
  QueryAllTypesLowerBoundingTypes ::
    ConstraintSetQuery (ShallowType,ShallowType)
  QueryAllBinaryOperations ::
    ConstraintSetQuery (UVar,BinaryOperator,UVar,UVar)
  QueryAllQualOrVarLowerBoundingQVar ::
    ConstraintSetQuery (QualOrVar, QVar)
  QueryAllTypeOrVarLowerBoundingQVar ::
    ConstraintSetQuery (TypeOrVar, QVar)
  QueryAllQVarLowerBoundingQVar ::
    ConstraintSetQuery (QVar, QVar)
  QueryAllMonomorphicQualifiedUpperConstraint ::
    ConstraintSetQuery (QVar, Set TQual)
  QueryAllOpaqueLowerBoundedConstraints ::
    ConstraintSetQuery (OpaqueVar, ShallowType)
  QueryAllOpaqueUpperBoundedConstraints ::
    ConstraintSetQuery (ShallowType, OpaqueVar)
  QueryTypeOrAnyVarByAnyVarLowerBound ::
    AnyTVar -> ConstraintSetQuery UVarBound
  QueryTypeByUVarUpperBound ::
    UVar -> ConstraintSetQuery ShallowType
  QueryTypeByQVarUpperBound ::
    QVar -> ConstraintSetQuery ShallowType
  QueryQualOrVarByQVarLowerBound ::
    QVar -> ConstraintSetQuery QualOrVar
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
  QueryPolyLineageByOrigin ::
    QVar -> ConstraintSetQuery QVar
  QueryOpaqueBounds ::
    OpaqueVar -> ConstraintSetQuery (ShallowType, ShallowType) -- lower, upper

-- TODO: this routine is a prime candidate for optimization once the
--       ConstraintSet type is fancier.
-- |Performs a query against a constraint set.  The results are returned as a
--  list in no particular order.
csQuery :: (Ord r) => ConstraintSet -> ConstraintSetQuery r -> [r]
csQuery (ConstraintSet csSet) query =
  let cs = Set.toList csSet in
  case query of
    QueryAllTypesLowerBoundingAnyVars -> do
      c <- cs
      case c of
        IntermediateConstraint (CLeft t) (CRight a) -> return (t,someVar a)
        QualifiedLowerConstraint (CLeft t) qa -> return (t,someVar qa)
        _ -> mzero
    QueryAllTypesLowerBoundingTypes -> do
      IntermediateConstraint (CLeft t) (CLeft t') <- cs
      return (t,t')
    QueryAllBinaryOperations -> do
      BinaryOperatorConstraint a1 op a2 a3 <- cs
      return (a1,op,a2,a3)
    QueryAllQualOrVarLowerBoundingQVar -> do
      QualifiedIntermediateConstraint qv1 qv2 <- cs
      CRight qa <- return qv2 -- guard $ "of the form QVar"
      return (qv1,qa)
    QueryAllTypeOrVarLowerBoundingQVar -> do
      QualifiedLowerConstraint ta qa <- cs
      return (ta,qa)
    QueryAllQVarLowerBoundingQVar -> do
      QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) <- cs
      return (qa1, qa2)
    QueryAllMonomorphicQualifiedUpperConstraint -> do
      MonomorphicQualifiedUpperConstraint qa qs <- cs
      return (qa, qs)
    QueryAllOpaqueLowerBoundedConstraints -> do
      IntermediateConstraint (CLeft (SOpaque oa)) (CLeft t) <- cs
      return (oa, t)
    QueryAllOpaqueUpperBoundedConstraints -> do
      IntermediateConstraint (CLeft t) (CLeft (SOpaque oa)) <- cs
      return (t, oa)
    QueryTypeOrAnyVarByAnyVarLowerBound sa ->
      case sa of
        SomeUVar a -> do
          c <- cs
          case c of
            IntermediateConstraint (CRight a') ta | a == a' ->
              return $ CLeft ta
            QualifiedLowerConstraint (CRight a') qa | a == a' ->
              return $ CRight qa
            _ -> mzero
        SomeQVar qa -> do
          c <- cs
          case c of
            QualifiedUpperConstraint qa' ta | qa == qa' ->
              return $ CLeft ta
            QualifiedIntermediateConstraint (CRight qa') (CRight qa'')
                | qa == qa' ->
              return $ CRight qa''
            _ -> mzero
    QueryTypeByUVarUpperBound a -> do
      IntermediateConstraint (CLeft t) (CRight a') <- cs
      guard $ a == a'
      return t
    QueryTypeByQVarUpperBound qa -> do
      QualifiedLowerConstraint (CLeft t) qa' <- cs
      guard $ qa == qa'
      return t
    QueryQualOrVarByQVarLowerBound qa -> do
      QualifiedIntermediateConstraint qv1 qv2 <- cs
      guard $ (CRight qa) == qv1
      return qv2
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
    QueryPolyLineageByOrigin qa -> do
      PolyinstantiationLineageConstraint qa1 qa2 <- cs
      guard $ qa == qa2
      return qa1
    QueryOpaqueBounds oa -> do
      OpaqueBoundConstraint oa' lb ub <- cs
      guard $ oa == oa'
      return (lb,ub)

