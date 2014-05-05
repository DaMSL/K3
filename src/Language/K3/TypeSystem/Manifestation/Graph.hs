-- | Dependent type-manifestation using graph traversal.
--
-- This module defines an algorithm and supporting machinery to perform dependent type manifestation
-- on a consistent constraint set obtained from a successful typechecking run.
--
-- The algorithm picks types for type variables whilst respecting constraints between them.
-- Equivalent variables are naturally manifested identically, constrained variables are manifested
-- such that the manifest types still satisfy their constraints.
module Language.K3.TypeSystem.Manifestation.Graph where

import Data.Maybe (fromJust)

import qualified Data.Graph.Wrapper as G
import qualified Data.Map as M
import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.TypeSystem
import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.Coproduct
import Language.K3.TypeSystem.Data.Types
import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Manifestation.Data

type ManifestGraph = G.Graph (S.Set UID) (BoundType, K3 Type, K3 Type)

-- | Construct a ManifestGraph from the result of typechecking.
--
-- This involves creating a sanitized version of the constraint set (containing only source
-- variables) and turning constraints into edges.
fromTypecheckResult :: TypecheckResult -> Maybe (M.Map Int (S.Set (Int, Int)))
fromTypecheckResult result = case tcExprTypes result of
    Nothing -> Nothing
    Just (tVarMap, constraintSet) -> Just $ narrowAndReduce tVarMap (indexBoundingConstraintsByUVar constraintSet)
  where
    narrowAndReduce :: M.Map UID AnyTVar -> M.Map UVar (S.Set Constraint) -> M.Map Int (S.Set (Int, Int))
    narrowAndReduce tvm cm = M.fromList
                [ (t, mcs)
                | (UID t, SomeUVar tv) <- M.toList tvm
                , let (Just wcs) = M.lookup tv cm
                , let ncs = S.filter (isValidIntermediateConstraint $ reverseMap tvm) wcs
                , let mcs = S.map (translatePair (reverseMap tvm) . intermediateConstraintToPair) ncs
                ]
    translatePair :: M.Map Int Int -> (Int, Int) -> (Int, Int)
    translatePair m (i, j) = (fromJust $ M.lookup i m, fromJust $ M.lookup j m)

    reverseMap :: M.Map UID AnyTVar -> M.Map Int Int
    reverseMap = M.fromList . map (\(UID i, SomeUVar u) -> (tvarIdNum $ tvarId u, i)) . M.toList

    intermediateConstraintToPair :: Constraint -> (Int, Int)
    intermediateConstraintToPair (IntermediateConstraint (CRight u) (CRight v)) =
        (tvarIdNum $ tvarId u, tvarIdNum $ tvarId v)
    intermediateConstraintToPair _ = undefined

    isValidIntermediateConstraint :: M.Map Int Int -> Constraint -> Bool
    isValidIntermediateConstraint m (IntermediateConstraint (CRight u) (CRight v)) =
        M.member (tvarIdNum (tvarId u)) m && M.member (tvarIdNum $ tvarId v) m
    isValidIntermediateConstraint _ _ = False
