{-# LANGUAGE PatternGuards #-}

-- | Dependent type-manifestation using graph traversal.
--
-- This module defines an algorithm and supporting machinery to perform dependent type manifestation
-- on a consistent constraint set obtained from a successful typechecking run.
--
-- The algorithm picks types for type variables whilst respecting constraints between them.
-- Equivalent variables are naturally manifested identically, constrained variables are manifested
-- such that the manifest types still satisfy their constraints.
module Language.K3.TypeSystem.Manifestation.Graph where

import Data.Functor
import Data.Maybe
import Data.Tuple (swap)

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
fromTypecheckResult :: TypecheckResult -> Maybe (G.Graph (S.Set UID) (S.Set UID))
fromTypecheckResult result = consolidateGraph <$> tcExprTypes result
  where
    consolidateGraph (tvm, cs) = constructGraph $ narrowAndReduce tvm (indexBoundingConstraintsByUVar cs)
    constructGraph m = G.fromVerticesEdges (zip (M.keys m) (M.keys m)) (S.toList $ S.unions $ M.elems m)

narrowAndReduce :: M.Map UID AnyTVar -> M.Map UVar (S.Set Constraint)
                -> M.Map (S.Set UID) (S.Set (S.Set UID, S.Set UID))
narrowAndReduce tVarMap cm = M.fromList
            [ (uset, ncs)
            | (atvar, uset) <- M.toList reverseMap
            , let (Just wcs) = M.lookup (fromJust $ onlyUVar atvar) cm
            , let ncs = S.fromList . catMaybes $ map sanitizeIntermediateConstraint (S.toList wcs)
            ]
  where
    -- | A reverse mapping, from basic IDs to UIDs, for all the UIDs we care about.
    reverseMap :: M.Map AnyTVar (S.Set UID)
    reverseMap = collapseReverseMap tVarMap

    collapseReverseMap :: (Ord k, Ord v) => M.Map k v -> M.Map v (S.Set k)
    collapseReverseMap = M.fromListWith S.union . map (fmap S.singleton . swap) . M.toList

    translatePair :: M.Map Int Int -> (Int, Int) -> (Int, Int)
    translatePair m (i, j) = (fromJust $ M.lookup i m, fromJust $ M.lookup j m)

    sanitizeIntermediateConstraint :: Constraint -> Maybe (S.Set UID, S.Set UID)
    sanitizeIntermediateConstraint (IntermediateConstraint (CRight u) (CRight v))
        | Just uUID <- M.lookup (someVar u) reverseMap
        , Just vUID <- M.lookup (someVar v) reverseMap = Just (uUID, vUID)
        | otherwise = Nothing
    sanitizeIntermediateConstraint _ = Nothing

    intermediateConstraintToPair :: Constraint -> (Int, Int)
    intermediateConstraintToPair (IntermediateConstraint (CRight u) (CRight v)) =
        (tvarIdNum $ tvarId u, tvarIdNum $ tvarId v)
    intermediateConstraintToPair _ = undefined
