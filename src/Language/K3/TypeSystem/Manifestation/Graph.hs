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

import Control.Applicative

import Data.Function
import Data.Functor.Identity
import Data.Maybe
import Data.Tuple (swap)

import qualified Data.Graph.Wrapper as G
import qualified Data.Map as M
import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.Coproduct
import Language.K3.TypeSystem.Data.Types
import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Data.Result

import Language.K3.TypeSystem.Manifestation.Data

-- | A graph containing a set of equivalent type variables at each vertex, and an edge from
-- supertypes to subtypes.
type ManifestGraph = G.Graph (S.Set UID) (BoundType, K3 Type, K3 Type)

-- | Construct a ManifestGraph from the result of typechecking.
--
-- This involves creating a sanitized version of the constraint set (containing only source
-- variables) and turning constraints into edges.
fromTypecheckResult :: TypecheckResult -> Maybe ManifestGraph
fromTypecheckResult result = do
    (tVarMap, constraintSet) <- tcExprTypes result
    boundsMap <- tcExprBounds result

    lowerFunctionBounds <- fmap (S.map swap) $ M.lookup () $ indexAllTypesLowerBoundingAnyVars constraintSet
    upperFunctionBounds <- M.lookup () $ indexAllTypesUpperBoundingAnyVars constraintSet

    let polarityMap = deducePolarity (S.union lowerFunctionBounds upperFunctionBounds)

    let narrowedConstraintMap = narrowAndReduce tVarMap (indexBoundingConstraintsByUVar constraintSet)
    let consolidatedVertices = attachPayload polarityMap tVarMap boundsMap narrowedConstraintMap
    let consolidatedEdges = map swap . S.toList . S.unions $ M.elems narrowedConstraintMap

    return $ G.fromVerticesEdges consolidatedVertices consolidatedEdges
  where
    -- | Construct the payload for each set of equivalent type variables.
    attachPayload pm tvm bm ncm =
        [ (u, (p, lb, ub))
        | u <- M.keys ncm
        , let p = andBoundType $ catMaybes $ map (\k -> M.lookup k tvm >>= \k' -> M.lookup k' pm) $ S.toList u
        , let Just (lb, ub) = M.lookup (S.findMin u) bm
        ]

    andBoundType :: [BoundType] -> BoundType
    andBoundType [] = UpperBound
    andBoundType (LowerBound:_) = LowerBound
    andBoundType (_:bs) = andBoundType bs

decideManifestation :: ManifestGraph -> M.Map (S.Set UID) (K3 Type)
decideManifestation g = propagateChoice g (G.topologicalSort g)
  where
    propagateChoice :: ManifestGraph -> [S.Set UID] -> M.Map (S.Set UID) (K3 Type)
    propagateChoice g [] = M.empty
    propagateChoice g (i:is) = M.insert i currentBound $ propagateChoice restrict is
      where
        currentPolarity = let (p, lb, ub) = G.vertex g i in p
        currentBound = let (p, lb, ub) = G.vertex g i in if p == UpperBound then ub else lb
        restrict = runIdentity $ G.traverseWithKey traverseF g
        traverseF k (bt, lb, rb)
            | k == i = pure (bt, lb, rb)
            | k `elem` G.successors g i && currentPolarity == UpperBound = pure (bt, lb, rb)
            | k `elem` G.successors g i && currentPolarity == LowerBound = pure (bt, lb, currentBound)
            | otherwise = pure (bt, lb, rb)

-- | Given a constraint set and a set of UIDs of variables to care about, narrow the constraint set
-- down to only those variables, and reduce the constraints on those variables to only other
-- variables in the set.
narrowAndReduce :: M.Map UID AnyTVar -> M.Map UVar (S.Set Constraint)
                -> M.Map (S.Set UID) (S.Set (S.Set UID, S.Set UID))
narrowAndReduce tVarMap cm = M.fromList
        [ (uset, ncs)
        | (atvar, uset) <- M.toList reverseMap
        , let (Just wcs) = M.lookup (fromJust $ onlyUVar atvar) cm
        , let ncs = S.fromList . catMaybes $ map sanitizeConstraint (S.toList wcs)
        ]
  where
    -- | A reverse mapping, from basic IDs to UIDs, for all the UIDs we care about.
    reverseMap :: M.Map AnyTVar (S.Set UID)
    reverseMap = collapseReverseMap tVarMap

    -- | A generic map reversal function -- Turn a map from keys to values into a map from sets of
    -- values sharing a common key, to that common key.
    collapseReverseMap :: (Ord k, Ord v) => M.Map k v -> M.Map v (S.Set k)
    collapseReverseMap = M.fromListWith S.union . map (fmap S.singleton . swap) . M.toList

    -- | Turn a constraint into a pair of UID sets. Nothing for constraints we don't care about.
    sanitizeConstraint :: Constraint -> Maybe (S.Set UID, S.Set UID)
    sanitizeConstraint (IntermediateConstraint (CRight u) (CRight v))
        | Just uUID <- M.lookup (someVar u) reverseMap
        , Just vUID <- M.lookup (someVar v) reverseMap = Just (uUID, vUID)
        | otherwise = Nothing
    sanitizeConstraint _ = Nothing

-- | Compute the preferred bound type for each variable occurring in a constraint set, based on its
-- appearance in positions of positive and negative polarities.
deducePolarity :: S.Set (AnyTVar, ShallowType) -> M.Map AnyTVar BoundType
deducePolarity bounds = assignUnionFind sinkBounds unionFind
  where
    -- | Get all subsets containing the given element.
    getOccurs :: Ord a => a -> S.Set (S.Set a) -> S.Set (S.Set a)
    getOccurs x = S.filter (S.member x)

    -- | Collapse a set of subsets into a single subset.
    collapseSet :: Ord a => S.Set (S.Set a) -> S.Set (S.Set a) -> S.Set (S.Set a)
    collapseSet s ss = S.insert (flatten ss) $ S.difference s ss

    -- | Add a single function constraint to the union find, merging sets as necessary.
    addToUnionFind :: S.Set (S.Set Variance) -> (AnyTVar, ShallowType) -> S.Set (S.Set Variance)
    addToUnionFind s (t, SFunction u v) = collapseSet union $ collapseSet counion s
      where
        union = S.unions [
                S.singleton $ S.fromList [Covariant t, Contravariant $ someVar u],
                getOccurs (Covariant t) s,
                getOccurs (Contravariant $ someVar u) s
            ]
        counion = S.unions [
                S.singleton $ S.fromList [Covariant t, Covariant $ someVar v],
                getOccurs (Covariant t) s,
                getOccurs (Covariant $ someVar v) s
            ]
    addToUnionFind s _ = s

    -- | Deeply propagate a set of assignments through a union-find data structure.
    assignUnionFind :: M.Map AnyTVar BoundType -> S.Set (S.Set Variance) -> M.Map AnyTVar BoundType
    assignUnionFind m s
        | S.null s = m
        | otherwise = M.union m m''
      where
        (m', s') = M.foldlWithKey' propagateAssignments (M.empty, s) m
        m'' = assignUnionFind m' s'

    -- | Shalowly propagate a set of assignments one step through a union find data structure.
    propagateAssignments :: (M.Map AnyTVar BoundType, S.Set (S.Set Variance)) -> AnyTVar -> BoundType
                         -> (M.Map AnyTVar BoundType, S.Set (S.Set Variance))
    propagateAssignments (m, s) t b = (M.union m newAssignments, S.difference s (S.union positive negative))
      where
        positive = getOccurs (Covariant t) s
        negative = getOccurs (Contravariant t) s

        newAssignments = M.fromList $
            [ (q, flipFromVariance v b)
            | v <- S.toList (flatten positive)
            , S.null $ getVar v
            , let q = S.findMin (getVar v)
            ] ++
            [ (q, flipFromVariance v (flipBoundType b))
            | v <- S.toList (flatten negative)
            , S.null  $ getVar v
            , let q = S.findMin (getVar v)
            ]

    -- | Alter a bound type depending on a variance position.
    flipFromVariance :: Variance -> BoundType -> BoundType
    flipFromVariance (Invariant k) _ = k
    flipFromVariance (Covariant t) b = b
    flipFromVariance (Contravariant t) b = flipBoundType b

    -- | Compute the union find of a set of constraints.
    unionFind :: S.Set (S.Set Variance)
    unionFind = S.foldl' addToUnionFind S.empty bounds

    -- | Get the variable corresponding to a variance.
    getVar :: Variance -> S.Set AnyTVar
    getVar (Invariant _) = S.empty
    getVar (Covariant t) = S.singleton t
    getVar (Contravariant t) = S.singleton t

    -- | Compute the intial bounds to boostrap the assignment process.
    sinkBounds :: M.Map AnyTVar BoundType
    sinkBounds = M.fromList [(v, LowerBound) | v <- S.toList sinkVars]

    -- | Sink variables are type variables which represent functions, but are themselves not used as
    -- an argument or return type of a function. We generally want these to be positive.
    sinkVars :: S.Set AnyTVar
    sinkVars = S.difference functionVars subFunctionVars

    functionVars :: S.Set AnyTVar
    functionVars = S.map fst bounds

    subFunctionVars :: S.Set AnyTVar
    subFunctionVars = flatten $ S.map (getBoundVars . snd) bounds

    getBoundVars :: ShallowType -> S.Set AnyTVar
    getBoundVars (SFunction x y) = S.fromList $ map someVar [x, y]
    getBoundVars _ = S.empty

-- TODO: Do I need Invariant?
data Variance
    = Invariant BoundType
    | Covariant AnyTVar
    | Contravariant AnyTVar
  deriving (Eq, Ord, Show)

-- | Union a set of sets.
flatten :: Ord a => S.Set (S.Set a) -> S.Set a
flatten = S.unions . S.toList

-- | Complement for bound types.
flipBoundType :: BoundType -> BoundType
flipBoundType LowerBound = UpperBound
flipBoundType UpperBound = LowerBound
