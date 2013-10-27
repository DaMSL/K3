{-# LANGUAGE DataKinds, ScopedTypeVariables, TupleSections, TemplateHaskell, FlexibleInstances, FlexibleContexts #-}

{-|
  This module defines routines to unify equivalent variables in a constraint
  set (except those protected by configuration).
-}

module Language.K3.TypeSystem.Simplification.EquivalenceUnification
( simplifyByConstraintEquivalenceUnification
, simplifyByBoundEquivalenceUnification
, simplifyByStructuralEquivalenceUnification
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.Reader
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Simplification.Data
import Language.K3.TypeSystem.Within
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- |A routine which finds variables which are constrained to be equivalent and
--  unifies them.  The resulting constraint set is guaranteed to be
--  equicontradictory.  This routine is safe for use on unclosed constraint
--  sets.
simplifyByConstraintEquivalenceUnification ::
  ConstraintSet -> SimplifyM ConstraintSet
simplifyByConstraintEquivalenceUnification cs =
  bracketLogM _debugI 
    (boxToString $
      ["Unifying by constraint equivalence on: "] %+ prettyLines cs)
    (\cs' -> boxToString $
      ["Unified by constraint equivalence on: "] %+ prettyLines cs %$
        indent 2 (["yielding: "] %+ prettyLines cs')) $
  do
    _debug $ boxToString $
      ["Unifying by constraint equivalence on: "] %+ prettyLines cs
    uvarEquivs <- findEquivPairs onlyUVar $
                    csQuery cs $ QueryAllUVarLowerBoundingUVar ()
    qvarEquivs <- findEquivPairs onlyQVar $
                    csQuery cs $ QueryAllQVarLowerBoundingQVar ()
    uvarCrossEquivs <- findProductPairs onlyUVar
                          (csQuery cs $ QueryAllUVarLowerBoundingQVar ())
                          (csQuery cs $ QueryAllQVarLowerBoundingUVar ())
    qvarCrossEquivs <- findProductPairs onlyQVar
                          (csQuery cs $ QueryAllQVarLowerBoundingUVar ())
                          (csQuery cs $ QueryAllUVarLowerBoundingQVar ())
    let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList $
                      Map.unionWith Set.union qvarEquivs qvarCrossEquivs
    let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList $
                      Map.unionWith Set.union uvarEquivs uvarCrossEquivs
    return $ logDiscoveries "constraint" qvarRepls uvarRepls $
      replaceVariables qvarRepls uvarRepls cs
  where
    findProductPairs :: forall q q'.
                        (AnyTVar -> Maybe (TVar q))
                     -> [(TVar q, TVar q')]
                     -> [(TVar q', TVar q)]
                     -> SimplifyM (Map (TVar q) (Set (TVar q)))
    findProductPairs destr pairs1 pairs2 =
      -- PERF: maybe use an indexing structure to avoid the cartesian product?
      findEquivPairs destr
        [ (x1,x2)
        | (x1,y1) <- pairs1
        , (y2,x2) <- pairs2
        , y1 == y2
        ]
    findEquivPairs :: forall q.
                      (AnyTVar -> Maybe (TVar q))
                   -> [(TVar q, TVar q)]
                   -> SimplifyM (Map (TVar q) (Set (TVar q)))
    findEquivPairs destr pairs = do
      let initMap = Map.unionsWith Set.union $
                      map (uncurry Map.singleton . second Set.singleton) $
                      filterPairsBySymmetry pairs
      symmetricRelationshipMapToReplacementMap destr initMap

-- |A routine which finds variables with identical concrete upper and lower
--  bound sets and unifies them.  This routine is safe for use only on closed
--  constraint sets.
simplifyByBoundEquivalenceUnification ::
  ConstraintSet -> SimplifyM ConstraintSet
simplifyByBoundEquivalenceUnification cs =
  bracketLogM _debugI
    (boxToString $ ["Unifying by bound equivalence on: "] %+ prettyLines cs)
    (\cs' -> boxToString $
      ["Unified by bound equivalence on: "] %+ prettyLines cs %$
        indent 2 (["yielding: "] %+ prettyLines cs')) $
  do
    let vars = extractVariables cs
    uvarEquivs <- findEquivsByIdentification onlyUVar identifyUVarBounds $
                    mapMaybe onlyUVar $ Set.toList vars
    qvarEquivs <- findEquivsByIdentification onlyQVar identifyQVarBounds $
                    mapMaybe onlyQVar $ Set.toList vars
    let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList uvarEquivs
    let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList qvarEquivs
    return $ logDiscoveries "bound" qvarRepls uvarRepls $
      replaceVariables qvarRepls uvarRepls cs
  where
    identifyUVarBounds :: UVar -> SimplifyM (Set ShallowType, Set ShallowType)
    identifyUVarBounds a = do
      let lbs = Set.fromList $ csQuery cs $ QueryTypeByUVarUpperBound a
      let ubs = Set.fromList $ csQuery cs $ QueryTypeByUVarLowerBound a
      return (lbs,ubs)
    identifyQVarBounds :: QVar
                       -> SimplifyM ( Set ShallowType
                                    , Set ShallowType
                                    , Set (Set TQual)
                                    , Set (Set TQual) )
    identifyQVarBounds qa = do
      let lbs = Set.fromList $ csQuery cs $ QueryTypeByQVarUpperBound qa
      let ubs = Set.fromList $ csQuery cs $ QueryTypeByQVarLowerBound qa
      let lqbs = Set.fromList $ csQuery cs $ QueryTQualSetByQVarUpperBound qa
      let uqbs = Set.fromList $ csQuery cs $ QueryTQualSetByQVarLowerBound qa
      return (lbs,ubs,lqbs,uqbs)

-- |A routine to unify type variables that are part of a structural equivalence.
--  If two type variables a1 and a2 in the set can be shown to be structurally
--  identical (e.g. every constraint imposed upon them recursively is the same
--  up to alpha renaming), then it is safe to replace all variables in one
--  structure with the variables from the other.
simplifyByStructuralEquivalenceUnification ::
  ConstraintSet -> SimplifyM ConstraintSet
simplifyByStructuralEquivalenceUnification cs =
  bracketLogM _debugI
    (boxToString $ ["Unifying by structural equivalence on: "] %+
      prettyLines cs)
    (\cs' -> boxToString $
      ["Unified by structural equivalence on: "] %+ prettyLines cs %$
        indent 2 (["yielding: "] %+ prettyLines cs')) $
  do
    let vars = Set.toList $ extractVariables cs
    let equivsOverUVars = performTests $ mapMaybe onlyUVar vars
    let equivsOverQVars = performTests $ mapMaybe onlyQVar vars
    let (qvarPreEquivs, uvarPreEquivs) =
          mconcatMultiMaps *** mconcatMultiMaps $
            unzip [equivsOverQVars, equivsOverUVars]
    uvarEquivs <- symmetricRelationshipMapToReplacementMap
                    onlyUVar uvarPreEquivs
    qvarEquivs <- symmetricRelationshipMapToReplacementMap
                    onlyQVar qvarPreEquivs
    let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList uvarEquivs
    let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList qvarEquivs
    return $ logDiscoveries "structural" qvarRepls uvarRepls $
      replaceVariables qvarRepls uvarRepls cs
  where
    performTests :: (WithinAlignable (TVar q))
                 => [TVar q] -> (Map QVar (Set QVar), Map UVar (Set UVar))
    performTests vars =
      let mapOp = Map.map Set.singleton in
      mconcatMultiMaps *** mconcatMultiMaps $ unzip $
        map ((mapOp *** mapOp) . uncurry testStructuralEquivalence)
            [ (v1,v2) | v1 <- vars, v2 <- vars, v1 /= v2 ]
    testStructuralEquivalence :: (WithinAlignable (TVar q))
                              => TVar q -> TVar q
                              -> (Map QVar QVar, Map UVar UVar)
    testStructuralEquivalence v1 v2 =
      fromMaybe (Map.empty, Map.empty) $ listToMaybe $
        proveMutuallyWithin (v1, cs) (v2, cs) 
    
-- * Helpful utility functions

logDiscoveries :: String -> Map QVar QVar -> Map UVar UVar -> a -> a
logDiscoveries name qm um =
  _debugI
      (if Map.null um && Map.null qm
        then "Unficiation by " ++ name ++ " equivalence made no discoveries."
        else boxToString $
          ["Unification by " ++ name ++ " equivalence discovered:"] %$
            indent 2 (
              ["Unqualified variables: "] %+ prettyVarMap um %$
              ["Qualified variables:   "] %+ prettyVarMap qm
      ))

mconcatMultiMaps :: (Ord a, Ord b) => [Map a (Set b)] -> Map a (Set b)
mconcatMultiMaps = Map.unionsWith Set.union

prettyVarMap :: Map (TVar q) (TVar q) -> [String]
prettyVarMap m =
  sequenceBoxes maxWidth ", " $ map (uncurry prettyPair) $ Map.toList m
  where
    prettyPair v v' = prettyLines v %+ ["~="] %+ prettyLines v'

-- |Finds an equivalence map over a single kind of type variable.  This is
--  accomplished by using an identification function on each variable and then
--  defining the equivalence by equal identifications.
findEquivsByIdentification :: forall q a.
                              (Eq a, Ord a)
                           => (AnyTVar -> Maybe (TVar q))
                           -> (TVar q -> SimplifyM a)
                           -> [TVar q]
                           -> SimplifyM (Map (TVar q) (Set (TVar q)))
findEquivsByIdentification destr identify vars = do
  varsToIdsMap <- Map.fromList <$> mapM varToMapping vars
  let equivVarsByIdsMap = symmetricRelationshipMap varsToIdsMap
  symmetricRelationshipMapToReplacementMap destr equivVarsByIdsMap
  where
    varToMapping x = (x,) <$> identify x

symmetricRelationshipMapToReplacementMap ::
  (AnyTVar -> Maybe (TVar q)) ->
  Map (TVar q) (Set (TVar q)) ->
  SimplifyM (Map (TVar q) (Set (TVar q)))
symmetricRelationshipMapToReplacementMap destr origMap = do
  -- Calculate closed map
  let closedMap = closeEquivMap origMap
  -- Remove the prohibited replacements from the map
  cleanedMap <- cleanPreservations destr closedMap
  -- Pick a priority for the elements in the map
  let finalMap = prioritizeReplacementMap cleanedMap
  -- Finally, remove silly (identity) replacements from the map
  return $ Map.mapWithKey Set.delete finalMap

-- |Given a mapping from an entity onto a description of its identity, produces
--  a map describing for each entity to which entities it is equivalent.
symmetricRelationshipMap :: (Eq a, Eq b, Ord a, Ord b)
                         => Map a b -> Map a (Set a)
symmetricRelationshipMap m =
  let inverseMap = mconcatMultiMaps $
        map (uncurry $ flip Map.singleton . Set.singleton) $ Map.toList m in
  let relateMap =
        Map.map (fromMaybe Set.empty . (`Map.lookup` inverseMap)) m in
  Map.mapWithKey (\k -> Set.filter (maybe False (Set.member k) .
                          (`Map.lookup` relateMap))) relateMap

cleanPreservations :: (AnyTVar -> Maybe (TVar q))
                   -> Map (TVar q) (Set (TVar q))
                   -> SimplifyM (Map (TVar q) (Set (TVar q)))
cleanPreservations destr m = do
  toPreserve <- Set.fromList . mapMaybe destr . Set.toList .
                    preserveVars <$> ask 
  return $ Map.map (Set.\\ toPreserve) m
                            
closeEquivMap :: forall a. (Eq a, Ord a)
              => Map a (Set a) -> Map a (Set a)
closeEquivMap = leastFixedPoint closeEquivMapOnce

closeEquivMapOnce :: forall a. (Eq a, Ord a) => Map a (Set a) -> Map a (Set a)
closeEquivMapOnce m =
  Map.map extendSet m
  where
    extendSet :: Set a -> Set a
    extendSet as =
      Set.unions $ as : mapMaybe (`Map.lookup` m) (Set.toList as)

-- |Given a list of pairs, reduces it to another list such that @(x,y)@ is
--  in the list only if @(y,x)@ is also in the list.
filterPairsBySymmetry :: (Eq a, Ord a) => [(a,a)] -> [(a,a)]
filterPairsBySymmetry xs =
  let xS = Set.fromList xs in
  filter (\(x,y) -> (y,x) `Set.member` xS) xs
  
-- |A function to select groups of type variable substitutions to use.  The
--  input mapping describes equivalences; the output mapping describes which
--  variables will actually do the replacement (to prevent replacing each
--  variable by the other).
prioritizeReplacementMap :: Map (TVar q) (Set (TVar q))
                         -> Map (TVar q) (Set (TVar q))
prioritizeReplacementMap replMap =
  fst $ foldr selectGroups (Map.empty, Set.empty) $ Map.toList replMap
  where
    -- |A function which selects groups of type variable substitutions to
    --  use.  The output type is a pair between the mapping of equivalences
    --  which is intended to be used (initially empty) and the set of
    --  variables which, already being part of an equivalence in the map,
    --  should not be included as a key.  By the end, the key-value pairs
    --  should be disjoint from one another when viewed as simple sets of
    --  variables.
    selectGroups :: (TVar q, Set (TVar q))
                 -> (Map (TVar q) (Set (TVar q)), Set (TVar q))
                 -> (Map (TVar q) (Set (TVar q)), Set (TVar q))
    selectGroups (k,v) (m, used) =
      if k `Set.member` used
        then (m, used)
        else (Map.insert k (v Set.\\ used) m, used `Set.union` v)

equivToSubstitutions :: (TVar q, Set (TVar q)) -> Map (TVar q) (TVar q)
equivToSubstitutions (a, as) =
  Map.fromList $ map (,a) $ Set.toList as
