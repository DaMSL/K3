{-# LANGUAGE DataKinds, ScopedTypeVariables, TupleSections, TemplateHaskell #-}

{-|
  This module defines a routine to unify equivalent variables in a constraint
  set (except those protected by configuration).  This routine should produce
  an equivalent constraint set even if the set in question isn't fully closed,
  although full closure helps in discovering equivalences.
-}

module Language.K3.TypeSystem.Simplification.EquivalenceUnification
( simplifyByUnification
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.Trans.Reader
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Simplification.Data
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- |A routine which performs simplification by unification of equivalent
--  variables.  This routine is guaranteed to be equicontradictory: the
--  resulting constraint set will contradict on closure iff the provided
--  constraint set does.
simplifyByUnification :: ConstraintSet -> SimplifyM ConstraintSet
simplifyByUnification cs = do
  _debug $ boxToString $
    ["Unifying by equivalence on: "] %+ prettyLines cs
  uvarEquivs <- findEquivPairs onlyUVar $
                  csQuery cs QueryAllUVarLowerBoundingUVar
  qvarEquivs <- findEquivPairs onlyQVar $
                  csQuery cs QueryAllQVarLowerBoundingQVar
  uvarCrossEquivs <- findProductPairs onlyUVar
                        (csQuery cs QueryAllUVarLowerBoundingQVar)
                        (csQuery cs QueryAllQVarLowerBoundingUVar)
  qvarCrossEquivs <- findProductPairs onlyQVar
                        (csQuery cs QueryAllQVarLowerBoundingUVar)
                        (csQuery cs QueryAllUVarLowerBoundingQVar)
  let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList $
                    Map.unionWith Set.union qvarEquivs qvarCrossEquivs
  let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList $
                    Map.unionWith Set.union uvarEquivs uvarCrossEquivs
  let cs' = replaceVariables qvarRepls uvarRepls cs
  _debug $ boxToString $
    ["Unified by equivalence on: "] %+ prettyLines cs %$
      indent 2 (["yielding: "] %+ prettyLines cs')
  return cs'
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
      -- Calculate initial map
      let initMap = Map.unionsWith Set.union $
                      map (uncurry Map.singleton . second Set.singleton) $
                      filterPairsBySymmetry pairs
      -- Calculate closed map
      let maps = iterate closeEquivMap initMap
      let mapPairs = zip maps $ tail maps
      let closedMap = head $ map fst $ filter (uncurry (==)) mapPairs
      -- Remove the prohibited replacements from the map
      toPreserve <- Set.fromList . mapMaybe destr . Set.toList .
                        preserveVars <$> ask 
      let cleanedMap = Map.map (Set.\\ toPreserve) closedMap
      -- Pick a priority for the elements in the map (so we don't replace
      -- a1 with a2 and then a2 with a1 again)
      let finalMap = fst $ foldr selectGroups (Map.empty, Set.empty) $
                        Map.toList cleanedMap
      -- Finally, remove silly (identity) replacements from the map
      return $ Map.mapWithKey Set.delete finalMap
    closeEquivMap :: forall q.
                     Map (TVar q) (Set (TVar q))
                  -> Map (TVar q) (Set (TVar q))
    closeEquivMap m =
      Map.fromList $ map (second extendSet) $ Map.toList m
      where
        extendSet :: Set (TVar q) -> Set (TVar q)
        extendSet as =
          Set.unions $ as : mapMaybe (`Map.lookup` m) (Set.toList as)
    -- |Given a list of pairs, reduces it to another list such that @(x,y)@ is
    --  in the list only if @(y,x)@ is also in the list.
    filterPairsBySymmetry :: (Eq a, Ord a) => [(a,a)] -> [(a,a)]
    filterPairsBySymmetry xs =
      let xS = Set.fromList xs in
      filter (\(x,y) -> (y,x) `Set.member` xS) xs
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
