{-# LANGUAGE TemplateHaskell, ScopedTypeVariables, DataKinds, TupleSections #-}

{-|
  This module provides common definitions for equivalence unification.
-}
module Language.K3.TypeSystem.Simplification.EquivalenceUnification.Common
( simplifyByEquivalenceUnification

, EquivocatorFunction
, Equivocator(..)
, TVarEquivMap
, UVarEquivMap
, QVarEquivMap
, VarEquivMap

, mconcatMultiMaps
, findEquivsByIdentification
, symmetricRelationshipMap
, symmetricRelationshipMapToReplacementMap
, cleanPreservations
, closeEquivMap
, filterPairsBySymmetry
, prioritizeReplacementMap
, equivToSubstitutions
) where

import Control.Applicative
import Control.Monad.Reader
import Control.Monad.Writer
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Simplification.Common
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- * Simplification routine

-- |A routine which, given a simplifier, will perform its substitution on a
--  constraint set.
simplifyByEquivalenceUnification :: Equivocator -> Simplifier
simplifyByEquivalenceUnification (Equivocator name eqvrFn) cs =
  bracketLogM _debugI
    (boxToString $
      ["Unifying by " ++ name ++ " on: "] %+ prettyLines cs)
    (\cs' -> boxToString $
      ["Unified by " ++ name ++ " on: "] %+ prettyLines cs %$
        indent 2 (["yielding: "] %+ prettyLines cs')) $
    do
      (qvarEquivs,uvarEquivs) <- eqvrFn cs
      let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList uvarEquivs
      let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList qvarEquivs
      tellSubstitution (qvarRepls, uvarRepls)
      return $ logDiscoveries qvarRepls uvarRepls $
        replaceVariables qvarRepls uvarRepls cs
    where
      logDiscoveries qm um = _debugI
        (if Map.null um && Map.null qm
          then "Unficiation by " ++ name ++ " made no discoveries."
          else boxToString $
            ["Unification by " ++ name ++ " discovered:"] %$
              indent 2 (
                ["Unqualified variables: "] %+ prettyVarMap um %$
                ["Qualified variables:   "] %+ prettyVarMap qm
              )
        )

-- * Relevant data types

-- |A type alias for a routine which will compute a variable equivalence mapping
--  from a constraint set.  The resulting mapping should map each variable to
--  the set of variables to which it is equivalent.  It is acceptable for this
--  mapping to be asymmetric as long as it is overly sparse; this will simply
--  result in loss of some equivalences.
type EquivocatorFunction = ConstraintSet -> SimplifyM VarEquivMap

-- |A data type describing an equivocator function and its name.
data Equivocator = Equivocator String EquivocatorFunction

-- |An alias for a type variable equivalence map.
type TVarEquivMap q = Map (TVar q) (Set (TVar q))
type UVarEquivMap = TVarEquivMap UnqualifiedTVar
type QVarEquivMap = TVarEquivMap QualifiedTVar
type VarEquivMap = (QVarEquivMap, UVarEquivMap)

-- * Helpful utility functions

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
                           -> SimplifyM (TVarEquivMap q)
findEquivsByIdentification destr identify vars = do
  varsToIdsMap <- Map.fromList <$> mapM varToMapping vars
  let equivVarsByIdsMap = symmetricRelationshipMap varsToIdsMap
  symmetricRelationshipMapToReplacementMap destr equivVarsByIdsMap
  where
    varToMapping x = (x,) <$> identify x

symmetricRelationshipMapToReplacementMap ::
  (AnyTVar -> Maybe (TVar q)) ->
  Map (TVar q) (Set (TVar q)) ->
  SimplifyM (TVarEquivMap q)
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
                   -> SimplifyM (TVarEquivMap q)
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
prioritizeReplacementMap :: TVarEquivMap q -> TVarEquivMap q
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
                 -> (TVarEquivMap q, Set (TVar q))
                 -> (TVarEquivMap q, Set (TVar q))
    selectGroups (k,v) (m, used) =
      if k `Set.member` used
        then (m, used)
        else (Map.insert k (v Set.\\ used) m, used `Set.union` v)

equivToSubstitutions :: (TVar q, Set (TVar q)) -> Map (TVar q) (TVar q)
equivToSubstitutions (a, as) =
  Map.fromList $ map (,a) $ Set.toList as
