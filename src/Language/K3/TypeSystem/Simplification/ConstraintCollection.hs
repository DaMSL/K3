{-# LANGUAGE DataKinds, ScopedTypeVariables, TupleSections #-}

{-|
  This module defines a garbage collection routine for constraint sets.  It uses
  a number of strategies to reduce the size of the constraint set while
  retaining all of the relevant information.
-}

module Language.K3.TypeSystem.Simplification.ConstraintCollection
( simplify
, SimplificationConfig(..)
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans
import Control.Monad.Trans.Reader
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ReplaceVariables

-- |The record used to configure the simplification routine.  The set of
--  variables @preserveVars@ are variables which must be present by the
--  completion of the simplification process.
data SimplificationConfig
  = SimplificationConfig
      { preserveVars :: Set AnyTVar
      }

-- |The top level simplification routine for constraint sets.  This routine also
--  accepts a configuration in the form of a @SimplificationConfig@ type.
simplify :: SimplificationConfig -> ConstraintSet -> ConstraintSet
simplify config cs = flip runReader config $
  return cs >>= simplifyByUnification

-- |The monad under which simplification occurs.
type SimplifyM = Reader SimplificationConfig

-- |A routine which performs simplification by unification of equivalent
--  variables.
simplifyByUnification :: ConstraintSet -> SimplifyM ConstraintSet
simplifyByUnification cs = do
  uvarEquivs <- findEquivPairs (\(SomeUVar a) -> a) $
                  csQuery cs QueryAllUVarLowerBoundingUVar
  qvarEquivs <- findEquivPairs (\(SomeQVar a) -> a) $
                  csQuery cs QueryAllQVarLowerBoundingQVar
  let qvarRepls = mconcat $ map equivToSubstitutions $ Map.toList qvarEquivs
  let uvarRepls = mconcat $ map equivToSubstitutions $ Map.toList uvarEquivs
  let cs' = replaceVariables qvarRepls uvarRepls cs
  return cs'
  where
    findEquivPairs :: forall q.
                      (AnyTVar -> TVar q)
                   -> [(TVar q, TVar q)]
                   -> SimplifyM (Map (TVar q) (Set (TVar q)))
    findEquivPairs destr pairs = do
      -- Calculate initial map
      let initMap = Map.unionsWith Set.union $
                      map (uncurry Map.singleton . second Set.singleton) pairs
      -- Calculate closed map
      let maps = iterate closeEquivMap initMap
      let mapPairs = zip maps $ tail maps
      let closedMap = head $ map fst $ filter (uncurry (==)) mapPairs
      -- Remove the prohibited replacements from the map
      toPreserve <- Set.fromList . map destr . Set.toList . preserveVars <$> ask 
      let cleanedMap = Map.map (Set.\\ toPreserve) closedMap
      -- Pick a priority for the elements in the map (so we don't replace
      -- a1 with a2 and then a2 with a1 again)
      let finalMap = fst $ foldr selectGroups (Map.empty, Set.empty) $
                        Map.toList cleanedMap
      -- Finally, remove silly (identity) replacements from the map
      return $ Map.mapWithKey Set.delete finalMap
      where
        closeEquivMap :: Map (TVar q) (Set (TVar q))
                      -> Map (TVar q) (Set (TVar q))
        closeEquivMap m =
          Map.fromList $ map (second extendSet) $ Map.toList m
          where
            extendSet :: Set (TVar q) -> Set (TVar q)
            extendSet as =
              Set.unions $ as : mapMaybe (`Map.lookup` m) (Set.toList as)
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
