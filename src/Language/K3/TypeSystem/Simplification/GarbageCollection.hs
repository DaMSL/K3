{-|
  This module defines a constraint garbage collection routine by reachability.
  Any constraints which are not connected (directly or indirectly) to variables
  protected by the configuration will be deleted.  This routine is *not*
  equicontradictory, but it will always preserve contradictions caused by the
  protected type variables and their associated constraints.
-}

module Language.K3.TypeSystem.Simplification.GarbageCollection
( simplifyByGarbageCollection
) where

import Control.Applicative
import Control.Monad.Trans.Reader
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Simplification.Data

-- |Performs garbage collection on the provided constraint set.
simplifyByGarbageCollection :: ConstraintSet -> SimplifyM ConstraintSet
simplifyByGarbageCollection cs = do
  let reachability = closeMMap $
                        mmapConcat $ map createReachability $ csToList cs
  toPreserve <- preserveVars <$> ask
  let liveVars = Set.unions $ mapMaybe (`Map.lookup` reachability) $
                    Set.toList toPreserve
  return $ csFromList $ filter (hasVar liveVars) $ csToList cs
  where
    mmapAppend :: (Ord a, Ord b)
               => Map a (Set b) -> Map a (Set b) -> Map a (Set b)
    mmapAppend = Map.unionWith Set.union
    mmapConcat :: (Ord a, Ord b) => [Map a (Set b)] -> Map a (Set b)
    mmapConcat = foldr mmapAppend Map.empty
    closeMMapOnce :: (Ord a) => Map a (Set a) -> Map a (Set a)
    closeMMapOnce m =
      Map.map (Set.unions . mapMaybe (`Map.lookup` m) . Set.toList) m
    closeMMap :: (Ord a) => Map a (Set a) -> Map a (Set a)
    closeMMap m =
      let m' = closeMMapOnce m in
      if m == m' then m else closeMMap m'
    createReachability :: Constraint -> Map AnyTVar (Set AnyTVar)
    createReachability c =
      let s = extractVariables c in
      mmapConcat $ map (`Map.singleton` s) $ Set.toList s
    hasVar :: Set AnyTVar -> Constraint -> Bool
    hasVar vs c = not $ Set.null $ vs `Set.intersection` extractVariables c
