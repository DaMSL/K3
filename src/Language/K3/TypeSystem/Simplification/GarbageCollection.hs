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

-- FIXME: This simplification destroys opaque bounding constraints!  (It's not
--        marking opaque variables as "alive".)

-- |Performs garbage collection on the provided constraint set.
simplifyByGarbageCollection :: ConstraintSet -> SimplifyM ConstraintSet
simplifyByGarbageCollection cs = do
  let reachability = closeMMap $
                        mmapConcat $ map createReachability $ csToList cs
  toPreserve <- preserveVars <$> ask
  let liveVars = Set.unions $ mapMaybe (`Map.lookup` reachability) $
                    Set.toList $ Set.map varLike toPreserve
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
    varsInConstraint :: Constraint -> Set VarLike
    varsInConstraint c =
      let tvars = extractVariables c in
      let ovars = extractOpaques c in
      Set.map varLike tvars `Set.union` Set.map varLike ovars
    createReachability :: Constraint -> Map VarLike (Set VarLike)
    createReachability c =
      let vars = varsInConstraint c in
      mmapConcat $ map (`Map.singleton` vars) $ Set.toList vars
    hasVar :: Set VarLike -> Constraint -> Bool
    hasVar vs c =
      not $ Set.null $ vs `Set.intersection` varsInConstraint c
    extractOpaques :: Constraint -> Set OpaqueVar
    extractOpaques c = case c of
      OpaqueBoundConstraint oa _ _ -> Set.singleton oa
      IntermediateConstraint (CLeft (SOpaque oa)) _ -> Set.singleton oa
      IntermediateConstraint _ (CLeft (SOpaque oa)) -> Set.singleton oa
      IntermediateConstraint _ _ ->  Set.empty
      QualifiedIntermediateConstraint _ _ -> Set.empty
      QualifiedLowerConstraint _ _ -> Set.empty
      QualifiedUpperConstraint _ _ -> Set.empty
      MonomorphicQualifiedUpperConstraint _ _ -> Set.empty
      PolyinstantiationLineageConstraint _ _ -> Set.empty

data VarLike
  = VarLikeAnyTVar AnyTVar
  | VarLikeOpaqueVar OpaqueVar
  deriving (Eq, Ord, Show)

class IsVarLike a where
  varLike :: a -> VarLike
instance IsVarLike AnyTVar where
  varLike = VarLikeAnyTVar
instance IsVarLike OpaqueVar where
  varLike = VarLikeOpaqueVar
