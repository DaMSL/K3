{-# LANGUAGE ViewPatterns, GADTs, TypeFamilies, DataKinds, KindSignatures, MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances #-}
{-|
  This module defines the data structures associated with constraint maps.  A
  constraint map is a structure used in the decidable subtyping approximation;
  it relates each type variable of a closed, consistent constraint set with its
  upper and lower bounds.
-}
module Language.K3.TypeSystem.Subtyping.ConstraintMap
( ConstraintMap
, lowerBound
, upperBound
, VarBound(..)
, UVarBound
, QVarBound
, cmSing
, cmUnion
, cmBoundsOf
, ConstraintMapBoundable(..)
, kernel
, isContractive
) where

import Control.Arrow
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data

-- |A data structure representing a constraint map: K in the grammar presented
--  in spec sec 6.1.  We use positive polarity to represent <= and
--  negative polarity to represent >=.
data ConstraintMap
  = ConstraintMap (Map (UVar, TPolarity) (Set UVarBound))
                  (Map (QVar, TPolarity) (Set QVarBound))
  deriving (Eq, Ord, Show)
  
lowerBound :: TPolarity
lowerBound = Positive

upperBound :: TPolarity
upperBound = Negative

data family VarBound (a :: TVarQualification) :: *
data instance VarBound UnqualifiedTVar
  = UBType ShallowType
  | UBUVar UVar
  | UBQVar QVar
  deriving (Eq, Ord, Show)
data instance VarBound QualifiedTVar
  = QBType ShallowType
  | QBUVar UVar
  | QBQVar QVar
  | QBQualSet (Set TQual)
  deriving (Eq, Ord, Show)

type UVarBound = VarBound UnqualifiedTVar
type QVarBound = VarBound QualifiedTVar

cmGetMapFor :: TVar a -> ConstraintMap
            -> Map (TVar a, TPolarity) (Set (VarBound a))
cmGetMapFor sa (ConstraintMap um qm) =
  case sa of
    QTVar _ _ _ -> qm
    UTVar _ _ _ -> um
    
cmSing :: TVar a -> TPolarity -> VarBound a -> ConstraintMap
cmSing sa pol bnd =
  case sa of
    QTVar _ _ _ ->
      ConstraintMap Map.empty $ Map.singleton (sa,pol) $ Set.singleton bnd
    UTVar _ _ _ ->
      ConstraintMap (Map.singleton (sa,pol) (Set.singleton bnd)) Map.empty

cmUnion :: ConstraintMap -> ConstraintMap -> ConstraintMap
cmUnion = mappend

cmBoundsOf :: TVar a -> TPolarity -> ConstraintMap -> Set (VarBound a)
cmBoundsOf sa pol cm = fromMaybe Set.empty $ Map.lookup (sa,pol) $
                        cmGetMapFor sa cm
                        
class ConstraintMapBoundable a b where
  cmBound :: a -> VarBound b
instance ConstraintMapBoundable ShallowType UnqualifiedTVar where
  cmBound = UBType
instance ConstraintMapBoundable UVar UnqualifiedTVar where
  cmBound = UBUVar
instance ConstraintMapBoundable QVar UnqualifiedTVar where
  cmBound = UBQVar
instance ConstraintMapBoundable ShallowType QualifiedTVar where
  cmBound = QBType
instance ConstraintMapBoundable UVar QualifiedTVar where
  cmBound = QBUVar
instance ConstraintMapBoundable QVar QualifiedTVar where
  cmBound = QBQVar
instance ConstraintMapBoundable (Set TQual) QualifiedTVar where
  cmBound = QBQualSet

instance Monoid ConstraintMap where
  mempty = ConstraintMap mempty mempty
  mappend (ConstraintMap a1 a2) (ConstraintMap b1 b2) =
    ConstraintMap (f a1 b1) (f a2 b2)
    where
      f :: (Ord k, Ord a) => Map k (Set a) -> Map k (Set a) -> Map k (Set a)
      f = Map.unionWith Set.union

-- |A routine for computing the kernel of a constraint set.  This routine is
--  only valid if the provided constraint set has already been closed.
kernel :: ConstraintSet -> ConstraintMap
kernel (csToList -> cs) =
  mconcat $ map constraintKernel cs
  where
    constraintKernel :: Constraint -> ConstraintMap
    constraintKernel c = case c of
      IntermediateConstraint (CRight a) (CLeft t) ->
        cmSing a lowerBound (UBType t)
      IntermediateConstraint (CLeft t) (CRight a) ->
        cmSing a upperBound (UBType t)
      QualifiedLowerConstraint (CLeft t) qa ->
        cmSing qa upperBound (QBType t)
      QualifiedUpperConstraint qa (CLeft t) ->
        cmSing qa lowerBound (QBType t)
      QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
        cmSing qa lowerBound (QBQualSet qs)
      QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
        cmSing qa upperBound (QBQualSet qs)
      IntermediateConstraint (CRight a1) (CRight a2) ->
        cmSing a1 lowerBound (UBUVar a2) `mappend`
        cmSing a2 upperBound (UBUVar a1)
      QualifiedLowerConstraint (CRight a) qa ->
        cmSing a lowerBound (UBQVar qa) `mappend`
        cmSing qa upperBound (QBUVar a)
      QualifiedUpperConstraint qa (CRight a) ->
        cmSing qa lowerBound (QBUVar a) `mappend`
        cmSing a upperBound (UBQVar qa)
      QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
        cmSing qa1 lowerBound (QBQVar qa2) `mappend`
        cmSing qa2 upperBound (QBQVar qa1)
      IntermediateConstraint (CLeft _) (CLeft _) -> mempty
      QualifiedIntermediateConstraint (CLeft _) (CLeft _) -> mempty
      BinaryOperatorConstraint _ _ _ _ -> mempty

-- |A routine to determine whether or not a given constraint map is contractive
--  for the specified polarity.
isContractive :: ConstraintMap -> Bool
-- TODO: for performance reasons, this information should be cached somehow.
-- Primitive subtyping must ask for contractiveness of each new constraint map
-- it builds, so this information should be gathered incrementally.
isContractive (ConstraintMap m1 m2) =
  isContractivePol Positive || isContractivePol Negative
  where
    isContractivePol :: TPolarity -> Bool
    isContractivePol pol =
      cycleHunt $ someVars (Map.keys m1) `Set.union` someVars (Map.keys m2)
      where
        someVars :: [(TVar a, TPolarity)] -> Set AnyTVar
        someVars x = Set.fromList $ map (someVar . fst) x
        findReachable :: Set AnyTVar -> AnyTVar -> [(Bool,Set AnyTVar)]
        findReachable visited v =
          if v `Set.member` visited
            then [(True,visited)]
            else
              let immediatelyReachable = 
                    case v of
                      SomeQVar qa ->
                        let bounds = fromMaybe Set.empty $
                                        Map.lookup (qa,pol) m2 in
                        nub $ map (\bnd -> case bnd of
                                      QBUVar a -> Just $ someVar a
                                      QBQVar a -> Just $ someVar a
                                      _ -> Nothing) $ Set.toList bounds
                      SomeUVar a ->
                        let bounds = fromMaybe Set.empty $
                                        Map.lookup (a,pol) m1 in
                        nub $ map (\bnd -> case bnd of
                                      UBUVar a' -> Just $ someVar a'
                                      UBQVar a' -> Just $ someVar a'
                                      _ -> Nothing) $ Set.toList bounds
              in do
                  next <- immediatelyReachable
                  case next of
                    Nothing -> [(False, Set.insert v visited)]
                    Just nv -> findReachable (Set.insert v visited) nv
        cycleHunt :: Set AnyTVar -> Bool
        cycleHunt vars =
          let var = Set.findMin vars in
          let (result,visited) =
                foldr (\(b1,s1) (b2,s2) -> (b1 && b2, s1 `Set.union` s2))
                  (False,Set.empty) (findReachable Set.empty var)
          in
          let nextVars = vars `Set.difference` visited in
          result || cycleHunt nextVars
