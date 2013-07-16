{-|
  This module defines the type constraint closure operation.
-}
module Language.K3.TypeSystem.Closure
( calculateClosure
) where

import Control.Monad
import qualified Data.Map as Map
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Closure.BinOp

-- * Closure routine

-- |Performs transitive constraint closure on a given constraint set.
calculateClosure :: ConstraintSet -> ConstraintSet
calculateClosure cs =
  let (cs',cont) = calculateClosureStep cs in
  if cont then calculateClosure cs' else cs'

-- |Performs a single transitive constraint closure step on a given constraint
--  set.  Returns the resulting constraint set and a flag indicating whether or
--  not any changes were made during this step.
calculateClosureStep :: ConstraintSet -> (ConstraintSet, Bool)
calculateClosureStep cs =
  let newCs = csUnions $ map ($ cs) closureFunctions in
  (cs `csUnion` newCs, not $ newCs `csSubset` cs)
  where
    closureFunctions =
      [ closeTransitivity
      , closeImmediate
      , closeBinaryOperations
      , closeQualifiedTransitivity
      , closeQualifiedRead
      , closeQualifiedWrite
      ]

-- * Closure rules

-- |Performs closure for transitivity.
closeTransitivity :: ConstraintSet -> ConstraintSet
closeTransitivity cs = csFromList $ do
  (t,a) <- csquery cs QueryAllTypesLowerBoundingUVars
  ta <- csquery cs $ QueryTypeOrVarByUVarLowerBound a
  return $ constraint t ta
  
-- |Performs immediate type closure.  This routine calculates the closure for
--  all immediate type-to-type constraints.
closeImmediate :: ConstraintSet -> ConstraintSet
closeImmediate cs = csUnions $ do
  (t1,t2) <- csquery cs QueryAllTypesLowerBoundingTypes
  case (t1,t2) of
    (SFunction a1 a2, SFunction a3 a4) ->
      give [constraint a2 a4, constraint a3 a1]
    (SOption qa1, SOption qa2) ->
      give [constraint qa1 qa2]
    (SIndirection qa1, SIndirection qa2) ->
      give [constraint qa1 qa2]
    (STuple qas1, STuple qas2) | length qas1 == length qas2 ->
      give $ zipWith constraint qas1 qas2
    (SRecord m1, SRecord m2)
      | Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1 ->
          give $ Map.elems $ Map.intersectionWith constraint m1 m2
    _ -> mzero
  where
    give = return . csFromList
  
-- |Performs binary operation closure.
closeBinaryOperations :: ConstraintSet -> ConstraintSet
closeBinaryOperations cs = csUnions $ do
  (a1,op,a2,a3) <- csquery cs QueryAllBinaryOperations
  t1 <- csquery cs $ QueryTypeByUVarUpperBound a1
  t2 <- csquery cs $ QueryTypeByUVarUpperBound a2
  let ans = binOpType op t1 t2
  case ans of
    Nothing -> mzero
    Just (ta3,cs') ->
      return $ cs' `csUnion` csSing (constraint ta3 a3)

closeQualifiedTransitivity :: ConstraintSet -> ConstraintSet
closeQualifiedTransitivity cs = csFromList $ do
  (qv1,qv2) <- csquery cs QueryAllQualOrVarLowerBoundingQualOrVar
  qv3 <- csquery cs $ QueryQualOrVarByQualOrVarLowerBound qv2
  return $ constraint qv1 qv3

closeQualifiedRead :: ConstraintSet -> ConstraintSet
closeQualifiedRead cs = csFromList $ do
  (ta,qa) <- csquery cs QueryAllTypeOrVarLowerBoundingQVar
  ta' <- csquery cs $ QueryTypeOrVarByQVarLowerBound qa
  return $ constraint ta ta'

closeQualifiedWrite :: ConstraintSet -> ConstraintSet
closeQualifiedWrite cs = csFromList $ do
  (qa1,qa2) <- csquery cs QueryAllQVarLowerBoundingQVar
  ta <- csquery cs $ QueryTypeOrVarByQVarUpperBound qa2
  return $ constraint ta qa1
