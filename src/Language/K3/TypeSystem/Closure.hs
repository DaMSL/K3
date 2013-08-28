{-# LANGUAGE TemplateHaskell #-}

{-|
  This module defines the type constraint closure operation.
-}
module Language.K3.TypeSystem.Closure
( calculateClosure
) where

import Control.Monad
import qualified Data.Map as Map
import qualified Data.Set as Set

import Language.K3.Logger
import Language.K3.Pretty
import Language.K3.TypeSystem.Closure.BinOp
import Language.K3.TypeSystem.Data

$(loggingFunctions)

-- * Closure routine

-- |Performs transitive constraint closure on a given constraint set.
calculateClosure :: ConstraintSet -> ConstraintSet
calculateClosure cs =
  _debugI (boxToString $ ["Closing constraints: "] %$
    indent 2  (prettyLines cs)) $
  let (cs',cont) = calculateClosureStep cs in
  if cont then calculateClosure cs' else _debugIPretty "Finished closure: " cs'

-- |Performs a single transitive constraint closure step on a given constraint
--  set.  Returns the resulting constraint set and a flag indicating whether or
--  not any changes were made during this step.
calculateClosureStep :: ConstraintSet -> (ConstraintSet, Bool)
calculateClosureStep cs =
  let newCs = csUnions $ map applyClosureFunction closureFunctions in
  (cs `csUnion` newCs, not $ newCs `csSubset` cs)
  where
    closureFunctions =
      [ (closeTransitivity, "Transitivity")
      , (closeImmediate, "Immediate Type")
      , (closeBinaryOperations, "Binary Operation")
      , (closeQualifiedTransitivity, "Qual Transitivity")
      , (closeQualifiedRead, "Qualified Read")
      , (closeQualifiedWrite, "Qualified Write")
      , (closeMonomorphicTransitivity, "Monomorphic Transitivity")
      , (closeMonomorphicBounding, "Monomorphic Bounding")
      , (opaqueLowerBound, "Opaque Lower Bound")
      , (opaqueUpperBound, "Opaque Upper Bound")
      ]
    applyClosureFunction (fn, name) =
      let cs' = fn cs in
      -- TODO: if we're not on debugging level, avoid calculating "learned"
      let learned = csFromSet $ csToSet cs' Set.\\ csToSet cs in
      if csNull learned then cs' else
        flip _debugI cs' $ boxToString $
          ["Constraint closure (" ++ name ++ ") learned: "] %$
          indent 2 (prettyLines learned)

-- * Closure rules

-- Each of these functions generates a constraint set which represents the
-- new additions to the constraint set *only*; the result does not necessarily
-- include constraints from the original constraint set.

-- |Performs closure for transitivity.
closeTransitivity :: ConstraintSet -> ConstraintSet
closeTransitivity cs = csFromList $ do
  (t,sa) <- csQuery cs QueryAllTypesLowerBoundingAnyVars
  bnd <- csQuery cs $ QueryTypeOrAnyVarByAnyVarLowerBound sa
  return $ case bnd of
    CLeft ta -> t <: ta
    CRight qa -> t <: qa
  
-- |Performs immediate type closure.  This routine calculates the closure for
--  all immediate type-to-type constraints.
closeImmediate :: ConstraintSet -> ConstraintSet
closeImmediate cs = csUnions $ do
  (t1,t2) <- csQuery cs QueryAllTypesLowerBoundingTypes
  case (t1,t2) of
    (SFunction a1 a2, SFunction a3 a4) ->
      give [a2 <: a4, a3 <: a1]
    (STrigger a1, STrigger a2) ->
      give [a2 <: a1]
    (SOption qa1, SOption qa2) ->
      give [qa1 <: qa2]
    (SIndirection qa1, SIndirection qa2) ->
      give [qa1 <: qa2]
    (STuple qas1, STuple qas2) | length qas1 == length qas2 ->
      give $ zipWith (<:) qas1 qas2
    (SRecord m1, SRecord m2)
      | Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1 ->
          give $ Map.elems $ Map.intersectionWith (<:) m1 m2
    _ -> mzero
  where
    give = return . csFromList
  
-- |Performs binary operation closure.
closeBinaryOperations :: ConstraintSet -> ConstraintSet
closeBinaryOperations cs = csUnions $ do
  (a1,op,a2,a3) <- csQuery cs QueryAllBinaryOperations
  t1 <- csQuery cs $ QueryTypeByUVarUpperBound a1
  t2 <- csQuery cs $ QueryTypeByUVarUpperBound a2
  let ans = binOpType op t1 t2
  case ans of
    Nothing -> mzero
    Just (ta3,cs') ->
      return $ cs' `csUnion` csSing (ta3 <: a3)

closeQualifiedTransitivity :: ConstraintSet -> ConstraintSet
closeQualifiedTransitivity cs = csFromList $ do
  (qv1,qa2) <- csQuery cs QueryAllQualOrVarLowerBoundingQVar
  qv3 <- csQuery cs $ QueryQualOrVarByQVarLowerBound qa2
  return $ qv1 <: qv3

closeQualifiedRead :: ConstraintSet -> ConstraintSet
closeQualifiedRead cs = csFromList $ do
  (ta,qa) <- csQuery cs QueryAllTypeOrVarLowerBoundingQVar
  ta' <- csQuery cs $ QueryTypeOrVarByQVarLowerBound qa
  return $ ta <: ta'

closeQualifiedWrite :: ConstraintSet -> ConstraintSet
closeQualifiedWrite cs = csFromList $ do
  (qa1,qa2) <- csQuery cs QueryAllQVarLowerBoundingQVar
  ta <- csQuery cs $ QueryTypeOrVarByQVarUpperBound qa2
  return $ ta <: qa1

closeMonomorphicTransitivity :: ConstraintSet -> ConstraintSet
closeMonomorphicTransitivity cs = csFromList $ do
  (qa2,qs) <- csQuery cs QueryAllMonomorphicQualifiedUpperConstraint
  qa1 <- csQuery cs $ QueryPolyLineageByOrigin qa2
  return $ MonomorphicQualifiedUpperConstraint qa1 qs

closeMonomorphicBounding :: ConstraintSet -> ConstraintSet
closeMonomorphicBounding cs = csFromList $ do
  (qa2,qs) <- csQuery cs QueryAllMonomorphicQualifiedUpperConstraint
  return $ qa2 <: qs

opaqueLowerBound :: ConstraintSet -> ConstraintSet
opaqueLowerBound cs = csFromList $ do
  (oa,t) <- csQuery cs QueryAllOpaqueLowerBoundedConstraints
  guard $ SOpaque oa /= t
  (_,ub) <- csQuery cs $ QueryOpaqueBounds oa
  return $ ub <: t

opaqueUpperBound :: ConstraintSet -> ConstraintSet
opaqueUpperBound cs = csFromList $ do
  (t,oa) <- csQuery cs QueryAllOpaqueUpperBoundedConstraints
  guard $ SOpaque oa /= t
  (lb,_) <- csQuery cs $ QueryOpaqueBounds oa
  return $ t <: lb
