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

import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger

$(loggingFunctions)

-- * Closure routine

-- |Performs transitive constraint closure on a given constraint set.
calculateClosure :: ConstraintSet -> ConstraintSet
calculateClosure cs =
  _debugI (boxToString $ ["Closing constraints: "] %$
    indent 2 (prettyLines cs)) $
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
      , (closeLowerBoundingExtendedRecord, "Close Lower Ext. Record")
      , (closeUpperBoundingExtendedRecord, "Close Upper Ext. Record")
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
closeTransitivity cs = csFromList $
  (do
    (t,sa) <- csQuery cs $ QueryAllTypesLowerBoundingAnyVars ()
    bnd <- csQuery cs $ QueryTypeOrAnyVarByAnyVarLowerBound sa
    return $ case bnd of
      CLeft ta -> t <: ta
      CRight qa -> t <: qa
  ) ++
  (do
    (sa,t) <- csQuery cs $ QueryAllTypesUpperBoundingAnyVars ()
    bnd <- csQuery cs $ QueryTypeOrAnyVarByAnyVarUpperBound sa
    return $ case bnd of
      CLeft ta -> ta <: t
      CRight qa -> qa <: t
  )

-- |Performs immediate type closure.  This routine calculates the closure for
--  all immediate type-to-type constraints.
closeImmediate :: ConstraintSet -> ConstraintSet
closeImmediate cs = csUnions $ do
  (t1,t2) <- csQuery cs $ QueryAllTypesLowerBoundingTypes ()
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
    (SRecord m1 oas1 _, SRecord m2 oas2 _)
      | Set.null oas1 && Set.null oas2 &&
        Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1 ->
          give $ Map.elems $ Map.intersectionWith (<:) m1 m2
    _ -> mzero
  where
    give = return . csFromList

-- |Performs closure for opaque-extended records in a lower-bounding position.
closeLowerBoundingExtendedRecord :: ConstraintSet -> ConstraintSet
closeLowerBoundingExtendedRecord cs = csUnions $ do
  (SRecord m oas ctOpt, t) <- csQuery cs $ QueryAllTypesLowerBoundingTypes ()
  case t of
    SOpaque oa -> guard $ not $ oa `Set.member` oas
    _ -> return ()
  oa' <- Set.toList oas
  (_, ta_U) <- csQuery cs $ QueryOpaqueBounds oa'
  t_U <- getUpperBoundsOf cs ta_U
  case recordConcat [t_U, SRecord m (Set.delete oa' oas) ctOpt] of
    Left _ ->
      -- In this situation, there is an inherent conflict in the record type.
      -- We'll detect this in inconsistency (to keep the closure function
      -- simple); for now, just bail on the rule.
      mzero
    Right t' -> return $ csSing $ t' <: t

-- |Performs closure for opaque-extended records in an upper-bounding position.
closeUpperBoundingExtendedRecord :: ConstraintSet -> ConstraintSet
closeUpperBoundingExtendedRecord cs = csUnions $ do
  (t,SRecord m oas ctOpt) <- csQuery cs $ QueryAllTypesLowerBoundingTypes ()
  moa <- Nothing : map Just (Set.toList oas) {- Nothing adds base record -}
  case moa of
    Nothing -> {-Default case-} return $ csSing $ t <: SRecord m Set.empty ctOpt
    Just oa -> {-Opaque case -} return $ csSing $ t <: SOpaque oa

closeQualifiedTransitivity :: ConstraintSet -> ConstraintSet
closeQualifiedTransitivity cs = csFromList $ do
  (qv1,qa2) <- csQuery cs $ QueryAllQualOrVarLowerBoundingQVar ()
  qv3 <- csQuery cs $ QueryQualOrVarByQVarLowerBound qa2
  return $ qv1 <: qv3

closeQualifiedRead :: ConstraintSet -> ConstraintSet
closeQualifiedRead cs = csFromList $ do
  (ta,qa) <- csQuery cs $ QueryAllTypeOrVarLowerBoundingQVar ()
  ta' <- csQuery cs $ QueryTypeOrVarByQVarLowerBound qa
  return $ ta <: ta'

closeQualifiedWrite :: ConstraintSet -> ConstraintSet
closeQualifiedWrite cs = csFromList $ do
  (qa1,qa2) <- csQuery cs $ QueryAllQVarLowerBoundingQVar ()
  ta <- csQuery cs $ QueryTypeOrVarByQVarUpperBound qa2
  return $ ta <: qa1

closeMonomorphicTransitivity :: ConstraintSet -> ConstraintSet
closeMonomorphicTransitivity cs = csFromList $ do
  (qa2,qs) <- csQuery cs $ QueryAllMonomorphicQualifiedUpperConstraint ()
  qa1 <- csQuery cs $ QueryPolyLineageByOrigin qa2
  return $ MonomorphicQualifiedUpperConstraint qa1 qs

closeMonomorphicBounding :: ConstraintSet -> ConstraintSet
closeMonomorphicBounding cs = csFromList $ do
  (qa2,qs) <- csQuery cs $ QueryAllMonomorphicQualifiedUpperConstraint ()
  return $ qa2 <: qs

opaqueLowerBound :: ConstraintSet -> ConstraintSet
opaqueLowerBound cs = csFromList $ do
  (oa,t) <- csQuery cs $ QueryAllOpaqueLowerBoundedConstraints ()
  guard $ SOpaque oa /= t
  guard $ SRecord Map.empty (Set.singleton oa) Nothing /= t
  (_,ub) <- csQuery cs $ QueryOpaqueBounds oa
  t_U <- getUpperBoundsOf cs ub
  return $ t_U <: t

opaqueUpperBound :: ConstraintSet -> ConstraintSet
opaqueUpperBound cs = csFromList $ do
  (t,oa) <- csQuery cs $ QueryAllOpaqueUpperBoundedConstraints ()
  guard $ SOpaque oa /= t
  case t of
    SRecord _ oas _ -> guard $ not $ Set.member oa oas
    _ -> return ()
  (lb,_) <- csQuery cs $ QueryOpaqueBounds oa
  t_L <- getLowerBoundsOf cs lb
  return $ t <: t_L
