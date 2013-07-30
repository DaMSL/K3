{-# LANGUAGE GADTs, ScopedTypeVariables, FlexibleContexts, Rank2Types, TupleSections #-}

{-|
  This module represents the implementation of K3 spec sec. 6: the subtyping of
  constrained types.  This implementation is an adaptation of the theory
  presented in Subtyping Constrained Types by Trifonov and Smith; at the time
  of this writing, the paper can be found at
  @http://pl.cs.jhu.edu/papers/sas96.pdf@.
  
  This module defines a computable function, @isSubtypeOf@, which will determine
  if one polymorphic constrained type is a subtype of another using a
  /conservative/ approximation as outlined in the paper above.  Whether a
  computable function exists to calculate the precise answer to this relation
  is unknown as of the time of this writing.  The approximation provided here
  fails in cases where the proof of subtyping proceeds by exhaustive enumeration
  of cases in the type system; in practice, this does not appear to be an
  impediment to use.
-}
module Language.K3.TypeSystem.Subtyping
( isSubtypeOf
) where

import Control.Applicative
import Control.Monad
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Subtyping.ConstraintMap

isSubtypeOf :: forall m. (FreshVarI m) => QuantType -> QuantType -> m Bool
isSubtypeOf qt1@(QuantType sas' qa' cs') qt2@(QuantType sas'' qa'' cs'') =
  -- First, ensure that the type variable sets are disjoint.  If this is not
  -- the case, then perform alpha-renaming as necessary.
  let overlap = sas' `Set.intersection` sas'' in
  if not $ Set.null overlap
    then do
      -- We're going to recurse on a quantified type which does not overlap in
      -- variable names.
      uvarMap <- freshVarsFor $ mapMaybe onlyUVar $ Set.toList overlap
      qvarMap <- freshVarsFor $ mapMaybe onlyQVar $ Set.toList overlap
      isSubtypeOf qt1 $ replaceVariables qvarMap uvarMap qt2
    else do
      let cs = cs' `csUnion` cs'' `csUnion` csSing (constraint qa' qa'')
      let cs''' = calculateClosure cs
      let k' = kernel cs'
      mk'' <- canonicalize $ kernel cs''
      case mk'' of
        Nothing -> return False
        Just k'' ->
          return $ consistent cs'''
                && proveAll k' sas' cs''' LowerMode
                && proveAll k'' sas'' cs''' UpperMode
  where
    freshVarsFor :: [TVar a] -> m (Map (TVar a) (TVar a))
    freshVarsFor vars =
      Map.fromList <$> mapM (\x -> (x,) <$>
                          freshVar (TVarAlphaRenamingOrigin x)) vars
    proveAll :: (IsConstraintMap cmType)
             => cmType
             -> Set AnyTVar
             -> ConstraintSet
             -> PrimitiveSubtypeMode cmType
             -> Bool
    proveAll cm sas cs mode =
      all (generalPrimitiveSubtype mode Set.empty cm) constraints
      where
        constraints :: [Constraint]
        constraints = concatMap constraintsFrom (Set.toList sas)
        constraintsFrom :: AnyTVar -> [Constraint]
        constraintsFrom sa =
          case sa of
            SomeQVar qa ->
              csQuery cs (QueryBoundingConstraintsByQVar qa)
            SomeUVar a ->
              csQuery cs (QueryBoundingConstraintsByUVar a)
              
-- |A data type describing the modes of primitive subtyping.
data PrimitiveSubtypeMode cmType where
  LowerMode :: PrimitiveSubtypeMode ConstraintMap
  UpperMode :: PrimitiveSubtypeMode CanonicalConstraintMap

-- |A general implementation for both primitive subtyping relations.  This
--  function accepts a mode describing which relation to represent.  This
--  function evaluates to @True@ if a proof of primitive subtyping can be found
--  and @False@ if it cannot.  Note that binary operator constraints and other
--  constraints which are not immediate bounds are not defined in primitive
--  subtyping and thus produce a @False@; it is the responsibility of the caller
--  to filter if necessary.
--
--  This function requires the invariant that all type variables appearing in
--  the @Constraint@ (as well as in the result of constraint closure) will
--  appear somewhere in the @ConstraintMap@.
generalPrimitiveSubtype :: forall cmType.
                           (IsConstraintMap cmType)
                        => PrimitiveSubtypeMode cmType -> Set Constraint
                        -> cmType -> Constraint -> Bool
generalPrimitiveSubtype mode visited cm c =
  let pol = polarityOfMode mode in
  case (c,mode) of
    -- Binary operators aren't subject to primitive subtyping.
    (BinaryOperatorConstraint{},_) -> False
    -- Reflexivity rule.  We use the documented invariant to avoid checking for
    -- the presence of the variable in each case.
    (IntermediateConstraint (CRight a) (CRight a'),_) | a == a' -> True
    (QualifiedIntermediateConstraint (CRight qa) (CRight qa'),_) | qa == qa' ->
      True
    -- Visited rule.  If the bound has already been visited, we are finished.
    _ | c `Set.member` visited -> True
    -- Decomposition rule.  Perform closure on this constraint and proceed on
    -- each result.
    (IntermediateConstraint (CLeft _) (CLeft _),_) ->
      let cs = calculateClosure $ csSing c in
      let cs' = filter (==c) $ csToList cs in
      consistent cs &&
        all (generalPrimitiveSubtype mode (Set.insert c visited) cm) cs'
    -- Qualifier Decomposition.
    (QualifiedIntermediateConstraint (CLeft qs1) (CLeft qs2),_) ->
      qs1 `Set.isSubsetOf` qs2
    -- Type Lower Bound Assumption
    (IntermediateConstraint (CRight a) (CLeft t),_) ->
      typeOrAnyVarBoundAssumption a t lowerBound
    (QualifiedUpperConstraint qa (CLeft t),_) ->
      typeOrAnyVarBoundAssumption qa t lowerBound
    -- Type Upper Bound Assumption
    (IntermediateConstraint (CLeft t) (CRight a),_) ->
      typeOrAnyVarBoundAssumption a t upperBound
    (QualifiedLowerConstraint (CLeft t) qa,_) ->
      typeOrAnyVarBoundAssumption qa t upperBound
    -- Variable Bound Assumption.  Because these can be ambiguous -- the same
    -- constraint can fire both the lower and upper rules -- they are cased
    -- together.  Note that this rule only applies in the lower relation.
    (IntermediateConstraint (CRight a) (CRight a'), LowerMode) ->
      typeOrAnyVarBoundAssumption a a' lowerBound ||
      typeOrAnyVarBoundAssumption a' a upperBound
    (QualifiedUpperConstraint qa (CRight a), LowerMode) ->
      typeOrAnyVarBoundAssumption qa a lowerBound ||
      typeOrAnyVarBoundAssumption a qa upperBound
    (QualifiedLowerConstraint (CRight a) qa, LowerMode) ->
      typeOrAnyVarBoundAssumption a qa lowerBound ||
      typeOrAnyVarBoundAssumption qa a upperBound
    (QualifiedIntermediateConstraint (CRight qa) (CRight qa'), LowerMode) ->
      typeOrAnyVarBoundAssumption qa qa' lowerBound ||
      typeOrAnyVarBoundAssumption qa' qa upperBound
    -- Dual Variable Lower Type Lower Bound.  Note that this rule only applies
    -- in the upper relation.
    (IntermediateConstraint (CRight a) (CRight a'), UpperMode) ->
      filteredTypeOrAnyVarBoundAssumption a a' isUConcrete lowerBound
    (QualifiedLowerConstraint (CRight a) qa', UpperMode) ->
      filteredTypeOrAnyVarBoundAssumption a qa' isUConcrete lowerBound
    (QualifiedUpperConstraint qa (CRight a'), UpperMode) ->
      filteredTypeOrAnyVarBoundAssumption qa a' isQConcrete lowerBound
    (QualifiedIntermediateConstraint (CRight qa) (CRight qa'), UpperMode) ->
      filteredTypeOrAnyVarBoundAssumption qa qa' isQConcrete lowerBound
    -- Qualifiers Lower Bound Assumption
    (QualifiedIntermediateConstraint (CRight qa) (CLeft qs),_) ->
      or (do
        bound <- Set.toList $
                    cmBoundsOf qa (lowerBound `mappend` pol) cm
        cbound <- case bound of
                    QBQualSet qs' -> return $ CLeft qs'
                    QBQVar qa' -> return $ CRight qa'
                    _ -> mzero
        let c' = QualifiedIntermediateConstraint cbound (CLeft qs)
        return $ generalPrimitiveSubtype mode (Set.insert c visited) cm c')
    -- Qualifiers Upper Bound Assumption
    (QualifiedIntermediateConstraint (CLeft qs) (CRight qa),_) ->
      or (do
        bound <- Set.toList $
                    cmBoundsOf qa (upperBound `mappend` pol) cm
        cbound <- case bound of
                    QBQualSet qs' -> return $ CLeft qs'
                    QBQVar qa' -> return $ CRight qa'
                    _ -> mzero
        let c' = QualifiedIntermediateConstraint (CLeft qs) cbound
        return $ generalPrimitiveSubtype mode (Set.insert c visited) cm c')
  where
    -- |Determines the polarity for a given subtyping mode.  This polarity is
    --  used to control the sort of bound for which the relation looks in the
    --  constraint map.
    polarityOfMode :: PrimitiveSubtypeMode a -> TPolarity
    polarityOfMode mode' = case mode' of
                            LowerMode -> Negative
                            UpperMode -> Positive
    -- |This function represents the Type/Variable Lower/Upper Bound Assumption
    --  rules.  The Type/Variable split is represented by the second parameter;
    --  the Lower/Upper split is represented by the third.
    typeOrAnyVarBoundAssumption ::
      forall a b.
      ( ConstraintConstructor2 ShallowType b
      , ConstraintConstructor2 UVar b
      , ConstraintConstructor2 QVar b
      , ConstraintConstructor2 b ShallowType
      , ConstraintConstructor2 b UVar
      , ConstraintConstructor2 b QVar
      , ConstraintMapBoundable b a
      ) => TVar a -- ^The variable in the constraint
        -> b -- ^The other bound of the constraint
        -> TPolarity -- ^The side of the constraint the variable is on.
        -> Bool
    typeOrAnyVarBoundAssumption sa tov =
      filteredTypeOrAnyVarBoundAssumption sa tov (const True)
    -- |This function represents the Type/Variable Lower/Upper Bound Assumption
    --  rules.  The Type/Variable split is represented by the second parameter;
    --  the Lower/Upper split is represented by the third.
    filteredTypeOrAnyVarBoundAssumption ::
      forall a b.
      ( ConstraintConstructor2 ShallowType b
      , ConstraintConstructor2 UVar b
      , ConstraintConstructor2 QVar b
      , ConstraintConstructor2 b ShallowType
      , ConstraintConstructor2 b UVar
      , ConstraintConstructor2 b QVar
      , ConstraintMapBoundable b a
      ) => TVar a -- ^The variable in the constraint
        -> b -- ^The other bound of the constraint
        -> (VarBound a -> Bool) -- ^A filter used to control which bounds of the
                                --  variable are used.
        -> TPolarity -- ^The side of the constraint the variable is on.
        -> Bool
    filteredTypeOrAnyVarBoundAssumption sa tov boundFilter boundDir =
      let pol = polarityOfMode mode in
      or (do
        bound <- filter boundFilter $ Set.toList $
                    cmBoundsOf sa (mconcat [lowerBound, pol, boundDir]) cm
        let Just c' = boundToConstraint sa tov bound boundDir
        return $ generalPrimitiveSubtype mode (Set.insert c visited) cm c')
    -- |Creates a constraint from bound information.
    boundToConstraint ::
      forall a b.
      ( ConstraintConstructor2 ShallowType b
      , ConstraintConstructor2 UVar b
      , ConstraintConstructor2 QVar b
      , ConstraintConstructor2 b ShallowType
      , ConstraintConstructor2 b UVar
      , ConstraintConstructor2 b QVar
      , ConstraintMapBoundable b a
      ) =>TVar a -- ^The type variable that the bound is replacing.  (Used for
                 -- exhaustive matching only.)
        -> b -- ^The non-replaced side of the constraint.
        -> VarBound a -- ^The bound to use in the constraint instead of the
                      --  given variable.
        -> TPolarity -- ^The position of the bound with respect to
                     --  the constraint which is not replaced.
        -> Maybe Constraint
    boundToConstraint sa' tov bnd boundDir =
      case (sa',bnd,boundDir) of
        (UTVar{}, UBType t', Positive) -> Just $ constraint t' tov
        (UTVar{}, UBUVar a, Positive) -> Just $ constraint a tov
        (UTVar{}, UBQVar qa, Positive) -> Just $ constraint qa tov
        (QTVar{}, QBType t', Positive) -> Just $ constraint t' tov
        (QTVar{}, QBUVar a, Positive) -> Just $ constraint a tov
        (QTVar{}, QBQVar qa, Positive) -> Just $ constraint qa tov
        (UTVar{}, UBType t', Negative) -> Just $ constraint tov t'
        (UTVar{}, UBUVar a, Negative) -> Just $ constraint tov a
        (UTVar{}, UBQVar qa, Negative) -> Just $ constraint tov qa
        (QTVar{}, QBType t', Negative) -> Just $ constraint tov t'
        (QTVar{}, QBUVar a, Negative) -> Just $ constraint tov a
        (QTVar{}, QBQVar qa, Negative) -> Just $ constraint tov qa
        -- We can't handle qual sets because this function is general enough to
        -- be used in cases where the non-replaced side is a UVar; as a result,
        -- we'd have to require e.g. "ConstraintConstructor2 b (Set TQual)",
        -- which we can't always assume.  Qual sets are handled in a different
        -- case match in the primary function.
        (QTVar{}, QBQualSet _, _) -> Nothing
