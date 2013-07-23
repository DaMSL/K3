{-# LANGUAGE GADTs, ScopedTypeVariables, FlexibleContexts, Rank2Types #-}

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

import Control.Monad
import Data.Monoid
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Subtyping.ConstraintMap

isSubtypeOf :: QuantType -> QuantType -> Bool
isSubtypeOf = undefined -- TODO

-- |An implementation of the primitive subtyping relation.  This function
--  evaluates to @True@ if a proof of primitive subtyping can be found and
--  @False@ if it cannot.  Note that binary operator constraints and other
--  constraints which are not immediate bounds are not defined in primitive
--  subtyping and thus produce a @False@; it is the responsibility of the caller
--  to filter if necessary.
--
--  This function requires the invariant that all type variables appearing in
--  the @Constraint@ (as well as in the result of constraint closure) will
--  appear somewhere in the @ConstraintMap@.
primitiveSubtype :: TPolarity -> ConstraintMap -> Constraint -> Bool
primitiveSubtype pol cm c =
  case c of
    -- Binary operators aren't subject to primitive subtyping.
    BinaryOperatorConstraint{} -> False
    -- Decomposition rule.  Perform closure on this constraint and proceed on
    -- each result.
    IntermediateConstraint (CLeft _) (CLeft _) ->
      let cs = filter (==c) $ csToList $ calculateClosure $ csSing c in
      all (primitiveSubtype pol cm) cs
    -- Qualifier Decomposition.
    QualifiedIntermediateConstraint (CLeft qs1) (CLeft qs2) ->
      qs1 `Set.isSubsetOf` qs2
    -- Reflexivity rule.  We use the documented invariant to avoid checking for
    -- the presence of the variable in each case.
    IntermediateConstraint (CRight a) (CRight a') | a == a' -> True
    QualifiedIntermediateConstraint (CRight qa) (CRight qa') | qa == qa' ->
      True
    -- Type Lower Bound Assumption
    IntermediateConstraint (CRight a) (CLeft t) ->
      typeOrAnyVarBoundAssumption a t lowerBound
    QualifiedUpperConstraint qa (CLeft t) ->
      typeOrAnyVarBoundAssumption qa t lowerBound
    -- Type Upper Bound Assumption
    IntermediateConstraint (CLeft t) (CRight a) ->
      typeOrAnyVarBoundAssumption a t upperBound
    QualifiedLowerConstraint (CLeft t) qa ->
      typeOrAnyVarBoundAssumption qa t upperBound
    -- Variable Bound Assumption.  Because these can be ambiguous -- the same
    -- constraint can fire both the lower and upper rules -- they are cased
    -- together.
    IntermediateConstraint (CRight a) (CRight a') ->
      typeOrAnyVarBoundAssumption a a' lowerBound ||
      typeOrAnyVarBoundAssumption a' a upperBound
    QualifiedUpperConstraint qa (CRight a) ->
      typeOrAnyVarBoundAssumption qa a lowerBound ||
      typeOrAnyVarBoundAssumption a qa upperBound
    QualifiedLowerConstraint (CRight a) qa ->
      typeOrAnyVarBoundAssumption a qa lowerBound ||
      typeOrAnyVarBoundAssumption qa a upperBound
    QualifiedIntermediateConstraint (CRight qa) (CRight qa') ->
      typeOrAnyVarBoundAssumption qa qa' lowerBound ||
      typeOrAnyVarBoundAssumption qa' qa upperBound
    -- Qualifiers Lower Bound Assumption
    QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
      let cm' = cm `mappend` cmSing qa lowerBound (cmBound qs) in
      isContractive cm' &&
      or (do
        bound <- Set.toList $
                    cmBoundsOf qa (lowerBound `mappend` pol) cm
        cbound <- case bound of
                    QBQualSet qs' -> return $ CLeft qs'
                    QBQVar qa' -> return $ CRight qa'
                    _ -> mzero
        let c' = QualifiedIntermediateConstraint cbound (CLeft qs)
        return $ primitiveSubtype pol cm' c')
    -- Qualifiers Upper Bound Assumption
    QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
      let cm' = cm `mappend` cmSing qa upperBound (cmBound qs) in
      isContractive cm' &&
      or (do
        bound <- Set.toList $
                    cmBoundsOf qa (upperBound `mappend` pol) cm
        cbound <- case bound of
                    QBQualSet qs' -> return $ CLeft qs'
                    QBQVar qa' -> return $ CRight qa'
                    _ -> mzero
        let c' = QualifiedIntermediateConstraint (CLeft qs) cbound
        return $ primitiveSubtype pol cm' c')
  where
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
    typeOrAnyVarBoundAssumption sa tov boundDir =
      let cm' = cm `mappend` cmSing sa boundDir (cmBound tov) in
      isContractive cm' &&
      or (do
        bound <- Set.toList $
                    cmBoundsOf sa (mconcat [lowerBound, pol, boundDir]) cm
        let Just c' = boundToConstraint sa bound
        return $ primitiveSubtype pol cm' c')
      where
        boundToConstraint :: TVar a -> VarBound a -> Maybe Constraint
        boundToConstraint sa' bnd =
          case (sa',bnd,boundDir) of
            (UTVar{}, UBType t',Positive) -> Just $ constraint t' tov
            (UTVar{}, UBUVar a,Positive) -> Just $ constraint a tov
            (UTVar{}, UBQVar qa,Positive) -> Just $ constraint qa tov
            (QTVar{}, QBType t',Positive) -> Just $ constraint t' tov
            (QTVar{}, QBUVar a,Positive) -> Just $ constraint a tov
            (QTVar{}, QBQVar qa,Positive) -> Just $ constraint qa tov
            (UTVar{}, UBType t',Negative) -> Just $ constraint tov t'
            (UTVar{}, UBUVar a,Negative) -> Just $ constraint tov a
            (UTVar{}, UBQVar qa,Negative) -> Just $ constraint tov qa
            (QTVar{}, QBType t',Negative) -> Just $ constraint tov t'
            (QTVar{}, QBUVar a,Negative) -> Just $ constraint tov a
            (QTVar{}, QBQVar qa,Negative) -> Just $ constraint tov qa
            (QTVar{}, QBQualSet _,_) -> Nothing

{-
  = IntermediateConstraint TypeOrVar TypeOrVar
  | QualifiedLowerConstraint TypeOrVar QVar
  | QualifiedUpperConstraint QVar TypeOrVar
  | QualifiedIntermediateConstraint QualOrVar QualOrVar
  | BinaryOperatorConstraint UVar BinaryOperator UVar UVar
-}