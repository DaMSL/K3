{-# LANGUAGE GADTs, ScopedTypeVariables, FlexibleContexts, Rank2Types, TupleSections #-}

{-|
  This module represents the implementation of the Subtyping section of the K3
  specification.  This implementation is an adaptation of the theory presented
  in Subtyping Constrained Types by Trifonov and Smith; at the time of
  this writing, the paper can be found at
  @http://pl.cs.jhu.edu/papers/sas96.pdf@.
  
  This module defines a function @checkSubtype@ which will produce either
  @Nothing@ if its first argument can be proven to be a subtype of its second or
  @Just@ a @SubtypeError@ otherwise.  Whether a computable function exists to
  calculate the precise answer to this question is unknown at the time of this
  writing.  The approximation provided here fails in cases where the proof of
  subtyping proceeds by exhaustive enumeration of cases in the type system; in
  practice, this does not appear to be an impediment to use.
-}
module Language.K3.TypeSystem.Subtyping
( module Language.K3.TypeSystem.Subtyping.Error
, checkSubtype
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Either
import qualified Data.Foldable as Foldable
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Traversable as Trav

import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Subtyping.ConstraintMap
import Language.K3.TypeSystem.Subtyping.Error

checkSubtype :: forall m. (TypeErrorI m, FreshVarI m)
             => NormalQuantType -> NormalQuantType
             -> m (Either SubtypeError ())
checkSubtype qt1@(QuantType sas' qa' cs') qt2@(QuantType sas'' qa'' cs'') =
  -- First, ensure that the type variable sets are disjoint.  If this is not
  -- the case, then perform alpha-renaming as necessary.
  let overlap = sas' `Set.intersection` sas'' in
  if not $ Set.null overlap
    then do
      -- We're going to recurse on a quantified type which does not overlap in
      -- variable names.
      uvarMap <- freshVarsFor freshUVar $ mapMaybe onlyUVar $ Set.toList overlap
      qvarMap <- freshVarsFor freshQVar $ mapMaybe onlyQVar $ Set.toList overlap
      checkSubtype qt1 $ replaceVariables qvarMap uvarMap qt2
    else do
      -- Proceed to calculate the approximation as defined in the spec.
      let cs = csUnions [cs', cs'', csSing $ qa' <: qa'']
      let cs''' = calculateClosure cs
      let k' = kernel $ calculateClosure cs'
      mk'' <- canonicalize $ kernel $ calculateClosure cs''
      case mk'' of
        Nothing -> undefined -- TODO
        Just k'' ->
          if consistent cs'''
            then proveAll k' k'' (sas' `Set.union` sas'') cs'''
            else return $ Left $ InconsistentSubtypeClosure cs'''
  where
    freshVarsFor :: (TVarOrigin a -> m (TVar a))
                 -> [TVar a] -> m (Map (TVar a) (TVar a))
    freshVarsFor freshVar vars =
      Map.fromList <$> mapM (\x -> (x,) <$>
                          freshVar (TVarAlphaRenamingOrigin x)) vars
    proveAll :: (IsConstraintMap cmType)
             => cmType
             -> CanonicalConstraintMap
             -> Set AnyTVar
             -> ConstraintSet
             -> m (Either SubtypeError ())
    proveAll cm1 cm2 vars cs = do
      xs <- mapM checkPrimitiveSubtype constraints
      return $ mconcat <$> sequence xs
      where
        checkPrimitiveSubtype :: Constraint
                              -> m (Either SubtypeError ())
        checkPrimitiveSubtype c =
          case primitiveSubtype Set.empty cm1 cm2 c of
            Left (PrimitiveSubtypeFailureByInternalError ie) ->
              internalTypeError $ PrimitiveSubtypeInvariantViolated ie
            Left (PrimitiveSubtypeFailureByErrors errs) ->
              return $ Left $ UnprovablePrimitiveSubtype 
                  (cmToConstraintMap cm1) (cmToConstraintMap cm2) c errs
            Right () -> return $ Right ()
        constraints :: [Constraint]
        constraints = concatMap constraintsFrom $ Set.toList vars
        constraintsFrom :: AnyTVar -> [Constraint]
        constraintsFrom sa =
          case sa of
            SomeQVar qa ->
              csQuery cs (QueryBoundingConstraintsByQVar qa)
            SomeUVar a ->
              csQuery cs (QueryBoundingConstraintsByUVar a)

-- |An ADT describing the manners in which @primitiveSubtype@ can fail.
data PrimitiveSubtypeFailure
  = PrimitiveSubtypeFailureByErrors (Seq PrimitiveSubtypeError)
  | PrimitiveSubtypeFailureByInternalError InternalPrimitiveSubtypeError
  deriving (Show)

failPlus :: PrimitiveSubtypeFailure -> PrimitiveSubtypeFailure
         -> PrimitiveSubtypeFailure
failPlus x y = case (x,y) of
  (PrimitiveSubtypeFailureByInternalError _, _) -> x
  (_, PrimitiveSubtypeFailureByInternalError _) -> y
  (PrimitiveSubtypeFailureByErrors es1, PrimitiveSubtypeFailureByErrors es2) ->
    PrimitiveSubtypeFailureByErrors $ es1 Seq.>< es2

{-|
  An implementation of the primitive subtyping relation.  This function will
  determine if a proof of primitive subtyping can be constructed from the
  provided information, generating a @SubtypeError@ if it cannot.  Note that
  this function only operates on simple subtype constraints; binary operator
  constraints and other similar forms will generate an error.  It is the
  responsibility of the caller to filter as necessary.
  
  This function requires the invariant that all type variables appearing in the
  @Constraint@ appear in a key in one of the @ConstraintMap@s.  This is largely
  satisfied by the fact that constraint closure does not introduce new type
  variables.
-}
primitiveSubtype :: forall cmType. (IsConstraintMap cmType)
                 => Set Constraint
                 -> cmType
                 -> CanonicalConstraintMap
                 -> Constraint
                 -> Either PrimitiveSubtypeFailure ()
primitiveSubtype visited cm1 cm2 c =
  case c of
    -- Several forms of constraint aren't subject to primitive subtyping.
    BinaryOperatorConstraint{} -> failHard
    MonomorphicQualifiedUpperConstraint{} -> failHard
    PolyinstantiationLineageConstraint{} -> failHard
    -- Reflexivity rule.  We use the documented invariant to avoid checking for
    -- the presence of the variable in the constraint maps.
    IntermediateConstraint (CRight a) (CRight a') | a == a' ->
      success
    QualifiedIntermediateConstraint (CRight qa) (CRight qa') | qa == qa' ->
      success
    -- Revisit rule.  If the bound has already been visited, we are finished.
    _ | c `Set.member` visited -> success
    -- Known bound rules.  If the bound was already present in the constraint
    -- map, we are finished.
    IntermediateConstraint lb (CRight a)
      | knownBound lb a upperBound -> success
    IntermediateConstraint (CRight a) ub
      | knownBound ub a lowerBound -> success
    QualifiedIntermediateConstraint lb (CRight qa)
      | knownBound lb qa upperBound -> success
    QualifiedIntermediateConstraint (CRight qa) ub
      | knownBound ub qa lowerBound -> success
    QualifiedLowerConstraint lb qa
      | knownBound lb qa upperBound -> success
    QualifiedUpperConstraint qa ub
      | knownBound ub qa lowerBound -> success
    -- Decomposition rule.  Perform closure on this constraint and proceed on
    -- each result.
    IntermediateConstraint (CLeft t1) (CLeft t2) ->
      let cs = calculateClosure $ csSing c in
      let cs' = filter (==c) $ csToList cs in
      if inconsistent cs
        then failSoft $ InconsistentSubtypeTypeDecomposition t1 t2
        else requireAll $ map recurse cs'
    -- Qualifier Decomposition.
    QualifiedIntermediateConstraint (CLeft qs1) (CLeft qs2) ->
      unless (qs1 `Set.isSubsetOf` qs2) $ failSoft $
        InconsistentSubtypeQualifierDecomposition qs1 qs2
    -- Bound rules.
    IntermediateConstraint (CLeft t) (CRight a) ->
      twoBoundRules upperBound a t
    IntermediateConstraint (CRight a) (CLeft t) ->
      twoBoundRules lowerBound a t
    IntermediateConstraint (CRight a1) (CRight a2) ->
      requireOne [ twoBoundRules lowerBound a1 a2
                 , twoBoundRules upperBound a2 a1 ]
    QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
      twoBoundRules upperBound qa qs
    QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
      twoBoundRules lowerBound qa qs
    QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
      requireOne [ twoBoundRules lowerBound qa1 qa2
                 , twoBoundRules upperBound qa2 qa1 ]
    QualifiedLowerConstraint (CLeft t) qa ->
      twoBoundRules lowerBound qa t
    QualifiedLowerConstraint (CRight a) qa ->
      requireOne [ twoBoundRules lowerBound a qa
                 , twoBoundRules upperBound qa a ]
    QualifiedUpperConstraint qa (CLeft t) ->
      twoBoundRules upperBound qa t
    QualifiedUpperConstraint qa (CRight a) ->
      requireOne [ twoBoundRules lowerBound qa a
                 , twoBoundRules upperBound a qa ]
  where
    recurse = primitiveSubtype (Set.insert c visited) cm1 cm2
    success = return ()
    failSoft :: PrimitiveSubtypeError -> Either PrimitiveSubtypeFailure ()
    failSoft = Left . PrimitiveSubtypeFailureByErrors . Seq.singleton
    failHard = error $ "Primitive subtyping was called on incorrect form of "
                    ++ "constraint (this should never happen!): "
                    ++ show c
    requireAll :: [Either PrimitiveSubtypeFailure ()]
               -> Either PrimitiveSubtypeFailure ()
    requireAll xs =
      let (errs,_) = partitionEithers xs in
      unless (null errs) $ Left $ foldl1 failPlus errs
    requireOne :: [Either PrimitiveSubtypeFailure ()]
               -> Either PrimitiveSubtypeFailure ()
    requireOne xs = do
      let (errs',succs) = partitionEithers xs
      errs <- mapM getErrors errs'
      when (null succs) $
        failSoft $ DisjunctiveSubtypeProofFailure $ concat $
                  fmap Foldable.toList errs
      where
        getErrors :: PrimitiveSubtypeFailure
                  -> Either PrimitiveSubtypeFailure (Seq PrimitiveSubtypeError)
        getErrors psf = case psf of
          PrimitiveSubtypeFailureByInternalError _ -> Left psf
          PrimitiveSubtypeFailureByErrors errs -> Right errs
    knownBound :: (ConstraintMapBoundable b q, Ord (VarBound q))
               => b -> TVar q -> TPolarity -> Bool
    knownBound bound v boundSide =
      let f :: (IsConstraintMap cmt) => cmt -> Bool
          f cm = cmBound bound `Set.member` cmBoundsOf v boundSide cm in
      f cm1 || f cm2
    -- |Generally represents the bound rules in the specification.
    --  Two polarities appear in this function's arguments.  The first polarity
    --  controls the variance of this rule: when @pol@ is @Positive@, the
    --  "Right" rules are used and the constraint is contracting; when @pol@ is
    --  @Negative@, the "Left" rules are used and the constraint is expanding.
    --  The second polarity together with the variable and generic bound
    --  represent a constraint; when @boundDir@ is @lowerBound@, @v@ is a lower
    --  bound of @bnd@; when @boundDir@ is @upperBound@, the opposite is true.
    generalBoundRule :: forall cmt q.
                        (IsConstraintMap cmt, BoundsToConstraint q)
                     => TPolarity
                     -> cmt
                     -> TPolarity
                     -> TVar q
                     -> VarBound q
                     -> Either PrimitiveSubtypeFailure ()
    generalBoundRule pol cm boundDir v bnd =
      let bounds = Set.toList $ cmBoundsOf v (pol `mappend` boundDir) cm in
      requireOne $ map checkOneBound bounds
      where
        checkOneBound :: VarBound q -> Either PrimitiveSubtypeFailure ()
        checkOneBound bnd' = case boundsToConstraint boundDir bnd bnd' of
                                Just c' -> recurse c'
                                Nothing -> return ()
                                  -- The nothing case occurs when the provided
                                  -- bound can't be used to construct the
                                  -- constraint
    twoBoundRules :: forall q c.
                     (BoundsToConstraint q, ConstraintMapBoundable c q)
                  => TPolarity -> TVar q -> c
                  -> Either PrimitiveSubtypeFailure ()
    twoBoundRules boundDir v rawBnd =
      let bnd = cmBound rawBnd in
      let leftResult = generalBoundRule Negative cm1 boundDir v bnd in
      let rightResult = generalBoundRule Positive cm2 boundDir v bnd in
      requireOne [leftResult, rightResult]

{-
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
    -- Several forms of constraint aren't subject to primitive subtyping.
    (BinaryOperatorConstraint{},_) -> False
    (MonomorphicQualifiedUpperConstraint{},_) -> False
    (PolyinstantiationLineageConstraint{},_) -> False
    -- Reflexivity rule.  We use the documented invariant to avoid checking for
    -- the presence of the variable in each case.
    (IntermediateConstraint (CRight a) (CRight a'),_) | a == a' -> True
    (QualifiedIntermediateConstraint (CRight qa) (CRight qa'),_) | qa == qa' ->
      True
    -- Revisit rule.  If the bound has already been visited, we are finished.
    _ | c `Set.member` visited -> True
    -- Known bound rules.  If the bound was already present in the constraint
    -- map, we are finished.
    (IntermediateConstraint lb (CRight a),_)
      | cmBound lb `Set.member` cmBoundsOf a upperBound cm -> True
    (IntermediateConstraint (CRight a) ub,_)
      | cmBound ub `Set.member` cmBoundsOf a lowerBound cm -> True
    (QualifiedIntermediateConstraint lb (CRight qa),_)
      | cmBound lb `Set.member` cmBoundsOf qa upperBound cm -> True
    (QualifiedIntermediateConstraint (CRight qa) ub,_)
      | cmBound ub `Set.member` cmBoundsOf qa lowerBound cm -> True
    (QualifiedLowerConstraint lb qa,_)
      | cmBound lb `Set.member` cmBoundsOf qa upperBound cm -> True
    (QualifiedUpperConstraint qa ub,_)
      | cmBound ub `Set.member` cmBoundsOf qa lowerBound cm -> True
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
-}