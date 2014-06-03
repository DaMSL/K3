{-# LANGUAGE ScopedTypeVariables, DataKinds, FlexibleInstances, FlexibleContexts, MultiParamTypeClasses, UndecidableInstances, TemplateHaskell #-}

{-|
  This module defines a routine for determining structural containment on a type
  variable and set of constraints.  This routine is used to define the "within"
  operator used in annotation concatenation.
-}

module Language.K3.TypeSystem.Within
( mutuallyWithin
, isWithin
, proveMutuallyWithin
, proveWithin
, WithinAlignable(..)
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.State
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- PERF: This whole thing uses a naive set exploration.  Just indexing the
--       constraints before the work is done could speed things up a bit.  This
--       improvement will probably be achieved just by creating a sensible
--       ConstraintSet data structure.  (To be fair, this kind of operation is
--       easily worst-case exponential; K3, however, tends not to generate many
--       union types and so it shouldn't be bad in practice.)

-- |Determines whether or not two constrained types are structurally equivalent.
mutuallyWithin :: forall c el q.
                  ( Pretty c, Ord el, WithinAlignable el
                  , WithinAlignable (TVar q)
                  , CSL.ConstraintSetLike el c
                  , CSL.ConstraintSetLikePromotable ConstraintSet c)
               => (TVar q,c) -> (TVar q,c) -> Bool
mutuallyWithin ct1 ct2 = not $ null $ proveMutuallyWithin ct1 ct2

-- |Determines whether or not one constrained type is contained by another.
isWithin :: forall c el q.
            ( Pretty c, Ord el, WithinAlignable el
            , WithinAlignable (TVar q)
            , CSL.ConstraintSetLike el c
            , CSL.ConstraintSetLikePromotable ConstraintSet c)
         => (TVar q,c) -> (TVar q,c) -> Bool
isWithin = isWithinUnder (Map.empty, Map.empty)

-- |Determines whether or not one constrained type is contained by another under
--  some set of alignment constraints.  All proofs will use the provided
--  alignment mapping.
isWithinUnder :: forall c el q.
                 ( Pretty c, Ord el, WithinAlignable el
                 , WithinAlignable (TVar q)
                 , CSL.ConstraintSetLike el c
                 , CSL.ConstraintSetLikePromotable ConstraintSet c)
              => WithinMap -> (TVar q,c) -> (TVar q,c) -> Bool
isWithinUnder m ct1 ct2 = not $ null $ proveWithinUnder m ct1 ct2

-- |Proves that two constrained types are structurally equivalent.
proveMutuallyWithin :: forall c el q.
                       ( Pretty c, Ord el, WithinAlignable el
                       , WithinAlignable (TVar q)
                       , CSL.ConstraintSetLike el c
                       , CSL.ConstraintSetLikePromotable ConstraintSet c)
                    => (TVar q,c) -> (TVar q,c) -> [WithinMap]
proveMutuallyWithin ct1 ct2 = concat $ do
  proof <- proveWithin ct1 ct2
  return $ proveWithinUnder proof ct2 ct1

-- |Proves that a constrained type is structurally contained within another.
--  The result of this function is a list of variable mapping pairs that align
--  the variables from the first set with the variables from the second set.
--  Each variable mapping that appears is a separate proof.
proveWithin :: forall c el q.
               ( Pretty c, Ord el, WithinAlignable el
               , WithinAlignable (TVar q)
               , CSL.ConstraintSetLike el c
               , CSL.ConstraintSetLikePromotable ConstraintSet c)
            => (TVar q,c) -> (TVar q,c) -> [WithinMap]
proveWithin = proveWithinUnder (Map.empty, Map.empty)

-- |Proves that a constrained type is structurally contained within another.
--  The result of this function is a list of variable mapping pairs that align
--  the variables from the first set with the variables from the second set.
--  Each variable mapping that appears is a separate proof.
proveWithinUnder :: forall c el q.
                    ( Pretty c, Ord el, WithinAlignable el
                    , WithinAlignable (TVar q)
                    , CSL.ConstraintSetLike el c
                    , CSL.ConstraintSetLikePromotable ConstraintSet c)
                 => WithinMap -> (TVar q,c) -> (TVar q,c) -> [WithinMap]
proveWithinUnder initMap (v,cs) (v',cs') =
  bracketLog _debugI
    (boxToString $
      ["Checking "] %+ prettyLines v %+ ["\\"] %+ prettyLines cs %$
      ["  within "] %+ prettyLines v' %+ ["\\"] %+ prettyLines cs')
    (\answer -> boxToString $
      ["Checking "] %+ prettyLines v %+ ["\\"] %+ prettyLines cs %$
      ["  within "] %+ prettyLines v' %+ ["\\"] %+ prettyLines cs' %$
      ["    was: "] %+
        if null answer then ["unsuccessful"] else ["successful"]) $
  let initState = (Set.fromList $ CSL.toList cs', initMap) in
  map (snd . snd) $
      runStateT
        (withinAlign v v' >> mconcat <$> mapM deduct (CSL.toList cs))
        initState
  where
    -- |Given one element, find its match and remove it.  Each step should also
    --  force alignment of the variable mapping.
    deduct :: el -> WithinM el ()
    deduct e = do
      e' <- withinStep e
      modify $ first $ Set.delete e'
      return ()
      where
        withinStep el = do
          el' <- lift =<< Set.toList . fst <$> get
          withinAlign el el'
          return el'

-- |A data type for the mappings used to align variables during the @isWithin@
--  test.
type WithinMap = (Map QVar QVar, Map UVar UVar)
-- |A monad used during the within test.  The state contains the set of
--  unmatched constraints from the right side of the relation as well as a
--  variable alignment map.  (The unmatched constraints from the left side of
--  the relation are handled in the @isWithin@ check itself.)  The overall monad
--  is the list monad, which models disjunctive computation over all suitable
--  alignment maps.
type WithinM el = StateT (Set el, WithinMap) []

success :: (Monad m) => m ()
success = return ()

class WithinAlignable t where
  withinAlign :: t -> t -> WithinM e ()
  
instance WithinAlignable Constraint where
  withinAlign c c' =
    case (c,c') of
      (IntermediateConstraint ta1 ta2, IntermediateConstraint ta1' ta2') ->
        withinAlign ta1 ta1' >> withinAlign ta2 ta2'
      (IntermediateConstraint _ _, _) -> mzero
      (QualifiedLowerConstraint ta qa, QualifiedLowerConstraint ta' qa') ->
        withinAlign ta ta' >> withinAlign qa qa'
      (QualifiedLowerConstraint _ _, _) -> mzero
      (QualifiedUpperConstraint qa ta, QualifiedUpperConstraint qa' ta') ->
        withinAlign qa qa' >> withinAlign ta ta'
      (QualifiedUpperConstraint _ _, _) -> mzero
      ( QualifiedIntermediateConstraint qv1 qv2
       ,QualifiedIntermediateConstraint qv1' qv2' ) ->
        withinAlign qv1 qv1' >> withinAlign qv2 qv2'
      (QualifiedIntermediateConstraint _ _, _) -> mzero
      ( MonomorphicQualifiedUpperConstraint qa qs
       ,MonomorphicQualifiedUpperConstraint qa' qs' ) ->
        guard (qs == qs') >> withinAlign qa qa'
      (MonomorphicQualifiedUpperConstraint _ _, _) -> mzero
      ( PolyinstantiationLineageConstraint qa1 qa2
       ,PolyinstantiationLineageConstraint qa1' qa2' ) ->
        withinAlign qa1 qa1' >> withinAlign qa2 qa2'
      (PolyinstantiationLineageConstraint _ _, _) -> mzero
      (OpaqueBoundConstraint oa t1 t2, OpaqueBoundConstraint oa' t1' t2') ->
        guard (oa == oa') >> withinAlign t1 t1' >> withinAlign t2 t2'
      (OpaqueBoundConstraint _ _ _, _) -> mzero  

instance WithinAlignable TypeOrVar where
  withinAlign ta ta' =
    case (ta, ta') of
      (CLeft t, CLeft t') -> withinAlign t t'
      (CRight a, CRight a') -> withinAlign a a'
      (CLeft _, CRight _) -> mzero
      (CRight _, CLeft _) -> mzero

instance WithinAlignable QualOrVar where
  withinAlign qv qv' =
    case (qv, qv') of
      (CLeft q, CLeft q') -> guard $ q == q'
      (CRight qa, CRight qa') -> withinAlign qa qa'
      (CLeft _, CRight _) -> mzero
      (CRight _, CLeft _) -> mzero

instance WithinAlignable QVar where
  withinAlign qa qa' = do
    mqa'' <- Map.lookup qa . fst . snd <$> get
    case mqa'' of
      Nothing -> modify $ second $ first $ Map.insert qa qa'
      Just qa'' -> guard $ qa' == qa''

instance WithinAlignable UVar where
  withinAlign a a' = do
    ma'' <- Map.lookup a . snd . snd <$> get
    case ma'' of
      Nothing -> modify $ second $ second $ Map.insert a a'
      Just a'' -> guard $ a' == a''

instance WithinAlignable ShallowType where
  withinAlign t t' =
    case (t,t') of
      (SFunction a1 a2, SFunction a1' a2') ->
        withinAlign a1 a1' >> withinAlign a2 a2'
      (SFunction _ _, _) ->
        mzero
      (STrigger a, STrigger a') ->
        withinAlign a a'
      (STrigger _, _) ->
        mzero
      (SBool, SBool) ->
        success
      (SBool, _) ->
        mzero
      (SInt, SInt) ->
        success
      (SInt, _) ->
        mzero
      (SReal, SReal) ->
        success
      (SReal, _) ->
        mzero
      (SNumber, SNumber) ->
        success
      (SNumber, _) ->
        mzero
      (SString, SString) ->
        success
      (SString, _) ->
        mzero
      (SAddress, SAddress) ->
        success
      (SAddress, _) ->
        mzero
      (SOption qa, SOption qa') ->
        withinAlign qa qa'
      (SOption _, _) ->
        mzero
      (SIndirection qa, SIndirection qa') ->
        withinAlign qa qa'
      (SIndirection _, _) ->
        mzero
      (STuple qas, STuple qas') -> do
        guard $ length qas == length qas'
        mconcat <$> zipWithM withinAlign qas qas'
      (STuple _, _) ->
        mzero
      (SRecord m oas _, SRecord m' oas' _) -> do
        guard $ oas == oas'
        guard $ Map.keys m == Map.keys m'
        mconcat <$> mapM (uncurry withinAlign)
                          (Map.elems $ Map.intersectionWith (,) m m')
      (SRecord _ _ _, _) ->
        mzero
      (STop, STop) ->
        success
      (STop, _) ->
        mzero
      (SBottom, SBottom) ->
        success
      (SBottom, _) ->
        mzero
      (SOpaque oa, SOpaque oa') ->
        guard $ oa == oa'
      (SOpaque _, _) ->
        mzero
