{-# LANGUAGE TemplateHaskell, FlexibleContexts, GeneralizedNewtypeDeriving, TypeSynonymInstances, FlexibleInstances, ScopedTypeVariables, GADTs #-}

{-|
  This module defines a simplifier which can be used to unify variables that
  participate in a structural equivalence.
-}
module Language.K3.TypeSystem.Simplification.EquivalenceUnification.StructuralEquivalence
( simplifyByStructuralEquivalenceUnification
, structuralEquivocator
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Simplification.Common
import Language.K3.TypeSystem.Simplification.EquivalenceUnification.Common
import Language.K3.TypeSystem.Within
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |A routine to identify type variables that are part of a structural
--  equivalence.  If two type variables a1 and a2 in the set can be shown to be
--  structurally identical (e.g. every constraint imposed upon them recursively
--  is the same up to alpha renaming), then it is safe to replace all variables
--  in one structure with the variables from the other.
simplifyByStructuralEquivalenceUnification :: Simplifier
simplifyByStructuralEquivalenceUnification =
  simplifyByEquivalenceUnification structuralEquivocator

-- |A routine to identify type variables that are part of a structural
--  equivalence.  If two type variables a1 and a2 in the set can be shown to be
--  structurally identical (e.g. every constraint imposed upon them recursively
--  is the same up to alpha renaming), then it is safe to replace all variables
--  in one structure with the variables from the other.
structuralEquivocator :: Equivocator
structuralEquivocator = Equivocator "structural equivalence" f
  where
    f :: EquivocatorFunction
    f cs = do
      let vars = Set.toList $ extractVariables cs
      let equivsOverUVars = performTests $ mapMaybe onlyUVar vars
      let equivsOverQVars = performTests $ mapMaybe onlyQVar vars
      let (qvarPreEquivs, uvarPreEquivs) =
            mconcatMultiMaps *** mconcatMultiMaps $
              unzip [equivsOverQVars, equivsOverUVars]
      uvarEquivs <- symmetricRelationshipMapToReplacementMap
                      onlyUVar uvarPreEquivs
      qvarEquivs <- symmetricRelationshipMapToReplacementMap
                      onlyQVar qvarPreEquivs
      return (qvarEquivs, uvarEquivs)
      where
        performTests :: (WithinAlignable (TVar q)) => [TVar q] -> VarEquivMap
        performTests vars =
          -- Set up the structural equivalence tests.  Each result is a proof
          -- of equivalence carrying multiple variable equivalences.
          let equivalences = mapMaybe (uncurry testStructuralEquivalence)
                              [ (v1,v2) | v1 <- vars, v2 <- vars, v1 /= v2 ] in
          -- Unfortunately, for an equivalence involving N variables, there are
          -- N ways to find it.  This means that using all of those proofs would
          -- perform a lot of wasted operations.  So we'll just take the first
          -- proof and send it back, assuming that the caller will do the
          -- substitution and then try again.
          let mapOp = Map.map Set.singleton in
          (mapOp *** mapOp) $
            fromMaybe (Map.empty, Map.empty) $
              listToMaybe equivalences
        testStructuralEquivalence :: (WithinAlignable (TVar q))
                                  => TVar q -> TVar q
                                  -> Maybe (Map QVar QVar, Map UVar UVar)
        testStructuralEquivalence v1 v2 =
          listToMaybe $ proveStructuralEquivalence cs v1 v2

{-
This section defines a monad for structural equivalence.  This monad is used
internally by this module and nowhere else.  It is separate from the Within
module's content because it operates under the assumption that both entities
have the same constraint set; this allows it to be specialized.
-}

-- |Attempts to find proofs of structural equivalence between two type variables
--  in a given constraint set.  If any such proofs exist, they are returned as
--  variable substitution mappings.
proveStructuralEquivalence :: ConstraintSet -> TVar q -> TVar q
                           -> [VariableSubstitution]
proveStructuralEquivalence cs var var' =
  let computation = align (someVar var) (someVar var') in
  map snd $ runStateT (runReaderT (unStructEquivM computation) cs)
                      (Map.empty, Map.empty)

-- |A monad under which structural equivalence checks are performed.  This monad
--  is a list monad in which each result is a single proof of alignment.  The
--  proof includes a state monad to build up a variable alignment map.
newtype StructEquivM a
  = StructEquivM
      { unStructEquivM :: ReaderT ConstraintSet
                            (StateT VariableSubstitution [])
                              a }
  deriving ( Monad, MonadPlus, MonadReader ConstraintSet
           , MonadState VariableSubstitution, Functor, Applicative)

-- |A typeclass for forcing the alignment of variables in a structural
--  equivalence proof.
class StructEquivAlign a where
  align :: a -> a -> StructEquivM ()

instance StructEquivAlign (Set Constraint) where
  align cs1 cs2 = case (Set.null cs1, Set.null cs2) of
    (True,True) -> return ()
    (False,True) -> mzero
    (True,False) -> mzero
    (False,False) -> do
      let c1 = Set.findMin cs1
      c2 <- StructEquivM $ lift $ lift $ Set.toList cs2
      align c1 c2
      align (Set.delete c1 cs1) (Set.delete c2 cs2)

instance StructEquivAlign Constraint where
  align c1 c2 = case (c1,c2) of
    (IntermediateConstraint tov1 tov2, IntermediateConstraint tov1' tov2') ->
      align tov1 tov1' >> align tov2 tov2'
    (IntermediateConstraint _ _, _) -> mzero
    (QualifiedLowerConstraint tov qa, QualifiedLowerConstraint tov' qa') ->
      align tov tov' >> align qa qa'
    (QualifiedLowerConstraint _ _, _) -> mzero
    (QualifiedUpperConstraint qa tov, QualifiedUpperConstraint qa' tov') ->
      align qa qa' >> align tov tov'
    (QualifiedUpperConstraint _ _, _) -> mzero
    (   QualifiedIntermediateConstraint qov1 qov2
      , QualifiedIntermediateConstraint qov1' qov2') ->
      align qov1 qov1' >> align qov2 qov2'
    (QualifiedIntermediateConstraint _ _, _) -> mzero
    (   MonomorphicQualifiedUpperConstraint qa qs
      , MonomorphicQualifiedUpperConstraint qa' qs') ->
      align qs qs' >> align qa qa'
    (MonomorphicQualifiedUpperConstraint _ _, _) -> mzero
    (   PolyinstantiationLineageConstraint qa1 qa2
      , PolyinstantiationLineageConstraint qa1' qa2') ->
      align qa1 qa1' >> align qa2 qa2'
    (PolyinstantiationLineageConstraint _ _, _) -> mzero
    (   OpaqueBoundConstraint oa tov1 tov2
      , OpaqueBoundConstraint oa' tov1' tov2') ->
      guard (oa == oa') >> align tov1 tov1' >> align tov2 tov2'
    (OpaqueBoundConstraint _ _ _, _) -> mzero

instance (StructEquivAlign a, StructEquivAlign b)
      => StructEquivAlign (Coproduct a b) where
  align x y = case (x,y) of
    (CLeft x', CLeft y') -> align x' y'
    (CRight x', CRight y') -> align x' y'
    (CLeft _, CRight _) -> mzero
    (CRight _, CLeft _) -> mzero

instance StructEquivAlign ShallowType where
  align x y = case (x,y) of
    (SFunction a1 a2, SFunction a1' a2') -> align a1 a1' >> align a2 a2'
    (SFunction _ _, _) -> mzero
    (STrigger a, STrigger a') -> align a a'
    (STrigger _, _) -> mzero
    (SBool, SBool) -> success
    (SBool, _) -> mzero
    (SInt, SInt) -> success
    (SInt, _) -> mzero
    (SReal, SReal) -> success
    (SReal, _) -> mzero
    (SNumber, SNumber) -> success
    (SNumber, _) -> mzero
    (SString, SString) -> success
    (SString, _) -> mzero
    (SAddress, SAddress) -> success
    (SAddress, _) -> mzero
    (SOption a, SOption a') -> align a a'
    (SOption _, _) -> mzero
    (SIndirection a, SIndirection a') -> align a a'
    (SIndirection _, _) -> mzero
    (STuple as, STuple a's) -> do
      guard $ length as == length a's
      mconcat <$> zipWithM align as a's
    (STuple _, _) -> mzero
    (SRecord m oas _, SRecord m' oas' _) -> do
      guard $ oas == oas'
      guard $ Map.keys m == Map.keys m'
      mconcat <$> mapM (uncurry align)
                       (Map.elems $ Map.intersectionWith (,) m m')
    (SRecord _ _ _, _) -> mzero
    (STop, STop) -> success
    (STop, _) -> mzero
    (SBottom, SBottom) -> success
    (SBottom, _) -> mzero
    (SOpaque oa, SOpaque oa') -> guard $ oa == oa'
    (SOpaque _, _) -> mzero
    where success = return ()

instance StructEquivAlign (Set TQual) where
  align qs1 qs2 = guard $ qs1 == qs2

instance StructEquivAlign AnyTVar where
  align var var' = case (var,var') of
    (SomeUVar a, SomeUVar a') ->
      doVarAlign snd second QueryBoundingConstraintsByUVar a a'
    (SomeQVar qa, SomeQVar qa') ->
      doVarAlign fst first QueryBoundingConstraintsByQVar qa qa'
    (SomeUVar _, SomeQVar _) ->
      mzero
    (SomeQVar _, SomeUVar _) ->
      mzero

instance StructEquivAlign UVar where
  align = doVarAlign snd second QueryBoundingConstraintsByUVar

instance StructEquivAlign QVar where
  align = doVarAlign fst first QueryBoundingConstraintsByQVar

doVarAlign :: (VariableSubstitution -> Map (TVar q) (TVar q))
           -> (   (Map (TVar q) (TVar q) -> Map (TVar q) (TVar q))
                -> VariableSubstitution
                -> VariableSubstitution  )
           -> (TVar q -> ConstraintSetQuery Constraint)
           -> TVar q
           -> TVar q
           -> StructEquivM ()
doVarAlign tupleAccess tupleUpdate boundingQuery var var' = do
  substMap <- tupleAccess <$> get
  case Map.lookup var substMap of
    Just var'' -> guard $ var' == var''
    Nothing -> do
      modify $ tupleUpdate $ Map.insert var var'
      cs <- ask
      align (Set.fromList $ csQuery cs $ boundingQuery var)
            (Set.fromList $ csQuery cs $ boundingQuery var')
