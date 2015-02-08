{-# LANGUAGE TemplateHaskell, ScopedTypeVariables #-}

{-|
  This module defines a simplifier which unifies variables which are constrained
  to have the same type at all times.
-}
module Language.K3.TypeSystem.Simplification.EquivalenceUnification.ConstraintEquivalence
( simplifyByConstraintEquivalenceUnification
, constraintEquivocator
) where

import Control.Arrow
import qualified Data.Map as Map
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Simplification.Common
import Language.K3.TypeSystem.Simplification.EquivalenceUnification.Common
import Language.K3.Utils.Logger

$(loggingFunctions)


-- |A routine which unifies type variables which are always constrained to be
--  equivalent as a result of immediate symmetric constraints.
simplifyByConstraintEquivalenceUnification :: Simplifier
simplifyByConstraintEquivalenceUnification =
  simplifyByEquivalenceUnification constraintEquivocator

-- |A routine which finds type variables which are always constrained to be
--  equivalent as a result of immediate symmetric constraints.
constraintEquivocator :: Equivocator
constraintEquivocator = Equivocator "constraint equivalence" f
  where
    f :: EquivocatorFunction
    f cs = do
      uvarEquivs <- findEquivPairs onlyUVar $
                      csQuery cs $ QueryAllUVarLowerBoundingUVar ()
      qvarEquivs <- findEquivPairs onlyQVar $
                      csQuery cs $ QueryAllQVarLowerBoundingQVar ()
      uvarCrossEquivs <- findProductPairs onlyUVar
                            (csQuery cs $ QueryAllUVarLowerBoundingQVar ())
                            (csQuery cs $ QueryAllQVarLowerBoundingUVar ())
      qvarCrossEquivs <- findProductPairs onlyQVar
                            (csQuery cs $ QueryAllQVarLowerBoundingUVar ())
                            (csQuery cs $ QueryAllUVarLowerBoundingQVar ())
      return ( Map.unionWith Set.union qvarEquivs qvarCrossEquivs
             , Map.unionWith Set.union uvarEquivs uvarCrossEquivs )
      where
        findProductPairs :: forall q q'.
                            (AnyTVar -> Maybe (TVar q))
                         -> [(TVar q, TVar q')]
                         -> [(TVar q', TVar q)]
                         -> SimplifyM (TVarEquivMap q)
        findProductPairs destr pairs1 pairs2 =
          -- PERF: maybe use an indexing structure to avoid the cartesian product?
          findEquivPairs destr
            [ (x1,x2)
            | (x1,y1) <- pairs1
            , (y2,x2) <- pairs2
            , y1 == y2
            ]
        findEquivPairs :: forall q.
                          (AnyTVar -> Maybe (TVar q))
                       -> [(TVar q, TVar q)]
                       -> SimplifyM (TVarEquivMap q)
        findEquivPairs destr pairs = do
          let initMap = Map.unionsWith Set.union $
                          map (uncurry Map.singleton . second Set.singleton) $
                          filterPairsBySymmetry pairs
          symmetricRelationshipMapToReplacementMap destr initMap

