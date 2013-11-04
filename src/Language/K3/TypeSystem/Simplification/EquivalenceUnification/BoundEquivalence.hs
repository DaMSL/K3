{-# LANGUAGE TemplateHaskell #-}

{-|
  This module defines a simplifier which unifies type variables that have the
  exact same sets of lower and upper bounds.
-}
module Language.K3.TypeSystem.Simplification.EquivalenceUnification.BoundEquivalence
( simplifyByBoundEquivalenceUnification
, boundEquivocator
) where

import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Simplification.Common
import Language.K3.TypeSystem.Simplification.EquivalenceUnification.Common
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |A routine which unifies equivalent variables by a common set of upper
--  and lower bounds.  This routine is safe to use only on closed constraint
--  sets.
simplifyByBoundEquivalenceUnification :: Simplifier
simplifyByBoundEquivalenceUnification =
  simplifyByEquivalenceUnification boundEquivocator

-- |A routine which identifies equivalent variables by a common set of upper
--  and lower bounds.  This routine is safe to use only on closed constraint
--  sets.
boundEquivocator :: Equivocator
boundEquivocator = Equivocator "bound equivalence" f
  where
    f :: EquivocatorFunction
    f cs = do
      let vars = extractVariables cs
      uvarEquivs <- findEquivsByIdentification onlyUVar identifyUVarBounds $
                      mapMaybe onlyUVar $ Set.toList vars
      qvarEquivs <- findEquivsByIdentification onlyQVar identifyQVarBounds $
                      mapMaybe onlyQVar $ Set.toList vars
      return (qvarEquivs, uvarEquivs)
      where
        identifyUVarBounds :: UVar
                           -> SimplifyM (Set ShallowType, Set ShallowType)
        identifyUVarBounds a = do
          let lbs = Set.fromList $ csQuery cs $ QueryTypeByUVarUpperBound a
          let ubs = Set.fromList $ csQuery cs $ QueryTypeByUVarLowerBound a
          return (lbs,ubs)
        identifyQVarBounds :: QVar
                           -> SimplifyM ( Set ShallowType
                                        , Set ShallowType
                                        , Set (Set TQual)
                                        , Set (Set TQual) )
        identifyQVarBounds qa = do
          let lbs = Set.fromList $ csQuery cs $ QueryTypeByQVarUpperBound qa
          let ubs = Set.fromList $ csQuery cs $ QueryTypeByQVarLowerBound qa
          let lqbs = Set.fromList $ csQuery cs $
                        QueryTQualSetByQVarUpperBound qa
          let uqbs = Set.fromList $ csQuery cs $
                        QueryTQualSetByQVarLowerBound qa
          return (lbs,ubs,lqbs,uqbs)

