{-# LANGUAGE TemplateHaskell, FlexibleContexts #-}

{-|
  This module defines a simplifier which can be used to unify variables that
  participate in a structural equivalence.
-}
module Language.K3.TypeSystem.Simplification.EquivalenceUnification.StructuralEquivalence
( simplifyByStructuralEquivalenceUnification
, structuralEquivocator
) where

import Control.Arrow
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
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

-- * Equivocators

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
          listToMaybe $ proveMutuallyWithin (v1, cs) (v2, cs) 
      
