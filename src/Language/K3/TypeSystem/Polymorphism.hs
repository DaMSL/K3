{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, GADTs, ScopedTypeVariables #-}
{-|
  This module contains functionality related to K3's let-bound polymorphism
  model.
-}
module Language.K3.TypeSystem.Polymorphism
( generalize
, polyinstantiate
) where

import Control.Applicative
import Control.Monad.Reader
import Control.Monad.Trans.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Core.Common
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Morphisms.ReplaceVariables

-- * Generalization

-- |Generalizes a type to produce a quantified type.
generalize :: TNormEnv -> QVar -> ConstraintSet -> QuantType
generalize (TEnv env) qa cs =
  let cs' = calculateClosure cs in
  let reachableQVars = Set.map SomeQVar $ Set.fromList $
                          runReader (runListT $ reachableFromQVar qa) cs' in
  let freeEnvVars = Set.unions $ map openVars $ Map.elems env in
  let quantSet = extractVariables cs' `Set.difference` reachableQVars
                                      `Set.difference` freeEnvVars in
  QuantType quantSet qa cs
  where
    openVars :: QuantType -> Set AnyTVar
    openVars (QuantType bound var cs'') =
      Set.insert (SomeQVar var) (extractVariables cs'') `Set.difference` bound

type Reachability = ListT (Reader ConstraintSet) QVar

reachableUnions :: [Reachability] -> Reachability
reachableUnions xs = do
  cs <- ask
  let rs = map (\x -> runReader (runListT x) cs) xs
  ListT $ return $ concat rs

reachableFromQVar :: QVar -> Reachability
reachableFromQVar qa =
  reachableUnions [immediate, lowerBound]
  where
    immediate = do
      cs <- ask
      tqs <- ListT $ return $ csQuery cs $ QueryTQualSetByQVarUpperBound qa
      guard $ TMut `Set.member` tqs
      return qa
    lowerBound = do
      cs <- ask
      t <- ListT $ return $ csQuery cs $ QueryTypeByQVarUpperBound qa
      reachableFromType t

reachableFromType :: ShallowType -> Reachability
reachableFromType t = case t of
  SOption qa -> reachableFromQVar qa
  SIndirection qa -> reachableFromQVar qa
  STuple qas -> reachableUnions $ map reachableFromQVar qas
  SRecord m -> reachableUnions $ map reachableFromQVar $ Map.elems m
  _ -> mzero

-- * Polyinstantiation

-- |Polyinstantiates a quantified type.
polyinstantiate
             :: forall m. (FreshVarI m)
             => Span -- ^The span at which this polyinstantiation occurred.
             -> QuantType -- ^The type to polyinstantiate.
             -> m (QVar, ConstraintSet) -- ^The result of polyinstantiation.
polyinstantiate inst (QuantType boundSet qa cs) = do
  (qvarMap,uvarMap) <- mconcat <$> mapM freshMap (Set.toList boundSet)
  return $ replaceVariables qvarMap uvarMap (qa,cs)
  where
    freshMap :: AnyTVar -> m (Map QVar QVar, Map UVar UVar)
    freshMap var =
      case var of
        SomeQVar qa' -> do
          qa'' <- freshVar $ TVarPolyinstantiationOrigin qa' inst
          return (Map.singleton qa' qa'', Map.empty)
        SomeUVar a' -> do
          a'' <- freshVar $ TVarPolyinstantiationOrigin a' inst
          return (Map.empty, Map.singleton a' a'')
