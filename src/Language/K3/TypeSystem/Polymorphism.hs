{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}
{-|
  This module contains functionality related to K3's let-bound polymorphism
  model.
-}
module Language.K3.TypeSystem.Polymorphism
(
) where

import Control.Monad.Reader
import Control.Monad.Trans
import Control.Monad.Trans.List
import qualified Data.Map as Map
import qualified Data.Set as Set

import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Data

-- * Generalization

-- |Generalizes a type to produce a quantified type.
generalize :: TNormEnv -> QVar -> ConstraintSet -> QuantType
generalize env qa cs =
  let cs' = calculateClosure cs in
  let reachableQVars = runReader (runListT $ reachableFromQVar qa) cs' in
  undefined -- TODO

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
