{-# LANGUAGE MultiParamTypeClasses, FunctionalDependencies #-}

{-|
  This module contains a typeclass used by the indexing structure code to define
  the generated structure's interface to its callers.
-}
module Language.K3.Utils.IndexedSet.Class
( IndexedSet(..)
) where

import Data.Set (Set)
import qualified Data.Set as Set

-- |Indicates that the type @s@ is an indexed structure over element type @e@
--  with query type @q@.  @q@ is a query type of kind @* -> *@ where its type
--  argument dictates the type of value returned by the query.
class IndexedSet s e q | s -> e, s -> q where
  empty :: s
  singleton :: e -> s
  insert :: e -> s -> s
  union :: s -> s -> s
  unions :: [s] -> s
  query :: s -> q r -> Set r
  toSet :: s -> Set e
  fromSet :: Set e -> s
  -- defaults implementations:
  unions = foldr union empty
  singleton = (`insert` empty)
  fromSet = foldr insert empty . Set.toList
