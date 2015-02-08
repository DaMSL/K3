{-|
  This module defines some simplification routines for constraint sets.  Each
  of these routines executes a different simpification algorithm and each
  algorithm provides somewhat different invariants with regards to e.g.
  equicontradiction.
-}

module Language.K3.TypeSystem.Simplification
( module X
) where

import Language.K3.TypeSystem.Simplification.Common as X
import Language.K3.TypeSystem.Simplification.EquivalenceUnification as X
import Language.K3.TypeSystem.Simplification.GarbageCollection as X
