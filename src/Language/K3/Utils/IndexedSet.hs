{-|
  This module defines routines and utilities associated with the creation and
  use of a data structure which is equivalent to a set but maintains a number of
  indexes for quick containment checks.  The containment checks must be
  enumerated statically.
-}
module Language.K3.Utils.IndexedSet
( module X
) where

import Language.K3.Utils.IndexedSet.Class as X
import Language.K3.Utils.IndexedSet.TemplateHaskell as X
