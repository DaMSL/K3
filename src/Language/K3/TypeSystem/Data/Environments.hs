{-|
  A module containing basic data structures related to environments in the K3
  type system.  This module does not define aliases for specific environment
  types to avoid cyclic references; such aliases are declared where they are
  most relevant.
-}
module Language.K3.TypeSystem.Data.Environments
( module X
) where

import Language.K3.TypeSystem.Data.Environments.Common as X
import Language.K3.TypeSystem.Data.Environments.Type as X
