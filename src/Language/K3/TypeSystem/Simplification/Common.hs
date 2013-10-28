module Language.K3.TypeSystem.Simplification.Data
( SimplificationConfig(..)
, SimplifyM
, runSimplifyM
) where

import Control.Monad.Reader
import Data.Set

import Language.K3.TypeSystem.Data

-- |The record used to configure common properties of the simplification
--  routines.  The set of variables @preserveVars@ are variables which must be
--  present by the completion of the simplification process.
data SimplificationConfig
  = SimplificationConfig
      { preserveVars :: Set AnyTVar
      }

-- |The monad under which simplification occurs.
type SimplifyM = Reader SimplificationConfig

runSimplifyM :: SimplificationConfig -> SimplifyM a -> a
runSimplifyM = flip runReader
