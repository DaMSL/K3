{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

-- Type definitions to avoid circular imports.
module Language.K3.Codegen.CPP.Materialization.Hints where

import Control.DeepSeq
import Data.Typeable
import GHC.Generics (Generic)

data Method
  = ConstReferenced
  | Referenced
  | Moved
  | Copied
 deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- Decisions as pertaining to an identifier at a given expression specify how the binding is to be
-- populated (initialized), and how it is to be written back (finalized, if necessary).
data Decision = Decision { inD :: Method, outD :: Method }
              deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- The most conservative decision is to initialize a new binding by copying its initializer, and to
-- finalize it by copying it back. There might be a strictly better strategy, but this is the
-- easiest to write.
defaultDecision :: Decision
defaultDecision = Decision { inD = Copied, outD = Copied }

{- Typeclass Instances -}
instance NFData Method
instance NFData Decision
