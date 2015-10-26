{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

-- Type definitions to avoid circular imports.
module Language.K3.Codegen.CPP.Materialization.Hints where

import Control.DeepSeq
import Data.Binary
import Data.Hashable
import Data.Serialize
import Data.Typeable
import GHC.Generics (Generic)

data Method
  = ConstReferenced
  | Referenced
  | Moved
  | Copied
  | Forwarded
 deriving (Eq, Ord, Read, Show, Typeable, Generic)

defaultMethod :: Method
defaultMethod = Copied

data Direction = In | Ex deriving (Eq, Ord, Read, Show, Typeable, Generic)

{- Typeclass Instances -}
instance NFData Method
instance NFData Direction

instance Binary Method
instance Binary Direction

instance Serialize Method
instance Serialize Direction

instance Hashable Method
instance Hashable Direction
