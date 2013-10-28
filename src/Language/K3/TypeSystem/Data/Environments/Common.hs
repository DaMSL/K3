{-|
  A module containing basic data structures related to environments in the K3
  type system.  This module does not define aliases for specific environment
  types to avoid cyclic references; such aliases are declared where they are
  most relevant.
-}
module Language.K3.TypeSystem.Data.Environments.Common
( TEnv
, TEnvId(..)
) where

import Data.Map (Map)

import Language.K3.Core.Common
import Language.K3.Utils.Pretty

-- |Type environments.
type TEnv a = Map TEnvId a

-- |Type environment identifiers.
data TEnvId
  = TEnvIdentifier Identifier
  | TEnvIdContent
  | TEnvIdHorizon
  | TEnvIdFinal
  | TEnvIdSelf
  deriving (Eq, Ord, Read, Show)

instance Pretty TEnvId where
  prettyLines i = case i of
    TEnvIdentifier i' -> [i']
    TEnvIdContent -> ["content"]
    TEnvIdHorizon -> ["horizon"]
    TEnvIdFinal -> ["structure"]
    TEnvIdSelf -> ["self"]
