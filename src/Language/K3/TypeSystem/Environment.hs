{-|
  Defines environment operations.
-}
module Language.K3.TypeSystem.Environment
( envMerge
) where

import qualified Data.Map as Map

import Language.K3.Core.Type.Data

envMerge :: TEnv a -> TEnv a -> TEnv a
envMerge (TEnv x) (TEnv y) = TEnv $ Map.union y x
