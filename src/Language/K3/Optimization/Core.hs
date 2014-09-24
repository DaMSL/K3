module Language.K3.Optimization.Core where

import qualified Data.Set as S

import Language.K3.Core.Common

data OptHint
    -- | The sets of identifiers which can be bound by reference, by copy without writeback, and by
    -- copy with writeback.
    = BindHint (S.Set Identifier, S.Set Identifier, S.Set Identifier)
  deriving (Eq, Read, Show)
