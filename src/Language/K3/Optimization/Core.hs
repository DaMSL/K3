module Language.K3.Optimization.Core where

import qualified Data.Set as S

import Language.K3.Core.Common

data OptHint
    -- | The sets of identifiers which can be bound by reference, by copy without writeback, and by
    -- copy with writeback.
    = BindHint (S.Set Identifier, S.Set Identifier, S.Set Identifier)

    -- | Whether or not the argument in an application can be passed in as-is (True), or with a
    -- move.
    | PassHint Bool

    -- | Whether or not a function should accept its argument by copy (True) or by reference.
    | FuncHint Bool
  deriving (Eq, Read, Show)
