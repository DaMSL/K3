module Language.K3.Transform.Hints where

import qualified Data.Set as S

import Language.K3.Core.Common

data OptHint
    -- | The sets of identifiers which can be bound by reference, by copy without writeback, and by
    -- copy with writeback.
    = BindHint (S.Set Identifier, S.Set Identifier, S.Set Identifier)

    -- | Whether or not the argument in an application can be passed in as-is (True), or with a
    -- move.
    | PassHint Bool

    -- | Whether or not a function's argument is read-only in its implementation.
    | FuncHint Bool

    -- | Partitioning of a function's closure capture into whether it wants the closure to be
    -- referenced, moved or copied.
    | CaptHint (S.Set Identifier, S.Set Identifier, S.Set Identifier)
  deriving (Eq, Ord, Read, Show)
