{-|
  This module contains general type manipulation utilities.
-}
module Language.K3.TypeSystem.Utils
( recordOf
) where

import Control.Monad
import Data.Set as Set
import Data.Map as Map

import Language.K3.TypeSystem.Data

-- |Concatenates a set of concrete record types.  @Nothing@ is produced if
--  any of the types are not records (or top) or if the record types overlap.
recordOf :: [ShallowType] -> Maybe ShallowType
recordOf = foldM concatRecs (SRecord Map.empty) . Prelude.filter (/=STop)
  where
    concatRecs :: ShallowType -> ShallowType -> Maybe ShallowType
    concatRecs t1 t2 =
      case (t1,t2) of
        (SRecord m1, SRecord m2) ->
          if Set.null $ Set.fromList (Map.keys m1) `Set.intersection`
                        Set.fromList (Map.keys m2)
            then Just $ SRecord $ m1 `Map.union` m2
            else Nothing
        _ -> Nothing
