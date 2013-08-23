{-|
  This module contains utilities related to the @Either@ type.
-}
module Language.K3.Utils.Either
( gatherParallelErrors
) where

import Data.Either
import Data.Monoid

-- |Using @Either@ as a mechanism for error handling, perform numerous
--  computations concurrently.  Error terms must be monoidal and thus are
--  concatenated if any errors occr.
gatherParallelErrors :: (Monoid a) => [Either a b] -> Either a [b]
gatherParallelErrors xs =
  let (errs,results) = partitionEithers xs in
  if null errs then Right results else Left $ mconcat errs
