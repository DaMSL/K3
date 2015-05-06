{-# LANGUAGE TemplateHaskell #-}

{-|
  This module contains K3's logging facilities.  It provides access to both
  logging configuration and actual logging operations.  It is primarily a
  set of thin convenience wrappers and code generators which make use of
  HSlogger.  Logging targets are assumed to be module names.
-}
module Language.K3.Utils.Logger
( module X,
  logVoid,
  logAction
) where

import Language.K3.Utils.Logger.Config as X
import Language.K3.Utils.Logger.Generators as X
import Language.K3.Utils.Logger.Operations as X

import Control.Monad
import Debug.Trace

$(loggingFunctions)

-- | Simple monadic logging.
--   These functions are useful for debugging while developing, since it
--   does not require a codebase rebuild with cabal debugging flags.
logVoid :: (Functor m, Monad m) => Bool -> String -> m ()
logVoid asTrace s = if asTrace then trace s $ return ()
                               else void $ _debug s

logAction :: (Functor m, Monad m) => Bool -> (Maybe a -> Maybe String) -> m a -> m a
logAction asTrace msgF action = do
 doLog (msgF Nothing)
 result <- action
 doLog (msgF $ Just result)
 return result
 where doLog Nothing  = return ()
       doLog (Just s) = logVoid asTrace s
