{-# LANGUAGE CPP #-}

{-|
  This module contains the logging operations supported by K3's logging system.
  These operations are very simple and are not intended to be used directly.
  Callers will typically want to generate module-specific functions via the
  @Language.K3.Logging.Generators@ module.
-}
module Language.K3.Utils.Logger.Operations
( k3logI
, k3logIPretty
, k3logM
, k3logMPretty
, bracketLog
, bracketLogM
) where

import System.Log

import Language.K3.Utils.Pretty

#ifdef DEBUG
import Control.DeepSeq
import System.IO.Unsafe
import System.Log.Logger
#endif

{-
WARNING: This logging module makes use of unsafePerformIO to prevent polluting
the entire codebase with a logging monad.  As a result, it is *very* fragile
and, if modified incorrectly, is subject to subtle deadlocking problems.  Please
be very careful when modifying.
-}

-- |A function, similar to @Debug.Trace.trace@, to log a messge in K3 in an
--  inline fashion.
k3logI :: String -- ^The name of the module doing the logging.
       -> Priority -- ^The logging level.
       -> String -- ^The message to log.
       -> a -- ^The value to use as the result of the logging expression.
       -> a

#ifdef DEBUG
k3logI moduleName logLevel message =
  deepseq message $
  unsafePerformIO $! do
    logM moduleName logLevel message
    return id
#else
k3logI _ _ _ = id
#endif

-- |A function to log a pretty-printable value along with a prefix message.
k3logIPretty :: (Pretty a)
             => String -- ^The name of the module doing the logging.
             -> Priority -- ^The logging level.
             -> String -- ^The message prefix.
             -> a -- ^The value to use as the result of the logging expression.
             -> a
k3logIPretty moduleName logLevel prefix value =
  k3logI moduleName logLevel (boxToString $ [prefix] %+ prettyLines value) value

-- |A function to log a message in a monad.  *Note:* This routine is only
--  helpful if the monad is sufficiently strict to pull on the resulting unit.
--  A monad under @EitherT@ is strict enough; a monad under @ReaderT@ or
--  @StateT@ is not.
k3logM :: (Monad m) => String -> Priority -> String -> m ()
k3logM moduleName logLevel message =
  k3logI moduleName logLevel message $ return ()

-- |A function to log a pretty-printable value in a monad.
k3logMPretty :: (Monad m, Pretty a)
             => String -> Priority -> String -> a -> m ()
k3logMPretty moduleName logLevel prefix value =
  k3logM moduleName logLevel (boxToString $ [prefix] %+ prettyLines value)

-- |A bracket logging function designed for wrapping around a computation.
--  It expects a logging routine such as @_debugI@.  The start message is
--  generic; the end message is based on the reuslt of computation.  The "end"
--  of this bracket occurs whenever the data on which the end logging message
--  function pulls has been computed.
bracketLog :: (String -> a -> a) -> String -> (a -> String) -> a -> a
bracketLog logFn startMsg endMsgFn val =
  let result = logFn startMsg val in logFn (endMsgFn result) result

bracketLogM :: (Monad m)
            => (String -> m a -> m a) -> String -> (a -> String) -> m a -> m a
bracketLogM logFn startMsg endMsgFn val = do
  result <- logFn startMsg val
  logFn (endMsgFn result) $ return result
