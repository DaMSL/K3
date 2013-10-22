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
) where

import Control.DeepSeq
import System.IO.Unsafe
import System.Log
import System.Log.Logger

import Language.K3.Utils.Pretty

-- FIXME: k3logI may cause a deadlock if the expression in @value@ also invokes
--        k3logI.  Because unit does not, it appears that k3logM is safe; this
--        means generated TH functions like _debug would be safe, but _debugI
--        may not be.  Figure out how to make Haskell behave in this manner.

-- |A function, similar to @Debug.Trace.trace@, to log a messge in K3 in an
--  inline fashion.
k3logI :: String -- ^The name of the module doing the logging.
       -> Priority -- ^The logging level.
       -> String -- ^The message to log.
       -> a -- ^The value to use as the result of the logging expression.
       -> a
k3logI moduleName logLevel message value =
  deepseq message $
  unsafePerformIO $! do
    logM moduleName logLevel message
    return value
  
-- |A function to log a pretty-printable value along with a prefix message.
k3logIPretty :: (Pretty a)
             => String -- ^The name of the module doing the logging.
             -> Priority -- ^The logging level.
             -> String -- ^The message prefix.
             -> a -- ^The value to use as the result of the logging expression.
             -> a 
k3logIPretty moduleName logLevel prefix value =
  k3logI moduleName logLevel (boxToString $ [prefix] %+ prettyLines value) value

-- |A function to log a message in a monad.
k3logM :: (Monad m) => String -> Priority -> String -> m ()
k3logM moduleName logLevel message =
  k3logI moduleName logLevel message $ return ()

-- |A function to log a pretty-printable value in a monad.
k3logMPretty :: (Monad m, Pretty a)
             => String -> Priority -> String -> a -> m ()
k3logMPretty moduleName logLevel prefix value =
  k3logM moduleName logLevel (boxToString $ [prefix] %+ prettyLines value)
