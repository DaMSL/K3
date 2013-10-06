{-|
  This module contains K3's logging facilities.  It provides access to both
  logging configuration and actual logging operations.  It is primarily a
  set of thin convenience wrappers and code generators which make use of
  HSlogger.  Logging targets are assumed to be module names.
-}
module Language.K3.Utils.Logger
( module X
) where

import Language.K3.Utils.Logger.Config as X
import Language.K3.Utils.Logger.Generators as X
import Language.K3.Utils.Logger.Operations as X
