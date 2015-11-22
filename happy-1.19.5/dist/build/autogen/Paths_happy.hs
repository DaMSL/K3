module Paths_happy (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName, getSysconfDir
  ) where

import qualified Control.Exception as Exception
import Data.Version (Version(..))
import System.Environment (getEnv)
import Prelude

catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch

version :: Version
version = Version [1,19,5] []
bindir, libdir, datadir, libexecdir, sysconfdir :: FilePath

bindir     = "/home-4/yliu120@jhu.edu/.cabal/bin"
libdir     = "/home-4/yliu120@jhu.edu/.cabal/lib/x86_64-linux-ghc-7.10.2/happy-1.19.5-IVgO6IWbp3Z8Cs1ThxCaP0"
datadir    = "/home-4/yliu120@jhu.edu/.cabal/share/x86_64-linux-ghc-7.10.2/happy-1.19.5"
libexecdir = "/home-4/yliu120@jhu.edu/.cabal/libexec"
sysconfdir = "/home-4/yliu120@jhu.edu/.cabal/etc"

getBinDir, getLibDir, getDataDir, getLibexecDir, getSysconfDir :: IO FilePath
getBinDir = catchIO (getEnv "happy_bindir") (\_ -> return bindir)
getLibDir = catchIO (getEnv "happy_libdir") (\_ -> return libdir)
getDataDir = catchIO (getEnv "happy_datadir") (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "happy_libexecdir") (\_ -> return libexecdir)
getSysconfDir = catchIO (getEnv "happy_sysconfdir") (\_ -> return sysconfdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
