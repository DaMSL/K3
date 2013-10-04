module Language.K3.Driver.Common where

import Data.Functor

import System.IO

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Parser
import Language.K3.Driver.Options

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

parseK3Input :: Options -> IO (Either String (K3 Declaration))
parseK3Input opts = do
    h <- openFileOrStdIn $ input opts
    parseK3 <$> hGetContents h


