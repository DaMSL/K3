module Language.K3.Driver.Batch where

import Data.Functor

import System.IO

import Language.K3.Parser
import Language.K3.Pretty

import Language.K3.Driver.Options

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

runBatch :: Options -> IO ()
runBatch op = do
    h <- openFileOrStdIn $ input op

    p <- parseK3 <$> hGetContents h

    case p of
        Left e -> putStrLn e
        Right q -> putStrLn $ pretty q
