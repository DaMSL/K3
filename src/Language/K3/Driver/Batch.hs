module Language.K3.Driver.Batch where

import System.IO

import Language.K3.Driver.Interactive

runBatch :: Handle -> IO ()
runBatch h = hGetContents h >>= repl
