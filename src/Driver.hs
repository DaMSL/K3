-- | Primary Driver for the K3 Ecosystem.

import Control.Monad

import System.IO

import Options.Applicative

import Language.K3.Driver.Options

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
    putStrLn $ "Mode: " ++ show (mode op)
    putStrLn $ "Verbosity: " ++ show (verbosity op)
    putStrLn $ "Input: " ++ show (input op)

-- | Top-Level.
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
