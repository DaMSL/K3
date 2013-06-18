{-# LANGUAGE LambdaCase #-}

module Language.K3.Driver.Interactive where

import Control.Monad

import System.Console.Readline
import System.Exit

import Language.K3.Interpreter
import Language.K3.Parser
import Language.K3.Pretty

-- | Run an interactive prompt, reading evaulating and printing at each step.
runInteractive :: IO ()
runInteractive = readline ">>> " >>= \case
    Nothing -> exitSuccess
    Just input -> addHistory input >> repl input >> runInteractive

-- | A single round of parsing, interpretation and pretty-printing.
repl :: String -> IO ()
repl input = do
    case runK3Parser expr input of
        Left e -> print e
        Right t -> do
            putStr $ pretty t
            i <- runInterpretation [] $ expression t
            print i
