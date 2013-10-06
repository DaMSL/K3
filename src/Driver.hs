-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Options.Applicative

import Language.K3.Logger
import Language.K3.Pretty
import Language.K3.Pretty.Syntax

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options


-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
  putStrLn $ "Mode: "      ++ pretty (mode op)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform op)
  putStrLn $ "Input: "     ++ show (input op)

  void $ mapM_ configureByInstruction $ logging $ inform op
    -- ^ Process logging directives

  case mode op of
    Compile   c -> compile c
    Interpret i -> interpret i
    Print     p -> printer p

  where compile (CompileOptions _ _) = error "Compiler not yet implemented."

        interpret im@(Batch _ _) = runBatch op im
        interpret Interactive    = error "Interactive Mode is not yet implemented."

        printer PrintAST    = parseK3Input (input op) >>= either parseError (putStrLn . pretty)
        printer PrintSyntax = parseK3Input (input op) >>= either parseError (putStrLn . programS)

        parseError s = putStrLn $ "Could not parse input: " ++ s


-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
