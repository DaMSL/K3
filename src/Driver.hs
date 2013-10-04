-- | Primary Driver for the K3 Ecosystem.

import Options.Applicative

import Language.K3.Pretty

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options


-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
  putStrLn $ "Mode: " ++ show (mode op)
  putStrLn $ "Verbosity: " ++ show (verbosity op)
  putStrLn $ "Input: " ++ show (input op)

  case mode op of
    Compile   c -> compile c
    Interpret i -> interpret i
    Print     p -> printer p

  where compile (CompileOptions _ _) = error "Compiler not yet implemented."

        interpret (Batch r)   = putStrLn ("Role: " ++ show r) >> runBatch op (Batch r)
        interpret Interactive = error "Interactive Mode is not yet implemented."

        printer PrintAST    = parseK3Input op >>= either parseError (putStrLn . pretty)
        printer PrintSyntax = error "Syntax printer not yet implemented."

        parseError s = putStrLn $ "Could not parse input: " ++ s

-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
