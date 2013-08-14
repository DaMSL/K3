-- | Primary Driver for the K3 Ecosystem.

import Options.Applicative

import Language.K3.Driver.Batch
import Language.K3.Driver.Options

-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
    putStrLn $ "Mode: " ++ show (mode op)
    putStrLn $ "Verbosity: " ++ show (verbosity op)
    putStrLn $ "Input: " ++ show (input op)

    case mode op of
        Batch r -> putStrLn ("Role: " ++ show r) >> runBatch op
        Interactive -> error "Interactive Mode is not yet implemented."

-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
