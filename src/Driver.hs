-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Data.Char

import Options.Applicative

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Transform.Conflicts

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck
import qualified Language.K3.Compiler.Haskell as HaskellC
import qualified Language.K3.Compiler.CPP as CPPC

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
    Typecheck   -> k3Program >>= either parseError typecheck
    Analyze   a -> analyzer a
  where compile cOpts@(CompileOptions lang _ _ _) = case map toLower lang of
          "haskell" -> HaskellC.compile op cOpts
          "cpp" -> CPPC.compile op cOpts
          _         -> error $ lang ++ " compilation not supported."

        interpret im@(Batch {}) = runBatch op im
        interpret Interactive    = error "Interactive Mode is not yet implemented."

        printer PrintAST    = k3Program >>= either parseError (putStrLn . pretty)
        printer PrintSyntax = k3Program >>= either parseError printProgram
        k3Program           = parseK3Input (includes $ paths op) (input op)
        printProgram        = either syntaxError putStrLn . programS

        analyzer Conflicts  = k3Program 
           >>= either parseError (putStrLn . pretty . startConflicts . startAnnotate)
        
        parseError s   = putStrLn $ "Could not parse input: " ++ s
        syntaxError s  = putStrLn $ "Could not print program: " ++ s


-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
