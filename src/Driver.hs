-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Data.Char

import Options.Applicative

import Language.K3.Codegen.Haskell
import Language.K3.TypeSystem

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

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
    Compile   c -> compile' c
    Interpret i -> interpret i
    Print     p -> printer p

  where compile' (CompileOptions lang out) = case maybe "" (map toLower) lang of 
          "types"   -> k3Program >>= either parseError (putStrLn . show . typecheckProgram)
          "haskell" -> do
                          let progName = maybe "A.hs" id out
                          prog <- k3Program 
                          either parseError (doCompile progName) prog
          _         -> error "Compiler not yet implemented."

        doCompile n p = either compileError putStrLn $ compile (generate n p)

        interpret im@(Batch _ _) = runBatch op im
        interpret Interactive    = error "Interactive Mode is not yet implemented."

        printer PrintAST    = k3Program >>= either parseError (putStrLn . pretty)
        printer PrintSyntax = k3Program >>= either parseError printProgram
        k3Program           = parseK3Input (includes $ paths op) (input op)
        printProgram        = either syntaxError putStrLn . programS

        parseError s   = putStrLn $ "Could not parse input: " ++ s
        syntaxError s  = putStrLn $ "Could not print program: " ++ s
        compileError s = putStrLn $ "Could not generate code: " ++ s


-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
