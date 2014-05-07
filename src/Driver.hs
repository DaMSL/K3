-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Data.Char

import Options.Applicative

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Transform.Conflicts
import Language.K3.Transform.Interpreter.BindAlias
import Language.K3.Transform.AnnotationGraph

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck
import qualified Language.K3.Compiler.Haskell as HaskellC
import qualified Language.K3.Compiler.CPP as CPPC
import qualified Language.K3.Core.Utils as CoreUtils

import qualified Data.List as L

-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
  putStrLn $ "Mode: "      ++ pretty (mode op)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform op)
  putStrLn $ "Input: "     ++ show (input op)

  void $ mapM_ configureByInstruction $ logging $ inform op
    -- ^ Process logging directives

  -- Load files for any global variables
  preLoadVals <- mapM parsePreloads $ preLoad op
  putStrLn $ "Pre:" ++ concat (L.intersperse ", " (preLoad op))
  let addPreloadVals role = CoreUtils.prependToRole role preLoadVals

  case mode op of
    Compile   c -> compile c
    Interpret i -> interpret i addPreloadVals
    Print     p -> printer   (plainPrint     p) (printMode p) addPreloadVals
    Typecheck t -> k3Program (plainTypecheck t) (typecheck . addPreloadVals)
    Analyze   a -> analyzer  (plainAnalysis  a) (analyzeMode a)

  where compile cOpts@(CompileOptions lang _ _ _ _ _) = case map toLower lang of
          "haskell" -> HaskellC.compile op cOpts
          "cpp"     -> CPPC.compile op cOpts
          _         -> error $ lang ++ " compilation not supported."

        -- addF is a function adding code to the main program
        interpret im@(Batch {}) addF = runBatch op im addF
        interpret Interactive   _    = error "Interactive Mode is not yet implemented."

        printer plain PrintAST    addF = k3Program plain (putStrLn . pretty . addF)
        printer plain PrintSyntax addF = k3Program plain (printProgram . addF)
        printProgram = either syntaxError putStrLn . programS

        analyzer plain Conflicts    = k3Program plain (putStrLn . pretty . getAllConflicts)
        analyzer plain Tasks        = k3Program plain (putStrLn . pretty . getAllTasks)
        analyzer plain ProgramTasks = k3Program plain (putStrLn . show . getProgramTasks)
        analyzer plain ProxyPaths   = k3Program plain (putStrLn . pretty  . labelBindAliases)

        analyzer plain AnnotationProvidesGraph = k3Program plain (putStrLn . show . providesGraph)
        analyzer plain FlatAnnotations         = k3Program plain (putStrLn . show . flattenAnnotations)

        k3Program plain f = parseK3Input plain (includes $ paths op) (input op) >>= either parseError f
        parseError s      = putStrLn $ "Could not parse input: " ++ s
        syntaxError s     = putStrLn $ "Could not print program: " ++ s

        parsePreloads :: String -> IO (K3 Declaration)
        parsePreloads file = do
          p <- parseK3Input False (includes $ paths op) file
          case p of
            Left e  -> error e
            Right q -> return q

-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
