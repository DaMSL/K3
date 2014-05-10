-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Data.Char

import Options.Applicative

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Analysis.Conflicts
import Language.K3.Analysis.Interpreter.BindAlias
import Language.K3.Analysis.AnnotationGraph

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

import qualified Language.K3.Compiler.Haskell as HaskellC
import qualified Language.K3.Compiler.CPP     as CPPC
import qualified Language.K3.Core.Utils       as CoreUtils

import qualified Data.List as L

-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch opts = do
  putStrLn $ "Mode: "      ++ pretty (mode opts)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform opts)
  putStrLn $ "Input: "     ++ show (input opts)

  void $ mapM_ configureByInstruction $ logging $ inform opts
    -- ^ Process logging directives

  withK3Program $ \prog ->
    case mode opts of
      Compile   c -> compile c prog
      Interpret i -> interpret i prog
      Print     p -> printer (printMode p) prog
      Typecheck _ -> typecheck prog
      Analyze   a -> analyzer (analyzeMode a) prog


  where
    withK3Program f = do
      parseResult <- parseK3Input (noFeed opts) (includes $ paths opts) (input opts)
      either parseError (\parsedProg -> prepend parsedProg >>= f) parseResult
    
    compile cOpts@(CompileOptions lang _ _ _ _ _ _) = case map toLower lang of
      "haskell" -> HaskellC.compile opts cOpts
      "cpp"     -> CPPC.compile opts cOpts
      _         -> error $ lang ++ " compilation not supported."

    -- addF is a function adding code to the main program
    interpret im@(Batch {}) = runBatch opts im
    interpret Interactive   = const $ error "Interactive Mode is not yet implemented."

    printer PrintAST    = putStrLn . pretty
    printer PrintSyntax = either syntaxError putStrLn . programS

    analyzer Conflicts               = putStrLn . pretty . getAllConflicts
    analyzer Tasks                   = putStrLn . pretty . getAllTasks
    analyzer ProgramTasks            = putStrLn . show   . getProgramTasks
    analyzer ProxyPaths              = putStrLn . pretty . labelBindAliases
    analyzer AnnotationProvidesGraph = putStrLn . show   . providesGraph
    analyzer FlatAnnotations         = putStrLn . show   . flattenAnnotations 

    parseError    s = putStrLn $ "Could not parse input: " ++ s
    syntaxError   s = putStrLn $ "Could not print program: " ++ s

    -- Load files for any global variables
    prepend prog = do
      putStrLn $ "Pre:" ++ concat (L.intersperse ", " $ preLoad opts)
      preloadDecls <- mapM parsePreloads $ preLoad opts
      return $ CoreUtils.prependToRole prog preloadDecls

    parsePreloads :: String -> IO (K3 Declaration)
    parsePreloads file = do
      p <- parseK3Input False (includes $ paths opts) file
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
