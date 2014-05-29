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
import Language.K3.Analysis.Effect

import Language.K3.Transform.Normalization
import Language.K3.Transform.Simplification
import Language.K3.Transform.Profiling
import Language.K3.Transform.Common(cleanGeneration)

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
      Analyze   a -> analyzer (analyzeMode a) (analyzeOutputMode a) prog
  where
    withK3Program f = do
      parseResult <- parseK3Input (noFeed opts) (includes $ paths opts) (input opts)
      either parseError f parseResult
    
    compile cOpts@(CompileOptions lang _ _ _ _ _ _) = case map toLower lang of
      "haskell" -> HaskellC.compile opts cOpts
      "cpp"     -> CPPC.compile opts cOpts
      _         -> error $ lang ++ " compilation not supported."

    -- addF is a function adding code to the main program
    interpret im@(Batch {}) = runBatch opts im
    interpret Interactive   = const $ error "Interactive Mode is not yet implemented."

    printer PrintAST    = putStrLn . pretty
    printer PrintSyntax = either syntaxError putStrLn . programS

    analyzer Conflicts               prtMode = printer prtMode . getAllConflicts
    analyzer Tasks                   prtMode = printer prtMode . getAllTasks
    analyzer ProgramTasks                  _ = putStrLn . show   . getProgramTasks
    analyzer ProxyPaths              prtMode = printer prtMode . labelBindAliases
    analyzer AnnotationProvidesGraph       _ = putStrLn . show   . providesGraph
    analyzer FlatAnnotations               _ = putStrLn . show   . flattenAnnotations 
    analyzer EffectNormalization     prtMode = printer prtMode . normalizeProgram
    analyzer FoldConstants           prtMode = printEither prtMode . foldProgramConstants
    analyzer Effects                 prtMode = withTypecheckedProgram (effectAnalysis prtMode)
    analyzer Simplify                prtMode = printEither prtMode .
                                         either Left eliminateDeadProgramCode 
                                                   . foldProgramConstants
                                                   . normalizeProgram
    analyzer Profiling               prtMode = printer prtMode . (cleanGeneration "profiling") . addProfiling

    effectAnalysis prtMode p _ = either putStrLn (printer prtMode) $ analyzeEffects p

    printEither PrintAST    = putStrLn . either id pretty
    printEither PrintSyntax = either putStrLn (either syntaxError putStrLn . programS)

    parseError    s = putStrLn $ "Could not parse input: " ++ s
    syntaxError   s = putStrLn $ "Could not print program: " ++ s


-- | Top-Level.
main :: IO ()
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
