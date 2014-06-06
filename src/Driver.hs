-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Control.Arrow (first)
import Data.Char
import Data.List(foldl')

import qualified Options.Applicative as Options
import Options.Applicative((<>), (<*>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Analysis.Conflicts
import Language.K3.Analysis.Interpreter.BindAlias
import Language.K3.Analysis.AnnotationGraph
import Language.K3.Analysis.Effect
import Language.K3.Analysis.HMTypes.Inference

import qualified Language.K3.Transform.Normalization as Normalization
import qualified Language.K3.Transform.Simplification as Simplification
import qualified Language.K3.Transform.Profiling as Profiling
import qualified Language.K3.Transform.RemoveROBinds as RemoveROBinds
import Language.K3.Transform.Common(cleanGeneration)

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

import qualified Language.K3.Compiler.Haskell as HaskellC
import qualified Language.K3.Compiler.CPP     as CPPC

-- | Mode Dispatch.
run :: Options -> IO ()
run opts = do
  putStrLn $ "Mode: "      ++ pretty (mode opts)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform opts)
  putStrLn $ "Input: "     ++ show (input opts)

  void $ mapM_ configureByInstruction $ logging $ inform opts
    -- ^ Process logging directives

  -- 1. Parsing
  parseResult <- parseK3Input (noFeed opts) (includes $ paths opts) (input opts)
  case parseResult of
    Right prog -> dispatch (mode opts) prog
    Left err -> parseError err
  where
    -- perform all transformations
    transform ts prog      = foldl' (flip analyzer) (prog, "") ts

    dispatch :: Mode -> K3 Declaration -> IO ()
    dispatch (Compile c) p   = compile c p
    dispatch (Interpret i) p = interpret i p
    dispatch (Typecheck t) p = case (choose_typechecker t) p of
      Left s  -> putStrLn s
      Right p' -> printer PrintAST p'
    dispatch (Analyze a) p   = doAnalyze (printMode a) (aoTransform a) p

    choose_typechecker opts = if (quickTypes opts)
                              then inferProgramTypes
                              else typecheck
    compile cOpts prog = do
      let (p, str) = transform (coTransform cOpts) prog
      putStrLn str
      case map toLower $ outLanguage cOpts of
        "haskell" -> HaskellC.compile opts cOpts p
        "cpp"     -> CPPC.compile opts cOpts p
        _         -> error $ outLanguage cOpts ++ " compilation not supported."

    interpret im@(Batch {}) prog = do
      let (p, str) = transform (ioTransform im) prog
      putStrLn str
      runBatch opts im p
    interpret Interactive _      = error "Interactive Mode is not yet implemented."

    -- Print out the program
    printer PrintAST    = putStrLn . pretty
    printer PrintSyntax = either syntaxError putStrLn . programS

    doAnalyze prtMode ts prog = do
      let (p, str) = transform ts prog
      printer prtMode p
      putStrLn str

      -- Using arrow combinators to make this simpler
      -- first/second passes the other part of the pair straight through
    analyzer Conflicts x           = first getAllConflicts x
    analyzer Tasks x               = first getAllTasks x
    analyzer ProgramTasks (p,s)    = (p, s ++ show (getProgramTasks p))
    analyzer ProxyPaths x          = first labelBindAliases x
    analyzer AnnotationProvidesGraph (p,s) = (p, s ++ show (providesGraph p))
    analyzer FlatAnnotations (p,s) = (p, s ++ show (flattenAnnotations p))
    analyzer EffectNormalization x = first Normalization.normalizeProgram x
    analyzer FoldConstants x       = wrapEither Simplification.foldProgramConstants x
    analyzer Effects x             = wrapEither analyzeEffects . wrapEither typecheck $ x
    analyzer DeadCodeElimination x = wrapEither Simplification.eliminateDeadProgramCode x
    analyzer Profiling x           = first (cleanGeneration "profiling" . Profiling.addProfiling) x
    analyzer ReadOnlyBinds x       = first (cleanGeneration "ro_binds" . RemoveROBinds.transform) x

    -- If we produce a proper program, put it first. Otherwise put the original program first
    -- and add to the string
    wrapEither f (p, str) = case f p of
      Left s   -> (p, str++s)
      Right p' -> (p', str)

    parseError    s = putStrLn $ "Could not parse input: " ++ s
    syntaxError   s = putStrLn $ "Could not print program: " ++ s


-- | Top-Level.
main :: IO ()
main = Options.execParser options >>= run
  where
    options = Options.info (Options.helper <*> programOptions) $ Options.fullDesc
        <> Options.progDesc "The K3 Compiler."
        <> Options.header "The K3 Compiler."
