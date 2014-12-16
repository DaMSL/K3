-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Control.Arrow (first)
import Data.Char
import Data.List(foldl')
import Data.Maybe
import Data.Tuple

import qualified Options.Applicative as Options
import Options.Applicative((<>), (<*>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import qualified Data.Text                    as T
import qualified Language.K3.Utils.PrettyText as PT

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.Evaluation

import Language.K3.Analysis.Interpreter.BindAlias
import Language.K3.Analysis.AnnotationGraph
import Language.K3.Analysis.HMTypes.Inference

import qualified Language.K3.Analysis.Provenance.Inference  as Provenance
import qualified Language.K3.Analysis.SEffects.Inference    as SEffects
import qualified Language.K3.Analysis.Effects.InsertEffects as Effects
import qualified Language.K3.Analysis.Effects.Purity        as Pure

import qualified Language.K3.Transform.Normalization  as Normalization
import qualified Language.K3.Transform.Simplification as Simplification
import qualified Language.K3.Transform.Profiling      as Profiling
import qualified Language.K3.Transform.RemoveROBinds  as RemoveROBinds
import qualified Language.K3.Transform.TriggerSymbols as TriggerSymbols

import Language.K3.Transform.Common(cleanGeneration)

import Language.K3.Codegen.KTrace.KTraceDB ( kTrace )
import Language.K3.Stages

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

--import qualified Language.K3.Compiler.Haskell as HaskellC
import qualified Language.K3.Compiler.CPP     as CPPC

-- | Mode Dispatch.
run :: Options -> IO ()
run opts = do
  putStrLn $ "Mode: "      ++ pretty (mode opts)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform opts)
  putStrLn $ "Input: "     ++ show (input opts)

  void $ mapM_ configureByInstruction $ logging $ inform opts
    -- ^ Process logging directives

  -- Parse, splice, and dispatch based on command mode.
  parseResult  <- parseK3Input (noFeed opts) (includes $ paths opts) (input opts)
  case parseResult of
    Left err      -> parseError err
    Right parsedP -> if noMP opts then dispatch (mode opts) parsedP
                     else metaprogram parsedP

  where
    metaprogram :: K3 Declaration -> IO ()
    metaprogram p = do
      mp <- evalMetaprogram (metaprogramOpts $ mpOpts opts) Nothing Nothing p
      either spliceError (dispatch $ mode opts) mp

    dispatch :: Mode -> K3 Declaration -> IO ()
    dispatch (Parse popts) p = if not $ null $ poStages popts
                                then runStagesThenPrint popts p
                                else analyzeThenPrint popts p

    dispatch (Compile c)   p = compile c p
    dispatch (Interpret i) p = interpret i p
    dispatch (Typecheck t) p = case chooseTypechecker t p of
      Left s   -> putStrLn s                              >> putStrLn "ERROR"
      Right p' -> printer (PrintAST False False False) p' >> putStrLn "SUCCESS"

    -- Compilation dispatch.
    compile cOpts prog = do
      let (p, str) = transform (coTransform cOpts) prog
      putStrLn str
      case map toLower $ outLanguage cOpts of
        "cpp"     -> CPPC.compile opts cOpts p
        "ktrace"  -> either putStrLn putStrLn $ kTrace (kTraceOptions cOpts) p
        _         -> error $ outLanguage cOpts ++ " compilation not supported."
        --"haskell" -> HaskellC.compile opts cOpts p

    -- Interpreter dispatch.
    interpret im@(Batch {}) prog = do
      let (p, str) = transform (ioTransform im) prog
      putStrLn str
      runBatch opts im p
    interpret Interactive _      = error "Interactive Mode is not yet implemented."

    -- Typechecking dispatch.
    chooseTypechecker opts' p =
      if noQuickTypes opts' then typecheck p else quickTypecheckOpts opts' p

    quickTypecheckOpts opts' p = inferProgramTypes p >>=
      \p' -> if printQuickTypes opts' then return p' else translateProgramTypes p'

    -- quickTypecheck p = inferProgramTypes p >>= translateProgramTypes

    -- Perform all transformations
    transform ts prog = foldl' (flip (analyzer $ analysisOpts opts)) (prog, "") ts

    -- Print out the program
    printer (PrintAST st se sc) p =
      let filterF = catMaybes $
                     [if st && se then Just stripTypeAndEffectAnns
                      else if st  then Just stripTypeAnns
                      else if se  then Just stripEffectAnns
                      else Nothing]
                      ++ [if sc then Just stripComments else Nothing]
      in putStrLn . pretty $ foldl (flip ($)) p filterF

    printer PrintSyntax p = either syntaxError putStrLn $ programS p

    runStagesThenPrint popts prog = do
      let sprogE = foldM (\p (stg, f) -> if stg `elem` (poStages popts) then f p else Right p)
                        prog
                        [ (PSOptimization, (\p -> runOptPasses p >>= return . fst))
                        , (PSCodegen, flip runCGPasses 3)]
      either putStrLn (printer (parsePrintMode popts) . stripAllProperties . stripTypeAndEffectAnns) sprogE

    analyzeThenPrint popts prog = do
      let (p, str) = transform (poTransform popts) prog
      printer (parsePrintMode popts) p
      putStrLn str

    ifFlag v f keys aopts = concatMap (\k -> maybe v (f v k . read) $ lookup k aopts) keys

    -- Using arrow combinators to make this simpler
    -- first/second passes the other part of the pair straight through
    analyzer :: [(String, String)] -> TransformMode -> (K3 Declaration, String) -> (K3 Declaration, String)
    analyzer _ ProxyPaths x                  = first labelBindAliases x
    analyzer _ AnnotationProvidesGraph (p,s) = (p, s ++ show (providesGraph p))
    analyzer _ FlatAnnotations (p,s)         = (p, s ++ show (flattenAnnotations p))
    analyzer _ EffectNormalization x         = first Normalization.normalizeProgram x
    analyzer _ FoldConstants x               = wrapEither Simplification.foldProgramConstants x
    
    analyzer aopts Provenance x = flip wrapEitherS x $ \p str -> do
      (np, ppenv) <- Provenance.inferProgramProvenance p
      return $ (np, addEnv str ppenv)
      where addEnv str ppenv = str ++ ifFlag "" (withEnv ppenv) ["showprovenance"] aopts
            withEnv ppenv v _ b = if b then "Provenance pointers:\n" ++ (T.unpack $ PT.pretty ppenv) else v
    
    analyzer aopts SEffects x = flip wrapEitherS x $ \p str -> do
      (np,  ppenv) <- Provenance.inferProgramProvenance p
      (np', fpenv) <- SEffects.inferProgramEffects ppenv np
      return (np', addEnv str ppenv fpenv)
      where addEnv str ppenv fpenv = str ++ ifFlag "" (withEnv ppenv fpenv) ["showprovenance", "showeffects"] aopts
            withEnv ppenv _ _ "showprovenance" True = "Provenance pointers:\n" ++ (T.unpack $ PT.pretty ppenv)
            withEnv _ fpenv _ "showeffects"    True = "Effect pointers:\n"     ++ (T.unpack $ PT.pretty fpenv)
            withEnv _ _ v _ _ = v

    analyzer _ Effects x = flip first x $
      (uncurry Effects.expandProgram . swap . Effects.runConsolidatedAnalysis)

    analyzer _ DeadCodeElimination x  = flip wrapEither x $
      (uncurry Simplification.eliminateDeadProgramCode . swap . Effects.runConsolidatedAnalysis)

    analyzer _ Profiling x      = first (cleanGeneration "profiling" . Profiling.addProfiling) x
    analyzer _ Purity x         = first ((\(d,e) -> Pure.runPurity e d) . Effects.runConsolidatedAnalysis) x
    analyzer _ ReadOnlyBinds x  = first (cleanGeneration "ro_binds" . RemoveROBinds.transform) x
    analyzer _ TriggerSymbols x = wrapEither TriggerSymbols.triggerSymbols x

    -- Deprecated analyses
    --analyzer _ Conflicts x                   = first getAllConflicts x
    --analyzer _ Tasks x                       = first getAllTasks x
    --analyzer _ ProgramTasks (p,s)            = (p, s ++ show (getProgramTasks p))
    analyzer _ a (p,s)          = (p, unwords [s, "unhandled analysis", show a])

    -- Option handling utilities
    metaprogramOpts (Just mpo) =
      Just $ defaultMPEvalOptions { mpInterpArgs  = (unpair $ interpreterArgs  mpo)
                                  , mpSearchPaths = (moduleSearchPath mpo) }
      where unpair = concatMap (\(x,y) -> [x,y])

    metaprogramOpts Nothing = Just $ defaultMPEvalOptions

    -- If we produce a proper program, put it first. Otherwise put the original program first
    -- and add to the string
    wrapEither f (p, str) = case f p of
      Left s   -> (p, str++s)
      Right p' -> (p', str)

    wrapEitherS f (p, str) = case f p str of
      Left s          -> (p, str++s)
      Right (p',str') -> (p', str')

    parseError  s = putStrLn $ "Could not parse input: " ++ s
    spliceError s = putStrLn $ "Could not process metaprogram: " ++ s
    syntaxError s = putStrLn $ "Could not print program: " ++ s

    -- Temporary testing function.
    -- testProperties p = inferProgramUsageProperties p
    --                      >>= Simplification.inferFusableProgramApplies
    --                      >>= Simplification.fuseProgramTransformers


-- | Top-Level.
main :: IO ()
main = Options.execParser options >>= run
  where
    options = Options.info (Options.helper <*> programOptions) $ Options.fullDesc
        <> Options.progDesc "The K3 Compiler."
        <> Options.header "The K3 Compiler."
