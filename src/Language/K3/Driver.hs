{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-- | Primary Driver for the K3 Ecosystem.

import Control.Monad
import Control.Arrow (second)

import Criterion.Measurement

import Data.Char
import Data.Maybe

import GHC.IO.Encoding
import System.Directory (getCurrentDirectory)

import qualified Options.Applicative as Options
import Options.Applicative((<>), (<*>))

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.Evaluation

import Language.K3.Analysis.HMTypes.Inference

import Language.K3.Codegen.KTrace.KTraceDB ( kTrace )
import Language.K3.Stages

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Typecheck

--import qualified Language.K3.Driver.CompilerTarget.Haskell as HaskellC
import qualified Language.K3.Driver.CompilerTarget.CPP     as CPPC

-- | Mode Dispatch.
run :: Options -> IO ()
run opts = do
  initializeTime
  setLocaleEncoding     utf8
  setFileSystemEncoding utf8
  setForeignEncoding    utf8

  putStrLn $ "Mode: "      ++ pretty (mode opts)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform opts)
  putStrLn $ "Input: "     ++ show (input opts)

  void $ mapM_ configureByInstruction $ logging $ inform opts
    -- ^ Process logging directives

  case (splicedAstIn opts, mode opts) of
    (_, Compile copts) | ccStage copts == Stage2 -> compile copts (DC.role "__global" [])

    (True, _) -> do
      prog <- parseSplicedASTInput (input opts)
      dispatch (mode opts) prog

    (False, _) -> do
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
    dispatch (Parse popts) p = postParse popts p
    dispatch (Compile c)   p = compile c p
    dispatch (Interpret i) p = interpret i p
    dispatch (Typecheck t) p = case chooseTypechecker t p of
      Left s   -> putStrLn s >> putStrLn "ERROR"
      Right p' -> printer (PrintAST False False False False) p' >> putStrLn "SUCCESS"

    -- Parsing dispatch.
    postParse pOpts prog = do
      tp <- transform (poStages pOpts) prog
      case tp of
        Left s -> putStrLn s
        Right (p,rp) -> do
          (if saveAST opts then outputAST pOpts pretty "k3ast" p else return ())
          (if saveRawAST opts then outputAST pOpts show "k3ar" p else return ())
          printStages pOpts (p,rp)

    -- Compilation dispatch.
    compile cOpts prog = do
      case map toLower $ outLanguage cOpts of
        "cpp"     -> CPPC.compile opts cOpts (\f p -> withTransform (coStages cOpts) p f) prog
        "ktrace"  -> withTransform (coStages cOpts) prog $ \p -> either putStrLn putStrLn (kTrace (kTraceOptions cOpts) p)
        _         -> error $ outLanguage cOpts ++ " compilation not supported."
        --"haskell" -> HaskellC.compile opts cOpts p

    -- Interpreter dispatch.
    interpret im@(Batch {}) prog = do
      sp <- transform (ioStages im) prog
      case sp of
        Left s -> putStrLn s
        Right (p, rp) -> printTransformReport rp >> runBatch opts im p

    interpret Interactive _ = error "Interactive Mode is not yet implemented."

    -- Typechecking dispatch.
    chooseTypechecker opts' p =
      if noQuickTypes opts' then typecheck p else quickTypecheckOpts opts' p

    quickTypecheckOpts opts' p = inferProgramTypes p >>=
      \(p',_) -> if printQuickTypes opts' then return p' else translateProgramTypes p'

    -- quickTypecheck p = inferProgramTypes p >>= translateProgramTypes

    -- Stage-based transformation
    transform cstages prog = foldM processStage (Right (prog, [])) cstages

    processStage (Right (p,lg)) (SCompile Nothing)      = runOptPasses p >>= prettyReport lg
    processStage (Right (p,lg)) (SCompile (Just cSpec)) = runDeclOptPasses cSpec Nothing p >>= prettyReport lg
    processStage (Right (p,lg)) SCodegen = runCGPasses p >>= \rE -> return (rE >>= return . (,lg))
    processStage (Left s) _ = return $ Left s

    withTransform cstages prog f = transform cstages prog >>= \case
      Left s -> putStrLn s
      Right (p, rp) -> printTransformReport rp >> f p

    -- Print out, or save, the program
    formatAST toStr (PrintAST st se sc sp) p =
      let filterF = catMaybes $
                     [if st && se then Just stripTypeAndEffectAnns
                      else if st  then Just stripTypeAnns
                      else if se  then Just stripEffectAnns
                      else Nothing]
                      ++ [if sc then Just stripComments else Nothing]
                      ++ [if sp then Just stripProperties else Nothing]
      in Right $ toStr $ foldl (flip ($)) p filterF

    formatAST _ PrintSyntax p = programS p

    printer pm p = either syntaxError putStrLn $ formatAST pretty pm p

    printStages popts (prog, rp) = do
      printer (parsePrintMode popts) prog
      printTransformReport rp

    printTransformReport rp = do
      putStrLn $ sep ++ "Report" ++ sep
      putStrLn $ boxToString rp
      where sep = replicate 20 '='

    prettyReport lg npE = return (npE >>= return . (second $ (lg ++) . prettyLines))

    outputAST popts toStr ext p = do
      cwd <- getCurrentDirectory
      let output = case input opts of {"-" -> "a"; x -> x }
      either putStrLn (\s -> outputStrFile s $ outputFilePath cwd output ext)
        $ formatAST toStr (parsePrintMode popts) p

    -- Option handling utilities
    metaprogramOpts (Just mpo) =
      Just $ defaultMPEvalOptions { mpInterpArgs  = (unpair $ interpreterArgs  mpo)
                                  , mpSearchPaths = (moduleSearchPath mpo) }
      where unpair = concatMap (\(x,y) -> [x,y])

    metaprogramOpts Nothing = Just $ defaultMPEvalOptions

    parseError  s = putStrLn $ "Could not parse input: " ++ s
    spliceError s = putStrLn $ "Could not process metaprogram: " ++ s
    syntaxError s = putStrLn $ "Could not print program: " ++ s


-- | Top-Level.
main :: IO ()
main = Options.execParser options >>= run
  where
    options = Options.info (Options.helper <*> programOptions) $ Options.fullDesc
        <> Options.progDesc "The K3 Compiler."
        <> Options.header "The K3 Compiler."
