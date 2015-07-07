{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Primary Driver for the K3 Ecosystem.
module Language.K3.Driver.Driver where

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Control.Monad.State

import Criterion.Measurement

import Data.Char
import Data.Maybe

import GHC.IO.Encoding
import System.Directory (getCurrentDirectory)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.Evaluation

import Language.K3.Analysis.Core ( minimalProgramDecls )
import Language.K3.Analysis.HMTypes.Inference ( inferProgramTypes, translateProgramTypes )

import qualified Language.K3.Codegen.KTrace.KTraceDB as KT ( kTrace )

import Language.K3.Stages ( TransformM, TransformSt )
import qualified Language.K3.Stages as ST

import Language.K3.Driver.Batch
import Language.K3.Driver.Common
import Language.K3.Driver.Options

--import qualified Language.K3.Driver.CompilerTarget.Haskell as HaskellC
import qualified Language.K3.Driver.CompilerTarget.CPP     as CPPC

type DriverM = ExceptT String IO

runDriverM :: DriverM a -> IO (Either String a)
runDriverM = runExceptT

runDriverM_ :: DriverM a -> IO ()
runDriverM_ m = runDriverM m >>= either putStrLn (const $ return ())

liftE :: IO (Either String a) -> DriverM a
liftE m = ExceptT m

liftEitherM :: Either String a -> DriverM a
liftEitherM e = liftE $ return e

reasonM :: String -> DriverM a -> DriverM a
reasonM msg m = withExceptT (msg ++) m


-- | Stage-based transformation pipeline
transformM :: CompileStages -> K3 Declaration -> TransformM (K3 Declaration, [String])
transformM cstages prog = foldM processStage (prog, []) cstages
  where
    processStage (p,lg) SBatchOpt        = chainLog   lg $ ST.runOptPassesM p
    processStage (p,lg) SDeclPrepare     = chainLog   lg $ ST.runDeclPreparePassesM p
    processStage (p,lg) (SDeclOpt cSpec) = wrapReport lg $ ST.runDeclOptPassesM cSpec Nothing p
    processStage (p,lg) SCodegen         = chainLog   lg $ ST.runCGPassesM p

    chainLog   lg m = m >>= return . (,lg)
    wrapReport lg m = m >>= \np -> get >>= \st -> return (np, lg ++ (prettyLines $ ST.report st))

runTransformSt :: CompileStages -> TransformSt -> K3 Declaration -> IO (Either String ((K3 Declaration, [String]), TransformSt))
runTransformSt cstages initSt prog = ST.runTransformM initSt $ transformM cstages prog

runTransform :: Maybe ParGenSymS -> CompileStages -> K3 Declaration -> IO (Either String ((K3 Declaration, [String]), TransformSt))
runTransform symSOpt cstages prog = ST.st0 symSOpt prog >>= either (return . Left) (\st -> runTransformSt cstages st prog)

evalTransformSt :: CompileStages -> TransformSt -> K3 Declaration -> IO (Either String (K3 Declaration, [String]))
evalTransformSt cstages initSt prog = runTransformSt cstages initSt prog >>= either (return . Left) (return . Right . fst)

evalTransform :: Maybe ParGenSymS -> CompileStages -> K3 Declaration -> IO (Either String (K3 Declaration, [String]))
evalTransform symSOpt cstages prog = runTransform symSOpt cstages prog >>= either (return . Left) (return . Right . fst)


-- | Driver printing helpers

printer :: PrintMode -> K3 Declaration -> IO ()
printer pm p = either (putStrLn . (syntaxError ++)) putStrLn $ formatAST pretty pm p
  where syntaxError = "Could not print program: "


printStages :: ParseOptions -> (K3 Declaration, [String]) -> IO ()
printStages popts (prog, rp) = do
  printer (parsePrintMode popts) prog
  printTransformReport rp

printTransformReport :: [String] -> IO ()
printTransformReport rp = do
  putStrLn $ sep ++ "Report" ++ sep
  putStrLn $ boxToString rp
  where sep = replicate 20 '='

printMinimal :: [String] -> [String] -> IO ()
printMinimal userdecls reqdecls = do
  putStrLn $ unwords $ ["Declarations needed for:"] ++ userdecls
  putStrLn $ unlines reqdecls


-- | AST formatting.
formatAST :: (K3 Declaration -> String) -> PrintMode -> K3 Declaration -> Either String String
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


-- | Read in a K3 program and its includes (no parsing).
k3read :: Options -> DriverM [String]
k3read opts = liftIO $ readK3Input (includes $ paths opts) $ inputProgram $ input opts

-- | Read and parse a K3 program.
k3in :: Options -> DriverM (K3 Declaration)
k3in opts = if splicedInput ioOpts
               then liftIO $ parseSplicedASTInput inP
               else reasonM parseError $ liftE $ parseK3Input nfP includesP inP

  where ioOpts     = input opts
        inP        = inputProgram ioOpts
        nfP        = noFeed ioOpts
        includesP  = (includes $ paths opts)
        parseError = "Could not parse input: "

-- | Save out K3's internal representation.
k3out :: IOOptions -> ParseOptions -> K3 Declaration -> DriverM ()
k3out ioOpts pOpts prog = liftIO $ do
  when (saveAST    ioOpts) (outputAST pOpts pretty "k3ast" (parsePrintMode pOpts) prog)
  when (saveRawAST ioOpts) (outputAST pOpts show   "k3ar"  (parsePrintMode pOpts) prog)
  when (saveSyntax ioOpts) (outputAST pOpts show   "k3s"   PrintSyntax            prog)

  where outputAST popts toStr ext printMode p = do
          cwd <- getCurrentDirectory
          let output = case inputProgram ioOpts of {"-" -> "a"; x -> x }
          either putStrLn (\s -> outputStrFile s $ outputFilePath cwd output ext)
            $ formatAST toStr printMode p


-- | Evaluate any metaprograms present in a program.
metaprogram :: Options -> K3 Declaration -> DriverM (K3 Declaration)
metaprogram opts p = if noMP $ input opts
                       then return p
                       else reasonM spliceError $ eval
  where
    eval = liftE $ evalMetaprogram (mkOpts $ mpOpts opts) Nothing Nothing p

    mkOpts Nothing = Just $ defaultMPEvalOptions
    mkOpts (Just mpo) =
      Just $ defaultMPEvalOptions { mpInterpArgs  = (unpair $ interpreterArgs mpo)
                                  , mpSearchPaths = (moduleSearchPath mpo) }

    unpair = concatMap (\(x,y) -> [x,y])
    spliceError = "Could not process metaprogram: "


-- | Parser dispatch.
parse :: IOOptions -> ParseOptions -> K3 Declaration -> DriverM ()
parse ioOpts pOpts prog = do
    (xP, report) <- liftE $ evalTransform Nothing (poStages pOpts) prog
    k3out ioOpts pOpts xP
    minP <- reasonM syntaxError $ minimize pOpts (xP, report)
    liftIO $ either (printStages pOpts) (uncurry printMinimal) minP

  where syntaxError = "Could not print program: "


-- | Program minimization.
minimize :: ParseOptions -> (K3 Declaration, [String])
         -> DriverM (Either (K3 Declaration, [String]) ([String], [String]))
minimize (poMinimize -> userdecls) (p,rp) =
  if null userdecls
    then return $ Left (p,rp)
    else liftEitherM (minimalProgramDecls userdecls p) >>= \reqdecls -> return (Right (userdecls, reqdecls))


-- | Compiler dispatch.
compile :: Options -> CompileOptions -> K3 Declaration -> DriverM ()
compile opts cOpts prog = do
  case map toLower $ outLanguage cOpts of
    "cpp"     -> liftIO $ CPPC.compile opts cOpts (\f p -> withTransform (coStages cOpts) p f) prog
    "ktrace"  -> liftIO $ withTransform (coStages cOpts) prog $ ktrace (kTraceOptions cOpts)
    _         -> throwE $ outLanguage cOpts ++ " compilation not supported."
    --"haskell" -> HaskellC.compile opts cOpts p

  where withTransform cstages p f = evalTransform Nothing cstages p >>= \case
          Left s -> putStrLn s
          Right (xP, rp) -> printTransformReport rp >> f xP


-- | KTrace dispatch
ktrace :: [(String, String)] -> K3 Declaration -> IO ()
ktrace ktOpts p = either putStrLn putStrLn $ KT.kTrace ktOpts p


-- | Interpreter dispatch.
interpret :: Options -> InterpretOptions -> K3 Declaration -> DriverM ()
interpret opts im@(Batch {}) prog = liftIO $ do
  sp <- evalTransform Nothing (ioStages im) prog
  case sp of
    Left s -> putStrLn s
    Right (p, rp) -> printTransformReport rp >> runBatch opts im p

interpret _ Interactive _ = throwE "Interactive Mode is not yet implemented."


-- | Mode dispatch.
dispatch :: Options -> K3 Declaration -> DriverM ()
dispatch opts prog = case mode opts of
    Parse     pOpts -> parse (input opts) pOpts prog
    Compile   cOpts -> compile opts cOpts prog
    Interpret iOpts -> interpret opts iOpts prog

    Typecheck t -> do
        tP <- chooseTypechecker t prog
        liftIO $ printer (PrintAST False False False False) tP

    m -> throwE $ "Invalid dispatch mode: " ++ show m

  where
    -- Typechecking dispatch.
    chooseTypechecker opts' p | noQuickTypes opts' = throwE "Constraint subtyping type system disabled"
                              | otherwise = quickTypecheckOpts opts' p

    quickTypecheckOpts opts' p = liftEitherM $ do
      (tP, _) <- inferProgramTypes p
      if printQuickTypes opts' then return tP else translateProgramTypes tP

initialize :: Options -> DriverM ()
initialize opts = liftIO $ do
  initializeTime
  setLocaleEncoding     utf8
  setFileSystemEncoding utf8
  setForeignEncoding    utf8

  putStrLn $ "Mode: "      ++ pretty (mode opts)
  putStrLn $ "Verbosity: " ++ show (verbosity $ inform opts)
  putStrLn $ "Logging:   " ++ show (logging $ inform opts)
  putStrLn $ "Input: "     ++ show (input opts)

  void $ mapM_ configureByInstruction $ logging $ inform opts
    -- ^ Process logging directives
