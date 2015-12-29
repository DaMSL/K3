{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Primary Driver for the K3 Ecosystem.
module Language.K3.Driver.Driver where

import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Except
import Control.Monad.State

import Data.Char
import Data.Maybe
import Debug.Trace

import Criterion.Measurement
import Database.HsSqlPpp.Parser
import GHC.IO.Encoding
import System.Directory (getCurrentDirectory)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

import Language.K3.Parser ( stitchK3Includes )
import Language.K3.Parser.ProgramBuilder ( defaultRoleName )
import qualified Language.K3.Parser.SQL as SQL

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.Evaluation

import Language.K3.Analysis.Core ( minimalProgramDecls )
import Language.K3.Analysis.HMTypes.Inference ( inferProgramTypes, translateProgramTypes )

import qualified Language.K3.Codegen.CPP.Materialization.Inference as MatI

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
    processStage (p,lg) SDeclPrepare           = trace "Running SDeclPrepare stage."     $ chainLog   lg $ ST.runDeclPreparePassesM p
    processStage (p,lg) SCGPrepare             = trace "Running SCGPrepare stage."       $ chainLog   lg $ ST.runCGPreparePassesM p
    processStage (p,lg) (SMaterialization dbg) = trace "Running SMaterialization stage." $ chainLog   lg $ ST.materializationPass dbg ST.mz0 (MatI.prepareInitialIState dbg p) p
    processStage (p,lg) (SCodegen mzfs)        = trace "Running SCodegen stage."         $ chainLog   lg $ ST.runCGPassesM mzfs p
    processStage (p,lg) (SDeclOpt cSpec)       = trace "Running SDeclOpt stage."         $ wrapReport lg $ ST.runDeclOptPassesM cSpec Nothing p

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

printMinimal :: IOOptions -> ParseOptions -> ([String], [(String, (String, K3 Declaration))]) -> IO ()
printMinimal ioOpts pOpts (userdecls, reqdecls) = do
  putStrLn $ unwords $ ["Declarations needed for:"] ++ userdecls
  putStrLn $ unlines $ map fst reqdecls
  k3outIO ioOpts pOpts $ DC.role defaultRoleName $ map (snd . snd) reqdecls


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
k3outIO :: IOOptions -> ParseOptions -> K3 Declaration -> IO ()
k3outIO ioOpts pOpts prog = do
  when (saveAST    ioOpts) (outputAST pretty "k3ast" (parsePrintMode pOpts) prog)
  when (saveRawAST ioOpts) (outputAST show   "k3ar"  (parsePrintMode pOpts) prog)
  when (saveSyntax ioOpts) (outputAST show   "k3s"   PrintSyntax            prog)

  where outputAST toStr ext printMode p = do
          cwd <- getCurrentDirectory
          let output = case inputProgram ioOpts of {"-" -> "a"; x -> x }
          either putStrLn (\s -> outputStrFile s $ outputFilePath cwd output ext)
            $ formatAST toStr printMode p

k3out :: IOOptions -> ParseOptions -> K3 Declaration -> DriverM ()
k3out ioOpts pOpts prog = liftIO $ k3outIO ioOpts pOpts prog

-- | Evaluate any metaprograms present in a program.
metaprogram :: Options -> K3 Declaration -> DriverM (K3 Declaration)
metaprogram opts p = if noMP $ input opts
                       then return p
                       else reasonM spliceError $ eval
  where
    eval = liftE $ evalMetaprogram (mkOpts $ mpOpts opts) Nothing Nothing p

    mkOpts Nothing = Just $ defaultMPEvalOptions
    mkOpts (Just mpo) =
      Just $ defaultMPEvalOptions { mpInterpArgs  = unpair $ interpreterArgs mpo
                                  , mpSearchPaths = moduleSearchPath mpo
                                  , mpSerial      = serialMetaprogram mpo }

    unpair = concatMap (\(x,y) -> [x,y])
    spliceError = "Could not process metaprogram: "


-- | Parser dispatch.
parse :: IOOptions -> ParseOptions -> K3 Declaration -> DriverM ()
parse ioOpts pOpts prog = do
    (xP, report) <- liftE $ evalTransform Nothing (poStages pOpts) prog
    k3out ioOpts pOpts xP
    minP <- reasonM syntaxError $ minimize pOpts (xP, report)
    liftIO $ either (printStages pOpts) (printMinimal ioOpts pOpts) minP

  where syntaxError = "Could not print program: "


-- | Program minimization.
minimize :: ParseOptions -> (K3 Declaration, [String])
         -> DriverM (Either (K3 Declaration, [String]) ([String], [(String, (String, K3 Declaration))]))
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

sql :: SQLOptions -> Options -> DriverM ()
sql sqlopts opts = do
  stmtE <- liftIO $ parseStatementsFromFile path
  flip (either (liftIO . putStrLn . show)) stmtE $ \stmts -> do
    (env, isg) <- sqlstmts stmts
    sqlcg env isg

  where
    dependencies = map (\s -> "include \"" ++ s ++ "\"")
                     [ "Annotation/Collection.k3"
                     , "Core/Barrier.k3"
                     , "Distributed/SQLTransformers.k3" ]

    path         = inputProgram $ input opts
    includePaths = includes $ paths opts
    nf           = noFeed $ input opts

    printParse = sqlPrintParse sqlopts
    asSyntax   = case sqlPrintMode sqlopts of
                   PrintSyntax -> True
                   _ -> False

    sqlstmts stmts = do
      void $ if printParse then printStmts stmts else return ()
      ((rstmts, stggraph, pstr, gstrs), env) <- liftEitherM $ SQL.runSQLParseEM SQL.sqlenv0 $ do
          nstmts <- SQL.sqloptimize stmts
          if sqlDistributedPlan sqlopts
            then do
              (sstmts, stggraph) <- SQL.sqlstage nstmts
              (\a b -> (sstmts, stggraph, unlines a, b)) <$> SQL.sqlstringify sstmts <*> SQL.sqldepgraph sstmts
            else
              (\a b -> (nstmts, [], unlines a, b)) <$> SQL.sqlstringify nstmts <*> SQL.sqldepgraph nstmts

      liftIO $ putStrLn $ boxToString $ ["Program", pstr, "", "Dependency Graph"] %$ gstrs
      return (env, (rstmts, stggraph))

    sqlcg env sg = do
      (prog, _) <- liftEitherM $ SQL.runSQLParseEM env $ SQL.sqlcodegen (sqlDistributedPlan sqlopts) sg
      if sqlUntyped sqlopts
        then liftIO $ putStrLn $ pretty prog
        else sqlprog prog

    sqlprog prog = do
      sprog <- liftE $ stitchK3Includes nf includePaths dependencies prog
      mprog <- metaprogram opts sprog
      case (sqlDoCompile sqlopts, sqlCompile sqlopts) of
        (True, Just cOpts) -> dispatch (opts {mode = Compile cOpts}) mprog
        (_, _) -> dispatch (opts {mode = Parse pmOpts}) mprog

    pmOpts = ParseOptions (sqlPrintMode sqlopts) (defaultCompileStages ST.mz0 LocalCompiler ST.cs0) []

    printProg p = do
      encprog <- if asSyntax then liftEitherM $ programS p else return $ pretty p
      liftIO $ putStrLn encprog

    printStmts stmts = liftIO $ do
      putStrLn $ replicate 40 '='
      void $ forM stmts $ \s -> putStrLn $ show s
      putStrLn $ replicate 40 '='
