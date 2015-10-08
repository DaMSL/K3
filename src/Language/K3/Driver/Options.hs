{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Control.Applicative
import Control.Arrow ( second )
import Options.Applicative

import Data.Binary
import Data.Serialize

import Data.Char
import Data.Map ( Map )
import qualified Data.Map as Map
import Data.List.Split
import Data.Maybe

import GHC.Generics ( Generic )

import System.FilePath
import System.Log

import Language.K3.Stages ( CompilerSpec(..), StageSpec(..), cs0 )
import Language.K3.Runtime.Common ( SystemEnvironment )
import Language.K3.Runtime.Options
import Language.K3.Utils.Logger.Config

import Language.K3.Driver.Common

import Language.K3.Codegen.CPP.Types (CPPCGFlags(..))

import Language.K3.Utils.Pretty (
    Pretty(..), PrintConfig(..),
    indent, defaultPrintConfig, tersePrintConfig, simplePrintConfig
  )

-- | Program Options.
data Options = Options {
      mode          :: Mode
    , inform        :: InfoSpec
    , mpOpts        :: Maybe MetaprogramOptions
    , paths         :: PathOptions
    , input         :: IOOptions
    }
  deriving (Eq, Read, Show)

-- | Modes of Operation.
data Mode
    = Parse     ParseOptions
    | Compile   CompileOptions
    | Interpret InterpretOptions
    | Typecheck TypecheckOptions
    | Service   ServiceOperation
    | SQL       SQLOptions
  deriving (Eq, Read, Show)

-- | Compiler input and output options.
data IOOptions = IOOptions { inputProgram :: FilePath
                           , noFeed       :: Bool
                           , noMP         :: Bool
                           , splicedInput :: Bool
                           , saveAST      :: Bool
                           , saveRawAST   :: Bool
                           , saveSyntax   :: Bool }
               deriving (Eq, Read, Show)

-- | Parsing options.
data ParseOptions = ParseOptions { parsePrintMode :: PrintMode
                                 , poStages       :: CompileStages
                                 , poMinimize     :: [String] }
                    deriving (Eq, Read, Show)

-- | Metaprogramming options
data MetaprogramOptions
    = MetaprogramOptions { interpreterArgs  :: [(String, String)]
                         , moduleSearchPath :: [String] }
    deriving (Eq, Read, Show)

-- | Typechecking options
data TypecheckOptions = TypecheckOptions { noQuickTypes    :: Bool
                                         , printQuickTypes :: Bool }
                      deriving (Eq, Read, Show)

data CPPOptions = CPPOptions { cppFlags :: String, cppCGFlags :: CPPCGFlags } deriving (Eq, Generic, Read, Show)

-- | Compilation options datatypes.
data CompileOptions = CompileOptions
                      { outLanguage        :: String
                      , programName        :: String
                      , runtimePath        :: FilePath
                      , outputFile         :: Maybe FilePath
                      , buildDir           :: Maybe FilePath
                      , buildJobs          :: Int
                      , ccCmd              :: CPPCompiler
                      , ccStage            :: CPPStages
                      , cppOptions         :: CPPOptions
                      , kTraceOptions      :: [(String, String)]
                      , coStages           :: CompileStages
                      , useSubTypes        :: Bool
                      , optimizationLevel  :: OptimizationLevel
                      , astPrintMode       :: PrintMode
                      }
  deriving (Eq, Read, Show, Generic)

data CPPCompiler = GCC | Clang deriving (Eq, Read, Show, Generic)

data CPPStages = Stage1 | Stage2 | AllStages deriving (Eq, Read, Show, Generic)

-- | Interpretation options.
data InterpretOptions
    = Batch { network     :: Bool
            , sysEnv      :: SystemEnvironment
            , asExpr      :: Bool
            , isPar       :: Bool
            , printConfig :: PrintConfig
            , noConsole   :: Bool
            , ioStages    :: CompileStages
            }
    | Interactive
  deriving (Eq, Read, Show)

type OptimizationLevel = Int

-- | Logging and information output options.
data InfoSpec = InfoSpec { logging   :: LoggerOptions
                         , verbosity :: Verbosity }
                  deriving (Eq, Read, Show)

-- | Logging directives, passed through to K3.Logger.Config .
type LoggerInstruction = (String,Priority)
type LoggerOptions     = [LoggerInstruction]

-- | K3 compiler path related options
data PathOptions = PathOptions { includes :: [FilePath] }
                 deriving (Eq, Read, Show)

-- | K3 compiler service options
data ServiceOperation = RunMaster    ServiceOptions ServiceMasterOptions
                      | RunWorker    ServiceOptions
                      | SubmitJob    ServiceOptions RemoteJobOptions
                      | Shutdown     ServiceOptions
                      | QueryService ServiceOptions QueryOptions
                      deriving (Eq, Read, Show, Generic)

data ServiceOptions = ServiceOptions { serviceId       :: String
                                     , serviceHost     :: String
                                     , servicePort     :: Int
                                     , serviceThreads  :: Int
                                     , serviceLog      :: Either String FilePath
                                     , serviceLogLevel :: Priority
                                     , sHeartbeatEpoch :: Int
                                     , scompileOpts    :: CompileOptions }
                    deriving (Eq, Read, Show, Generic)

data ServiceMasterOptions
        = ServiceMasterOptions { sfinalStages  :: CompileStages }
        deriving (Eq, Read, Show)

data RemoteJobOptions = RemoteJobOptions { workerFactor     :: Map String Int
                                         , workerBlockSize  :: Map String Int
                                         , defaultBlockSize :: Int
                                         , reportSize       :: Int
                                         , rcStages         :: CompileStages }
                      deriving (Eq, Read, Show, Generic)

data QueryOptions = QueryOptions { qsargs :: Either [String] [Int] }
                    deriving (Eq, Read, Show, Generic)

-- | SQL frontend options.
data SQLOptions = SQLOptions { sqlPrintMode       :: PrintMode
                             , sqlPrintParse      :: Bool
                             , sqlUntyped         :: Bool
                             , sqlDistributedPlan :: Bool
                             , sqlDoCompile       :: Bool
                             , sqlCompile         :: Maybe CompileOptions }
                deriving (Eq, Read, Show, Generic)

-- | Verbosity levels.
data Verbosity
    = NullV
    | SoftV
    | LoudV
  deriving (Enum, Eq, Read, Show)

-- | Automatically generated serialization instances.
instance Binary RemoteJobOptions
instance Binary CompileOptions
instance Binary CPPOptions
instance Binary CompileStage
instance Binary CPPCompiler
instance Binary CPPStages
instance Binary PrintMode

instance Serialize RemoteJobOptions
instance Serialize CompileOptions
instance Serialize CPPOptions
instance Serialize CompileStage
instance Serialize CPPCompiler
instance Serialize CPPStages
instance Serialize PrintMode

{- Option parsing utilities -}

-- | Constructs a path list from a string of colon-separated paths.
delimitedList :: String -> String -> [String]
delimitedList sep = splitOn sep

pathList :: String -> [String]
pathList = delimitedList ":"

commaSepList :: String -> [String]
commaSepList = delimitedList ","

parseKVL :: String -> String -> String -> String -> [(String, String)]
parseKVL sepSym eqSym keyPrefix s = catMaybes $ map kvPair $ splitOn sepSym s
 where kvPair s' = case splitOn eqSym s' of
                     [x,y] -> Just (keyPrefix ++ x, y)
                     _     -> Nothing

keyValList :: String -> String -> [(String, String)]
keyValList = parseKVL ":" "="

specParamList :: String -> String -> [(String, String)]
specParamList = parseKVL "," "@"


{- Mode Options Parsing. -}

modeOptions :: Parser Mode
modeOptions = subparser (
         command "parse"     (info parseOptions     $ progDesc parseDesc)
      <> command "compile"   (info compileOptions   $ progDesc compileDesc)
      <> command "interpret" (info interpretOptions $ progDesc interpretDesc)
      <> command "typecheck" (info typecheckOptions $ progDesc typeDesc)
      <> command "service"   (info serviceOptions   $ progDesc serviceDesc)
      <> command "sql"       (info sqlOptions       $ progDesc sqlDesc)
    )
  where parseDesc     = "Parse a K3 program"
        compileDesc   = "Compile a K3 binary"
        interpretDesc = "Interpret a K3 program"
        typeDesc      = "Typecheck a K3 program"
        serviceDesc   = "Start a K3 compiler service"
        sqlDesc       = "Compile a SQL query"


{- Compiler input parsing -}

ioOptions :: Parser IOOptions
ioOptions = IOOptions <$> inputProgramOpt
                      <*> noFeedOpt
                      <*> noMPOpt
                      <*> splicedInputOpt
                      <*> saveAstOpt
                      <*> saveRawAstOpt
                      <*> saveSyntaxOpt

inputProgramOpt :: Parser FilePath
inputProgramOpt = last . fileOrStdin <$> (many $ argument str (   metavar "FILE"
                                                               <> help "K3 program file." ) )
  where fileOrStdin [] = ["-"]
        fileOrStdin x  = x

noFeedOpt :: Parser Bool
noFeedOpt = switch (   long "nofeed"
                    <> help "Process a program, ignoring data feeds." )

noMPOpt :: Parser Bool
noMPOpt = switch (   long "nometaprogram"
                  <> help "Process a program, skipping metaprogram evaluation." )

splicedInputOpt :: Parser Bool
splicedInputOpt = switch (   long "from-spliced-ast"
                          <> help "Process a pre-spliced AST input."  )

saveAstOpt :: Parser Bool
saveAstOpt = switch (   long "save-ast"
                     <> help "Save human-readable K3 AST used for compilation" )

saveRawAstOpt :: Parser Bool
saveRawAstOpt = switch (   long "save-raw-ast"
                        <> help "Save K3 AST used from compilation" )

saveSyntaxOpt :: Parser Bool
saveSyntaxOpt = switch (   long "save-syntax"
                        <> help "Save pretty-printed K3 program from compilation" )


{- Parsing mode options -}

-- | Parse mode
parseOptions :: Parser Mode
parseOptions = Parse <$> ( ParseOptions <$> printModeOpt "" <*> compileStagesOpt LocalCompiler <*> minimizeOpt )

-- | Print mode flags
printModeOpt :: String -> Parser PrintMode
printModeOpt astDefault = astPrintOpt astDefault <|> syntaxPrintOpt

astPrintOpt :: String -> Parser PrintMode
astPrintOpt astDefault = extract . keyValList "" <$> strOption (
                              long "ast"
                           <> value astDefault
                           <> help "Print AST output"
                           <> metavar "PRINTASTFLAGS"
                         )
   where extract l = PrintAST (key "notypes"    l)
                              (key "noeffects"  l)
                              (key "nocomments" l)
                              (key "noproperties" l)

         key k kvl = maybe False read $ lookup k kvl

syntaxPrintOpt :: Parser PrintMode
syntaxPrintOpt = flag' PrintSyntax (   long "syntax"
                                    <> help "Print syntax output" )

minimizeOpt :: Parser [String]
minimizeOpt = (\s -> if null s then [] else commaSepList s) <$> strOption (
                   long "minimaldecls"
                <> value ""
                <> metavar "MINIMALDECLS"
                <> help "Print minimal declarations needed for compilation" )


{- Compilation mode options -}

-- | Compiler options
compileOptions :: Parser Mode
compileOptions = Compile <$> compileOpts LocalCompiler

compileOpts :: CompilerType -> Parser CompileOptions
compileOpts ct = CompileOptions <$> outLanguageOpt
                                <*> progNameOpt
                                <*> runtimePathOpt
                                <*> outputFileOpt
                                <*> buildDirOpt
                                <*> buildJobsOpt
                                <*> ccCmdOpt
                                <*> ccStageOpt
                                <*> cppOpt
                                <*> ktraceOpt
                                <*> compileStagesOpt ct
                                <*> noQuickTypesOpt
                                <*> optimizationOpt
                                <*> printModeOpt "noeffects=True:notypes=True:noproperties=True"

defaultCompileStages :: CompilerType -> CompilerSpec -> CompileStages
defaultCompileStages ct cSpec = case ct of
    LocalCompiler       -> [SDeclPrepare, SDeclOpt cSpec, SCodegen]
    ServicePrepare      -> [SDeclPrepare]
    ServiceParallel     -> [SDeclOpt cSpec]
    ServiceFinal        -> [SDeclPrepare, SCodegen]
    ServiceClient       -> []
    ServiceClientRemote -> [SDeclOpt cSpec]

compileStagesOpt :: CompilerType -> Parser CompileStages
compileStagesOpt ct = extractStageAndSpec . keyValList "" <$> strOption (
                                 long flagName
                              <> value ""
                              <> metavar "STAGES"
                              <> help "Run compilation stages" )

   where
    flagName = case ct of
                  LocalCompiler       -> "fstage"
                  ServicePrepare      -> "sprepstage"
                  ServiceParallel     -> "sparstage"
                  ServiceFinal        -> "sfinstage"
                  ServiceClient       -> "scstage"
                  ServiceClientRemote -> "srstage"

    extractStageAndSpec kvl = case kvl of
      []  -> defaultCompileStages ct cs0
      [x] -> stageOf cs0 x
      h:t -> stageOf (specOf t) h

    -- | Local compilation stages definitions.
    stageOf _     ("none",      read -> True) = []
    stageOf cSpec ("declopt",   read -> True) = [SDeclPrepare, SDeclOpt cSpec]
    stageOf cSpec ("cg",        read -> True) = [SDeclPrepare, SDeclOpt cSpec, SCodegen]

    -- | Compiler service stages definitions.
    stageOf _     ("sprepare",  read -> True) = [SDeclPrepare]
    stageOf cSpec ("sparallel", read -> True) = [SDeclOpt cSpec]
    stageOf _     ("sfinal",    read -> True) = [SDeclPrepare, SCodegen]

    -- | Optimizer stage specification.
    stageOf cSpec ("oinclude", (splitOn "," ->  psl)) = [SDeclPrepare] ++ include cSpec psl
    stageOf cSpec ("oexclude", (splitOn "," -> npsl)) = [SDeclPrepare] ++ exclude cSpec npsl

    -- | Optimizer stage specification with final compilation.
    stageOf cSpec ("cinclude", (splitOn "," ->  psl)) = [SDeclPrepare] ++ include cSpec psl  ++ [SCodegen]
    stageOf cSpec ("cexclude", (splitOn "," -> npsl)) = [SDeclPrepare] ++ exclude cSpec npsl ++ [SCodegen]

    -- | Service worker optimization stage specification.
    stageOf cSpec ("sinclude", (splitOn "," ->  psl)) = include cSpec psl
    stageOf cSpec ("sexclude", (splitOn "," -> npsl)) = exclude cSpec npsl

    -- | Default handler.
    stageOf cSpec _ = defaultCompileStages ct cSpec

    include cSpec psl =
      let ss = stageSpec cSpec
      in [SDeclOpt $ cSpec {stageSpec = ss {passesToRun = Just psl}}]

    exclude cSpec npsl =
      let ss = stageSpec cSpec
      in [SDeclOpt $ cSpec {stageSpec = ss {passesToFilter = Just npsl}}]

    specOf kvl = foldl specParam cs0 kvl
    specParam cs (k,v) = case k of
      "@blockSize" -> cs {blockSize = read v}
      _ -> let ss = stageSpec cs
           in cs {stageSpec = ss {snapshotSpec = Map.insertWith (++) k (splitOn "," v) $ snapshotSpec ss}}

outLanguageOpt :: Parser String
outLanguageOpt = strOption ( short   'l'
                      <> long    "language"
                      <> value   defaultOutLanguage
                      <> help    "Specify compiler target language"
                      <> metavar "LANG" )

progNameOpt :: Parser String
progNameOpt = strOption (   short   'n'
                      <> long    "name"
                      <> value   defaultProgramName
                      <> help    "Program name"
                      <> metavar "PROGNAME" )

runtimePathOpt :: Parser FilePath
runtimePathOpt = strOption (
                       short   'r'
                    <> long    "runtime"
                    <> value   defaultRuntimeDir
                    <> help    "Specify runtime path"
                    <> metavar "RUNTIME" )

outputFileOpt :: Parser (Maybe FilePath)
outputFileOpt = validatePath . Just <$> option auto (
                       short   'o'
                    <> long    "output"
                    <> value   defaultOutputFile
                    <> help    "Specify output file"
                    <> metavar "OUTPUT" )
  where validatePath Nothing  = Nothing
        validatePath (Just p) = if isValid p then Just p else Nothing

buildDirOpt :: Parser (Maybe FilePath)
buildDirOpt = validatePath . Just <$> option auto (
                       short   'b'
                    <> long    "build"
                    <> value   defaultBuildDir
                    <> help    "Temporary build directory"
                    <> metavar "BUILDDIR" )
  where validatePath Nothing  = Nothing
        validatePath (Just p) = if isValid p then Just p else Nothing

buildJobsOpt :: Parser Int
buildJobsOpt = option auto (
                       short   'j'
                    <> long    "jobs"
                    <> value   defaultBuildJobs
                    <> help    "Parallel job builds"
                    <> metavar "BUILDJOBS" )

ccCmdOpt :: Parser CPPCompiler
ccCmdOpt = (gccFlag <|> clangFlag <|> pure Clang)

ccStageOpt :: Parser CPPStages
ccStageOpt = (stage1Flag <|> stage2Flag <|> allStagesFlag <|> pure AllStages)

gccFlag :: Parser CPPCompiler
gccFlag = flag' GCC (   long "gcc"
                     <> help "Use the g++ toolchain for C++ compilation" )

clangFlag :: Parser CPPCompiler
clangFlag = flag' Clang (   long "clang"
                         <> help "Use clang++ and LLVM for C++ compilation" )

stage1Flag :: Parser CPPStages
stage1Flag = flag' Stage1 ( short '1' <> long "stage1" <> help "Only run stage 1 compilation" )

stage2Flag :: Parser CPPStages
stage2Flag = flag' Stage2 ( short '2' <> long "stage2" <> help "Only run stage 2 compilation" )

allStagesFlag :: Parser CPPStages
allStagesFlag = flag' AllStages (long "allstages" <> help "Compile all stages")

cppOpt :: Parser CPPOptions
cppOpt = CPPOptions <$> strOption (long "cpp-flags" <> help "Specify CPP Flags" <> metavar "CPPFLAGS" <> value "")
                    <*> (CPPCGFlags <$> (fromMaybe False . fmap read . lookup "isolateLoopIndex" . keyValList ""
                                           <$> strOption (
                                               long "cg-options"
                                            <> value ""
                                            <> help "Code Generation Options"
                                            <> metavar "CGOptions")))

ktraceOpt :: Parser [(String, String)]
ktraceOpt = keyValList "" <$> strOption (
                 long "ktrace-flags"
              <> value ""
              <> help "Specify KTrace Flags"
              <> metavar "FLAGS" )

includeOpt :: Parser FilePath
includeOpt = strOption (
                long "CI"
             <> help "Specifies a C++ compiler include directory."
             <> metavar "DIRECTORY" )

libraryOpt :: Parser (Bool, FilePath)
libraryOpt = linkerDirOpt <|> libraryFileOpt

linkerDirOpt :: Parser (Bool, FilePath)
linkerDirOpt = (True,) <$> strOption (
                  long "CL"
               <> help "Specifies a C++ linker directory."
               <> metavar "DIRECTORY" )

libraryFileOpt :: Parser (Bool, FilePath)
libraryFileOpt = (False,) <$> strOption (
                    long "Cl"
                 <> help "Specifies a C++ library file."
                 <> metavar "FILE" )

-- | Interpretation options.
interpretOptions :: Parser Mode
interpretOptions = Interpret <$> (batchOptions <|> interactiveOptions)

-- | Options for Interactive Mode.
interactiveOptions :: Parser InterpretOptions
interactiveOptions = flag' Interactive (
        short 'i'
     <> long "interactive"
     <> help "Run in Interactive Mode" )

-- | Options for Batch Mode.
batchOptions :: Parser InterpretOptions
batchOptions = flag' Batch (
            short 'b'
         <> long "batch"
         <> help "Run in Batch Mode (default)"
        ) *> batchOpts
  where batchOpts = pure Batch <*> networkOpt
                               <*> sysEnvOptions
                               <*> elvlOpt
                               <*> parOpt
                               <*> printConfigOpt
                               <*> consoleOpt
                               <*> compileStagesOpt LocalCompiler

-- | Expression-Level flag.
elvlOpt :: Parser Bool
elvlOpt = switch (   short 'e'
                  <> long "expression"
                  <> help "Run in top-level expression mode." )

-- | Network mode flag.
networkOpt :: Parser Bool
networkOpt = switch (   short 'n'
                     <> long "network"
                     <> help "Run in Network Mode" )

-- | Parallel mode flag.
parOpt :: Parser Bool
parOpt = switch (   long "parallel"
                 <> help "Run the Parallel Engine" )

consoleOpt :: Parser Bool
consoleOpt = switch (   long "console"
                     <> help "Toggle the interpreter console" )

data InterpPrintVerbosity = PrintVerbose | PrintTerse | PrintTerseSimple

-- | Print options for interpreter
printConfigOpt :: Parser PrintConfig
printConfigOpt = choosePC <$> verbosePrintFlag <*> simplePrintFlag
  where choosePC _ PrintTerseSimple = simplePrintConfig
        choosePC PrintTerse _       = tersePrintConfig
        choosePC _     _            = defaultPrintConfig

        verbosePrintFlag = flag
                       PrintTerse
                       PrintVerbose
                       (long "verbose"
                       <> short 'v'
                       <> help "Verbose interpreter printout")

        -- | Simple logging for interpreter
        simplePrintFlag = flag
                            PrintVerbose
                            PrintTerseSimple
                            (long "simple"
                            <> help "Use simple printing format for logging")



-- | Typecheck options
typecheckOptions :: Parser Mode
typecheckOptions = Typecheck <$> (TypecheckOptions <$> noQuickTypesOpt <*> printQuickTypesOpt)

noQuickTypesOpt :: Parser Bool
noQuickTypesOpt = switch (
                       long    "no-quicktypes"
                    <> help    "Use constraint-based typesystem"
                )

printQuickTypesOpt :: Parser Bool
printQuickTypesOpt = switch (
                         long    "print-quicktypes"
                      <> help    "Show quicktypes as typechecker output"
                   )


optimizationOpt :: Parser OptimizationLevel
optimizationOpt = option auto
        (  long "optimize"
        <> short 'O'
        <> metavar "OPTIMIZE"
        <> value 0
        <> help "Optimization level")


-- | Information printing options.
informOptions :: Parser InfoSpec
informOptions = InfoSpec <$> loggingOptions <*> verbosityOptions


{- Compiler service options -}
serviceOptions :: Parser Mode
serviceOptions = Service <$> serviceOperOpt

serviceOperOpt :: Parser ServiceOperation
serviceOperOpt = helper <*> subparser (
         command "master" (info smasterOpt  $ progDesc smasterDesc)
      <> command "worker" (info sworkerOpt  $ progDesc sworkerDesc)
      <> command "submit" (info sjobOpt     $ progDesc sjobDesc)
      <> command "query"  (info squeryOpt   $ progDesc squeryDesc)
      <> command "halt"   (info shaltOpt    $ progDesc shaltDesc)
    )
  where smasterOpt = RunMaster    <$> serviceOpts ServicePrepare <*> serviceMasterOpts
        sworkerOpt = RunWorker    <$> serviceOpts ServiceParallel
        sjobOpt    = SubmitJob    <$> serviceOpts ServiceClient <*> remoteJobOpt
        squeryOpt  = QueryService <$> serviceOpts ServiceClient <*> querySOpt
        shaltOpt   = Shutdown     <$> serviceOpts ServiceClient

        smasterDesc   = "Run a K3 compiler service master"
        sworkerDesc   = "Run a K3 compiler service worker"
        sjobDesc      = "Submit a K3 compilation job"
        squeryDesc    = "Query the K3 compiler service"
        shaltDesc     = "Halt the K3 compiler service"

serviceOpts :: CompilerType -> Parser ServiceOptions
serviceOpts ct = ServiceOptions <$> serviceIdOpt
                                <*> serviceHostOpt
                                <*> servicePortOpt
                                <*> serviceThreadsOpt
                                <*> serviceLogOpt
                                <*> serviceLogLevelOpt
                                <*> serviceHeartbeatOpt
                                <*> compileOpts ct

serviceMasterOpts :: Parser ServiceMasterOptions
serviceMasterOpts = ServiceMasterOptions <$> compileStagesOpt ServiceFinal

serviceIdOpt :: Parser String
serviceIdOpt = strOption (   long    "svid"
                          <> value   ""
                          <> help    "Service entity identifier"
                          <> metavar "SERVICEID" )

serviceHostOpt :: Parser String
serviceHostOpt = strOption (   long    "host"
                            <> value   ""
                            <> help    "Compiler service bind address"
                            <> metavar "SERVICEHOST" )

servicePortOpt :: Parser Int
servicePortOpt = read <$> strOption (   long    "port"
                                     <> value   "10000"
                                     <> help    "Compiler service port"
                                     <> metavar "SERVICEPORT" )

serviceThreadsOpt :: Parser Int
serviceThreadsOpt = option auto (
                       short   'w'
                    <> long    "workers"
                    <> value   1
                    <> help    "Number of service worker threads"
                    <> metavar "NTHREADS" )

serviceLogOpt :: Parser (Either String FilePath)
serviceLogOpt = mkLog <$> strOption (   long    "svlog"
                                     <> value   "stdout"
                                     <> help    "Service log file or handle"
                                     <> metavar "SERVICELOG" )
  where mkLog s | (map toLower s) `elem` ["stdout", "stderr"] = Left $ map toLower s
                | otherwise = Right $ makeValid s

serviceLogLevelOpt :: Parser Priority
serviceLogLevelOpt = option auto (   long    "svloglevel"
                                  <> value   DEBUG
                                  <> help    "Service log level"
                                  <> metavar "SERVICELOGLVL" )

serviceHeartbeatOpt :: Parser Int
serviceHeartbeatOpt = option auto (   long    "heartbeat"
                                   <> value   10
                                   <> help    "Service heartbeat period"
                                   <> metavar "PERIOD" )

remoteJobOpt :: Parser RemoteJobOptions
remoteJobOpt = RemoteJobOptions <$> workerFactorOpt
                                <*> workerBlockSizeOpt
                                <*> jobBlockSizeOpt
                                <*> reportSizeOpt
                                <*> compileStagesOpt ServiceClientRemote

jobBlockSizeOpt :: Parser Int
jobBlockSizeOpt = option auto (
                       long    "blocksize"
                    <> value   16
                    <> help    "Remote job block size"
                    <> metavar "SIZE" )

reportSizeOpt :: Parser Int
reportSizeOpt = option auto (
                       long    "reportsize"
                    <> value   20
                    <> help    "Compile job report size"
                    <> metavar "REPSIZE" )

workerFactorOpt :: Parser (Map String Int)
workerFactorOpt = extract . keyValList ""
                    <$> strOption (    long    "workerfactor"
                                    <> value   ""
                                    <> help    "Worker assignment factor"
                                    <> metavar "WAFACTOR" )

  where extract = Map.fromList . map (second read)

workerBlockSizeOpt :: Parser (Map String Int)
workerBlockSizeOpt = extract . keyValList ""
                       <$> strOption (    long    "workerblocks"
                                       <> value   ""
                                       <> help    "Worker block sizes"
                                       <> metavar "WBLOCKS" )

  where extract = Map.fromList . map (second read)

querySOpt :: Parser QueryOptions
querySOpt = qworkerOpt <|> qprogOpt <|> allWorkerOpt <|> allProgOpt

qworkerOpt :: Parser QueryOptions
qworkerOpt = (\w -> QueryOptions $ Left w) . splitOn ","
                <$> strOption (   long    "qworkers"
                               <> value   ""
                               <> help    "Workers to query"
                               <> metavar "WAQUERY" )

qprogOpt :: Parser QueryOptions
qprogOpt = (\p -> QueryOptions $ Right $ map read p) . splitOn ","
                <$> strOption (   long    "qjobs"
                               <> value   ""
                               <> help    "Jobs to query"
                               <> metavar "JSQUERY" )

allWorkerOpt :: Parser QueryOptions
allWorkerOpt = flag' (QueryOptions $ Left [])
                 (    long    "qaworkers"
                   <> help    "Query all workers statuses" )

allProgOpt :: Parser QueryOptions
allProgOpt = flag' (QueryOptions $ Right [])
               (    long    "qajobs"
                 <> help    "Query all job statuses" )


{- SQL mode options -}

-- | SQL mode
sqlOptions :: Parser Mode
sqlOptions = SQL <$> ( SQLOptions <$> printModeOpt "" <*> sqlPrintParseOpt <*> sqlUntypedOpt
                                  <*> sqlDistributedOpt <*> sqlDoCompileOpt
                                  <*> optional (compileOpts LocalCompiler) )

sqlPrintParseOpt :: Parser Bool
sqlPrintParseOpt = switch (   long "sqlast"
                           <> help "Print SQL AST parsed." )

sqlUntypedOpt :: Parser Bool
sqlUntypedOpt = switch (   long "sqluntyped"
                        <> help "Print untyped SQL AST." )

sqlDistributedOpt :: Parser Bool
sqlDistributedOpt = switch (   long "sqldistributed"
                            <> help "Generated a distributed SQL query plan." )

sqlDoCompileOpt :: Parser Bool
sqlDoCompileOpt = switch (  long "sqlcompile"
                           <> help "Compile SQL binary." )


{- Top-level options -}

-- | Logging options.
loggingOptions :: Parser LoggerOptions
loggingOptions = many $ option (eitherReader parseInstruction) (
                       long "log"
                    <> help "Enable logging on TAG"
                    <> metavar "TAG"
                 )

-- | Path options.
pathOptions :: Parser PathOptions
pathOptions = PathOptions <$> many ( strOption (
                     short 'I'
                  <> long "include"
                  <> help "Includes a directory on the source code search path"
                  <> metavar "DIRECTORY"
                ))

-- | Verbosity options.
verbosityOptions :: Parser Verbosity
verbosityOptions = toEnum . roundVerbosity <$> option auto (
        short 'v'
     <> long "verbosity"
     <> help "Verbosity of Output. [0..2]"
     <> showDefault
     <> value 0
     <> metavar "LEVEL"
    )
  where
    roundVerbosity n
        | n < 0 = 0
        | n > 2 = 2
        | otherwise = n

-- | Metaprogram option parsing.
metaprogramOptions :: Parser (Maybe MetaprogramOptions)
metaprogramOptions = optional (MetaprogramOptions <$> mpinterpretArgOpt <*> mpModuleSearchPathOpt)

mpinterpretArgOpt :: Parser [(String, String)]
mpinterpretArgOpt = keyValList "-" <$> strOption (   long    "mpargs"
                                                  <> value   ""
                                                  <> help    "Metaprogram interpreter args"
                                                  <> metavar "MPINTERPARGS" )

mpModuleSearchPathOpt :: Parser [String]
mpModuleSearchPathOpt = pathList <$> strOption (   long    "mpsearch"
                                                <> value   ""
                                                <> help    "Metaprogram module search path"
                                                <> metavar "MPSEARCHPATH" )

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = Options <$> modeOptions
                         <*> informOptions
                         <*> metaprogramOptions
                         <*> pathOptions
                         <*> ioOptions

{- Instance definitions -}

instance Pretty Mode where
  prettyLines (Parse     pOpts) = ["Parse " ++ show pOpts]
  prettyLines (Compile   cOpts) = ["Compile " ++ show cOpts]
  prettyLines (Interpret iOpts) = ["Interpret "] ++ (indent 2 $ prettyLines iOpts)
  prettyLines (Typecheck tOpts) = ["Typecheck " ++ show tOpts]
  prettyLines (Service   sOpts) = ["Service " ++ show sOpts]
  prettyLines (SQL       sOpts) = ["SQL " ++ show sOpts]

instance Pretty InterpretOptions where
  prettyLines (Batch net env expr par printConf console cstages) =
    ["Batch"] ++ (indent 3 $ ["Network: " ++ show net]
                          ++ prettySysEnv env
                          ++ ["Expression: " ++ show expr]
                          ++ ["Parallel: "   ++ show par]
                          ++ ["Print: "      ++ show printConf]
                          ++ ["Console: "    ++ show console]
                          ++ ["Stages: "     ++ show cstages]
                 )

  prettyLines v = [show v]
