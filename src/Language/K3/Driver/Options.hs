{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Control.Applicative
import Options.Applicative

import qualified Data.Map as Map
import Data.List.Split
import Data.Maybe

import System.FilePath
import System.Log

import Language.K3.Stages ( CompilerSpec(..), StageSpec(..), cs0 )
import Language.K3.Runtime.Common ( SystemEnvironment )
import Language.K3.Runtime.Options
import Language.K3.Utils.Logger.Config

import Language.K3.Driver.Common

import Language.K3.Utils.Pretty (
    Pretty(..), PrintConfig(..),
    indent, defaultPrintConfig, tersePrintConfig, simplePrintConfig
  )

-- | Program Options.
data Options = Options {
      mode         :: Mode
    , inform       :: InfoSpec
    , paths        :: PathOptions
    , input        :: FilePath
    , noFeed       :: Bool
    , noMP         :: Bool
    , mpOpts       :: Maybe MetaprogramOptions
    , analysisOpts :: [(String, String)]
    }
  deriving (Eq, Read, Show)

-- | Modes of Operation.
data Mode
    = Parse     ParseOptions
    | Compile   CompileOptions
    | Interpret InterpretOptions
    | Typecheck TypecheckOptions
  deriving (Eq, Read, Show)

-- | Compilation pass options.
type CompileStages = [CompileStage]

data CompileStage = SCompile (Maybe CompilerSpec)
                  | SCodegen
                  deriving (Eq, Ord, Read, Show)

-- | Parsing options.
data ParseOptions = ParseOptions { parsePrintMode :: PrintMode,
                                   poStages       :: CompileStages }
                    deriving (Eq, Read, Show)

-- | Printing specification.
data PrintMode
    = PrintAST    { stripEffects :: Bool
                  , stripTypes   :: Bool
                  , stripCmts    :: Bool
                  , stripProps   :: Bool }
    | PrintSyntax
  deriving (Eq, Read, Show)

-- | Metaprogramming options
data MetaprogramOptions
    = MetaprogramOptions { interpreterArgs  :: [(String, String)]
                         , moduleSearchPath :: [String] }
    deriving (Eq, Read, Show)

-- | Typechecking options
data TypecheckOptions
    = TypecheckOptions { noQuickTypes    :: Bool
                       , printQuickTypes :: Bool }
  deriving (Eq, Read, Show)

-- | Compilation options datatype.
data CompileOptions = CompileOptions
                      { outLanguage        :: String
                      , programName        :: String
                      , runtimePath        :: FilePath
                      , outputFile         :: Maybe FilePath
                      , buildDir           :: Maybe FilePath
                      , ccCmd              :: CPPCompiler
                      , ccStage            :: CPPStages
                      , cppOptions         :: String
                      , kTraceOptions      :: [(String, String)]
                      , coStages           :: CompileStages
                      , useSubTypes        :: Bool
                      , optimizationLevel  :: OptimizationLevel
                      , saveAST            :: Bool
                      , astPrintMode       :: PrintMode
                      }
  deriving (Eq, Read, Show)

data CPPCompiler = GCC | Clang deriving (Eq, Read, Show)

data CPPStages = Stage1 | Stage2 | AllStages deriving (Eq, Read, Show)

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

-- | Verbosity levels.
data Verbosity
    = NullV
    | SoftV
    | LoudV
  deriving (Enum, Eq, Read, Show)


-- | Utility functions for options.

-- | Constructs a path list from a string of colon-separated paths.
pathList :: String -> [String]
pathList = splitOn ":"

parseKVL :: String -> String -> String -> String -> [(String, String)]
parseKVL sepSym eqSym keyPrefix s = catMaybes $ map kvPair $ splitOn sepSym s
 where kvPair s' = case splitOn eqSym s' of
                     [x,y] -> Just (keyPrefix ++ x, y)
                     _     -> Nothing

keyValList :: String -> String -> [(String, String)]
keyValList = parseKVL ":" "="

specParamList :: String -> String -> [(String, String)]
specParamList = parseKVL "," "@"


-- | Mode Options Parsing.
modeOptions :: Parser Mode
modeOptions = subparser (
         command "parse"     (info parseOptions     $ progDesc parseDesc)
      <> command "compile"   (info compileOptions   $ progDesc compileDesc)
      <> command "interpret" (info interpretOptions $ progDesc interpretDesc)
      <> command "typecheck" (info typecheckOptions $ progDesc typeDesc)
    )
  where parseDesc     = "Parse a K3 program"
        compileDesc   = "Compile a K3 binary"
        interpretDesc = "Interpret a K3 program"
        typeDesc      = "Typecheck a K3 program"


{- Common parsers -}

compileStagesOpt :: Parser CompileStages
compileStagesOpt = extractStageAndSpec . keyValList "" <$> strOption (
                        long "fstage"
                     <> value ""
                     <> metavar "STAGES"
                     <> help "Run compilation stages" )

   where
    extractStageAndSpec kvl = case kvl of
      []  -> [SCompile $ Just cs0, SCodegen]
      [x] -> stageOf cs0 x
      h:t -> stageOf (specOf t) h

    stageOf _     ("none",     read -> True)          = []
    stageOf _     ("opt",      read -> True)          = [SCompile Nothing]
    stageOf cSpec ("declopt",  read -> True)          = [SCompile $ Just cSpec]
    stageOf cSpec ("cg",       read -> True)          = [SCompile $ Just cSpec, SCodegen]
    stageOf cSpec ("oinclude", (splitOn "," ->  psl)) = let ss = stageSpec cSpec
                                                        in [SCompile $ Just cSpec {stageSpec = ss {passesToRun = Just psl}}]
    stageOf cSpec ("oexclude", (splitOn "," -> npsl)) = let ss = stageSpec cSpec
                                                        in [SCompile $ Just cSpec {stageSpec = ss {passesToFilter = Just npsl}}]
    stageOf cSpec ("cinclude", (splitOn "," ->  psl)) = let ss = stageSpec cSpec
                                                        in [SCompile $ Just cSpec {stageSpec = ss {passesToRun = Just psl}}, SCodegen]
    stageOf cSpec ("cexclude", (splitOn "," -> npsl)) = let ss = stageSpec cSpec
                                                        in [SCompile $ Just cSpec {stageSpec = ss {passesToFilter = Just npsl}}, SCodegen]
    stageOf cSpec _ = [SCompile $ Just cSpec, SCodegen]

    specOf kvl = foldl specParam cs0 kvl
    specParam cs (k,v) = case k of
      "@blockSize" -> cs {blockSize = read v}
      _ -> let ss = stageSpec cs
           in cs {stageSpec = ss {snapshotSpec = Map.insertWith (++) k (splitOn "," v) $ snapshotSpec ss}}


-- | Parse mode
parseOptions :: Parser Mode
parseOptions = Parse <$> (ParseOptions <$> printModeOpt "" <*> compileStagesOpt)

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


-- | Compiler options
compileOptions :: Parser Mode
compileOptions = fmap Compile $ CompileOptions
                            <$> outLanguageOpt
                            <*> progNameOpt
                            <*> runtimePathOpt
                            <*> outputFileOpt
                            <*> buildDirOpt
                            <*> ccCmdOpt
                            <*> ccStageOpt
                            <*> cppOpt
                            <*> ktraceOpt
                            <*> compileStagesOpt
                            <*> noQuickTypesOpt
                            <*> optimizationOpt
                            <*> saveAstOpt
                            <*> printModeOpt "noeffects=True:notypes=True"

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

ccCmdOpt :: Parser CPPCompiler
ccCmdOpt = (gccFlag <|> clangFlag <|> pure Clang)

ccStageOpt :: Parser CPPStages
ccStageOpt = (stage1Flag <|> stage2Flag <|> allStagesFlag <|> pure AllStages)

gccFlag :: Parser CPPCompiler
gccFlag = flag' GCC (
        long "gcc"
     <> help "Use the g++ toolchain for C++ compilation"
    )

clangFlag :: Parser CPPCompiler
clangFlag = flag' Clang (
        long "clang"
     <> help "Use the clang++ and LLVM toolchain for C++ compilation"
    )

stage1Flag :: Parser CPPStages
stage1Flag = flag' Stage1 (short '1' <> long "stage1" <> help "Only run stage 1 compilation")

stage2Flag :: Parser CPPStages
stage2Flag = flag' Stage2 (short '2' <> long "stage2" <> help "Only run stage 2 compilation")

allStagesFlag :: Parser CPPStages
allStagesFlag = flag' AllStages (long "allstages" <> help "Compile all stages")

cppOpt :: Parser String
cppOpt = strOption $ long "cpp-flags" <> help "Specify CPP Flags" <> metavar "CPPFLAGS" <> value ""

ktraceOpt :: Parser [(String, String)]
ktraceOpt = keyValList "" <$> strOption (
                 long "ktrace-flags"
              <> help "Specify KTrace Flags"
              <> metavar "KTRACEFLAGS"
              <> value ""
            )

includeOpt :: Parser FilePath
includeOpt = strOption (
                long "CI"
             <> help "Specifies a C++ compiler include directory."
             <> metavar "DIRECTORY"
           )

libraryOpt :: Parser (Bool, FilePath)
libraryOpt = linkerDirOpt <|> libraryFileOpt

linkerDirOpt :: Parser (Bool, FilePath)
linkerDirOpt = (True,) <$> strOption (
                  long "CL"
               <> help "Specifies a C++ linker directory."
               <> metavar "DIRECTORY"
             )

libraryFileOpt :: Parser (Bool, FilePath)
libraryFileOpt = (False,) <$> strOption (
                    long "Cl"
                 <> help "Specifies a C++ library file."
                 <> metavar "FILE"
               )

saveAstOpt :: Parser Bool
saveAstOpt = switch (   long    "save-ast"
                     <> help    "Save K3 AST used for compilation" )


-- | Interpretation options.
interpretOptions :: Parser Mode
interpretOptions = Interpret <$> (batchOptions <|> interactiveOptions)

-- | Options for Interactive Mode.
interactiveOptions :: Parser InterpretOptions
interactiveOptions = flag' Interactive (
        short 'i'
     <> long "interactive"
     <> help "Run in Interactive Mode"
    )

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
                               <*> compileStagesOpt

-- | Expression-Level flag.
elvlOpt :: Parser Bool
elvlOpt = switch (
        short 'e'
     <> long "expression"
     <> help "Run in top-level expression mode."
    )

-- | Network mode flag.
networkOpt :: Parser Bool
networkOpt = switch (
	short 'n'
     <> long "network"
     <> help "Run in Network Mode"
    )

-- | Parallel mode flag.
parOpt :: Parser Bool
parOpt = switch (
        long "parallel"
     <> help "Run the Parallel Engine"
    )

consoleOpt :: Parser Bool
consoleOpt = switch (
         long "console"
      <> help "Toggle the interpreter console"
    )

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

noFeedOpt :: Parser Bool
noFeedOpt = switch (
       long "nofeed"
    <> help "Process a program, ignoring data feeds." )

noMPOpt :: Parser Bool
noMPOpt = switch (
       long "nometaprogram"
    <> help "Process a program, skipping metaprogram evaluation." )

inputOptions :: Parser [FilePath]
inputOptions = fileOrStdin <$> (many $ argument str (
        metavar "FILE"
     <> help "K3 program file."
    ) )
  where fileOrStdin [] = ["-"]
        fileOrStdin x  = x

-- | Metaprogram option parsing.
metaprogramOptions :: Parser (Maybe MetaprogramOptions)
metaprogramOptions = optional (MetaprogramOptions <$> interpreterArgOpts <*> moduleSearchPathOpts)

interpreterArgOpts :: Parser [(String, String)]
interpreterArgOpts = keyValList "-" <$> strOption (   long    "mpargs"
                                                   <> value   ""
                                                   <> help    "Metaprogram interpreter args"
                                                   <> metavar "MPINTERPARGS" )

moduleSearchPathOpts :: Parser [String]
moduleSearchPathOpts = pathList <$> strOption (   long    "mpsearch"
                                               <> value   ""
                                               <> help    "Metaprogram module search path"
                                               <> metavar "MPSEARCHPATH" )

-- | Analysis option parsing.
analysisOptions :: Parser [(String, String)]
analysisOptions = keyValList "" <$> strOption (   long    "analysis"
                                               <> value   ""
                                               <> help    "Analysis pass arguments"
                                               <> metavar "ANALYSISARGS" )

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = mkOptions <$> modeOptions
                           <*> informOptions
                           <*> pathOptions
                           <*> noFeedOpt
                           <*> noMPOpt
                           <*> metaprogramOptions
                           <*> analysisOptions
                           <*> inputOptions
    where mkOptions m i p nf nmp mp an is = Options m i p (last is) nf nmp mp an

{- Instance definitions -}

instance Pretty Mode where
  prettyLines (Parse     pOpts) = ["Parse " ++ show pOpts]
  prettyLines (Compile   cOpts) = ["Compile " ++ show cOpts]
  prettyLines (Interpret iOpts) = ["Interpret"] ++ (indent 2 $ prettyLines iOpts)
  prettyLines (Typecheck tOpts) = ["Typecheck" ++ show tOpts]

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
