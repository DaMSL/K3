{-# LANGUAGE TupleSections #-}

-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Control.Applicative
import Options.Applicative

import System.FilePath
import System.Log

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
      mode      :: Mode
    , inform    :: InfoSpec
    , paths     :: PathOptions
    , input     :: FilePath
    , noFeed    :: Bool
    }
  deriving (Eq, Read, Show)

-- | Modes of Operation.
data Mode
    = Parse     ParseOptions
    | Compile   CompileOptions
    | Interpret InterpretOptions
    | Typecheck TypecheckOptions
    | Analyze   AnalyzeOptions
  deriving (Eq, Read, Show)

data PrintMode
    = PrintAST
    | PrintSyntax
  deriving (Eq, Read, Show)

-- | Parsing options.
data ParseOptions = ParseOptions { parsePrintMode :: PrintMode }
                    deriving (Eq, Read, Show)

-- | Compilation options datatype.
data CompileOptions = CompileOptions
                      { outLanguage  :: String
                      , programName  :: String
                      , runtimePath  :: FilePath
                      , outputFile   :: Maybe FilePath
                      , buildDir     :: Maybe FilePath
                      , ccCmd        :: CPPCompiler
                      , ccStage      :: CPPStages
                      , cppOptions   :: String
                      , coTransform  :: TransformOptions
                      , useSubTypes  :: Bool
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
            , ioTransform :: TransformOptions
            }
    | Interactive
  deriving (Eq, Read, Show)

-- | Typechecking options
data TypecheckOptions
    = TypecheckOptions { noQuickTypes    :: Bool 
                       , printQuickTypes :: Bool }
  deriving (Eq, Read, Show)

-- | Analyze Options.
data AnalyzeOptions
    = AnalyzeOptions { aoTransform      :: TransformOptions
                     , analyzePrintMode :: PrintMode   }
  deriving (Eq, Read, Show)

type TransformOptions = [TransformMode]

data TransformMode
    = Conflicts
    | Tasks
    | ProgramTasks
    | ProxyPaths
    | AnnotationProvidesGraph
    | FlatAnnotations
    | Effects
    | EffectNormalization
    | FoldConstants
    | DeadCodeElimination
    | Profiling
    | ReadOnlyBinds
  deriving (Eq, Read, Show)

-- | Logging and information output options.
data InfoSpec = InfoSpec { logging   :: LoggerOptions
                         , verbosity :: Verbosity }
                  deriving (Eq, Read, Show)

-- | Logging directives, passed through to K3.Logger.Config .
type LoggerInstruction = (String,Priority)
type LoggerOptions     = [LoggerInstruction]

-- | Path related options
data PathOptions = PathOptions { includes :: [FilePath] }
  deriving (Eq, Read, Show)

-- | Verbosity levels.
data Verbosity
    = NullV
    | SoftV
    | LoudV
  deriving (Enum, Eq, Read, Show)


-- | Mode Options Parsing.
modeOptions :: Parser Mode
modeOptions = subparser (
         command "parse"     (info parseOptions     $ progDesc parseDesc)
      <> command "compile"   (info compileOptions   $ progDesc compileDesc)
      <> command "interpret" (info interpretOptions $ progDesc interpretDesc)
      <> command "typecheck" (info typecheckOptions $ progDesc typeDesc)
      <> command "analyze"   (info analyzeOptions   $ progDesc analyzeDesc)
    )
  where parseDesc     = "Parse a K3 program"
        compileDesc   = "Compile a K3 binary"
        interpretDesc = "Interpret a K3 program"
        typeDesc      = "Typecheck a K3 program"
        analyzeDesc   = "Analyze a K3 program"

-- | Print mode flags
printModeOpt :: Parser PrintMode
printModeOpt = (astPrintOpt <|> syntaxPrintOpt)

astPrintOpt :: Parser PrintMode
astPrintOpt = flag' PrintAST (   long "ast"
                              <> help "Print AST output" )

syntaxPrintOpt :: Parser PrintMode
syntaxPrintOpt = flag' PrintSyntax (   long "syntax"
                                    <> help "Print syntax output" )

-- | Parse mode
parseOptions :: Parser Mode
parseOptions = Parse . ParseOptions <$> printModeOpt

-- | Transformation options
transformOptions :: Parser TransformOptions
transformOptions = concat <$> many transformMode

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
                            <*> transformOptions
                            <*> noQuickTypesOpt

outLanguageOpt :: Parser String
outLanguageOpt = option ( short   'l'
                      <> long    "language"
                      <> value   defaultOutLanguage
                      <> reader  str
                      <> help    "Specify compiler target language"
                      <> metavar "LANG" )

progNameOpt :: Parser String
progNameOpt = option (   short   'n'
                      <> long    "name"
                      <> value   defaultProgramName
                      <> reader  str
                      <> help    "Program name"
                      <> metavar "PROGNAME" )

runtimePathOpt :: Parser FilePath
runtimePathOpt = option (
                       short   'r'
                    <> long    "runtime"
                    <> reader  str
                    <> help    "Specify runtime path"
                    <> metavar "RUNTIME" )

outputFileOpt :: Parser (Maybe FilePath)
outputFileOpt = validatePath <$> option (
                       short   'o'
                    <> long    "output"
                    <> value   defaultOutputFile
                    <> reader (\s -> str s >>= return . Just)
                    <> help    "Specify output file"
                    <> metavar "OUTPUT" )
  where validatePath Nothing  = Nothing
        validatePath (Just p) = if isValid p then Just p else Nothing

buildDirOpt :: Parser (Maybe FilePath)
buildDirOpt = validatePath <$> option (
                       short   'b'
                    <> long    "build"
                    <> value   defaultBuildDir
                    <> reader (\s -> str s >>= return . Just)
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
                               <*> transformOptions

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

-- | Analyze options
analyzeOptions :: Parser Mode
analyzeOptions = Analyze <$> (AnalyzeOptions <$> transformOptions <*> printModeOpt)

-- Accept a precursor string
transformMode :: Parser [TransformMode]
transformMode   =  wrap <$> conflictsOpt
              <|> wrap <$> tasksOpt
              <|> wrap <$> programTasksOpt
              <|> wrap <$> proxyPathsOpt
              <|> wrap <$> annProvOpt
              <|> wrap <$> flatAnnOpt
              <|> wrap <$> effectOpt
              <|> wrap <$> normalizationOpt
              <|> wrap <$> foldConstantsOpt
              <|> wrap <$> deadCodeElimOpt
              <|> simplifyOpt
              <|> wrap <$> profilingOpt
              <|> wrap <$> readOnlyBindOpts
  where
    wrap x = [x]

conflictsOpt :: Parser TransformMode
conflictsOpt = flag' Conflicts (   long "fconflicts"
                                <> help "Print Conflicting Data Accesses for a K3 Program" )

tasksOpt :: Parser TransformMode
tasksOpt = flag' Tasks  (  long "ftasks"
                        <> help "Split Triggers into smaller tasks for parallelization" )

programTasksOpt :: Parser TransformMode
programTasksOpt = flag' ProgramTasks (   long "fprogramtasks"
                                      <> help "Find program-level tasks to be run in parallel " )

proxyPathsOpt :: Parser TransformMode
proxyPathsOpt = flag' ProxyPaths (   long "fproxypaths"
                                  <> help "Print bind paths for bind expressions" )

annProvOpt :: Parser TransformMode
annProvOpt = flag' AnnotationProvidesGraph (   long "fprovides-graph"
                                            <> help "Print bind paths for bind expressions" )

flatAnnOpt :: Parser TransformMode
flatAnnOpt = flag' FlatAnnotations (   long "fflat-annotations"
                                    <> help "Print bind paths for bind expressions" )

effectOpt :: Parser TransformMode
effectOpt = flag' Effects (   long "feffects"
                           <> help "Print program effects")

normalizationOpt :: Parser TransformMode
normalizationOpt = flag' EffectNormalization
                      (   long "fnormalize"
                       <> help "Print an effect-normalized program.")

foldConstantsOpt :: Parser TransformMode
foldConstantsOpt = flag' FoldConstants
                      (   long "ffold-constants"
                       <> help "Print a program after constant folding.")

deadCodeElimOpt :: Parser TransformMode
deadCodeElimOpt = flag' DeadCodeElimination
                      (   long "fdead-code"
                       <> help "Print a program after dead code elimination.")

simplifyOpt :: Parser [TransformMode]
simplifyOpt = flag' [EffectNormalization, FoldConstants, DeadCodeElimination]
                (   long "fsimplify"
                 <> (help $ "Print a program after running all simplification phases " ++
                            "(i.e., constant folding, DCE, CSE, etc)" ))

profilingOpt :: Parser TransformMode
profilingOpt = flag' Profiling
                (   long "fprofile"
                 <> (help $ "Add profiling points"))

readOnlyBindOpts :: Parser TransformMode
readOnlyBindOpts = flag' ReadOnlyBinds
                (   long "frobinds"
                 <> (help $ "Remove read-only binds"))

-- | Information printing options.
informOptions :: Parser InfoSpec
informOptions = InfoSpec <$> loggingOptions <*> verbosityOptions


{- Top-level options -}

-- | Logging options.
loggingOptions :: Parser LoggerOptions
loggingOptions = many $ option (
                       long "log"
                    <> help "Enable logging on TAG"
                    <> metavar "TAG"
                    <> eitherReader parseInstruction
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
verbosityOptions = toEnum . roundVerbosity <$> option (
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

inputOptions :: Parser [FilePath]
inputOptions = fileOrStdin <$> (many $ argument str (
        metavar "FILE"
     <> help "K3 program file."
    ) )
  where fileOrStdin [] = ["-"]
        fileOrStdin x  = x

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = mkOptions <$> modeOptions
                           <*> informOptions
                           <*> pathOptions
                           <*> noFeedOpt
                           <*> inputOptions
    where mkOptions m i p nf is = Options m i p (last is) nf

{- Instance definitions -}

instance Pretty Mode where
  prettyLines (Parse     pOpts) = ["Parse " ++ show pOpts]
  prettyLines (Compile   cOpts) = ["Compile " ++ show cOpts]
  prettyLines (Interpret iOpts) = ["Interpret"] ++ (indent 2 $ prettyLines iOpts)
  prettyLines (Typecheck tOpts) = ["Typecheck" ++ show tOpts]
  prettyLines (Analyze   aOpts) = ["Analyze" ++ show aOpts]

instance Pretty InterpretOptions where
  prettyLines (Batch net env expr par printConf console transform) =
    ["Batch"] ++ (indent 3 $ ["Network: " ++ show net]
                          ++ prettySysEnv env
                          ++ ["Expression: " ++ show expr]
                          ++ ["Parallel: "   ++ show par]
                          ++ ["Print: "      ++ show printConf]
                          ++ ["Console: "    ++ show console]
                          ++ ["Transform: "  ++ show transform]
                 )

  prettyLines v = [show v]
