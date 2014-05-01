-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Control.Applicative
import Options.Applicative
import Options.Applicative.Types

import System.FilePath
import System.Log

import Language.K3.Runtime.Common ( SystemEnvironment )
import Language.K3.Runtime.Options
import Language.K3.Utils.Logger.Config

import Language.K3.Driver.Common

import Language.K3.Utils.Pretty (Pretty(..), indent, defaultPrintConfig, PrintConfig(..), tersePrintConfig, simplePrintConfig)

-- | Program Options.
data Options = Options {
        mode      :: Mode,
        inform    :: InfoSpec,
        paths     :: PathOptions,
        input     :: FilePath,
        preLoad   :: [FilePath], -- files to load before input
        asIs      :: Bool
    }
  deriving (Eq, Read, Show)

-- | Modes of Operation.
data Mode
    = Compile   CompileOptions
    | Interpret InterpretOptions
    | Print     PrintOptions
    | Typecheck
    | Analyze   AnalyzeOptions
  deriving (Eq, Read, Show)

-- | Compilation options datatype.
data CompileOptions = CompileOptions { outLanguage :: String
                                     , programName :: String
                                     , outputFile  :: Maybe FilePath
                                     , buildDir    :: Maybe FilePath }
                        deriving (Eq, Read, Show)

-- | Interpretation options.
data InterpretOptions
    = Batch { network :: Bool
            , sysEnv :: SystemEnvironment
            , asExpr :: Bool
            , isPar  :: Bool
            , printConfig :: PrintConfig}
    | Interactive
  deriving (Eq, Read, Show)

-- | Pretty-printing options.
data PrintOutput
    = PrintAST
    | PrintSyntax
  deriving (Eq, Read, Show)

data PrintOptions
    = PrintOptions { printOutput :: PrintOutput }
  deriving (Eq, Read, Show)


-- | Analyze Options.
data AnalyzeOptions
    = Conflicts
    | Tasks
    | ProgramTasks
    | ProxyPaths
    | AnnotationProvidesGraph
    | FlatAnnotations
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

-- | Deprecated?
data Peer = Peer { peerHost :: String
                 , peerPort :: Int
                 , peerVals :: [(String, String)] }
              deriving (Eq, Read, Show)

-- | Mode Options Parsing.
modeOptions :: Parser Mode
modeOptions = subparser (
         command "compile"   (info compileOptions   $ progDesc compileDesc)
      <> command "interpret" (info interpretOptions $ progDesc interpretDesc)
      <> command "print"     (info printOptions     $ progDesc printDesc)
      <> command "typecheck" (info typeOptions      $ progDesc typeDesc)
      <> command "analyze"   (info analyzeOptions   $ progDesc analyzeDesc)
    )
  where compileDesc   = "Compile a K3 binary"
        interpretDesc = "Interpret a K3 program"
        printDesc     = "Print a K3 program"
        typeDesc      = "Typecheck a K3 program"
        analyzeDesc   = "Analyze a K3 program"

        typeOptions = NilP $ Just Typecheck

-- | Compiler options
compileOptions :: Parser Mode
compileOptions = mkCompile <$> outLanguageOpt <*> progNameOpt <*> outputFileOpt <*> buildDirOpt
  where mkCompile l n o b = Compile $ CompileOptions l n o b 

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


-- | Interpretation options.
interpretOptions :: Parser Mode
interpretOptions = Interpret <$> (batchOptions <|> interactiveOptions)

-- | Options for Batch Mode.
batchOptions :: Parser InterpretOptions
batchOptions = flag' Batch (
            short 'b'
         <> long "batch"
         <> help "Run in Batch Mode (default)"
        ) *> pure Batch <*> networkOptions <*> sysEnvOptions <*> elvlOptions <*> parOptions <*> interpPrintOptions

-- | Expression-Level flag.
elvlOptions :: Parser Bool
elvlOptions = switch (
        short 'e'
     <> long "expression"
     <> help "Run in top-level expression mode."
    )

-- | Options for Interactive Mode.
interactiveOptions :: Parser InterpretOptions
interactiveOptions = flag' Interactive (
        short 'i'
     <> long "interactive"
     <> help "Run in Interactive Mode"
    )

-- | Network mode flag.
networkOptions :: Parser Bool
networkOptions = switch (
	short 'n'
     <> long "network"
     <> help "Run in Network Mode"
    )

-- | Parallel mode flag.
parOptions :: Parser Bool
parOptions = switch (
        long "parallel"
     <> help "Run the Parallel Engine"
    )

data InterpPrintVerbosity = PrintVerbose | PrintTerse | PrintTerseSimple

-- | Print options for interpreter
interpPrintOptions :: Parser PrintConfig
interpPrintOptions = choosePC <$> verbosePrintFlag <*> simplePrintFlag
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
                  


-- | Printing options
printOptions :: Parser Mode
printOptions = mkPrint <$> (astPrintOpt <|> syntaxPrintOpt)
  where mkPrint o = Print $ PrintOptions o

astPrintOpt :: Parser PrintOutput
astPrintOpt = flag' PrintAST (   long "ast"
                              <> help "Print AST output" )

syntaxPrintOpt :: Parser PrintOutput
syntaxPrintOpt = flag' PrintSyntax (   long "syntax"
                                    <> help "Print syntax output" )
-- | Analyze options
analyzeOptions :: Parser Mode
analyzeOptions = Analyze <$> (    conflictsOpt <|> tasksOpt   <|> programTasksOpt <|> proxyPathsOpt
                              <|> annProvOpt   <|> flatAnnOpt )

conflictsOpt :: Parser AnalyzeOptions
conflictsOpt = flag' Conflicts (   long "conflicts"
                                <> help "Print Conflicting Data Accesses for a K3 Program" )

tasksOpt :: Parser AnalyzeOptions
tasksOpt = flag' Tasks (   long "tasks"
                        <> help "Split Triggers into smaller tasks for parallelization" )

programTasksOpt :: Parser AnalyzeOptions
programTasksOpt = flag' ProgramTasks (   long "programtasks"
                                      <> help "Find program-level tasks to be run in parallel " )

proxyPathsOpt :: Parser AnalyzeOptions
proxyPathsOpt = flag' ProxyPaths (   long "proxypaths"
                                  <> help "Print bind paths for bind expressions" )

annProvOpt :: Parser AnalyzeOptions
annProvOpt = flag' AnnotationProvidesGraph (   long "provides-graph"
                                            <> help "Print bind paths for bind expressions" )

flatAnnOpt :: Parser AnalyzeOptions
flatAnnOpt = flag' FlatAnnotations (   long "flat-annotations"
                                    <> help "Print bind paths for bind expressions" )

-- | Information printing options.
informOptions :: Parser InfoSpec
informOptions = InfoSpec <$> loggingOptions <*> verbosityOptions

-- | Logging options.
loggingOptions :: Parser LoggerOptions
loggingOptions = many $ option (
                       long "log"
                    <> help "Logging directive"
                    <> metavar "LOG_INSTRUCTION"
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

inputOptions :: Parser [FilePath]
inputOptions = commaSep1 <$> argument str (
        metavar "FILES"
     <> help "Initial source declarations to be loaded into the environment."
     <> value "-"
    )
      -- Separate a word list by commas
      where commaSep1 l  = fst $ foldr sep_fn ([[]], False) l
            sep_fn ',' (x, _)    = (x, True)
            sep_fn c ([], False)   = ([[c]], False)
            sep_fn c (x:xs, False) = ((c:x):xs, False)
            sep_fn c (xs, True)    = ([c]:xs, False)

asIsOption :: Parser Bool
asIsOption = switch (
    short 'a' <>
    long "as-is" <>
    help "Parse As-Is, without running the program builder." <>
    value False)

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = mkOptions <$> modeOptions <*> informOptions <*> pathOptions <*> inputOptions <*> asIsOption
    where mkOptions m i p is b = Options m i p (last is) (take (length is - 1) is) b

{- Instance definitions -}

instance Pretty Mode where
  prettyLines (Compile   cOpts) = ["Compile " ++ show cOpts] 
  prettyLines (Interpret iOpts) = ["Interpret"] ++ (indent 2 $ prettyLines iOpts)
  prettyLines (Print     pOpts) = ["Print " ++ show pOpts]
  prettyLines Typecheck         = ["Typecheck"]
  prettyLines (Analyze   aOpts) = ["Analyze" ++ show aOpts]

instance Pretty InterpretOptions where
  prettyLines (Batch net env expr par printOpts) =
    ["Batch"] ++ (indent 3 $ ["Network: " ++ show net]
                          ++ prettySysEnv env 
                          ++ ["Expression: " ++ show expr]
                          ++ ["Parallel: " ++ show par]
                          ++ ["Print: " ++ show printOpts])
  
  prettyLines v = [show v]

