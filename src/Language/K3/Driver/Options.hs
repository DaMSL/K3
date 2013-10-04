-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Control.Applicative
import Options.Applicative

import System.FilePath

import Language.K3.Runtime.Engine (SystemEnvironment)
import Language.K3.Runtime.Options

-- | Program Options.
data Options = Options {
        mode :: Mode,
        elvl :: Bool,
        verbosity :: Verbosity,
        input :: FilePath
    }
  deriving (Eq, Read, Show)

-- | Modes of Operation.
data Mode
    = Compile   CompileOptions
    | Interpret InterpretOptions
    | Print     PrintOptions
  deriving (Eq, Read, Show)

-- | Compilation options datatype.
data CompileOptions = CompileOptions { language   :: Maybe String
                                     , outputFile :: Maybe FilePath }
                        deriving (Eq, Read, Show)

-- | Interpretation options.
data InterpretOptions
    = Batch { sysEnv :: SystemEnvironment }
    | Interactive
  deriving (Eq, Read, Show)

-- | Pretty-printing options.
data PrintOptions
    = PrintAST
    | PrintSyntax
  deriving (Eq, Read, Show)

-- | Deprecated?
data Peer = Peer { peerHost :: String
                 , peerPort :: Int
                 , peerVals :: [(String, String)] }
              deriving (Eq, Read, Show)


data Verbosity
    = NullV
    | SoftV
    | LoudV
  deriving (Enum, Eq, Read, Show)


-- | Compiler options
compileOptions :: Parser Mode
compileOptions = mkCompile <$> languageOpt <*> outputFileOpt
  where mkCompile l o = Compile $ CompileOptions l o

languageOpt :: Parser (Maybe String)
languageOpt = option (   short   'l'
                      <> long    "language"
                      <> value   Nothing
                      <> help    "Specify compiler target language"
                      <> metavar "LANG" )

outputFileOpt :: Parser (Maybe FilePath)
outputFileOpt = validatePath <$> option (
                       short   'o'
                    <> long    "output"
                    <> value   Nothing
                    <> help    "Specify output file"
                    <> metavar "OUTPUT" )
  where validatePath Nothing  = Nothing
        validatePath (Just p) = if isValid p then Just p else Nothing


-- | Interpretation options
interpretOptions :: Parser Mode
interpretOptions = Interpret <$> (batchOptions <|> interactiveOptions)

-- | Options for Batch Mode.
batchOptions :: Parser InterpretOptions
batchOptions = flag' Batch (
            short 'b'
         <> long "batch"
         <> help "Run in Batch Mode (default)"
        ) *> pure Batch <*> sysEnvOptions

-- | Options for Interactive Mode.
interactiveOptions :: Parser InterpretOptions
interactiveOptions = flag' Interactive (
        short 'i'
     <> long "interactive"
     <> help "Run in Interactive Mode"
    )

-- | Printing options
printOptions :: Parser Mode
printOptions = Print <$> (astPrintOpt <|> syntaxPrintOpt)

astPrintOpt :: Parser PrintOptions
astPrintOpt = flag' PrintAST (   long "ast"
                              <> help "Print AST output" )

syntaxPrintOpt :: Parser PrintOptions
syntaxPrintOpt = flag' PrintSyntax (   long "syntax"
                                    <> help "Print syntax output" )


-- | Mode Options Parsing.
modeOptions :: Parser Mode
modeOptions = subparser (
         command "compile"   (info compileOptions   $ progDesc compileDesc)
      <> command "interpret" (info interpretOptions $ progDesc interpretDesc)
      <> command "print"     (info printOptions     $ progDesc printDesc)
    )
  where compileDesc   = "Compile a K3 binary"
        interpretDesc = "Interpret a K3 program"
        printDesc     = "Print a K3 program"

-- | Expression-Level flag.
elvlOptions :: Parser Bool
elvlOptions = switch (
        short 'e'
     <> long "expression"
     <> help "Run in top-level expression mode."
    )

-- | Verbosity Options.
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

inputOptions :: Parser FilePath
inputOptions = argument str (
        metavar "FILE"
     <> help "Initial source declarations to be loaded into the environment."
     <> value "-"
    )

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = Options <$> modeOptions <*> elvlOptions <*> verbosityOptions <*> inputOptions
