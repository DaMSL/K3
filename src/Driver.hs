-- | Primary Driver for the K3 Ecosystem.

import Control.Applicative
import Control.Monad

import Data.Maybe

import System.IO

import Options.Applicative

import Language.K3.Driver.Batch
import Language.K3.Driver.Interactive

-- | Program Options.
data Options = Options {
        mode :: Mode
    }

-- | Modes of Operation.
data Mode
    = Batch FilePath
    | Interactive
  deriving (Eq, Read, Show)

-- | Options for Batch Mode.
batchOptions :: Parser Mode
batchOptions = Batch <$> (
        flag' Batch (
            short 'b'
         <> long "batch"
         <> help "Run in Batch Mode"
        ) *> argument str (
            metavar "FILE"
        )
    )

-- | Options for Interactive Mode.
interactiveOptions :: Parser Mode
interactiveOptions = flag' Interactive (
        short 'i'
     <> long "interactive"
     <> help "Run in Interactive Mode"
    )

-- | Mode Options Parsing.
modeOptions :: Parser Mode
modeOptions = fmap (fromMaybe (Batch "-")) $ optional $
        batchOptions
    <|> interactiveOptions

-- | Program Options Parsing.
programOptions :: Parser Options
programOptions = Options <$> modeOptions

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

-- | Mode Dispatch.
dispatch :: Options -> IO ()
dispatch op = do
    case mode op of
        Batch f -> openFileOrStdIn f >>= runBatch
        Interactive -> runInteractive

-- | Top-Level.
main = execParser options >>= dispatch
  where
    options = info (helper <*> programOptions) $ fullDesc
        <> progDesc "The K3 Compiler."
        <> header "The K3 Compiler."
