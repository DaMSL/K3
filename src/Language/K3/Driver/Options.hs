-- | Options for the K3 Driver
module Language.K3.Driver.Options where

import Data.List.Split

import Control.Applicative
import Options.Applicative

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
    = Batch { peerList :: [Peer] }
    | Interactive
  deriving (Eq, Read, Show)

data Peer = Peer {
    peerHost :: String,
    peerPort :: Int,
    peerVals :: [(String, String)]
} deriving (Eq, Read, Show)

data Verbosity
    = NullV
    | SoftV
    | LoudV
  deriving (Enum, Eq, Read, Show)

-- | Options for Batch Mode.
batchOptions :: Parser Mode
batchOptions = flag' Batch (
            short 'b'
         <> long "batch"
         <> help "Run in Batch Mode (default)"
        ) *> pure Batch <*> some peerOptions

peerReader :: String -> Either ParseError Peer
peerReader peerDesc = case splitOn ":" peerDesc of
    (host:port:maps) -> Right $ Peer host (read port) [(k, v) | mapping <- maps, let (k:v:_) = splitOn "=" mapping]
    _ -> Left $ ErrorMsg "Invalid Peer Parse"

peerOptions :: Parser Peer
peerOptions = nullOption (
        short 'p'
     <> long "peer"
     <> reader peerReader
     <> help "Peer configuration in the format role:host:port."
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
modeOptions = batchOptions <|> interactiveOptions

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
