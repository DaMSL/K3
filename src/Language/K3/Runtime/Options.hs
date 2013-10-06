-- | Common option parsing for all K3 programs.
module Language.K3.Runtime.Options where

import Data.List.Split
import Data.Maybe

import Control.Applicative
import Options.Applicative

import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Expression as E

import Language.K3.Parser
import Language.K3.Runtime.Engine ( SystemEnvironment, PeerBootstrap )

peerBReader :: String -> Either ParseError (Address, PeerBootstrap)
peerBReader peerDesc = case splitOn ":" peerDesc of
    (host:port:maps) -> Right (
            Address (host, read port),
            ("me", E.address (E.constant $ E.CString host) (E.constant $ E.CInt $ read port)) :
            [(k, v) | mapping <- maps, let (k:s:_) = splitOn "=" mapping,
                      let pv = parseExpression s,
                      isJust pv,
                      let v = fromJust pv
            ]
        )
    _ -> Left $ ErrorMsg "Invalid Peer Parse"

peerBOptions :: Parser (Address, PeerBootstrap)
peerBOptions = nullOption (
        short 'p'
     <> long "peer"
     <> reader peerBReader
     <> help "Peer configuration in the format role:host:port."
    )

sysEnvOptions :: Parser SystemEnvironment
sysEnvOptions = some peerBOptions
