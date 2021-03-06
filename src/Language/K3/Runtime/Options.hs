-- | Common option parsing for all K3 programs.
module Language.K3.Runtime.Options where

import Control.Applicative
import Options.Applicative

import Text.Parser.Char
import Text.Parser.Combinators hiding ( option )
import Text.Parser.Token

import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Literal as LC
import Language.K3.Parser.DataTypes
import Language.K3.Parser
import Language.K3.Runtime.Common ( PeerBootstrap, SystemEnvironment )

peerBReader :: String -> Either String (Address, PeerBootstrap)
peerBReader peerDesc = either (Left . show) Right $ runK3Parser Nothing parser peerDesc
  where parser       = mkBootstrap <$> ipAddressP <* colon <*> portP
                                   <*> many ((,) <$> (colon *> identifier) <* symbol "=" <*> literal)
        ipAddressP   = some $ choice [alphaNum, oneOf "."]
        portP        = fromIntegral <$> natural

        mkBootstrap host port bootstrap =
          let addr  = Address (host, port)
              addrE = LC.address (LC.string host) (LC.int port)
          in
          (addr, ("me", addrE):(filter (("me" /=) . fst) bootstrap))

peerBOptions :: Parser (Address, PeerBootstrap)
peerBOptions = option (eitherReader peerBReader) (
        short 'p'
     <> long "peer"
     <> help "Peer configuration in the format role:host:port."
    )

sysEnvOptions :: Parser SystemEnvironment
sysEnvOptions = some peerBOptions
