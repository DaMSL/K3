-- | Common option parsing for all K3 programs.
module Language.K3.Runtime.Options where

import Control.Applicative
import Options.Applicative

import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Token

import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Expression as E

import Language.K3.Parser
import Language.K3.Runtime.Engine ( SystemEnvironment, PeerBootstrap )

peerBReader :: String -> Either ParseError (Address, PeerBootstrap)
peerBReader peerDesc = either (Left . ErrorMsg . show) Right $ runK3Parser parser peerDesc
  where parser       = mkBootstrap <$> ipAddressP <* colon <*> portP
                                   <*> many ((,) <$> (colon *> identifier) <* symbol "=" <*> expr)         
        ipAddressP   = some $ choice [alphaNum, oneOf "."]
        portP        = fromIntegral <$> natural
        
        mkBootstrap host port bootstrap = 
          let addr  = Address (host, port)
              addrE = E.address (E.constant $ E.CString host) (E.constant $ E.CInt $ port)
          in
          (addr, ("me", addrE):(filter (("me" /=) . fst) bootstrap))

peerBOptions :: Parser (Address, PeerBootstrap)
peerBOptions = nullOption (
        short 'p'
     <> long "peer"
     <> reader peerBReader
     <> help "Peer configuration in the format role:host:port."
    )

sysEnvOptions :: Parser SystemEnvironment
sysEnvOptions = some peerBOptions
