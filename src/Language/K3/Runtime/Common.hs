{-# LANGUAGE ViewPatterns #-}

-- | Common definitions for the Haskell runtime and code generator.
module Language.K3.Runtime.Common (
    PeerBootstrap
  , SystemEnvironment
  , defaultSystem
) where

import Data.List 

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Literal    as LC
import qualified Language.K3.Core.Constructor.Type       as TC

-- | A bootstrap environment for a peer.
type PeerBootstrap = [(Identifier, K3 Literal)]

-- | A system environment, to bootstrap a set of deployed peers.
type SystemEnvironment = [(Address, PeerBootstrap)]

defaultSystem :: SystemEnvironment
defaultSystem = [(defaultAddress, [ ("me",    defaultAddressL)
                                  , ("peers", defaultPeersL)
                                  , ("role",  defaultRoleL)])]
  where defaultAddressL = LC.address (LC.string "127.0.0.1") (LC.int 40000)
        defaultPeersL   = (LC.empty TC.address) @+ (LAnnotation "Collection")
        defaultRoleL    = LC.string ""
