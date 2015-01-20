module Language.K3.Codegen.CPP.Simplification (
  simplifyCPP,
) where

import Control.Applicative

import Control.Monad.Identity
import Control.Monad.State

import Language.K3.Codegen.CPP.Representation

type SimplificationS = ()
type SimplificationM = StateT SimplificationS Identity

runSimplificationM :: SimplificationM a -> SimplificationS -> (a, SimplificationS)
runSimplificationM s t = runIdentity $ runStateT s t

simplifyCPP :: [Definition] -> [Definition]
simplifyCPP ds = fst $ runSimplificationM (mapM simplifyCPPDefinition ds) ()

