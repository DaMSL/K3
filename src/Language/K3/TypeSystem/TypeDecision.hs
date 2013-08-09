{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
{-|
  This module implements the decision procedure used to construct a type
  environment for typechecking.
-}
module Language.K3.TypeSystem.TypeDecision
( typeDecision
) where

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment

-- |A function implementing the type decision procedure.  The tree at top level
--  is expected to be a @DRole@ declaration containing non-role declarations.
typeDecision :: K3 Declaration -> TypeDecideM (TNormEnv, TAliasEnv)
typeDecision = undefined

-- TODO: initial check to make sure that IDs don't overlap
