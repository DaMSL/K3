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

-- |A function implementing the type decision procedure.  The tree at top level
--  is expected to be a @DRole@ declaration containing non-role declarations.
typeDecision :: K3 Declaration -> (TNormEnv, TAliasEnv)
typeDecision = undefined
