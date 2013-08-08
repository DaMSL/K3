{-|
  A module containing the routines necessary to construct a skeletal environment
  for the type decision prodecure.
-}
module Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
(
) where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.TypeSystem.TypeDecision.Monad

-- |A type alias for skeletal type alias environments.
type TSkelAliasEnv = TEnv (TypeAliasEntry StubbedConstraintSet)

-- |A function which constructs a skeletal type environment for the type
--  decision procedure.
constructSkeletalAEnv :: K3 Declaration -> TypeDecideM TSkelAliasEnv
constructSkeletalAEnv decl = undefined
  {-case decl of
  DGlobal _ ->
    
  _ -> decisionError $ NonTopLevelDeclarationRole decl-}