{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
{-|
  This module implements the decision procedure used to construct a type
  environment for typechecking.
-}
module Language.K3.TypeSystem.TypeDecision
( typeDecision
) where

import Control.Applicative
import Control.Monad.Trans.Writer
import qualified Data.Map as Map
import Data.Monoid
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.TypeDecision.AnnotationInlining
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
import Language.K3.TypeSystem.TypeDecision.StubReplacement
import Language.K3.TypeSystem.Utils.K3Tree

-- |A function implementing the type decision procedure.  The tree at top level
--  is expected to be a @DRole@ declaration containing non-role declarations.
typeDecision :: K3 Declaration -> TypeDecideM (TAliasEnv, TNormEnv)
typeDecision decl = do
  -- Inline the annotations
  inlined <- inlineAnnotations decl
  -- Compute skeletal environment
  (skelAEnv, stubInfoMap) <- runWriterT $ constructSkeletalAEnv inlined
  -- Use that environment to substitute stubs
  inlinedStubMap <- calculateStubs skelAEnv stubInfoMap
  -- Then substitute the stubs in the environment itself
  let aEnv = envStubSubstitute inlinedStubMap skelAEnv
  -- Now calculate types using this environment for non-annotation declarations
  env <- mconcat <$> gatherParallelErrors
                          (map (calcExprDecl aEnv) (subForest decl))
  return (aEnv, env)
  where
    calcExprDecl :: TAliasEnv -> K3 Declaration -> TypeDecideM TNormEnv
    calcExprDecl aEnv decl' = case tag decl' of
      DGlobal i tExpr _ -> do
        (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr
        return $ Map.singleton (TEnvIdentifier i) $
          QuantType Set.empty qa cs
      DTrigger i tExpr _ -> do
        (a,cs) <- deriveUnqualifiedTypeExpression aEnv tExpr
        a' <- freshUVar . TVarSourceOrigin =<< uidOf decl'
        qa <- freshQVar . TVarSourceOrigin =<< uidOf decl'
        let cs' = csFromList [ STrigger a <: a' , a' <: qa ]
        return $ Map.singleton (TEnvIdentifier i) $
          QuantType Set.empty qa $ cs `csUnion` cs'
      DAnnotation _ _ -> return Map.empty
      DRole _ -> internalTypeError $ NonTopLevelDeclarationRole decl'
