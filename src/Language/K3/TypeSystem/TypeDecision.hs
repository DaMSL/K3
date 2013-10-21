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
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.TypeDecision.AnnotationInlining
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
import Language.K3.TypeSystem.TypeDecision.StubReplacement
import Language.K3.TypeSystem.Utils.K3Tree

-- |A function implementing the type decision procedure.  The tree at top level
--  is expected to be a @DRole@ declaration containing non-role declarations.
typeDecision :: K3 Declaration
             -> TypeDecideM (TAliasEnv, TNormEnv, TGlobalQuantEnv)
typeDecision decl = do
  -- Inline the annotations
  inlined <- inlineAnnotations decl
  -- Compute skeletal environment
  ((skelAEnv, skelREnv), stubInfoMap) <-
      runWriterT $ constructSkeletalEnvs inlined
  -- Use that environment to substitute stubs
  inlinedStubMap <- calculateStubs skelAEnv stubInfoMap
  -- Then substitute the stubs in the environment itself
  let aEnv = aEnvStubSubstitute inlinedStubMap skelAEnv
  let rEnv = rEnvStubSubstitute inlinedStubMap skelREnv
  -- Now calculate types using this environment for non-annotation declarations
  (env, rEnv') <- mconcat <$> gatherParallelErrors
                          (map (calcExprDecl aEnv) (subForest decl))
  return (aEnv, env, rEnv `envMerge` rEnv')
  where
    calcExprDecl :: TAliasEnv -> K3 Declaration
                 -> TypeDecideM (TNormEnv, TGlobalQuantEnv)
    calcExprDecl aEnv decl' = case tag decl' of
      DGlobal i tExpr _ -> do
        -- First, derive over the type expression
        (qa,cs,qEnv) <- derivePolymorphicTypeExpression aEnv tExpr
        -- Then generalize.  Because the type expression can't refer to a normal
        -- environment, it doesn't matter which one we provide.
        let qt = generalize Map.empty qa cs
        -- Generate the environment containing this result
        return ( Map.singleton (TEnvIdentifier i) qt
               , Map.singleton (TEnvIdentifier i) qEnv)
      DTrigger i tExpr _ -> do
        -- First, derive over the type expression
        (a,cs) <- deriveUnqualifiedTypeExpression aEnv tExpr
        -- Next, establish an appropriate constraint set for the trigger
        a' <- freshUVar . TVarSourceOrigin =<< uidOf decl'
        qa <- freshQVar . TVarSourceOrigin =<< uidOf decl'
        let cs' = csUnions [cs, STrigger a ~= a', a' ~= qa ]
        -- Then generalize.  Because the type expression can't refer to a normal
        -- environment, it doesn't matter which one we provide.
        let qt = generalize Map.empty qa cs'
        -- Finally, generate the result
        return (Map.singleton (TEnvIdentifier i) qt, Map.empty)
      DAnnotation _ _ _ -> return (Map.empty, Map.empty)
      DRole _ -> internalTypeError $ NonTopLevelDeclarationRole decl'
