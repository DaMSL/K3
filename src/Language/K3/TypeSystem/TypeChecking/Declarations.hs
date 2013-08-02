{-# LANGUAGE ScopedTypeVariables #-}

{-|
  A module containing operations which typecheck declarations.
-}
module Language.K3.TypeSystem.TypeChecking.Declarations
( deriveDeclarations
, deriveDeclaration
, deriveAnnotationMember
) where

import Control.Arrow
import Control.Applicative
import Control.Monad
import qualified Data.Map as Map
import Data.Monoid
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.TypeChecking.Basis

-- |A function to check whether a given pair of environments correctly describes
--  an AST.  The provided AST must be a Role declaration.
deriveDeclarations :: forall m. (FreshVarI m)
                   => TAliasEnv -- ^The existing type alias environment.
                   -> TNormEnv -- ^The existing type environment.
                   -> TAliasEnv -- ^The type alias environment to check.
                   -> TNormEnv -- ^The type environment to check.
                   -> K3 Declaration -- ^The AST of global declarations to use
                                     --  in the checking process.
                   -> TypecheckM m ()
deriveDeclarations aEnv env aEnv' env' decls =
  case tag &&& subForest $ decls of
    (DRole _, globals) -> do
      let aEnv'' = aEnv `envMerge` aEnv' 
      let env'' = env `envMerge` env'
      () <- mconcat <$> gatherParallelErrors
              (map (deriveDeclaration aEnv'' env'') globals)
      let (gIds, _, aIds) = mconcat $ map (idOf . tag) globals
      let xgIds = Set.map TEnvIdentifier gIds `Set.difference` envKeys env'
      let xaIds = Set.map TEnvIdentifier aIds `Set.difference` envKeys aEnv'
      unless (Set.null xgIds && Set.null xaIds) $
        typecheckError $ InternalError $
          ExtraDeclarationsInEnvironments xgIds xaIds 
    (_, _) -> typecheckError $ InternalError $
                DeclarationsDerivationTypeMismatch decls
  where
    envKeys m = Set.fromList $ Map.keys m
    idOf decl = case decl of
      DGlobal i _ _ -> (Set.singleton i, Set.empty, Set.empty)
      DRole i -> (Set.empty, Set.singleton i, Set.empty)
      DAnnotation i _ -> (Set.empty, Set.empty, Set.singleton i)

-- |A function to check whether a global has the type described in the provided
--  type environments.
deriveDeclaration :: forall m. (FreshVarI m)
                  => TAliasEnv -- ^The type alias environment in which to check.
                  -> TNormEnv -- ^The type environment in which to check.
                  -> K3 Declaration -- ^The AST of the declaration to check.
                  -> TypecheckM m ()
deriveDeclaration aEnv env decl =
  undefined -- TODO

-- |A function to derive a type for an annotation member.
deriveAnnotationMember :: forall m. (FreshVarI m)
                       => TAliasEnv -- ^The relevant type alias environment.
                       -> TNormEnv -- ^The relevant type environment.
                       -> AnnMemDecl -- ^The member to typecheck.
                       -> TypecheckM m (AnnMemType, ConstraintSet)
deriveAnnotationMember aEnv env decl =
  undefined -- TODO
