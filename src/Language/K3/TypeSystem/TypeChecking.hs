{-# LANGUAGE ScopedTypeVariables, ViewPatterns #-}
{-|
  This module contains the routines necessary to check type environments.  It
  represents the implementation of the Type Checking section in the K3
  specification.  Note that this module does not include any type inference
  logic.  That is, it does not generate the environment against which to check;
  it merely checks that environment against the AST.
-}
module Language.K3.TypeSystem.TypeChecking
( deriveDeclarations
, deriveDeclaration
, deriveAnnotationMember
, deriveTypeExpression
, deriveExpression
) where

import Control.Arrow
import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Either
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Monad.Iface.FreshVar

-- TODO: refactor to annotate the ASTs with type information

-- |A data structure representing typechecking errors.
data TypecheckingError
  = DeclarationsDerivationTypeMismatch (K3 Declaration)
      -- ^Indicates that a declaration tree was provided to the global
      --  declaration routine which was not a Role node.
  | ExtraDeclarationsInEnvironments (Set TEnvId) (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  deriving (Eq, Show)
  
-- |A type alias for typechecking environments.
type TypecheckM m a = EitherT (Seq TypecheckingError) m a

-- |An operation to execute multiple independent typechecking operations in
--  parallel.  If any operation fails, then the errors from *all* failed
--  operations are collected in the result.  Otherwise, the results of the
--  operations are reported as in @mapM@.
gatherParallelErrors :: forall m a. (FreshVarI m)
                     => [TypecheckM m a]
                     -> TypecheckM m [a]
gatherParallelErrors ops = EitherT $ do -- m, not TypecheckM m
  executed <- mapM runEitherT ops
  let (errs,vals) = partitionEithers executed
  if null errs
    then return $ Right vals
    else return $ Left $ foldl (Seq.><) Seq.empty errs
    
-- |Generates an error for a typechecking operation.
typecheckError :: (FreshVarI m) => TypecheckingError -> TypecheckM m a
typecheckError = EitherT . return . Left . Seq.singleton

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
        typecheckError $ ExtraDeclarationsInEnvironments xgIds xaIds 
    (_, _) -> typecheckError $ DeclarationsDerivationTypeMismatch decls
  where
    envKeys (TEnv m) = Set.fromList $ Map.keys m
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

-- |A function to derive the type of a type expression.
deriveTypeExpression :: forall m. (FreshVarI m)
                     => TAliasEnv -- ^The relevant type alias environment.
                     -> TNormEnv -- ^The relevant type environment.
                     -> K3 Type
                     -> TypecheckM m (UVar, ConstraintSet)
deriveTypeExpression aEnv env expr =
  undefined -- TODO

-- |A function to derive the type of an expression.
deriveExpression :: forall m. (FreshVarI m)
                 => TAliasEnv -- ^The relevant type alias environment.
                 -> TNormEnv -- ^The relevant type environment.
                 -> K3 Expression
                 -> TypecheckM m (UVar, ConstraintSet)
deriveExpression aEnv env expr =
  undefined -- TODO
  