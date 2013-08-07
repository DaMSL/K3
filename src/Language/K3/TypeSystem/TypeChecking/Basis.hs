{-# LANGUAGE ScopedTypeVariables, TemplateHaskell #-}

{-|
  This module contains basic data types and utilities used in typechecking.
-}
module Language.K3.TypeSystem.TypeChecking.Basis
( TypecheckingError(..)
, InternalTypecheckingError(..)
, TypecheckM
, gatherParallelErrors
, typecheckError
, freshTypecheckingVar

, assert0Children
, assert1Children
, assert2Children
, assert3Children
, assert4Children
, assert5Children
, assert6Children
, assert7Children
, assert8Children
) where

import Control.Applicative
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Either
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import Data.Tree
import Language.Haskell.TH as TH

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type as K3T
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar

-- * Types

-- |A data structure representing typechecking errors.
data TypecheckingError
  = InternalError InternalTypecheckingError
      -- ^Represents an internal typechecking error.  These represent bugs in
      --  the K3 software.
  | UnboundEnvironmentIdentifier Span TEnvId
      -- ^Indicates that, at the given location, the provided identifier is used
      --  but unbound.
  | UnboundTypeEnvironmentIdentifier Span TEnvId
      -- ^Indicates that, at the given location, the provided identifier is used
      --  but unbound.
  | NonAnnotationAlias Span TEnvId
      -- ^Indicates that, at the given location, a type annotation was expected
      --  but the named type alias identifier was bound to something other than
      --  an annotation.
  | NonQuantAlias Span TEnvId
      -- ^Indicates that, at the given location, a quantified type was expected
      --  but the named type alias identifier was bound to something other than
      --  a quantified type.
  | InvalidAnnotationConcatenation Span AnnotationConcatenationError
      -- ^ Indicates that, at the given location, the concatenation of a set of
      --   annotation types has failed.
  | InvalidCollectionInstantiation Span CollectionInstantiationError
      -- ^ Indicates that, at the given location, the instantiaton of a
      --   collection type has failed.  This occurs when two different positive
      --   instances for the same identifier exist; the identifier is provided.
  | InitializerForNegativeAnnotationMember Span
      -- ^ Indicates that an initializer appears for a negative annotation
      --   member.
  | NoInitializerForPositiveAnnotationMember Span
      -- ^ Indicates that an initializer does not appear for a positive
      --   annotation member.
  | AnnotationDepolarizationFailure Span DepolarizationError
      -- ^ Indicates that a depolarization error occurred while trying to
      --   derive over an annotation.
  | AnnotationConcatenationFailure Span AnnotationConcatenationError
      -- ^ Indicates that a concatenation error occurred while trying to
      --   derive over an annotation.
  | AnnotationSubtypeFailure Span AnnType AnnType
      -- ^ Indicates that the inferred type of an annotation was not a subtype
      --   of its declared type signature.  The first annotation type in the
      --   error is the inferred type; the second annotation type is the
      --   declared type.
  deriving (Eq, Show)

-- |A data structure representing /internal/ typechecking errors.  These errors
--  should never be seen by a K3 programmer in practice; they should be excluded
--  by the typechecker or by invariants provided by its callers.
data InternalTypecheckingError
  = DeclarationsDerivationTypeMismatch (K3 Declaration)
      -- ^Indicates that a declaration tree was provided to the global
      --  declaration routine which was not a Role node.
  | ExtraDeclarationsInEnvironments (Set TEnvId) (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | InvalidSpansInExpression (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnExpression (K3 Expression)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidExpressionChildCount (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had a
      --  number of children inappropriate for its tag.
  | PolymorphicSelfBinding QuantType Span
      -- ^Indicates that the special self binding was bound to a polymorphic
      --  type, which is illegal.
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | InvalidSpansInTypeExpression (K3 K3T.Type)
      -- ^Indicates that type derivation occurred on a type expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnType (K3 K3T.Type)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidTypeExpressionChildCount (K3 K3T.Type)
      -- ^Indicates that type derivation occurred on a type expression which had
      --  a number of children inappropriate for its tag.
  | InvalidSpecialBinding TEnvId (Maybe TypeAliasEntry)
      -- ^Indicates that, during derivation of an annotation member, a type
      --  alias was bound to a form which could not be understood.
  | MissingTypeParameter TParamEnv TEnvId
      -- ^Indicates that a type parameter environment was missing an environment
      --  identifier it was expected to have.
  | UnresolvedTypeParameters TParamEnv
      -- ^Indicates that a type parameter environment was non-empty when it was
      --  expected to be empty.
  | InvalidSpansInDeclaration (K3 Declaration)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | NestedRole (K3 Declaration)
      -- ^Indicates that a role declaration was nested beyond top level.  The
      --  K3 type system does not have an understanding of roles and so cannot
      --  typecheck them.
  deriving (Eq, Show)
  
-- |A type alias for typechecking environments.
type TypecheckM m a = EitherT (Seq TypecheckingError) m a

-- * Utilities

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
  return $ if null errs
            then Right vals
            else Left $ foldl (Seq.><) Seq.empty errs
    
-- |Generates an error for a typechecking operation.
typecheckError :: (FreshVarI m) => TypecheckingError -> TypecheckM m a
typecheckError = EitherT . return . Left . Seq.singleton

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingVar :: (FreshVarI m)
                     => Span -> TypecheckM m (TVar a)
freshTypecheckingVar s = lift . freshVar =<< return (TVarSourceOrigin s)

-- * Generated routines

-- Defines assertExpr0Children through assertExpr8Children and similarly for
-- assertTExpr#Children
class ErrorForWrongChildren a where
  childCountError :: K3 a -> InternalTypecheckingError
instance ErrorForWrongChildren Expression where
  childCountError = InvalidExpressionChildCount
instance ErrorForWrongChildren K3T.Type where
  childCountError = InvalidTypeExpressionChildCount
$(
  let mkAssertChildren :: Int -> Q [Dec]
      mkAssertChildren n = do
        let fname = mkName $ "assert" ++ show n ++ "Children"
        let ename = mkName "tree"
        let elnames = map (mkName . ("el" ++) . show) [1::Int .. n]
        let typN = mkName "a"
        let mtypN = mkName "m"
        let typ = varT typN
        let mtyp = varT mtypN
        let tupTyp = foldl appT (tupleT n) $ replicate n $ [t|K3 $(typ)|]
        ftype <- [t| (FreshVarI $(mtyp), ErrorForWrongChildren $(typ))
                      => K3 $(typ) -> TypecheckM $(mtyp) $(tupTyp) |]
        let ftype' = ForallT [PlainTV typN, PlainTV mtypN] [] ftype
        let signature = sigD fname $ return ftype'
        let badMatch = match wildP (normalB
              [| typecheckError $ InternalError $
                    childCountError $(varE ename) |]
              ) []
        let goodMatch = match (listP $ map varP elnames) (normalB $
                          appE ([|return|]) $ tupE $ map varE elnames) []
        let bodyExp = caseE ([|subForest $(varE ename)|]) [goodMatch, badMatch]
        let cl = clause [varP ename] (normalB bodyExp) []
        let impl = funD fname [cl]
        sequence [signature,impl]
  in
  concat <$> mapM mkAssertChildren [0::Int .. 8]
 )