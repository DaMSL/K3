{-# LANGUAGE ExistentialQuantification, StandaloneDeriving, ConstraintKinds, FlexibleInstances #-}

{-|
  A module describing the types of errors which may occur during typing.  These
  errors cover both type decision and type checking.
-}
module Language.K3.TypeSystem.Error
( TypeError(..)
, InternalTypeError(..)
) where

import qualified Data.Foldable as Foldable
import Data.List.Split
import Data.Sequence (Seq)
import Data.Set (Set)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type as K3T
import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Annotations.Error
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Utils

-- |A data structure representing typechecking errors.
data TypeError
  = InternalError InternalTypeError
      -- ^Represents an internal type error.  These represent bugs in the K3
      --  software; they occur when internal runtime invariants have been
      --  violated.
  | UnboundEnvironmentIdentifier UID TEnvId
      -- ^Indicates that, at the given node, the provided identifier is used
      --  but unbound.
  | UnboundTypeEnvironmentIdentifier UID TEnvId
      -- ^Indicates that, at the given node, the provided identifier is used
      --  but unbound.
  | NonAnnotationAlias UID TEnvId
      -- ^Indicates that, at the given node, a type annotation was expected
      --  but the named type alias identifier was bound to something other than
      --  an annotation.
  | NonQuantAlias UID TEnvId
      -- ^Indicates that, at the given node, a quantified type was expected
      --  but the named type alias identifier was bound to something other than
      --  a quantified type.
  | NonRecordContentSchema UID (K3 Type)
      -- ^Indicates that a collection's content schema type is not a record
      --  type.
  | ContentAndStructuralSchemaOverlap UID RecordConcatenationError
      -- ^Indicates that the user-selected content schema type overlaps with the
      --  collection's structural schema type.
  | InvalidAnnotationConcatenation UID AnnotationConcatenationError
      -- ^ Indicates that, at the given node, the concatenation of a set of
      --   annotation types has failed.
  | InitializerForNegativeAnnotationMember UID
      -- ^ Indicates that an initializer appears for a negative annotation
      --   member.
  | AnnotationDepolarizationFailure UID DepolarizationError
      -- ^ Indicates that a depolarization error occurred while trying to
      --   derive over an annotation.
  | AnnotationConcatenationFailure UID AnnotationConcatenationError
      -- ^ Indicates that a concatenation error occurred while trying to
      --   derive over an annotation.
  | MultipleDeclarationBindings Identifier [K3 Declaration]
      -- ^ Indicates that the program binds the same identifier to multiple
      --   declarations.
  | MultipleAnnotationBindings Identifier [AnnMemDecl]
      -- ^ Indicates that a given annotation binds the same identifier to
      --   multiple annotation declarations.
  | MissingAnnotationTypeParameter TEnvId
      -- ^Indicates that a required annotation parameter (e.g. content) is
      --  missing from the parameter environment.
  | CollectionDepolarizationError DepolarizationError
      -- ^Indicates that collection instantiation induced a depolarization
      --  error.
  | DeclarationClosureInconsistency
      Identifier
      ConstraintSet
      AnyTVar
      AnyTVar
      [ConsistencyError]
      -- ^ Indicates that the specified declaration's closure was inconsistent.
      --   The first qualified variable is the inferred type of the expression;
      --   the second qualified variable is the declared type.
  | AnnotationClosureInconsistencyInternal
      Identifier
      [ConsistencyError]
      -- ^ Indicates that an annotation is internally inconsistent.   This does
      --   not suggest that a signature fails to match an expression; this
      --   instead occurs when an expression cannot be inferred a consistent
      --   type (e.g. "provides x : int = 4 + true").
  | AnnotationClosureInconsistency
      Identifier
      Identifier
      [ConsistencyError]
      -- ^ Indicates that an annotation is inconsistent with its environment
      --   type.  This suggests that, while the expression of a member has a
      --   consistent type, it does not match the type of its signature.  The
      --   first identifier names the annotation; the second names the member.
  | RecordSignatureError UID RecordConcatenationError
      -- ^ Indicates that a type signature contained a record type which
      --   produced a concatenation error when it was interpreted.
  | DuplicateIdentifiersInRecordExpression UID (Set Identifier)
      -- ^ Indicates that a record expression contained duplicate identifiers
      --   (e.g. @{x=5,x=4}@).

deriving instance Show TypeError

instance Pretty TypeError where
  prettyLines e = case e of
    InternalError ie -> ["InternalError: "] %+ prettyLines ie
    DeclarationClosureInconsistency i cs sa1 sa2 ces ->
      ["Inconsistency in closure of " ++ i ++ ": "] %$
        indent 2 (
          ["Type "] %+ prettyLines sa1 %+ [" is not a subtype of "] %+
            prettyLines sa2 %$
          ["Constraint set:"] %$
            indent 2 (prettyLines cs) %$
          ["Errors:"] %$
            indent 2 (prettyLines ces)
        )
    AnnotationClosureInconsistencyInternal i errs ->
      ["Inconsistencies in closure of annotation " ++ i ++ ": "] %$
        indent 2 (
          foldl (%$) [] $ map prettyLines errs
        )
    _ -> splitOn "\n" $ show e

instance Pretty (Seq TypeError) where
  prettyLines = prettyLines . Foldable.toList
  
instance Pretty [TypeError] where
  prettyLines es =
    ["[ "] %+ foldl (%$) [] (map prettyLines es) +% ["] "]

data InternalTypeError
  = TopLevelDeclarationNonRole (K3 Declaration)
      -- ^Indicates that the top level of the AST is not a @DRole@ declaration.
  | NonTopLevelDeclarationRole (K3 Declaration)
      -- ^Indicates that a second level of the AST is a @DRole@ declaration.
  | InvalidUIDsInExpression (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnExpression (K3 Expression)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidExpressionChildCount (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had a
      --  number of children inappropriate for its tag.
  | InvalidUIDsInTypeExpression (K3 K3T.Type)
      -- ^Indicates that type derivation occurred on a type expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnType (K3 K3T.Type)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidTypeExpressionChildCount (K3 K3T.Type)
      -- ^Indicates that type derivation occurred on a type expression which had
      --  a number of children inappropriate for its tag.
  | InvalidDeclarationChildCount (K3 Declaration)
      -- ^Indicates that type derivation occurred on a declaration which had a
      --  number of children inappropriate for its tag.
  | MissingTypeParameter TParamEnv TEnvId
      -- ^Indicates that a type parameter environment was missing an environment
      --  identifier it was expected to have.
  | UnresolvedTypeParameters TParamEnv
      -- ^Indicates that a type parameter environment was non-empty when it was
      --  expected to be empty.
  | InvalidUIDsInDeclaration (K3 Declaration)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | ExtraDeclarationsInEnvironments (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers are included.
  | forall c. (ConstraintSetType c) => PolymorphicSelfBinding (QuantType c) UID
      -- ^Indicates that the special self binding was bound to a polymorphic
      --  type, which is illegal.
  | forall c. (ConstraintSetType c)
    => InvalidSpecialBinding TEnvId (Maybe (TypeAliasEntry c))
      -- ^Indicates that, during derivation of an annotation member, a type
      --  alias was bound to a form which could not be understood.
  | UnexpectedMemberAnnotationDeclaration (K3 Declaration) AnnMemDecl
      -- ^Indicates that, during type decision, an annotation member contained
      --  a member annotation declaration.  Such declarations should be inlined
      --  at the beginning of type decision.
  | MissingNegativeAnnotationMemberInEnvironment Identifier [Identifier]
      -- ^Indicates that an annotation type in the environment did not include
      --  a negative member that was inferred from its body.  The first
      --  argument names the annotation; the second names the missing members.
  | MissingPositiveAnnotationMemberInInferredType Identifier Identifier
      -- ^Indicates that an annotation was inferred a type that did not include
      --  a positive member that was found in the environment.  The first
      --  argument names the annotation; the second names the missing member.
  | HorizonTypeConstructionError (K3 Declaration) RecordConcatenationError
      -- ^Indicates that something went wrong while constructing the horizon
      --  type for an annotation derivation.  (This should never happen because
      --  the concatenation is with record containing only a fresh opaque.)
  | MissingIdentifierInGlobalQuantifiedEnvironment TGlobalQuantEnv Identifier
      -- ^Indicates that the @TGlobalQuantEnv@ used during typechecking did not
      --  include an entry for one of the globals in the declaration list.
  | UndeterminedArityForAnnotationMember AnnMemDecl Identifier
      -- ^Indicates that the type decision mechanism did not provide an aritied
      --  type for an identifier for which inference was performed.
  | InvalidUIDsInAnnMemDecl AnnMemDecl
      -- ^Indicates that an annotation member is either missing, or has multiple
      --  unique identifiers associated with it.

deriving instance Show InternalTypeError

instance Pretty InternalTypeError where
  prettyLines e = case e of
    InvalidUIDsInDeclaration decl     -> invalidUID "declaration" decl
    InvalidUIDsInTypeExpression tExpr -> invalidUID "type expression" tExpr
    InvalidUIDsInExpression expr      -> invalidUID "expression" expr
    _ -> splitOn "\n" $ show e
    where
      invalidUID name tree =
        ["Invalid UIDs in "++name++": "] %$ indent 2 (
            prettyLines tree
          )
