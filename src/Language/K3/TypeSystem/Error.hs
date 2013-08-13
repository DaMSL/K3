{-# LANGUAGE ExistentialQuantification, StandaloneDeriving #-}

{-|
  A module describing the types of errors which may occur during typing.  These
  errors cover both type decision and type checking.
-}
module Language.K3.TypeSystem.Error
( TypeError(..)
, InternalTypeError(..)
) where

import Data.Set (Set)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type as K3T
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Data

-- |A data structure representing typechecking errors.
data TypeError
  = InternalError InternalTypeError
      -- ^Represents an internal type error.  These represent bugs in the K3
      --  software; they occur when internal runtime invariants have been
      --  violated.
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
  | forall c. (ConstraintSetType c)
    => AnnotationSubtypeFailure Span (AnnType c) (AnnType c)
      -- ^ Indicates that the inferred type of an annotation was not a subtype
      --   of its declared type signature.  The first annotation type in the
      --   error is the inferred type; the second annotation type is the
      --   declared type.
  | forall c. (ConstraintSetType c)
    => DeclarationSubtypeFailure Span (QuantType c) (QuantType c)
      -- ^ Indicates that the inferred type of a global declaration was not a
      --   subtype of its declared type signature.  The first @QuantType@ is the
      --   inferred type; the second @QuantType@ is the declared type.
  | MultipleDeclarationBindings Identifier [K3 Declaration]
      -- ^ Indicates that the program binds the same identifier to multiple
      --   declarations.
  | MultipleAnnotationBindings Identifier [AnnMemDecl]
      -- ^ Indicates that a given annotation binds the same identifier to
      --   multiple annotation declarations.

deriving instance Show TypeError

data InternalTypeError
  = TopLevelDeclarationNonRole (K3 Declaration)
      -- ^Indicates that the top level of the AST is not a @DRole@ declaration.
  | NonTopLevelDeclarationRole (K3 Declaration)
      -- ^Indicates that a second level of the AST is a @DRole@ declaration.
  | InvalidSpansInExpression (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnExpression (K3 Expression)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidExpressionChildCount (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had a
      --  number of children inappropriate for its tag.
  | InvalidSpansInTypeExpression (K3 K3T.Type)
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
  | InvalidSpansInDeclaration (K3 Declaration)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | ExtraDeclarationsInEnvironments (Set TEnvId) (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | forall c. (ConstraintSetType c) => PolymorphicSelfBinding (QuantType c) Span
      -- ^Indicates that the special self binding was bound to a polymorphic
      --  type, which is illegal.
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | forall c. (ConstraintSetType c)
    => InvalidSpecialBinding TEnvId (Maybe (TypeAliasEntry c))
      -- ^Indicates that, during derivation of an annotation member, a type
      --  alias was bound to a form which could not be understood.
  | forall c. (ConstraintSetType c)
    => TypeInEnvironmentDoesNotMatchSignature TEnvId (QuantType c) (QuantType c)
      -- ^Indicates that a type found in a type environment is not a supertype
      --  of the type which was inferred from its signature.  This should never
      --  happen; in practice, these types should be nearly identical.  The
      --  declared type is the first @QuantType@; the type from the environment
      --  is the second.
  | UnexpectedMemberAnnotationDeclaration (K3 Declaration) AnnMemDecl
      -- ^Indicates that, during type decision, an annotation member contained
      --  a member annotation declaration.  Such declarations should be inlined
      --  at the beginning of type decision.

deriving instance Show InternalTypeError

