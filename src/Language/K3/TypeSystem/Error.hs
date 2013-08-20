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
import Language.K3.Pretty
import Language.K3.TypeSystem.Annotations.Error
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Subtyping.Error

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
  | InvalidAnnotationConcatenation UID AnnotationConcatenationError
      -- ^ Indicates that, at the given node, the concatenation of a set of
      --   annotation types has failed.
  | InvalidCollectionInstantiation UID CollectionInstantiationError
      -- ^ Indicates that, at the given node, the instantiaton of a
      --   collection type has failed.  This occurs when two different positive
      --   instances for the same identifier exist; the identifier is provided.
  | InitializerForNegativeAnnotationMember UID
      -- ^ Indicates that an initializer appears for a negative annotation
      --   member.
  | NoInitializerForPositiveAnnotationMember UID
      -- ^ Indicates that an initializer does not appear for a positive
      --   annotation member.
  | AnnotationDepolarizationFailure UID DepolarizationError
      -- ^ Indicates that a depolarization error occurred while trying to
      --   derive over an annotation.
  | AnnotationConcatenationFailure UID AnnotationConcatenationError
      -- ^ Indicates that a concatenation error occurred while trying to
      --   derive over an annotation.
  | forall c. (ConstraintSetType c)
    => AnnotationSubtypeFailure UID (AnnType c) (AnnType c) -- TODO:SubtypeError
      -- ^ Indicates that the inferred type of an annotation was not a subtype
      --   of its declared type signature.  The first annotation type in the
      --   error is the inferred type; the second annotation type is the
      --   declared type.
  | forall c. (ConstraintSetType c)
    => DeclarationSubtypeFailure UID (QuantType c) (QuantType c) SubtypeError
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

instance Pretty TypeError where
  prettyLines e = case e of
    DeclarationSubtypeFailure n qt1 qt2 e' ->
      ["DeclarationSubtypeFailure at node "] %+
      prettyLines n %+
      [":"] %$
      indent 2 (
        ["  "] %+ prettyLines qt1 %$
        ["≤ "] %+ prettyLines qt2 %$
        ["because "] %+ prettyLines e')
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
  | ExtraDeclarationsInEnvironments (Set TEnvId) (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | forall c. (ConstraintSetType c) => PolymorphicSelfBinding (QuantType c) UID
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
          SubtypeError
      -- ^Indicates that a type found in a type environment is not a supertype
      --  of the type which was inferred from its signature.  This should never
      --  happen; in practice, these types should be nearly identical.  The
      --  declared type is the first @QuantType@; the type from the environment
      --  is the second.
  | UnexpectedMemberAnnotationDeclaration (K3 Declaration) AnnMemDecl
      -- ^Indicates that, during type decision, an annotation member contained
      --  a member annotation declaration.  Such declarations should be inlined
      --  at the beginning of type decision.
  | PrimitiveSubtypeInvariantViolated InternalPrimitiveSubtypeError
      -- ^Indicates that primitive subtyping has failed in some way.

deriving instance Show InternalTypeError

