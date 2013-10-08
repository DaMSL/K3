{-|
  Contains error definitions for the @Language.K3.TypeSystem.Annotations@
  module.
-}
module Language.K3.TypeSystem.Annotations.Error
( AnnotationConcatenationError(..)
, DepolarizationError(..)
) where

import Data.Set (Set)

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data

-- |A data type describing the errors which can occur in concatenation.
data AnnotationConcatenationError
  = OverlappingPositiveMember Identifier
      -- ^Produced when two annotation members attempt to define the same
      --  identifier in a positive context.
  | IncompatibleTypeParameters TParamEnv TParamEnv
      -- ^Produced when two annotation types are concatenated and one has a
      --  different set of open type variables than the other.
  | DifferentMorphicArities Identifier
      -- ^Produced when two annotations are concatenated and the member in
      --  question is defined using both polymorphic and monomorphic signatures.
  | PolymorphicArityMembersNotEquivalent Identifier
      -- ^Produced when a concatenated member uses polymorphic signatures and
      --  those signatures are not equivalent.
  deriving (Eq, Show)

-- |A type describing an error in depolarization.
data DepolarizationError
  = DepolarizationConcatenationError AnnotationConcatenationError
      -- ^Indicates that the original term to be depolarized failed to
      --  concatenate with the empty set of members.
  | MultipleProvisions (Set Identifier)
      -- ^Indicates that the specified identifier was provided multiple times.
  | MultipleOpaques (Set OpaqueVar)
      -- ^Indicates that an opaque type was provided multiple times.
  deriving (Eq, Show)
