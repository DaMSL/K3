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
  deriving (Eq, Show)

-- |A type describing an error in depolarization.
data DepolarizationError
  = MultipleProvisions (Set Identifier)
      -- ^Indicates that the specified identifier was provided multiple times.
  | MultipleOpaques (Set OpaqueVar)
      -- ^Indicates that an opaque type was provided multiple times.
  deriving (Eq, Show)
