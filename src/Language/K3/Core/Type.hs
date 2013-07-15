{-# LANGUAGE TypeFamilies #-}

-- | Types in K3.
module Language.K3.Core.Type
( Type(..)
, Annotation(..)
, UnqualifiedTVar(..)
, QualifiedTVar(..)
, TVar(..)
, QTVar
, UTVar
, TQual(..)
, QuantType(..)
, AnnType(..)
, AnnBodyType(..)
, AnnMemType(..)
, TPolarity(..)
, TEnv(..)
, TEnvId(..)
, TypeAliasEntry(..)
, TAliasEnv
, TNormEnv
, TParamEnv
, ConstraintSet(..)
, TypeOrVar
, QualOrVar
, Constraint(..)
) where

import Data.List
import Data.Map (Map)
import Data.Monoid

import Language.K3.Core.Annotation
import Language.K3.Core.Common

-- * Basic types

-- | Tags in the Type Tree. Every type can be qualified with a mutability annotation.
data Type
    = TBool
    | TByte
    | TInt
    | TReal
    | TString
    | TOption
    | TIndirection
    | TTuple
    | TRecord [Identifier]
    | TCollection
    | TFunction
    | TAddress
    | TSource
    | TSink
    | TTrigger [Identifier]
  deriving (Eq, Read, Show)

-- | Annotations on types are the mutability qualifiers.
data instance Annotation Type
    = TMutable
    | TImmutable
    | TWitness
    | TSpan Span
    | TAnnotation Identifier
  deriving (Eq, Read, Show)

-- | TODO: pretty printing of type tree

-- * Full type grammar

-- |A tagging type describing unqualified type variables.
data UnqualifiedTVar = UnqualifiedTVar
  deriving (Eq, Read, Show)

-- |A tagging type describing qualified type variables.
data QualifiedTVar = QualifiedTVar
  deriving (Eq, Read, Show)

-- |A data structure representing type variables.
data TVar a
  = TVar
      a -- ^A description of the form of variable represented.
      Int -- ^The index for this type variable.  This index is unique across a
          --  typechecking pass; fresh variables receive a new index.
      [Span] -- ^A list of spans describing the points at which polyinstantiaton
             --  has occurred (where the leftmost element is the most recent).
             --  This list is extended whenever variables are polyinstantiated.
  deriving (Eq, Ord, Read, Show)
             
-- |A type alias for qualified type variables.
type QTVar = TVar QualifiedTVar
-- |A type alias for unqualified type variables.
type UTVar = TVar UnqualifiedTVar

-- |Type qualifiers.
data TQual = TMut | TImmut
  deriving (Eq, Read, Show)

-- |Quantified types.
data QuantType
  = QuantType
      [QTVar] -- ^The set of variables over which this type is polymorphic.
      QTVar -- ^The variable describing the type.
      ConstraintSet -- ^The constraints on the type.
  deriving (Eq, Read, Show)

-- |Annotation types.
data AnnType
  = AnnType
      TParamEnv -- ^The type parameter environment for the annotation type.
      AnnBodyType -- ^The body type of the annotation.
      ConstraintSet -- ^The set of constraints for the annotation.
  deriving (Eq, Read, Show)

-- |Annotation body types.
data AnnBodyType
  = AnnBodyType
      [AnnMemType] -- ^The set of methods in the annotation.
      [AnnMemType] -- ^The set of lifted attributes in the annotation.
      [AnnMemType] -- ^The set of schema attributes in the annotation.
  deriving (Eq, Read, Show)

-- |Annotation member types.
data AnnMemType = AnnMemType Identifier TPolarity QTVar
  deriving (Eq, Read, Show)

-- |A simple data type for polarities.
data TPolarity = Positive | Negative
  deriving (Eq, Read, Show)

-- |Type environments.
data TEnv a = TEnv (Map TEnvId a)
  deriving (Eq, Read, Show)

-- |Type environment identifiers.
data TEnvId
  = TEnvIdentifier Identifier
  | TEnvIdContent
  | TEnvIdHorizon
  | TEnvIdFinal
  | TEnvIdSelf
  deriving (Eq, Ord, Read, Show)

-- |Type alias environment entries.
data TypeAliasEntry = QuantAlias QuantType | AnnAlias AnnType

-- |An alias for type alias environments.
type TAliasEnv = TEnv TypeAliasEntry
-- |An alias for normal type environments.
type TNormEnv = TEnv QuantType
-- |An alias for type parameter environments.
type TParamEnv = TEnv UTVar

-- |A data type for sets of constraints.  This data type is not a type alias
--  to help ensure that it is treated abstractly by its users.  This will
--  permit structural changes for performance optimization in the future.
data ConstraintSet = ConstraintSet [Constraint]
  deriving (Eq, Read, Show)
  
instance Monoid ConstraintSet where
  mempty = ConstraintSet []
  mappend (ConstraintSet cs1) (ConstraintSet cs2) =
    ConstraintSet $ nub $ cs1 ++ cs2

-- |A type alias describing a type or a variable.
type TypeOrVar = Either (K3 Type) UTVar
-- |A type alias describing a type qualifier or qualified variable.
type QualOrVar = Either TQual QTVar 

-- |A data type to describe constraints.
data Constraint
  = IntermediateConstraint TypeOrVar TypeOrVar
  | QualifiedLowerConstraint TypeOrVar QTVar
  | QualifiedUpperConstraint QTVar TypeOrVar
  | QualifiedIntermediateConstraint QualOrVar QualOrVar
  -- TODO: Isomorphic Operator Datatype.
  {- | BinaryOperatorConstraint UTVar Operator UTVar UTVar -}
  deriving (Eq, Read, Show)
