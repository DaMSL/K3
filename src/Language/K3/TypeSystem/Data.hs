module Language.K3.TypeSystem.Data
( UnqualifiedTVar(..)
, QualifiedTVar(..)
, TVar(..)
, QVar
, UVar
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

import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Core.Common

-- * Types

-- TODO: data kinds for TVar?

-- |A tagging type describing unqualified type variables.
data UnqualifiedTVar = UnqualifiedTVar
  deriving (Eq, Ord, Read, Show)

-- |A tagging type describing qualified type variables.
data QualifiedTVar = QualifiedTVar
  deriving (Eq, Ord, Read, Show)

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
type QVar = TVar QualifiedTVar
-- |A type alias for unqualified type variables.
type UVar = TVar UnqualifiedTVar

-- |Type qualifiers.
data TQual = TMut | TImmut
  deriving (Eq, Ord, Read, Show)

-- |Quantified types.
data QuantType
  = QuantType
      [QVar] -- ^The set of variables over which this type is polymorphic.
      QVar -- ^The variable describing the type.
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
data AnnMemType = AnnMemType Identifier TPolarity QVar
  deriving (Eq, Read, Show)
  
-- |Shallow types
data ShallowType
  = SFunction UVar UVar
  | STrigger UVar
  | SBool
  | SInt
  | SFloat
  | SString
  | SOption QVar
  | SIndirection QVar
  | STuple [QVar]
  | SRecord (Map Identifier QVar)
  | STop
  | SBottom
  deriving (Eq, Ord, Read, Show)

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
type TParamEnv = TEnv UVar

-- |A type alias describing a type or a variable.
type TypeOrVar = Either ShallowType UVar
-- |A type alias describing a type qualifier or qualified variable.
type QualOrVar = Either TQual QVar


-- * Constraints

-- |A data type for sets of constraints.  This data type is not a type alias
--  to help ensure that it is treated abstractly by its users.  This will
--  permit structural changes for performance optimization in the future.
data ConstraintSet = ConstraintSet (Set Constraint)
  deriving (Eq, Ord, Read, Show)
  
instance Monoid ConstraintSet where
  mempty = ConstraintSet Set.empty
  mappend (ConstraintSet cs1) (ConstraintSet cs2) =
    ConstraintSet $ Set.union cs1 cs2

-- |A data type to describe constraints.
data Constraint
  = IntermediateConstraint TypeOrVar TypeOrVar
  | QualifiedLowerConstraint TypeOrVar QVar
  | QualifiedUpperConstraint QVar TypeOrVar
  | QualifiedIntermediateConstraint QualOrVar QualOrVar
  | BinaryOperatorConstraint UVar BinaryOperator UVar UVar
  -- TODO: unary prefix operator constraint?  it's not in the spec, but the
  --       implementation parses "!"
  deriving (Eq, Ord, Read, Show)

-- |A data type representing binary operators in the type system.
data BinaryOperator
  = BinOpAdd
  | BinOpSubtract
  | BinOpMultiply
  | BinOpDivide
  | BinOpEquals
  | BinOpGreater
  | BinOpLess
  | BinOpGreaterEq
  | BinOpLessEq
  | BinOpSequence
  | BinOpApply
  | BinOpSend
  deriving (Eq, Ord, Read, Show)
