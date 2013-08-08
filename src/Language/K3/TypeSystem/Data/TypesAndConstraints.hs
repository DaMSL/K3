{-# LANGUAGE GADTs, DataKinds, KindSignatures, StandaloneDeriving #-}

module Language.K3.TypeSystem.Data.TypesAndConstraints
( TVarQualification(..)
, TVar(..)
, TVarOrigin(..)
, QVar
, UVar
, AnyTVar(..)
, onlyUVar
, onlyQVar
, someVar
, TQual(..)
, allQuals
, QuantType(..)
, AnnType(..)
, AnnBodyType(..)
, AnnMemType(..)
, ShallowType(..)
, TPolarity(..)
, TEnv
, TEnvId(..)
, TypeAliasEntry(..)
, TAliasEnv
, TNormEnv
, TParamEnv
, TypeOrVar
, QualOrVar
, BinaryOperator(..)
, AnyOperator(..)
, Constraint(..)
, ConstraintSet(..)
) where

import Data.Map (Map)
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data.Utils

-- * Types

data TVarQualification
  = UnqualifiedTVar
  | QualifiedTVar
  deriving (Eq, Ord, Read, Show)

-- |A data structure representing type variables.  The first argument for each
--  constructor is an index uniquely identifying the type variable.   The
--  second is an origin describing why the type variable was created (for
--  history and tracking purposes).
data TVar (a :: TVarQualification) where
  QTVar :: Int
        -> TVarOrigin QualifiedTVar
        -> TVar QualifiedTVar
  UTVar :: Int
        -> TVarOrigin UnqualifiedTVar
        -> TVar UnqualifiedTVar

deriving instance Eq (TVar a)
deriving instance Ord (TVar a)
deriving instance Show (TVar a)

-- |A description of the origin of a given type variable.
data TVarOrigin (a :: TVarQualification)
  = TVarSourceOrigin Span -- ^Type variable produced directly from source code.
  | TVarPolyinstantiationOrigin (TVar a) Span
      -- ^Type variable is the result of polyinstantiation at a given source
      --  location.
  | TVarBoundGeneralizationOrigin [TVar a] TPolarity
      -- ^Type variable was created to generalize a number of existing
      --  variables.  The polarity describes the bounding direction: positive
      --  if this variable will be a lower bound, negative if it will be an
      --  upper bound.
  | TVarAlphaRenamingOrigin (TVar a)
      -- ^Type variable was created to provide an alpha renaming of another
      --  variable in order to ensure that the variables were distinct.
  | TVarCollectionInstantiationOrigin AnnType UVar
      -- ^The type variable was created as part of the instantiation of a
      --  collection type.
  | TVarAnnotationToFunctionOrigin AnnType
      -- ^Type variable was created to model an annotation as a function for the
      --  purposes of subtyping.
  deriving (Eq, Ord, Show)

-- |A type alias for qualified type variables.
type QVar = TVar QualifiedTVar
-- |A type alias for unqualified type variables.
type UVar = TVar UnqualifiedTVar

-- |A data type which carries any type of type variable.
data AnyTVar
  = SomeQVar QVar
  | SomeUVar UVar
  deriving (Eq, Ord, Show)

-- |A function to match only qualified type variables.
onlyQVar :: AnyTVar -> Maybe QVar
onlyQVar sa = case sa of
  SomeQVar qa -> Just qa
  SomeUVar _ -> Nothing
-- |A function to match only unqualified type variables.
onlyUVar :: AnyTVar -> Maybe UVar
onlyUVar sa = case sa of
  SomeQVar _ -> Nothing
  SomeUVar a -> Just a

someVar :: TVar a -> AnyTVar
someVar a = case a of
  QTVar{} -> SomeQVar a
  UTVar{} -> SomeUVar a

-- |Type qualifiers.
data TQual = TMut | TImmut
  deriving (Eq, Ord, Read, Show)
  
-- |A set of all qualifiers.
allQuals :: Set TQual
allQuals = Set.fromList [TMut, TImmut]

-- |Quantified types.
data QuantType
  = QuantType (Set AnyTVar) QVar ConstraintSet
      -- ^Constructs a quantified type.  The arguments are the set of variables
      --  over which the type is polymorphic, the variable describing the type,
      --  and the set of constraints on that variable
  deriving (Eq, Ord, Show)

-- |Annotation types.
data AnnType
  = AnnType TParamEnv AnnBodyType ConstraintSet
      -- ^Constructs an annotation type.  The arguments are the named parameter
      --  bindings for the annotation type, the body of the annotation type, and
      --  the set of constraints which apply to that body.
  deriving (Eq, Ord, Show)

-- |Annotation body types.
data AnnBodyType
  = AnnBodyType [AnnMemType] [AnnMemType]
      -- ^Constructs an annotation body type.  The arguments are the members of
      --  the annotation: lifted attributes first, schema attributes second.
  deriving (Eq, Ord, Show)

-- |Annotation member types.
data AnnMemType = AnnMemType Identifier TPolarity QVar
  deriving (Eq, Ord, Show)
  
-- |Shallow types
data ShallowType
  = SFunction UVar UVar
  | STrigger UVar
  | SBool
  | SInt
  | SReal
  | SString
  | SOption QVar
  | SIndirection QVar
  | STuple [QVar]
  | SRecord (Map Identifier QVar)
  | STop
  | SBottom
  deriving (Eq, Ord, Show)

-- |A simple data type for polarities.  Polarities are monoidal by XOR.  The
--  empty polarity is positive.
data TPolarity = Positive | Negative
  deriving (Eq, Ord, Read, Show)

instance Monoid TPolarity where
  mempty = Positive
  mappend x y = if x == y then Positive else Negative

-- |Type environments.
type TEnv a = Map TEnvId a

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
  deriving (Eq, Show)

-- |An alias for type alias environments.
type TAliasEnv = TEnv TypeAliasEntry
-- |An alias for normal type environments.
type TNormEnv = TEnv QuantType
-- |An alias for type parameter environments.
type TParamEnv = TEnv UVar

-- |A type alias describing a type or a variable.
type TypeOrVar = Coproduct ShallowType UVar
-- |A type alias describing a type qualifier or qualified variable.
type QualOrVar = Coproduct (Set TQual) QVar


-- * Constraints

-- |A data type to describe constraints.
data Constraint
  = IntermediateConstraint TypeOrVar TypeOrVar
  | QualifiedLowerConstraint TypeOrVar QVar
  | QualifiedUpperConstraint QVar TypeOrVar
  | QualifiedIntermediateConstraint QualOrVar QualOrVar
  | BinaryOperatorConstraint UVar BinaryOperator UVar UVar
  -- TODO: unary prefix operator constraint?  it's not in the spec, but the
  --       implementation parses "!"
  | MonomorphicQualifiedUpperConstraint QVar (Set TQual)
  | PolyinstantiationLineageConstraint QVar QVar
  deriving (Eq, Ord, Show)

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
  
-- |A data type representing some form of operator.
data AnyOperator
  = SomeBinaryOperator BinaryOperator
  deriving (Eq, Ord, Read, Show)
  
-- * Constraint sets

-- |A data type for sets of constraints.  This data type is not a type alias
--  to help ensure that it is treated abstractly by its users.  This will
--  permit structural changes for performance optimization in the future.
data ConstraintSet = ConstraintSet (Set Constraint)
  deriving (Eq, Ord, Show)
  
instance Monoid ConstraintSet where
  mempty = ConstraintSet Set.empty
  mappend (ConstraintSet cs1) (ConstraintSet cs2) =
    ConstraintSet $ Set.union cs1 cs2
