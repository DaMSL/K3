{-# LANGUAGE GADTs, DataKinds, KindSignatures, StandaloneDeriving, ConstraintKinds, ScopedTypeVariables #-}

module Language.K3.TypeSystem.Data.TypesAndConstraints
( TVarQualification(..)
, TVar(..)
, ConstraintSetType
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
, NormalQuantType
, AnnType(..)
, NormalAnnType
, AnnBodyType(..)
, AnnMemType(..)
, ShallowType(..)
, TPolarity(..)
, TEnv
, TEnvId(..)
, TypeAliasEntry(..)
, NormalTypeAliasEntry
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

import Data.Function
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

tvarId :: TVar a -> Int
tvarId a = case a of
            QTVar n _ -> n
            UTVar n _ -> n

instance Eq (TVar a) where
  (==) = (==) `on` tvarId
instance Ord (TVar a) where
  compare = compare `on` tvarId
deriving instance Show (TVar a)

-- |A constraint kind describing the constraints pap
type ConstraintSetType c = (Show c)

-- |A description of the origin of a given type variable.
data TVarOrigin (a :: TVarQualification)
  = TVarSourceOrigin UID -- ^Type variable produced directly from source code.
  | TVarPolyinstantiationOrigin (TVar a) UID
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
  | forall c. (ConstraintSetType c)
    => TVarCollectionInstantiationOrigin (AnnType c) UVar
      -- ^The type variable was created as part of the instantiation of a
      --  collection type.
  | forall c. (ConstraintSetType c)
    => TVarAnnotationToFunctionOrigin (AnnType c)
      -- ^Type variable was created to model an annotation as a function for the
      --  purposes of subtyping.

deriving instance Show (TVarOrigin a)

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

-- |Quantified types.  The constraint set type is left parametric for later use
--  by the environment decision procedure.
data QuantType c
  = QuantType (Set AnyTVar) QVar c
      -- ^Constructs a quantified type.  The arguments are the set of variables
      --  over which the type is polymorphic, the variable describing the type,
      --  and the set of constraints on that variable
  deriving (Eq, Ord, Show)
  
-- |A type alias for normal quantified types (which use normal constraint sets).
type NormalQuantType = QuantType ConstraintSet

-- |Annotation types.  The constraint set type is left parametric for later use
--  by the environment decision procedure.
data AnnType c
  = AnnType TParamEnv AnnBodyType c
      -- ^Constructs an annotation type.  The arguments are the named parameter
      --  bindings for the annotation type, the body of the annotation type, and
      --  the set of constraints which apply to that body.
  deriving (Eq, Ord, Show)
  
-- |A type alias for normal annotation types (which use normal constraint sets).
type NormalAnnType = AnnType ConstraintSet

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

-- |Type alias environment entries.  The type parameter is passed to the
--  underlying quantified and annotation types.
data TypeAliasEntry c = QuantAlias (QuantType c) | AnnAlias (AnnType c)
  deriving (Eq, Show)
  
-- |A type alias for normal alias entries (those which use normal constraint
--  sets).
type NormalTypeAliasEntry = TypeAliasEntry ConstraintSet

-- |An alias for type alias environments.
type TAliasEnv = TEnv NormalTypeAliasEntry
-- |An alias for normal type environments.
type TNormEnv = TEnv NormalQuantType
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
