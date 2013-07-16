{-# LANGUAGE MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances, TemplateHaskell, GADTs, TypeFamilies #-}

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
, ShallowType(..)
, TPolarity(..)
, TEnv(..)
, TEnvId(..)
, TypeAliasEntry(..)
, TAliasEnv
, TNormEnv
, TParamEnv
, TypeOrVar
, QualOrVar
, BinaryOperator(..)
, Constraint(..)
, constraint
, ConstraintSet
, csEmpty
, csSing
, csFromList
, csToList
, csSubset
, csUnion
, csUnions
, csQuery
, ConstraintSetQuery(..)
) where

import Control.Applicative
import Control.Monad
import Data.Map (Map)
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
type QualOrVar = Either (Set TQual) QVar


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
  
-- * Constraint sets

-- |A data type for sets of constraints.  This data type is not a type alias
--  to help ensure that it is treated abstractly by its users.  This will
--  permit structural changes for performance optimization in the future.
data ConstraintSet = ConstraintSet (Set Constraint)
  deriving (Eq, Ord, Read, Show)
  
instance Monoid ConstraintSet where
  mempty = ConstraintSet Set.empty
  mappend (ConstraintSet cs1) (ConstraintSet cs2) =
    ConstraintSet $ Set.union cs1 cs2

-- ** Accessors and updaters

-- TODO: Implement a considerably more efficient underlying data structure for
-- constraint sets.

csEmpty :: ConstraintSet
csEmpty = ConstraintSet $ Set.empty

csSing :: Constraint -> ConstraintSet
csSing = ConstraintSet . Set.singleton

csFromList :: [Constraint] -> ConstraintSet
csFromList = ConstraintSet . Set.fromList

csToList :: ConstraintSet -> [Constraint]
csToList (ConstraintSet cs) = Set.toList cs

csSubset :: ConstraintSet -> ConstraintSet -> Bool
csSubset (ConstraintSet a) (ConstraintSet b) = Set.isSubsetOf a b

csUnion :: ConstraintSet -> ConstraintSet -> ConstraintSet
csUnion (ConstraintSet a) (ConstraintSet b) = ConstraintSet $ a `Set.union` b

csUnions :: [ConstraintSet] -> ConstraintSet
csUnions css = ConstraintSet $ Set.unions $ map (\(ConstraintSet s) -> s) css

{-
  Queries against the constraint set are managed via the ConstraintSetQuery
  data type.  This data type allows queries to be expressed in such a way that
  an efficient implementation of ConstraintSet can use a uniform policy for
  indexing the answers.
-}
data ConstraintSetQuery r where
  QueryAllTypesLowerBoundingUVars ::
    ConstraintSetQuery (ShallowType,UVar)
  QueryAllTypesLowerBoundingTypes ::
    ConstraintSetQuery (ShallowType,ShallowType)
  QueryAllBinaryOperations ::
    ConstraintSetQuery (UVar,BinaryOperator,UVar,UVar)
  QueryAllQualOrVarLowerBoundingQualOrVar ::
    ConstraintSetQuery (QualOrVar, QualOrVar)
  QueryAllTypeOrVarLowerBoundingQVar ::
    ConstraintSetQuery (TypeOrVar, QVar)
  QueryAllQVarLowerBoundingQVar ::
    ConstraintSetQuery (QVar, QVar)
  QueryTypeOrVarByUVarLowerBound ::
    UVar -> ConstraintSetQuery TypeOrVar
  QueryTypeByUVarUpperBound ::
    UVar -> ConstraintSetQuery ShallowType
  QueryQualOrVarByQualOrVarLowerBound ::
    QualOrVar -> ConstraintSetQuery QualOrVar
  QueryTypeOrVarByQVarLowerBound ::
    QVar -> ConstraintSetQuery TypeOrVar
  QueryTypeOrVarByQVarUpperBound ::
    QVar -> ConstraintSetQuery TypeOrVar

-- TODO: this routine is a prime candidate for optimization once the
--       ConstraintSet type is fancier.
csQuery :: (Ord r) => ConstraintSet -> ConstraintSetQuery r -> [r]
csQuery (ConstraintSet csSet) query =
  let cs = Set.toList csSet in
  case query of
    QueryAllTypesLowerBoundingUVars -> do
      IntermediateConstraint (Left t) (Right a) <- cs
      return (t,a)
    QueryAllTypesLowerBoundingTypes -> do
      IntermediateConstraint (Left t) (Left t') <- cs
      return (t,t')
    QueryAllBinaryOperations -> do
      BinaryOperatorConstraint a1 op a2 a3 <- cs
      return (a1,op,a2,a3)
    QueryAllQualOrVarLowerBoundingQualOrVar -> do
      QualifiedIntermediateConstraint qv1 qv2 <- cs
      return (qv1,qv2)
    QueryAllTypeOrVarLowerBoundingQVar -> do
      QualifiedLowerConstraint ta qa <- cs
      return (ta,qa)
    QueryAllQVarLowerBoundingQVar -> do
      QualifiedIntermediateConstraint (Right qa1) (Right qa2) <- cs
      return (qa1, qa2)
    QueryTypeOrVarByUVarLowerBound a -> do
      IntermediateConstraint (Right a') ta <- cs
      guard $ a == a'
      return ta
    QueryTypeByUVarUpperBound a -> do
      IntermediateConstraint (Left t) (Right a') <- cs
      guard $ a == a'
      return t
    QueryQualOrVarByQualOrVarLowerBound qa -> do
      QualifiedIntermediateConstraint qa1 qa2 <- cs
      guard $ qa == qa1
      return qa2
    QueryTypeOrVarByQVarLowerBound qa -> do
      QualifiedUpperConstraint qa' ta <- cs
      guard $ qa == qa'
      return ta
    QueryTypeOrVarByQVarUpperBound qa -> do
      QualifiedLowerConstraint ta qa' <- cs
      guard $ qa == qa'
      return ta

-- ** Convenience routines

-- |A typeclass with convenience instances for constructing constraints.  This
--  constructor only works on 2-ary constraint constructors (which most
--  constraints have).
class ConstraintConstructor2 a b where
  constraint :: a -> b -> Constraint

{-
  The following Template Haskell creates various instances for the constraint
  function.  In each case of a TypeOrVar or QualOrVar, three variations are
  produced: one which takes the left side, one which takes the right, and one
  which takes the actual Either structure.
-}
$(
  -- The instances variable contains 5-tuples describing the varying positions
  -- in the typeclass instance template below.
  let instances =
        let typeOrVar = [ ([t|ShallowType|], [|Left|])
                        , ([t|UVar|], [|Right|])
                        , ([t|TypeOrVar|], [|id|])
                        ]
            qualOrVar = [ ([t|Set TQual|], [|Left|])
                        , ([t|QVar|], [|Right|])
                        , ([t|QualOrVar|], [|id|])
                        ]
        in
        -- Each of the following lists represent the 5-tuples for one constraint
        -- constructor.
        [ (t1, t2, [|IntermediateConstraint|], f1, f2)
        | (t1,f1) <- typeOrVar
        , (t2,f2) <- typeOrVar
        ]
        ++
        [ (t1, [t|QVar|], [|QualifiedLowerConstraint|], f1, [|id|])
        | (t1, f1) <- typeOrVar
        ]
        ++
        [ ([t|QVar|], t2, [|QualifiedUpperConstraint|], [|id|], f2)
        | (t2, f2) <- typeOrVar
        ]
        ++
        [ (t1, t2, [|QualifiedIntermediateConstraint|], f1, f2)
        | (t1, f1) <- qualOrVar
        , (t2, f2) <- qualOrVar
        ]
  in
  -- This function takes the 5-tuples and feeds them into the typeclass instance
  -- template, the actual instances.
  let mkInstance (t1, t2, cons, f1, f2) =
        [d|
          instance ConstraintConstructor2 $t1 $t2 where
            constraint a b = $cons ($f1 a) ($f2 b)
        |]
  in
  -- Rubber, meet road.
  concat <$> mapM mkInstance instances
 )
