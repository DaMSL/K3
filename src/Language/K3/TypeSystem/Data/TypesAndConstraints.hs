{-# LANGUAGE GADTs, DataKinds, KindSignatures, StandaloneDeriving, ConstraintKinds, ScopedTypeVariables, FlexibleInstances #-}

module Language.K3.TypeSystem.Data.TypesAndConstraints
( TVarQualification(..)
, TVar(..)
, tvarId
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
, UVarBound
, BinaryOperator(..)
, AnyOperator(..)
, Constraint(..)
, ConstraintSet(..)
) where

import Data.Function
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Core.Common
import Language.K3.Pretty
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

instance Pretty (TVar a) where
  prettyLines v = case v of
    QTVar n _ -> ["å" ++ show n]
    UTVar n _ -> ["α" ++ show n]

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
type ConstraintSetType c = (Pretty c, Show c)

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

instance Pretty AnyTVar where
  prettyLines sa = case sa of
                    SomeQVar qa -> prettyLines qa
                    SomeUVar a -> prettyLines a

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

instance Pretty TQual where
  prettyLines q = case q of
                    TMut -> ["mut"]
                    TImmut -> ["immut"]

instance Pretty (Set TQual) where
  prettyLines qs =
    ["{"] %+ intersperseBoxes [","] (map prettyLines $ Set.toList qs) %+ ["}"]
  
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

instance (Pretty c) => Pretty (QuantType c) where
  prettyLines (QuantType sas qa cs) =
    ["∀{"] %+
    sequenceBoxes maxWidth "," (map prettyLines $ Set.toList sas) %+
    ["}. "] %+
    prettyLines qa %+
    ["\\"] %/
    prettyLines cs
  
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
  
instance Pretty ShallowType where
  prettyLines t = case t of
    SFunction a a' -> prettyLines a %+ ["->"] %+ prettyLines a'
    STrigger a' -> ["trigger "] %+ prettyLines a'
    SBool -> ["bool"]
    SInt -> ["int"]
    SReal -> ["real"]
    SString -> ["string"]
    SOption qa -> ["option "] %+ prettyLines qa
    SIndirection qa -> ["ind "] %+ prettyLines qa
    STuple qas -> ["("] %+ intersperseBoxes [","] (map prettyLines qas) %+ [")"]
    SRecord rows ->
      let rowBox (i,qa) = [i] %+ prettyLines qa in
      ["{"] %+ intersperseBoxes [","] (map rowBox $ sort $ Map.toList rows) %+
      ["}"]
    STop -> ["⊤"]
    SBottom -> ["⊥"]

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
-- |A type alias describing a type or any kind of variable.
type UVarBound = Coproduct TypeOrVar QVar


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
  
instance Pretty Constraint where
  prettyLines c =
    case c of
      IntermediateConstraint ta1 ta2 -> binPretty ta1 ta2
      QualifiedLowerConstraint ta qa -> binPretty ta qa
      QualifiedUpperConstraint qa ta -> binPretty qa ta
      QualifiedIntermediateConstraint qv1 qv2 -> binPretty qv1 qv2
      BinaryOperatorConstraint a1 op a2 a3 ->
        prettyLines a1 %+ prettyLines op %+ prettyLines a2 %+ ["<:"] %+
        prettyLines a3
      MonomorphicQualifiedUpperConstraint qa qs ->
        prettyLines qa %+ ["<:"] %+ prettyLines qs
      PolyinstantiationLineageConstraint qa1 qa2 ->
        prettyLines qa1 %+ ["<<:"] %+ prettyLines qa2
    where
      binPretty x y = prettyLines x %+ ["<:"] %+ prettyLines y
  
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
  
instance Pretty BinaryOperator where
  prettyLines op = (case op of
      BinOpAdd -> "+"
      BinOpSubtract -> "-"
      BinOpMultiply -> "*"
      BinOpDivide -> "/"
      BinOpEquals -> "=="
      BinOpGreater -> ">"
      BinOpLess -> "<"
      BinOpGreaterEq -> ">="
      BinOpLessEq -> "<="
      BinOpSequence -> ";"
      BinOpApply -> ""
      BinOpSend -> "<-"
    ):[]
  
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

instance Pretty ConstraintSet where
  prettyLines (ConstraintSet cs) =
    ["{ "] %+
    (sequenceBoxes (max 1 $ maxWidth - 4) ", " $
        map prettyLines $ Set.toList cs) +%
    [" }"]