{-|
  A module defining basic data structures for the type manifestation process.
-}

module Language.K3.TypeSystem.Manifestation.Data
( BoundDictionary(..)

, BoundType(..)
, DelayedOperationTag(..) -- TODO: remove entirely?
, getBoundTypeName
, getBoundDefaultType
, getConcreteUVarBounds
, getConcreteQVarBounds
, getConcreteQVarQualifiers
, getQualifierOperation
, getDualBoundType
, getTyVarOp
) where

import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Common

-- * Bound dictionaries

data BoundDictionary
  = BoundDictionary
      { uvarBoundDict :: Map (UVar, BoundType) ShallowType
      , uvarOpaqueBoundDict :: Map (UVar, BoundType) (Set OpaqueVar)
      , qvarBoundDict :: Map (QVar, BoundType) ShallowType
      , qvarOpaqueBoundDict :: Map (QVar, BoundType) (Set OpaqueVar)
      , qvarQualDict :: Map (QVar, BoundType) (Set TQual)
      , ovarRangeDict :: Map (OpaqueVar, BoundType) TypeOrVar
      }
      
instance Monoid BoundDictionary where
  mempty =
    BoundDictionary Map.empty Map.empty Map.empty Map.empty Map.empty Map.empty
  mappend (BoundDictionary ubD uobD qbD qobD qqD orD)
          (BoundDictionary ubD' uobD' qbD' qobD' qqD' orD') =
    BoundDictionary
      (ubD `Map.union` ubD')
      (Map.unionWith Set.union uobD uobD')
      (qbD `Map.union` qbD')
      (Map.unionWith Set.union qobD qobD')
      (qqD `Map.union` qqD')
      (orD `Map.union` orD')

instance Pretty BoundDictionary where
  prettyLines dict = vconcats
    [ ["UVars:          "] %+
        prettyVarMap prettyBoundVarPair prettyLines (uvarBoundDict dict)
    , ["UVars opaque:   "] %+
        prettyVarMap prettyBoundVarPair normalPrettySet
          (uvarOpaqueBoundDict dict)
    , ["QVars:          "] %+
        prettyVarMap prettyBoundVarPair normalPrettyPair
          (Map.intersectionWith (,) (qvarBoundDict dict) (qvarQualDict dict))
    , ["QVars opaque:   "] %+
        prettyVarMap prettyBoundVarPair normalPrettySet
          (qvarOpaqueBoundDict dict)
    , ["Opaques bounds: "] %+
        explicitNormalPrettyMap normalPrettyPair prettyLines
          (ovarRangeDict dict)
    ]
    where
      prettyVarMap = explicitPrettyMap ["{"] ["}"] ", " ""
      prettyBoundVarPair :: (TVar q, BoundType) -> [String]
      prettyBoundVarPair (var, bt) =
        prettyLines var %+
        case bt of
          UpperBound -> ["≤"]
          LowerBound -> ["≥"]

-- * Bound types

-- |A data type representing a bounding direction.
data BoundType
  = UpperBound
  | LowerBound
  deriving (Eq, Ord, Show)

instance Pretty BoundType where
  prettyLines = (:[]) . (++" bound") . getBoundTypeName

-- TODO: remove this structure?
-- |An enumeration identifying the delayed operations over groups of types which
--  are used during manifestation.
data DelayedOperationTag
  = DelayedIntersectionTag
  | DelayedUnionTag
  deriving (Eq, Ord, Show)

-- |Retrieves the name of a bound type.
getBoundTypeName :: BoundType -> String
getBoundTypeName UpperBound = "upper"
getBoundTypeName LowerBound = "lower"

-- |Retrieves the default type for a given bounding direction.  This is
--  equivalent to the unit of the canonical merge (e.g. intersection or union).
getBoundDefaultType :: BoundType -> ShallowType
getBoundDefaultType UpperBound = STop
getBoundDefaultType LowerBound = SBottom

-- |Retrieves a function to generate an unqualified variable bounding query
--  based on bounding type.
getConcreteUVarBounds :: BoundType -> UVar -> ConstraintSetQuery ShallowType
getConcreteUVarBounds UpperBound = QueryTypeByUVarLowerBound
getConcreteUVarBounds LowerBound = QueryTypeByUVarUpperBound

-- |Retrieves a function to generate a qualified variable bounding query based
--  on bounding type.
getConcreteQVarBounds :: BoundType -> QVar -> ConstraintSetQuery ShallowType
getConcreteQVarBounds UpperBound = QueryTypeByQVarLowerBound
getConcreteQVarBounds LowerBound = QueryTypeByQVarUpperBound

-- |Retrieves a function to generate a query for a set of qualifiers based on
--  bounding type.
getConcreteQVarQualifiers :: BoundType -> QVar -> ConstraintSetQuery (Set TQual)
getConcreteQVarQualifiers UpperBound = QueryTQualSetByQVarLowerBound
getConcreteQVarQualifiers LowerBound = QueryTQualSetByQVarUpperBound

-- |Retrieves a qualifier set merging operation based on bounding types.
getQualifierOperation :: BoundType -> [Set TQual] -> Set TQual
getQualifierOperation UpperBound = Set.unions
getQualifierOperation LowerBound =
  foldl Set.intersection (Set.fromList [TMut, TImmut])

-- |Retrieves the dual of a bounding type.
getDualBoundType :: BoundType -> BoundType
getDualBoundType UpperBound = LowerBound
getDualBoundType LowerBound = UpperBound

-- |Retrieves the type variable operator identifying the sort of operation
--  performed by a given bounding type.
getTyVarOp :: BoundType -> TypeVariableOperator
getTyVarOp UpperBound = TyVarOpIntersection
getTyVarOp LowerBound = TyVarOpUnion

