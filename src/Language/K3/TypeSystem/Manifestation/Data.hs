{-|
  A module defining basic data structures for the type manifestation process.
-}

module Language.K3.TypeSystem.Manifestation.Data
( BoundType(..)
, upperBound
, lowerBound

, DelayedOperationTag(..)
, DelayedType(..)
, shallowToDelayed
) where

import Data.List
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.Utils.Pretty

-- |A data type describing bounding directions for type manifestation.  A value
--  of this type describes how manifestation proceeds to extract constraints
--  from the constraint set and controls the bounding direction of the resulting
--  type.
data BoundType
  = BoundType
      { getBoundTypeName :: String
      , getConcreteUVarBounds :: UVar -> ConstraintSetQuery ShallowType
      , getConcreteQVarBounds :: QVar -> ConstraintSetQuery ShallowType
      , getConcreteQVarQualifiers :: QVar -> ConstraintSetQuery (Set TQual)
      , getDelayedOperationTag :: DelayedOperationTag
      , getDelayedOperation :: [DelayedType] -> DelayedType
      , getQualifierOperation :: [Set TQual] -> Set TQual
      , getDualBoundType :: BoundType
      , getTyVarOp :: TypeVariableOperator
      }
      {-^
        Represents a bounding type.  The elements are:
        
            * @getConcreteUVarBounds@: A query to retrieve the concrete bounds
              for a given @UVar@ in the appropriate direction.

            * @getConcreteQVarBounds@: A query to retrieve the concrete type
              bounds for a given @QVar@ in the appropriate direction.

            * @getConcreteQVarQualifiers@: A query to retrieve the qualifier
              sets for a given @QVar@ in the appropriate direction.
              
            * @getDelayedOperation@: Retrieves a @DelayedOperation@ which
              describes this bound's operation for joining types.
      -}

upperBound :: BoundType
upperBound = BoundType
              { getBoundTypeName = "upper"
              , getConcreteUVarBounds = QueryTypeByUVarLowerBound
              , getConcreteQVarBounds = QueryTypeByQVarLowerBound
              , getConcreteQVarQualifiers = QueryTQualSetByQVarLowerBound
              , getDelayedOperationTag = DelayedIntersectionTag
              , getDelayedOperation = delayedIntersections
              , getQualifierOperation = Set.unions
              , getDualBoundType = lowerBound
              , getTyVarOp = TyVarOpIntersection
              }

lowerBound :: BoundType
lowerBound = BoundType
              { getBoundTypeName = "lower"
              , getConcreteUVarBounds = QueryTypeByUVarUpperBound
              , getConcreteQVarBounds = QueryTypeByQVarUpperBound
              , getConcreteQVarQualifiers = QueryTQualSetByQVarUpperBound
              , getDelayedOperationTag = DelayedUnionTag
              , getDelayedOperation = delayedUnions
              , getQualifierOperation =
                  foldl Set.intersection (Set.fromList [TMut, TImmut])
              , getDualBoundType = upperBound
              , getTyVarOp = TyVarOpUnion
              }
              
-- |An enumeration identifying the delayed operations over groups of types which
--  are used during manifestation.
data DelayedOperationTag
  = DelayedIntersectionTag
  | DelayedUnionTag
  deriving (Eq, Ord, Show)

-- |A version of @ShallowType@ which represents a partially distributed logical
--  connective over types (either union or intersection).  The option form, for
--  instance, represents the option of the union (or intersection, determined by
--  context) of the types given in the constructor.
data DelayedType
  = DFunction (Set UVar) (Set UVar)
  | DTrigger (Set UVar)
  | DBool
  | DInt
  | DReal
  | DString
  | DAddress
  | DOption (Set QVar)
  | DIndirection (Set QVar)
  | DTuple [Set QVar]
  | DRecord (Map Identifier (Set QVar)) (Set OpaqueVar)
  | DTop
  | DBottom
  | DOpaque (Set OpaqueVar)
  deriving (Eq, Ord, Show)

instance Pretty DelayedType where
  prettyLines t = case t of
    DFunction as as' -> prettySet as %+ ["->"] %+ prettySet as'
    DTrigger as' -> ["trigger "] %+ prettySet as'
    DBool -> ["bool"]
    DInt -> ["int"]
    DReal -> ["real"]
    DString -> ["string"]
    DAddress -> ["address"]
    DOption qas -> ["option "] %+ prettySet qas
    DIndirection qas -> ["ind "] %+ prettySet qas
    DTuple qass -> ["("] %+ intersperseBoxes [","] (map prettySet qass) %+ [")"]
    DRecord rows oas ->
      let rowBox (i,qas) = [i++":"] %+ prettySet qas in
      ["{"] %+ intersperseBoxes [","] (map rowBox $ sort $ Map.toList rows) +%
      ["}"] %+
      if Set.null oas then [] else
        ["&{"] %+ intersperseBoxes [","] (map prettyLines $ sort $
                                            Set.toList oas) +% ["}"]
    DTop -> ["⊤"]
    DBottom -> ["⊥"]
    DOpaque aos -> prettySet aos
    where
      prettySet :: (Pretty a) => Set a -> [String]
      prettySet xs = ["("] %+ intersperseBoxes [" | "]
                                (map prettyLines $ Set.toList xs) %+ [")"]

-- |A routine which "promotes" a shallow type to a delayed type.
shallowToDelayed :: ShallowType -> DelayedType
shallowToDelayed t = case t of
  SFunction a a' -> DFunction (Set.singleton a) (Set.singleton a')
  STrigger a -> DTrigger (Set.singleton a)
  SBool -> DBool
  SInt -> DInt
  SReal -> DReal
  SString -> DString
  SAddress -> DAddress
  SOption qa -> DOption (Set.singleton qa)
  SIndirection qa -> DIndirection (Set.singleton qa)
  STuple qas -> DTuple (map Set.singleton qas)
  SRecord m oas -> DRecord (Map.map Set.singleton m) oas
  STop -> DTop
  SBottom -> DBottom
  SOpaque oa -> DOpaque (Set.singleton oa)

-- |An implementation of intersection over a list of delayed types.  Note that
--  this operation is shallow; it is the responsibility of other code to address
--  the deep implications of the intersection (e.g. contravariance on
--  functions), but @mappend@ on a @DelayedIntersection@ will address the
--  shallow implications, such as unioning the set of record fields.
delayedIntersections :: [DelayedType] -> DelayedType
delayedIntersections ts =
  unDelayedIntersection $ mconcat $ map DelayedIntersection ts

-- |A newtype which defines the delayed intersection operation via an instance
--  of @Monoid@.
newtype DelayedIntersection
  = DelayedIntersection {unDelayedIntersection :: DelayedType }
  
instance Monoid DelayedIntersection where
  mempty = DelayedIntersection DTop
  mappend (DelayedIntersection t) (DelayedIntersection t') =
    DelayedIntersection $
      delayedMerge DBottom id (const DBottom) Map.unionWith t t'

-- |An implementation of intersection over a list of delayed types.  Note that
--  this operation is shallow; it is the responsibility of other code to address
--  the deep implications of the intersection (e.g. contravariance on
--  functions), but @mappend@ on a @DelayedIntersection@ will address the
--  shallow implications, such as unioning the set of record fields.
delayedUnions :: [DelayedType] -> DelayedType
delayedUnions ts =
  unDelayedUnion $ mconcat $ map DelayedUnion ts

-- |A newtype which defines the delayed intersection operation via an instance
--  of @Monoid@.
newtype DelayedUnion
  = DelayedUnion {unDelayedUnion :: DelayedType }
  
instance Monoid DelayedUnion where
  mempty = DelayedUnion DBottom
  mappend (DelayedUnion t) (DelayedUnion t') =
    DelayedUnion $ delayedMerge DTop (const DTop) id Map.intersectionWith t t'

-- |A routine which generalizes over the monoidal implementations of both
--  intersection and union.
delayedMerge :: DelayedType -- ^The default type when constructors do not match.
             -> (DelayedType -> DelayedType)
                  -- ^The routine used when one element is top.  This function
                  --  accepts the other element and produces the answer.
             -> (DelayedType -> DelayedType)
                  -- ^The routine used when one element is bottom.
             -> (   (Set QVar -> Set QVar -> Set QVar)
                 -> Map Identifier (Set QVar)
                 -> Map Identifier (Set QVar)
                 -> Map Identifier (Set QVar) )
                  -- ^The routine used to handle records' label mappings.
             -> DelayedType
                  -- ^The first type to merge.
             -> DelayedType
                  -- ^The second type to merge.
             -> DelayedType
delayedMerge tDefault fTop fBottom mapMerge t t'  =
      case (t, t') of
        (DTop, _) -> fTop t'
        (_, DTop) -> fTop t
        (DBottom, _) -> fBottom t'
        (_, DBottom) -> fBottom t
        (DFunction as1 as2, DFunction as1' as2') ->
          -- NOTE: contravariance of functions is handled elsewhere; this
          --       operation is shallow
          DFunction (as1 `Set.union` as1') (as2 `Set.union` as2')
        (DFunction _ _, _) -> tDefault
        (DTrigger as, DTrigger as') ->
          -- NOTE: as above, contravariance is handled elsewhere
          DTrigger (as `Set.union` as')
        (DTrigger _, _) -> tDefault
        (_, DTrigger _) -> tDefault
        (DBool, DBool) -> DBool
        (DBool, _) -> tDefault
        (DInt, DInt) -> DInt
        (DInt, _) -> tDefault
        (DReal, DReal) -> DReal
        (DReal, _) -> tDefault
        (DString, DString) -> DString
        (DString, _) -> tDefault
        (DAddress, DAddress) -> DAddress
        (DAddress, _) -> tDefault
        (DOption qas, DOption qas') -> DOption (qas `Set.union` qas') 
        (DOption _, _) -> tDefault
        (DIndirection qas, DIndirection qas') ->
          DIndirection (qas `Set.union` qas') 
        (DIndirection _, _) -> tDefault
        (DTuple qass, DTuple qass') ->
          if length qass /= length qass' then tDefault else
            DTuple $ zipWith Set.union qass qass'
        (DTuple _, _) -> tDefault
        (DRecord m oas, DRecord m' oas') ->
          DRecord (mapMerge Set.union m m') (oas `Set.union` oas')
        (DRecord _ _, _) -> tDefault
        (DOpaque oas, DOpaque oas') -> DOpaque (oas `Set.union` oas')
        (DOpaque _, _) -> tDefault
