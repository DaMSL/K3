{-# LANGUAGE GeneralizedNewtypeDeriving, DataKinds, TypeSynonymInstances, FlexibleInstances, FlexibleContexts, ScopedTypeVariables, TupleSections, TemplateHaskell #-}

{-|
  A module providing an algorithm to eliminate branching from constraint sets.
  This algorithm does not *completely* eliminate branching in all constraint
  sets; opaque types cannot be inspected and so cannot be unioned or
  intersected.
  
  At the completion of the @semisemiEliminateBranches@ routine, each type variable
  in the resulting constraint set will have only one /transparent/ (i.e.
  non-opaque) bound in each direction.  Performing this conversion often causes
  some loss of precision; for instance, no single lower bound can properly
  represent the type of a variable lower bounded by both bool and string, so the
  lower bound provided by this algorithm will instead be top.  Such loss of
  precision always creates a more restrictive, conservative type.
-}
module Language.K3.TypeSystem.Manifestation.BranchSemiElimination
( semiEliminateBranches
) where

import Control.Applicative
import Control.Monad.RWS
import Data.List (sort)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Traversable as Trav

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.Utils.Conditional
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Common

$(loggingFunctions)

semiEliminateBranches :: ConstraintSet -> ConstraintSet
semiEliminateBranches cs =
  let vars = extractVariables cs in
  let firstId = 1 + maximum (map (tvarIdNum . anyTVarId) $ Set.toList vars) in
  let initEnv = BranchElimEnvironment
                  { branchElimInputConstraints = cs
                  } in
  let initState = BranchElimState
                    { branchElimNextId = firstId
                    , uvarSignatures = Map.empty
                    , qvarSignatures = Map.empty
                    } in
  let computation = mconcat <$> mapM calculateBoundOfAnyVar (Set.toList vars) in
  let (_,_,genCs) = runRWS (unBranchElimM computation) initEnv initState in
  foldr csInsert genCs $ filter preservedConstraint $ csToList cs
  where
    -- |Determines if a constraint from the old set should be copied into the
    --  new set.
    preservedConstraint :: Constraint -> Bool
    preservedConstraint c = case c of
      OpaqueBoundConstraint _ _ _ -> True
      _ -> False
    calculateBoundOfAnyVar :: AnyTVar -> BranchElimM ()
    calculateBoundOfAnyVar var =
      case var of
        SomeQVar qa -> void $ calculateBoundsOfQVars $ Set.singleton qa
        SomeUVar a -> void $ calculateBoundsOfUVars $ Set.singleton a

-- * Branch elimination monad

-- |The branch elimination monad.  This monad is used to reconstruct a
--  constraint set in the form of a conservative approximation in which each
--  type variable has exactly one bound in each direction.
newtype BranchElimM a
  = BranchElimM
      { unBranchElimM :: RWS BranchElimEnvironment
                             ConstraintSet
                             BranchElimState
                             a
      }
  deriving ( Applicative, Functor, Monad, MonadReader BranchElimEnvironment
           , MonadState BranchElimState, MonadWriter ConstraintSet)

-- |The environment present during branch elimination.  Includes an original
--  constraint set over which elimination is occurring.
data BranchElimEnvironment
  = BranchElimEnvironment
      { branchElimInputConstraints :: ConstraintSet
      }

-- |The state maintained during branch elimination.  This permits the monad to
--  assign fresh variable IDs.  It also allows the monad to describe which type
--  variable in the new constraint set will acquire the responsibility of
--  representing the result of this operation.  This information is kept
--  persistent during branch elimination to ensure that recursive types are
--  properly modeled.
data BranchElimState
  = BranchElimState
      { branchElimNextId :: Int
      , uvarSignatures :: Map (Set UVar) UVar
      , qvarSignatures :: Map (Set QVar) QVar
      }
    
-- |Describes a set of variables and an operation performed upon them.
data VariableSignature q
  = VariableSignature (Set (TVar q)) BoundType
  deriving (Eq, Ord, Show)
  
freshBranchElimVar :: BranchElimM TVarID
freshBranchElimVar = do
  s <- get
  let n = branchElimNextId s
  put $ s { branchElimNextId = n + 1 }
  return $ TVarBasicID n

instance FreshVarI BranchElimM where
  freshUVar origin = (`UTVar` origin) <$> freshBranchElimVar
  freshQVar origin = (`QTVar` origin) <$> freshBranchElimVar

{-| A class to add an appropriate bounding constraint to the outgoing set. -}
-- This routine is restricted from _addBoundingConstraint below just to ensure
-- that none of the code accidentally transposes the arguments.
class AddBoundingConstraint a where
  addBoundingConstraint :: ( ConstraintConstructor2 a b
                           , ConstraintConstructor2 b a)
                        => BoundType -> a -> b -> BranchElimM ()

instance AddBoundingConstraint ShallowType where
  addBoundingConstraint = _addBoundingConstraint

instance AddBoundingConstraint (Set TQual) where
  addBoundingConstraint = _addBoundingConstraint
    
_addBoundingConstraint :: ( ConstraintConstructor2 a b
                          , ConstraintConstructor2 b a)
                       => BoundType -> a -> b -> BranchElimM ()
_addBoundingConstraint bt x y =
  tell $ csSing $ _getBoundConstructor bt x y

_getBoundConstructor :: ( ConstraintConstructor2 a b
                        , ConstraintConstructor2 b a)
                     => BoundType -> a -> b -> Constraint
_getBoundConstructor LowerBound = (<:)
_getBoundConstructor UpperBound = flip (<:)

-- * Branch elimination subroutines

-- |Calculates a conservative in-context bound of the provided set of variables
--  and enters it as a constraint in the outgoing set.  Returns the variable
--  which is used to represent this particular combination; its upper bound
--  is the intersection of the upper bounds of this set and its lower bound is
--  the union of the lower bounds of this set.
calculateBoundsOfUVars :: Set UVar -> BranchElimM UVar
calculateBoundsOfUVars =
  _calculateBoundsOfVars
    freshUVar
    (uvarSignatures <$> get)
    (\k v -> modify $
                \s -> s { uvarSignatures = Map.insert k v $ uvarSignatures s })
    getConcreteUVarBounds

-- |Calculates a conservative in-context bound of the provided set of variables
--  and enters it as a constraint in the outgoing set along with appropriate
--  qualifier constraints.
calculateBoundsOfQVars :: Set QVar -> BranchElimM QVar
calculateBoundsOfQVars qas = do
  qa <- _calculateBoundsOfVars
          freshQVar
          (qvarSignatures <$> get)
          (\k v -> modify $
                \s -> s { qvarSignatures = Map.insert k v $ qvarSignatures s })
          getConcreteQVarBounds
          qas
  calculateOneQualBoundOfVar qa UpperBound
  calculateOneQualBoundOfVar qa LowerBound
  return qa
  where
    calculateOneQualBoundOfVar :: ( ConstraintConstructor2 QVar (Set TQual)
                                  , ConstraintConstructor2 (Set TQual) QVar )
                               => QVar -> BoundType -> BranchElimM ()
    calculateOneQualBoundOfVar reprVar bt = do
      cs <- branchElimInputConstraints <$> ask
      let bounds = Set.fromList $
                      concatMap (csQuery cs . getConcreteQVarQualifiers bt)
                                (Set.toList qas)
      let qs = getQualifierOperation bt $ Set.toList bounds
      addBoundingConstraint bt qs reprVar

-- |Calculates bounds for a given variable in a generic fashion.
_calculateBoundsOfVars :: forall q.
                          ( ConstraintConstructor2 (TVar q) ShallowType
                          , ConstraintConstructor2 ShallowType (TVar q))
                       => (TVarOrigin q -> BranchElimM (TVar q))
                       -> (BranchElimM (Map (Set (TVar q)) (TVar q)))
                       -> (Set (TVar q) -> TVar q -> BranchElimM ())
                       -> (BoundType -> TVar q
                            -> ConstraintSetQuery ShallowType)
                       -> Set (TVar q)
                       -> BranchElimM (TVar q)
_calculateBoundsOfVars freshVar sigsM insertSigsM getConcreteVarBounds vars =
  bracketLogM _debugI
    (boxToString $
        ["Branch elimination calculating bounds for "] %+ normalPrettySet vars)
    (\var -> boxToString $
        ["Branch elimination determined bounds for "] %+
          normalPrettySet vars %+ [" represented by "] %+ prettyLines var)
  $
  do
    sigs <- sigsM
    case Map.lookup vars sigs of
      Just var -> return var
      Nothing -> do
        var' <- case Set.toList vars of
                  [var] -> return var
                  _ -> freshVar $ TVarFreshInBranchElimination vars
        insertSigsM vars var'
        calculateOneBoundOfVar var' UpperBound
        calculateOneBoundOfVar var' LowerBound
        return var'
  where
    calculateOneBoundOfVar :: ( ConstraintConstructor2 (TVar q) ShallowType
                               , ConstraintConstructor2 ShallowType (TVar q))
                            => TVar q -> BoundType -> BranchElimM ()
    calculateOneBoundOfVar reprVar bt = do
      cs <- branchElimInputConstraints <$> ask
      let bounds = Set.fromList $
                      concatMap (csQuery cs . getConcreteVarBounds bt)
                                (Set.toList vars)
      (oas,mt) <- calculateBoundOfTypes bt bounds
      whenJust mt $ \t -> addBoundingConstraint bt t reprVar
      mconcat <$> mapM (\oa -> addBoundingConstraint bt (SOpaque oa) reprVar)
                       (Set.toList oas)

-- |Calculates a conservative in-context bound for the provided set of
--  concrete shallow types.  This bound is in the form of a single transparent
--  type and a set of opaque types.
calculateBoundOfTypes :: BoundType -> Set ShallowType
                      -> BranchElimM (Set OpaqueVar, Maybe ShallowType)
calculateBoundOfTypes bt ts =
  let DelayedType oas mt =
        getDelayedOperation bt $ map shallowToDelayed $ Set.toList ts in
  (oas,) <$>
  case mt of
    Just t -> Just <$> case t of
      DFunction as a's ->
        SFunction <$> calculateBoundsOfUVars as <*> calculateBoundsOfUVars a's
      DTrigger as -> STrigger <$> calculateBoundsOfUVars as
      DBool -> return SBool
      DInt -> return SInt
      DReal -> return SReal
      DNumber -> return SNumber
      DString -> return SString
      DAddress -> return SAddress
      DOption qas -> SOption <$> calculateBoundsOfQVars qas
      DIndirection qas -> SIndirection <$> calculateBoundsOfQVars qas
      DTuple qass -> STuple <$> mapM calculateBoundsOfQVars qass
      DRecord m oas' -> SRecord <$> Trav.mapM calculateBoundsOfQVars m
                                <*> pure oas' -- TODO: is this handling correct?
      DTop -> return STop
      DBottom -> return SBottom
    Nothing -> return Nothing

-- * Delayed types

-- |A type structure representing a /delayed/ type: a type which has some form
--  of suspended operation (union or intersection) pending.  This is the top
--  level of the structure which conveys a set of opaques to be included in the
--  operation; they are not further broken down and so are kept separately.
data DelayedType
  = DelayedType (Set OpaqueVar) (Maybe InnerDelayedType)
  deriving (Eq, Ord, Show)

-- |A type structure representing the non-opaque components of a delayed type.
--  The option form, for instance, represents the option of the union or
--  intersection (as determined by context) of the types given in the
--  constructor.
data InnerDelayedType
  = DFunction (Set UVar) (Set UVar)
  | DTrigger (Set UVar)
  | DBool
  | DInt
  | DReal
  | DNumber
  | DString
  | DAddress
  | DOption (Set QVar)
  | DIndirection (Set QVar)
  | DTuple [Set QVar]
  | DRecord (Map Identifier (Set QVar)) (Set OpaqueVar)
  | DTop
  | DBottom
  deriving (Eq, Ord, Show)

instance Pretty DelayedType where
  prettyLines (DelayedType oas mt) =
    normalPrettySet oas %+ maybe [] ((["&"] %+) . prettyLines) mt

instance Pretty InnerDelayedType where
  prettyLines t = case t of
    DFunction as as' -> normalPrettySet as %+ ["->"] %+ normalPrettySet as'
    DTrigger as' -> ["trigger "] %+ normalPrettySet as'
    DBool -> ["bool"]
    DInt -> ["int"]
    DReal -> ["real"]
    DNumber -> ["number"]
    DString -> ["string"]
    DAddress -> ["address"]
    DOption qas -> ["option "] %+ normalPrettySet qas
    DIndirection qas -> ["ind "] %+ normalPrettySet qas
    DTuple qass ->
      ["("] %+ intersperseBoxes [","] (map normalPrettySet qass) %+ [")"]
    DRecord rows oas ->
      let rowBox (i,qas) = [i++":"] %+ normalPrettySet qas in
      ["{"] %+ intersperseBoxes [","] (map rowBox $ sort $ Map.toList rows) +%
      ["}"] %+
      if Set.null oas then [] else
        ["&{"] %+ intersperseBoxes [","] (map prettyLines $ sort $
                                            Set.toList oas) +% ["}"]
    DTop -> ["⊤"]
    DBottom -> ["⊥"]

-- |A routine which "promotes" a shallow type to a delayed type.
shallowToDelayed :: ShallowType -> DelayedType
shallowToDelayed t = case t of
  SFunction a a' -> f $ DFunction (Set.singleton a) (Set.singleton a')
  STrigger a -> f $ DTrigger (Set.singleton a)
  SBool -> f DBool
  SInt -> f DInt
  SReal -> f DReal
  SNumber -> f DNumber
  SString -> f DString
  SAddress -> f DAddress
  SOption qa -> f $ DOption (Set.singleton qa)
  SIndirection qa -> f $ DIndirection (Set.singleton qa)
  STuple qas -> f $ DTuple (map Set.singleton qas)
  SRecord m oas -> f $ DRecord (Map.map Set.singleton m) oas
  STop -> f DTop
  SBottom -> f DBottom
  SOpaque oa -> DelayedType (Set.singleton oa) Nothing
  where
    f = DelayedType Set.empty . Just

-- |Retrieves a delayed type merging operation based on bounding type.
getDelayedOperation :: BoundType -> [DelayedType] -> DelayedType
getDelayedOperation UpperBound = delayedIntersections
getDelayedOperation LowerBound = delayedUnions

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
  mempty = DelayedIntersection $ shallowToDelayed STop
  mappend (DelayedIntersection t) (DelayedIntersection t') =
    DelayedIntersection $
      delayedMerge DBottom special id (const DBottom) Map.unionWith t t'
    where
      special :: InnerDelayedType -> InnerDelayedType -> Maybe InnerDelayedType
      special DReal DNumber = Just DReal
      special DNumber DReal = Just DReal
      special DInt DNumber = Just DInt
      special DNumber DInt = Just DInt
      special DInt DReal = Just DInt
      special DReal DInt = Just DReal
      special _ _ = Nothing

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
  mempty = DelayedUnion $ shallowToDelayed SBottom
  mappend (DelayedUnion t) (DelayedUnion t') =
    DelayedUnion $
      delayedMerge DBottom special (const DTop) id Map.intersectionWith t t'
    where
      special :: InnerDelayedType -> InnerDelayedType -> Maybe InnerDelayedType
      special DReal DNumber = Just DNumber
      special DNumber DReal = Just DNumber
      special DInt DNumber = Just DNumber
      special DNumber DInt = Just DNumber
      special DInt DReal = Just DReal
      special DReal DInt = Just DReal
      special _ _ = Nothing

-- |A routine which generalizes over the monoidal implementations of both
--  intersection and union on delayed types.
delayedMerge :: InnerDelayedType -- ^The default type when constructors do not
                                 --  match.
             -> (InnerDelayedType -> InnerDelayedType -> Maybe InnerDelayedType)
                  -- ^A special case function to use when constructors do not
                  --  match.  This allows embedding of cases to handle shallow
                  --  subtyping.  When this function returns @Nothing@, the
                  --  above default is used.
             -> (InnerDelayedType -> InnerDelayedType)
                  -- ^The routine used when one element is top.  This function
                  --  accepts the other element and produces the answer.
             -> (InnerDelayedType -> InnerDelayedType)
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
delayedMerge tDefaultVal fSpecCase fTop fBottom mapMerge
    (DelayedType oas mit) (DelayedType oas' mit') =
  let mit'' = case (mit,mit') of
                (Nothing,Nothing) -> Nothing
                (Just _,Nothing) -> mit
                (Nothing,Just _) -> mit'
                (Just it,Just it') ->
                  Just $ delayedInnerMerge tDefaultVal fSpecCase fTop fBottom
                          mapMerge it it'
  in
  DelayedType (oas `Set.union` oas') mit''

-- |A routine which generalizes over the monoidal implementations of both
--  intersection and union on inner delayed types.
delayedInnerMerge ::
                InnerDelayedType -- ^The default type when constructors do not
                                 --  match.
             -> (InnerDelayedType -> InnerDelayedType -> Maybe InnerDelayedType)
                  -- ^A special case function to use when constructors do not
                  --  match.  This allows embedding of cases to handle shallow
                  --  subtyping.  When this function returns @Nothing@, the
                  --  above default is used.
             -> (InnerDelayedType -> InnerDelayedType)
                  -- ^The routine used when one element is top.  This function
                  --  accepts the other element and produces the answer.
             -> (InnerDelayedType -> InnerDelayedType)
                  -- ^The routine used when one element is bottom.
             -> (   (Set QVar -> Set QVar -> Set QVar)
                 -> Map Identifier (Set QVar)
                 -> Map Identifier (Set QVar)
                 -> Map Identifier (Set QVar) )
                  -- ^The routine used to handle records' label mappings.
             -> InnerDelayedType
                  -- ^The first type to merge.
             -> InnerDelayedType
                  -- ^The second type to merge.
             -> InnerDelayedType
delayedInnerMerge tDefaultVal fSpecCase fTop fBottom mapMerge t t'  =
  let tDefault = fromMaybe tDefaultVal $ fSpecCase t t' in
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
    (DNumber, DNumber) -> DReal
    (DNumber, _) -> tDefault
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
