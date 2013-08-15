{-# LANGUAGE ViewPatterns, GADTs, TypeFamilies, DataKinds, KindSignatures,
             MultiParamTypeClasses, TypeSynonymInstances, FlexibleInstances,
             TupleSections, ScopedTypeVariables, Rank2Types, TemplateHaskell,
             FlexibleContexts, UndecidableInstances #-}
{-|
  This module defines the data structures associated with constraint maps.  A
  constraint map is a structure used in the decidable subtyping approximation;
  it relates each type variable of a closed, consistent constraint set with its
  upper and lower bounds.
-}
module Language.K3.TypeSystem.Subtyping.ConstraintMap
( ConstraintMap
, lowerBound
, upperBound
, flipBound
, VarBound(..)
, UVarBound
, QVarBound
, isQConcrete
, isUConcrete
, cmSing
, cmUnion
, ConstraintMapBoundable(..)

, CanonicalConstraintMap
, IsConstraintMap(..)

, kernel
, isContractive
, canonicalize
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Trans.Maybe
import Control.Monad.Writer
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Traversable as Trav
import qualified Data.Set as Set

import Language.K3.Core.Common
import Language.K3.TemplateHaskell.Reduce
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ExtractVariables

-- * Constraint map data struccture and operations

-- |A data structure representing a constraint map: K in the grammar presented
--  in the Subtyping section of the specification.  We use positive polarity to
--  represent <= and negative polarity to represent >=.  That is, the
--  constraint map {a <= int} is represented as a singleton map from
--  @(a,lowerBound)@ to @int@.
data ConstraintMap
  = ConstraintMap (Map (UVar, TPolarity) (Set UVarBound))
                  (Map (QVar, TPolarity) (Set QVarBound))
  deriving (Eq, Ord, Show)

-- |Represents the lower bound operator <=.
lowerBound :: TPolarity
lowerBound = Positive

-- |Represents the upper bound operator >=.
upperBound :: TPolarity
upperBound = Negative

-- |Flips a bound operator.
flipBound :: TPolarity -> TPolarity
flipBound = mappend Negative

data family VarBound (a :: TVarQualification) :: *
data instance VarBound UnqualifiedTVar
  = UBType ShallowType
  | UBUVar UVar
  | UBQVar QVar
  deriving (Eq, Ord, Show)
data instance VarBound QualifiedTVar
  = QBType ShallowType
  | QBUVar UVar
  | QBQVar QVar
  | QBQualSet (Set TQual)
  deriving (Eq, Ord, Show)

type UVarBound = VarBound UnqualifiedTVar
type QVarBound = VarBound QualifiedTVar

isQConcrete :: QVarBound -> Bool
isQConcrete bnd = case bnd of
                QBType _ -> True
                QBQualSet _ -> True
                QBQVar _ -> False
                QBUVar _ -> False
isUConcrete :: UVarBound -> Bool
isUConcrete bnd = case bnd of
                UBType _ -> True
                UBQVar _ -> False
                UBUVar _ -> False

cmGetMapFor :: TVar a -> ConstraintMap
            -> Map (TVar a, TPolarity) (Set (VarBound a))
cmGetMapFor sa (ConstraintMap um qm) =
  case sa of
    QTVar{} -> qm
    UTVar{} -> um
    
cmSing :: TVar a -> TPolarity -> VarBound a -> ConstraintMap
cmSing sa pol bnd =
  case sa of
    QTVar{} ->
      ConstraintMap Map.empty $ Map.singleton (sa,pol) $ Set.singleton bnd
    UTVar{} ->
      ConstraintMap (Map.singleton (sa,pol) (Set.singleton bnd)) Map.empty

cmUnion :: ConstraintMap -> ConstraintMap -> ConstraintMap
cmUnion = mappend

class ConstraintMapBoundable a b where
  cmBound :: a -> VarBound b
instance ConstraintMapBoundable ShallowType UnqualifiedTVar where
  cmBound = UBType
instance ConstraintMapBoundable UVar UnqualifiedTVar where
  cmBound = UBUVar
instance ConstraintMapBoundable QVar UnqualifiedTVar where
  cmBound = UBQVar
instance ConstraintMapBoundable ShallowType QualifiedTVar where
  cmBound = QBType
instance ConstraintMapBoundable UVar QualifiedTVar where
  cmBound = QBUVar
instance ConstraintMapBoundable QVar QualifiedTVar where
  cmBound = QBQVar
instance ConstraintMapBoundable (Set TQual) QualifiedTVar where
  cmBound = QBQualSet
instance (ConstraintMapBoundable a c, ConstraintMapBoundable b c)
  => ConstraintMapBoundable (Coproduct a b) c where
  cmBound x = case x of
                CLeft y -> cmBound y
                CRight y -> cmBound y

instance Monoid ConstraintMap where
  mempty = ConstraintMap mempty mempty
  mappend (ConstraintMap a1 a2) (ConstraintMap b1 b2) =
    ConstraintMap (f a1 b1) (f a2 b2)
    where
      f :: (Ord k, Ord a) => Map k (Set a) -> Map k (Set a) -> Map k (Set a)
      f = Map.unionWith Set.union

-- * Canonical constraint map structure and operations

-- |A data structure representing a canonical constraint map.  Canonical
--  constraint maps admit fewer operations but guarantee that each type variable
--  has exactly one concrete bound of each form (type or qualifier set).
newtype CanonicalConstraintMap = CanonicalConstraintMap ConstraintMap

-- * Operations which work on both canonical and regular constraint maps.

class IsConstraintMap m where
  cmBoundsOf :: TVar a -> TPolarity -> m -> Set (VarBound a)
  cmAddVarBound :: AnyTVar -> TPolarity -> AnyTVar -> m -> m

instance IsConstraintMap ConstraintMap where
  cmBoundsOf sa pol cm = fromMaybe Set.empty $ Map.lookup (sa,pol) $
                            cmGetMapFor sa cm
  cmAddVarBound sa pol sa' cm =
    let cm' = 
          case (sa,sa') of
            (SomeQVar qa, SomeQVar qa') -> cmSing qa pol $ QBQVar qa'
            (SomeQVar qa, SomeUVar a') -> cmSing qa pol $ QBUVar a'
            (SomeUVar a, SomeQVar qa') -> cmSing a pol $ UBQVar qa'
            (SomeUVar a, SomeUVar a') -> cmSing a pol $ UBUVar a'
    in
    cm `mappend` cm'
                        
instance IsConstraintMap CanonicalConstraintMap where
  cmBoundsOf sa pol (CanonicalConstraintMap cm) = cmBoundsOf sa pol cm
  cmAddVarBound sa pol sa' (CanonicalConstraintMap cm) =
    CanonicalConstraintMap $ cmAddVarBound sa pol sa' cm

-- * Constraint map operations

-- |A routine for computing the kernel of a constraint set.  This routine is
--  only valid if the provided constraint set has already been closed.
kernel :: ConstraintSet -> ConstraintMap
kernel (csToList -> cs) =
  mconcat $ map constraintKernel cs
  where
    constraintKernel :: Constraint -> ConstraintMap
    constraintKernel c = case c of
      IntermediateConstraint (CRight a) (CLeft t) ->
        cmSing a lowerBound (UBType t)
      IntermediateConstraint (CLeft t) (CRight a) ->
        cmSing a upperBound (UBType t)
      QualifiedLowerConstraint (CLeft t) qa ->
        cmSing qa upperBound (QBType t)
      QualifiedUpperConstraint qa (CLeft t) ->
        cmSing qa lowerBound (QBType t)
      QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
        cmSing qa lowerBound (QBQualSet qs)
      QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
        cmSing qa upperBound (QBQualSet qs)
      IntermediateConstraint (CRight a1) (CRight a2) ->
        cmSing a1 lowerBound (UBUVar a2) `mappend`
        cmSing a2 upperBound (UBUVar a1)
      QualifiedLowerConstraint (CRight a) qa ->
        cmSing a lowerBound (UBQVar qa) `mappend`
        cmSing qa upperBound (QBUVar a)
      QualifiedUpperConstraint qa (CRight a) ->
        cmSing qa lowerBound (QBUVar a) `mappend`
        cmSing a upperBound (UBQVar qa)
      QualifiedIntermediateConstraint (CRight qa1) (CRight qa2) ->
        cmSing qa1 lowerBound (QBQVar qa2) `mappend`
        cmSing qa2 upperBound (QBQVar qa1)
      IntermediateConstraint (CLeft _) (CLeft _) -> mempty
      QualifiedIntermediateConstraint (CLeft _) (CLeft _) -> mempty
      BinaryOperatorConstraint{} -> mempty
      MonomorphicQualifiedUpperConstraint{} -> mempty
      PolyinstantiationLineageConstraint{} -> mempty

-- |A routine to determine whether or not a given constraint map is contractive
--  for the specified polarity.
isContractive :: ConstraintMap -> Bool
-- TODO: for performance reasons, this information should be cached somehow.
-- Primitive subtyping must ask for contractiveness of each new constraint map
-- it builds, so this information should be gathered incrementally.
isContractive (ConstraintMap m1 m2) =
  isContractivePol Positive || isContractivePol Negative
  where
    isContractivePol :: TPolarity -> Bool
    isContractivePol pol =
      cycleHunt $ someVars (Map.keys m1) `Set.union` someVars (Map.keys m2)
      where
        someVars :: [(TVar a, TPolarity)] -> Set AnyTVar
        someVars x = Set.fromList $ map (someVar . fst) x
        findReachable :: Set AnyTVar -> AnyTVar -> [(Bool,Set AnyTVar)]
        findReachable visited v =
          if v `Set.member` visited
            then [(True,visited)]
            else
              let immediatelyReachable = 
                    case v of
                      SomeQVar qa ->
                        let bounds = fromMaybe Set.empty $
                                        Map.lookup (qa,pol) m2 in
                        nub $ map (\bnd -> case bnd of
                                      QBUVar a -> Just $ someVar a
                                      QBQVar a -> Just $ someVar a
                                      _ -> Nothing) $ Set.toList bounds
                      SomeUVar a ->
                        let bounds = fromMaybe Set.empty $
                                        Map.lookup (a,pol) m1 in
                        nub $ map (\bnd -> case bnd of
                                      UBUVar a' -> Just $ someVar a'
                                      UBQVar a' -> Just $ someVar a'
                                      _ -> Nothing) $ Set.toList bounds
              in do
                  next <- immediatelyReachable
                  case next of
                    Nothing -> [(False, Set.insert v visited)]
                    Just nv -> findReachable (Set.insert v visited) nv
        cycleHunt :: Set AnyTVar -> Bool
        cycleHunt vars =
          let var = Set.findMin vars in
          let (result,visited) =
                foldr (\(b1,s1) (b2,s2) -> (b1 && b2, s1 `Set.union` s2))
                  (False,Set.empty) (findReachable Set.empty var)
          in
          let nextVars = vars `Set.difference` visited in
          result || cycleHunt nextVars

-- |A routine which computes the VarB function for a given constraint map as
--  defined in the Subtyping section of the specification.
varB :: AnyTVar -> TPolarity -> ConstraintMap -> Set AnyTVar
varB sa pol cm =
  accum Set.empty sa
  where
    -- |This accumulator function will build up a set of explored variables to
    --  find the transitive bounds of a single variable.  The second argument
    --  is the variable currently being explored.  This function is essentially
    --  performing its own occurrence check; it should terminate even if the
    --  constraint map is non-contractive.
    accum :: Set AnyTVar -> AnyTVar -> Set AnyTVar
    accum s sa' =
      let vars = Set.fromList $ case sa of
                  SomeQVar qa -> mapMaybe (boundToVar . (qa,)) $
                                    Set.toList $ cmBoundsOf qa pol cm
                  SomeUVar a -> mapMaybe (boundToVar . (a,)) $
                                    Set.toList $ cmBoundsOf a pol cm
      in
      let newVars = sa' `Set.delete` (vars `Set.difference` s) in
      Set.insert sa $ Set.union vars $ Set.unions $ map (accum vars) $
                                                        Set.toList newVars
    boundToVar :: forall a. (TVar a, VarBound a) -> Maybe AnyTVar
    boundToVar x = case x of
      (QTVar{}, QBQVar qa) -> Just $ someVar qa
      (QTVar{}, QBUVar a) -> Just $ someVar a
      (UTVar{}, UBQVar qa) -> Just $ someVar qa
      (UTVar{}, UBUVar a) -> Just $ someVar a
      _ -> Nothing

-- |A routine which computes the ConB function for a given constraint map as
--  defined in the Subtyping section of the specification.
conB :: AnyTVar -> TPolarity -> ConstraintMap -> Set ShallowType
conB = genConB boundToType
  where
    boundToType :: forall a. (TVar a, VarBound a) -> Maybe ShallowType
    boundToType x = case x of
      (QTVar{}, QBType t) -> Just t
      (UTVar{}, UBType t) -> Just t
      _ -> Nothing

-- |A routine which computes the ConB function for a given constraint map as
--  defined in the Subtyping section of the specification.
qualB :: AnyTVar -> TPolarity -> ConstraintMap -> Set (Set TQual)
qualB = genConB boundToQualSet
  where
    boundToQualSet :: forall a. (TVar a, VarBound a) -> Maybe (Set TQual)
    boundToQualSet x = case x of
      (QTVar{}, QBQualSet qs) -> Just qs
      _ -> Nothing

-- |A routine which generalizes over ConB and QualB in the Subtyping section
--  of the specification.
genConB :: forall b. (Ord b) => (forall a. (TVar a, VarBound a) -> Maybe b)
        -> AnyTVar -> TPolarity -> ConstraintMap -> Set b
genConB extract sa pol cm = Set.fromList $ do
  var <- Set.toList $ varB sa pol cm
  case var of
    SomeQVar qa -> conForVar qa
    SomeUVar a -> conForVar a
  where
    conForVar :: TVar a -> [b]
    conForVar v =
      mapMaybe (extract . (v,)) $ Set.toList $ cmBoundsOf v pol cm

-- |A common type structure describing the Bound function in the Subtyping
--  section of the specification.
type TypeBoundFn m a = Set a -> TPolarity
                             -> m (Maybe (a, ConstraintMap))

-- |A routine to compute the type part of the Bound function in the Subtyping
--  section of the specification.  When Bound is undefined, this function
--  evaluates to @Nothing@.
typeBound :: forall m. (FreshVarI m) => TypeBoundFn m ShallowType
typeBound tsSet pol =
  case Set.toList tsSet of
    [] -> case pol of
            Positive -> return $ Just (STop, mempty)
            Negative -> return $ Just (SBottom, mempty)
    [t] -> return $ Just (t, mempty)
    (mapM matchFunc -> Just bindings) -> do
      a0 <- freshUVar $ TVarBoundGeneralizationOrigin (map fst bindings)
                (flipBound pol)
      a0' <- freshUVar $ TVarBoundGeneralizationOrigin (map snd bindings) pol
      let cm' = mconcat $
                  map (cmSing a0 (flipBound pol) . UBUVar . fst) bindings ++
                  map (cmSing a0' pol . UBUVar . snd) bindings
      return $ Just (SFunction a0 a0', cm')
    (mapM matchTrig -> Just bindings) -> do
      a0 <- freshUVar $ TVarBoundGeneralizationOrigin bindings $ flipBound pol
      let cm' = mconcat $ map (cmSing a0 (flipBound pol) . UBUVar) bindings
      return $ Just (STrigger a0, cm')
    (mapM matchOption -> Just bindings) -> do
      qa0 <- freshQVar $ TVarBoundGeneralizationOrigin bindings pol
      let cm' = mconcat $ map (cmSing qa0 pol . QBQVar) bindings
      return $ Just (SOption qa0, cm')
    (mapM matchIndirection -> Just bindings) -> do
      qa0 <- freshQVar $ TVarBoundGeneralizationOrigin bindings pol
      let cm' = mconcat $ map (cmSing qa0 pol . QBQVar) bindings
      return $ Just (SIndirection qa0, cm')
    (sameLength <=< mapM matchTuple -> Just (bindings,_)) -> do
      (vars,cms) <- unzip <$> mapM boundForAlignedPosition (transpose bindings)
      return $ Just (STuple vars, mconcat cms)
    (mapM matchRecord -> Just bindings) -> do
      let mapMerger = case pol of
                        Positive -> Map.intersectionWith
                        Negative -> Map.unionWith
      let m = foldl' (mapMerger (++)) Map.empty $ map (fmap (:[])) bindings
      m' <- Trav.mapM boundForAlignedPosition m
      let (m'',cm') = runWriter $ Trav.mapM (\(x,y) -> tell y >> return x ) m'
      return $ Just (SRecord m'', cm')
    _:_:_ ->
      -- By this point, we have a two element list which is not comprised
      -- completely of any of the above.  We cannot proceed; there is no perfect
      -- bound.  (This isn't quite true; we could use top or bottom.  But the
      -- spec doesn't say this.)
      return Nothing
  where
    matchFunc :: ShallowType -> Maybe (UVar, UVar)
    matchFunc t = case t of
      SFunction a a' -> Just (a,a')
      _ -> Nothing
    matchTrig :: ShallowType -> Maybe UVar
    matchTrig t = case t of
      STrigger a -> Just a
      _ -> Nothing
    matchOption :: ShallowType -> Maybe QVar
    matchOption t = case t of
      SOption qa -> Just qa
      _ -> Nothing
    matchIndirection :: ShallowType -> Maybe QVar
    matchIndirection t = case t of
      SIndirection qa -> Just qa
      _ -> Nothing
    matchTuple :: ShallowType -> Maybe [QVar]
    matchTuple t = case t of
      STuple qas -> Just qas
      _ -> Nothing
    sameLength :: [[a]] -> Maybe ([[a]], Int)
    sameLength lsts =
      case lsts of
        [] -> Just ([],0)
        [lst] -> Just (lsts, length lst)
        lst:lsts' -> do
          (_,n) <- sameLength lsts'
          if length lst == n then Just (lsts,n) else Nothing
    boundForAlignedPosition :: [QVar] -> m (QVar, ConstraintMap)
    boundForAlignedPosition vars = do
      qa <- freshQVar $ TVarBoundGeneralizationOrigin vars pol
      return (qa, mconcat $ map (cmSing qa pol . QBQVar) vars)
    matchRecord :: ShallowType ->  Maybe (Map Identifier QVar)
    matchRecord t = case t of
      SRecord m -> Just m
      _ -> Nothing

-- |A routine to compute the qualifier set part of the Bound function in spec
--  sec 6.1.  When Bound is undefined, this function evaluates to @Nothing@.
qualsBound :: forall m. (FreshVarI m) => TypeBoundFn m (Set TQual)
qualsBound qsSet pol =
  let qss = Set.toList qsSet in
  let newSet =
        if null qss
          then case pol of
                  Positive -> Set.empty
                  Negative -> allQuals
          else
            let f = case pol of
                      Positive -> Set.union
                      Negative -> Set.intersection
            in foldl1' f qss
  in
  return $ Just (newSet, mempty)

-- |A function to canonicalize constraint maps.
canonicalize :: forall m. (FreshVarI m) => ConstraintMap
                                        -> m (Maybe CanonicalConstraintMap)
canonicalize theCm =
  runMaybeT $ CanonicalConstraintMap <$>
    (foldM canonicalizeFor theCm $ Set.toList $ extractVariables theCm)
  where
    canonicalizeFor :: ConstraintMap -> AnyTVar -> MaybeT m ConstraintMap
    canonicalizeFor cm sa = do
      cm' <- canonicalizeForDir cm sa Positive
      canonicalizeForDir cm' sa Negative
    canonicalizeForDir :: ConstraintMap -> AnyTVar -> TPolarity
                       -> MaybeT m ConstraintMap
    canonicalizeForDir cm sa pol =
      -- The following generalizes the creation of K' to reduce duplicate code.
      let cm' = deleteConcreteBounds sa pol cm in
      case sa of
        SomeQVar qa -> do
          cm1 <- canonicalizedMapFor qa (conB . SomeQVar) typeBound QBType
          cm2 <- canonicalizedMapFor qa (qualB . SomeQVar) qualsBound QBQualSet
          return $ mconcat $ [cm', cm1, cm2]
        SomeUVar a -> do
          cm1 <- canonicalizedMapFor a (conB . SomeUVar) typeBound UBType
          return $ mconcat $ [cm', cm1]
      where
        -- This routine actually creates the additions to make K'.
        canonicalizedMapFor :: forall a b.
                               TVar a
                            -> (TVar a -> TPolarity -> ConstraintMap -> Set b)
                            -> TypeBoundFn m b
                            -> (b -> VarBound a)
                            -> MaybeT m ConstraintMap
        canonicalizedMapFor a findBounds boundFn boundCons = MaybeT $ do
          let bnds = findBounds a pol cm
          result <- boundFn bnds (flipBound pol)
          case result of
            Nothing ->
              return $ Nothing
            Just (el, cm') ->
              return $ Just $ cmSing a pol (boundCons el) `mappend` cm'
    deleteConcreteBounds :: AnyTVar -> TPolarity -> ConstraintMap
                         -> ConstraintMap
    deleteConcreteBounds sa pol (ConstraintMap m1 m2) =
      case sa of
        SomeQVar qa -> ConstraintMap m1 (scrub qa isQConcrete m2)
        SomeUVar a -> ConstraintMap (scrub a isUConcrete m1) m2
      where
        scrub :: TVar a -> (VarBound a -> Bool)
              -> Map (TVar a, TPolarity) (Set (VarBound a))
              -> Map (TVar a, TPolarity) (Set (VarBound a))
        scrub sa' isConc m =
          let mbnds = Map.lookup (sa',pol) m in
          case mbnds of
            Nothing -> m
            Just bnds ->
              Map.insert (sa',pol) (Set.filter isConc bnds) m

-- Some convenient Template Haskell -------------------------------------------
$(  
  concat <$> mapM (defineCatInstance [t|Set AnyTVar|] ''ExtractVariables)
                [ ''ConstraintMap
                ]
 )
 
instance Reduce ExtractVariables (Map (QVar, TPolarity)
            (Set QVarBound)) (Set AnyTVar) where
  reduce ExtractVariables m = reduce ExtractVariables $ Map.toList m 

instance Reduce ExtractVariables (Map (UVar, TPolarity)
            (Set UVarBound)) (Set AnyTVar) where
  reduce ExtractVariables m = reduce ExtractVariables $ Map.toList m 

instance Reduce ExtractVariables QVarBound (Set AnyTVar) where
  reduce ExtractVariables bnd = case bnd of
    QBQVar qa -> extractVariables qa
    QBUVar a -> extractVariables a
    QBType t -> extractVariables t
    QBQualSet qs -> extractVariables qs

instance Reduce ExtractVariables UVarBound (Set AnyTVar) where
  reduce ExtractVariables bnd = case bnd of
    UBQVar qa -> extractVariables qa
    UBUVar a -> extractVariables a
    UBType t -> extractVariables t
  
