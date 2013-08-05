{-# LANGUAGE TupleSections, ScopedTypeVariables #-}
{-|
  This module contains functions for annotation types.
-}
module Language.K3.TypeSystem.Annotations
( instantiateAnnotation
, concatAnnTypes
, concatAnnBodies
, AnnotationConcatenationError(..)
, instantiateCollection
, CollectionInstantiationError(..)
, isAnnotationSubtypeOf
) where

import Control.Applicative
import Control.Arrow
import Control.Error.Util
import Control.Monad
import Control.Monad.Trans.Either
import Control.Monad.Trans.Maybe
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Common
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Subtyping
import Language.K3.TypeSystem.Utils

-- |Instantiates an annotation type.  If bindings appear in the parameters
--  which are not open type variables in the annotation type, they are ignored.
--  The resulting annotation type may still have open variables.
instantiateAnnotation :: TParamEnv -> AnnType -> AnnType
instantiateAnnotation p (AnnType p' b cs) =
  let substitutions = Map.elems $ Map.intersectionWith (,) p' p in
  let (b',cs') =
        replaceVariables Map.empty (Map.fromList substitutions) (b,cs) in
  AnnType (Map.difference p' p) b' cs'

-- |Defines concatenation of annotation types.
concatAnnType :: AnnType -> AnnType
              -> Either AnnotationConcatenationError AnnType
concatAnnType (AnnType p1 b1 cs1) ann2@(AnnType p2 _ _) = do
    let (AnnType p2' b2' cs2') = instantiateAnnotation p1 ann2
    unless (Map.null p2') $ Left $
      IncompatibleTypeParameters p1 p2
    (b3,cs3) <- concatAnnBody b1 b2'
    return $ AnnType p1 b3 $ csUnions [cs1,cs2',cs3]
    
-- |Defines concatenation over numerous annotation types.
concatAnnTypes :: [AnnType] -> Either AnnotationConcatenationError AnnType
concatAnnTypes = foldM concatAnnType emptyAnnotation

-- |Defines concatenation of annotation body types.
concatAnnBody :: AnnBodyType -> AnnBodyType
              -> Either AnnotationConcatenationError
                  (AnnBodyType, ConstraintSet)
concatAnnBody (AnnBodyType ms1 ms2) (AnnBodyType ms1' ms2') = do
  (ms1'',cs1) <- concatAnnMembers ms1 ms1'
  (ms2'',cs2) <- concatAnnMembers ms2 ms2'
  return (AnnBodyType ms1'' ms2'', csUnions [cs1, cs2])

-- |Defines concatenation over numerous annotation body types.
concatAnnBodies :: [AnnBodyType]
                -> Either AnnotationConcatenationError
                    (AnnBodyType, ConstraintSet)
concatAnnBodies bs =
  foldM f (emptyBody, csEmpty)  $ map (,csEmpty) bs
  where
    emptyBody = AnnBodyType [] []
    f (b1,cs1) (b2,cs2) = do
      (b3,cs3) <- concatAnnBody b1 b2
      return (b3, csUnions [cs1, cs2, cs3])
  
-- |Defines concatenation over (lists of) annotation member types.
concatAnnMembers :: [AnnMemType] -> [AnnMemType]
                 -> Either AnnotationConcatenationError
                      ([AnnMemType], ConstraintSet)
concatAnnMembers ms1 ms2 = do
  css <- mapM concatConstr [(m1,m2) | m1 <- ms1, m2 <- ms2]
  return (ms1 ++ ms2, csUnions css)
  where
    concatConstr :: (AnnMemType, AnnMemType)
                 -> Either AnnotationConcatenationError ConstraintSet
    concatConstr (AnnMemType i1 p1 qa1, AnnMemType i2 p2 qa2) =
      case (p1,p2) of
        _ | i1 /= i2 -> return csEmpty
        (Negative,Negative) -> return csEmpty
        (Positive,Negative) -> return $ csSing $ qa1 <: qa2
        (Negative,Positive) -> return $ csSing $ qa2 <: qa1
        (Positive,Positive) -> Left $ OverlappingPositiveMember i1

-- |A data type describing the errors which can occur in concatenation.
data AnnotationConcatenationError
  = OverlappingPositiveMember Identifier
      -- ^Produced when two annotation members attempt to define the same
      --  identifier in a positive context.
  | IncompatibleTypeParameters TParamEnv TParamEnv
      -- ^Produced when two annotation types are concatenated and one has a
      --  different set of open type variables than the other.
  deriving (Eq, Show)

-- |Defines depolarization of annotation members.  If depolarization is not
--  defined (e.g. because multiple annotations positively define the same
--  identifier), then an appropriate error is returned instead.
depolarize :: [AnnMemType]
           -> Either CollectionInstantiationError (ShallowType, ConstraintSet)
depolarize ms = do
  let ids = Set.toList $ Set.fromList $ map idOf ms -- dedup the list
  pairs <- mapM depolarizePart ids
  let (mt,cs) = (recordOf *** csUnions) $ unzip pairs
  case mt of
    Right t -> return (t,cs)
    Left (RecordIdentifierOverlap is) ->
      Left $ MultipleProvisions $ Set.findMin is
    Left (NonRecordType t) ->
      error $ "depolarize received non-record type complaint for " ++
              show t ++ ", which should never happen"
  where
    idOf :: AnnMemType -> Identifier
    idOf (AnnMemType i _ _) = i
    depolarizePart :: Identifier
                   -> Either CollectionInstantiationError
                        (ShallowType, ConstraintSet)
    depolarizePart i =
      let (posqas,negqas) = mconcat $ map extract ms in
      case (Set.size posqas, Set.null negqas) of
        (0,True) -> return (STop, csEmpty)
        (0,False) -> return ( SRecord $ Map.singleton i $ Set.findMin negqas
                            , csFromList [ qa1 <: qa2
                                         | qa1 <- Set.toList negqas
                                         , qa2 <- Set.toList negqas ])
        (1,_) ->
          let posqa = Set.findMin posqas in
          return ( SRecord $ Map.singleton i posqa
                 , csFromList [ posqa <: qa
                              | qa <- Set.toList negqas ])
        (_,_) -> Left $ MultipleProvisions i
      where
        extract :: AnnMemType -> (Set QVar, Set QVar)
        extract (AnnMemType i' p qa) =
          case p of
            _ | i /= i' -> (Set.empty,Set.empty)
            Positive -> (Set.singleton qa,Set.empty)
            Negative -> (Set.empty,Set.singleton qa)

-- |Defines instantiation of collection types.  If the instantiation is not
--  defined (e.g. because depolarization fails), then the clashing identifiers
--  are provided instead.
instantiateCollection :: (FreshVarI m)
                      => AnnType -> UVar
                      -> m (Either CollectionInstantiationError
                              (UVar, ConstraintSet))
instantiateCollection ann@(AnnType p (AnnBodyType ms1 ms2) cs') a_c =
  runEitherT $ do
    -- TODO: consider richer error reporting
    a_s :: UVar <- freshVar $ TVarCollectionInstantiationOrigin ann a_c
    (a_c', a_f', a_s') <- liftEither readParameters
    (t_s, cs_s) <- liftEither $ depolarize ms1
    (t_f, cs_f) <- liftEither $ depolarize ms2
    let cs'' = csFromList [ t_s <: a_s
                          , t_f <: a_f'
                          , a_f' <: t_f
                          , t_s <: a_s'
                          , a_s' <: t_s
                          ]
    let cs''' = replaceVariables Map.empty (Map.singleton a_c' a_c) $
                  csUnions [cs',cs_s,cs_f,cs'']
    return (a_s, cs''')
  where
    liftEither :: (Monad m) => Either a b -> EitherT a m b
    liftEither = EitherT . return
    readParameters :: Either CollectionInstantiationError (UVar, UVar, UVar)
    readParameters = do
      a_c' <- readParameter TEnvIdContent
      a_f' <- readParameter TEnvIdFinal
      a_s' <- readParameter TEnvIdSelf
      return (a_c', a_f', a_s')
      where
        readParameter envId =
          note (MissingAnnotationTypeParameter envId) $ Map.lookup envId p

data CollectionInstantiationError
  = MissingAnnotationTypeParameter TEnvId
      -- ^Indicates that a required annotation parameter (e.g. content) is
      --  missing from the parameter environment.
  | MultipleProvisions Identifier
      -- ^Indicates that the provided identifier is provided by multiple
      --  implementations.
  deriving (Eq, Show)

-- |Defines annotation subtyping.
isAnnotationSubtypeOf :: forall m. (FreshVarI m) => AnnType -> AnnType -> m Bool
isAnnotationSubtypeOf ann1 ann2 = do
  fun1 <- annToFun ann1
  fun2 <- annToFun ann2
  isSubtypeOf fun1 fun2
  where
    annToFun :: AnnType -> m QuantType
    annToFun ann@(AnnType p (AnnBodyType ms1 ms2) cs) = do
      let origin = TVarAnnotationToFunctionOrigin ann
      let mkPosNegRecs ms = ( recordTypeFromMembers Negative ms
                            , recordTypeFromMembers Positive ms )
      let (negTyps,posTyps) = unzip $ map mkPosNegRecs [ms1,ms2]
      let mkFresh n = mapM (const $ freshVar origin) [1::Int .. n]
      qa :: QVar <- freshVar origin
      a0 <- freshVar origin
      a0' <- freshVar origin
      negVars <- mkFresh $ length negTyps
      posVars <- mkFresh $ length posTyps
      let cs' = csUnions [ cs
                         , csFromList [ SFunction a0 a0' <: qa
                                      , a0 <: STuple negVars
                                      , STuple posVars <: a0'
                                      ]
                         , csFromList $ zipWith constraint negVars negTyps
                         , csFromList $ zipWith constraint posTyps posVars
                         ]
      let sas = Set.unions [ Set.singleton $ someVar qa
                           , Set.fromList $ map someVar negVars
                           , Set.fromList $ map someVar posVars
                           , Set.fromList $ map someVar $ Map.elems p ]
      return $ QuantType sas qa cs'
      where
        recordTypeFromMembers :: TPolarity -> [AnnMemType] -> ShallowType
        recordTypeFromMembers pol ms =
          SRecord $ Map.unions $ map memberToRecordEntry ms
          where
            memberToRecordEntry :: AnnMemType -> Map Identifier QVar
            memberToRecordEntry (AnnMemType i pol' qa) =
              if pol == pol' then Map.singleton i qa else Map.empty
