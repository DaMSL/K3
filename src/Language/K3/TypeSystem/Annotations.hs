{-# LANGUAGE TupleSections, ScopedTypeVariables, FlexibleContexts, ConstraintKinds #-}
{-|
  This module contains functions for annotation types.
-}
module Language.K3.TypeSystem.Annotations
( freshenAnnotation
, instantiateAnnotation
, concatAnnTypes
, concatAnnBodies
, depolarize

, readAnnotationSpecialParameters
, depolarizeOrError
, module Language.K3.TypeSystem.Annotations.Error
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Common
import Language.K3.TemplateHaskell.Reduce
import Language.K3.TemplateHaskell.Transform
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Annotations.Error
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Utils

-- |Freshens an annotation type.  All variables appearing within the annotation
--  type are replaced by fresh equivalents.  The provided @UID@ should identify
--  the node which caused this freshening operation.
freshenAnnotation :: forall m e c.
                     ( CSL.ConstraintSetLike e c
                     , Transform ReplaceVariables c
                     , Reduce ExtractVariables c (Set AnyTVar)
                     , FreshVarI m)
                  => UID -> AnnType c -> m (AnnType c)
freshenAnnotation u ann = do
  -- PERF: Implement this as a monadic transform so we only walk the tree once.
  let vars = extractVariables ann
  (ma,mqa) <- mconcat <$> mapM freshen (Set.toList vars)
  return $ replaceVariables mqa ma ann
  where
    freshen :: AnyTVar -> m (Map UVar UVar, Map QVar QVar)
    freshen sa =
      case sa of
        SomeQVar qa ->
          (\x -> (Map.empty, Map.singleton qa x))
            <$> freshQVar (TVarAnnotationFreshenOrigin qa u)
        SomeUVar a ->
          (\x -> (Map.singleton a x, Map.empty))
            <$> freshUVar (TVarAnnotationFreshenOrigin a u)

-- |Instantiates an annotation type.  If bindings appear in the parameters
--  which are not open type variables in the annotation type, they are ignored.
--  The resulting annotation type may still have open variables.
instantiateAnnotation :: ( CSL.ConstraintSetLike e c
                         , Transform ReplaceVariables c)
                      => TParamEnv -> AnnType c -> AnnType c
instantiateAnnotation p (AnnType p' b cs) =
  let substitutions = Map.elems $ Map.intersectionWith (,) p' p in
  let (b',cs') =
        replaceVariables Map.empty (Map.fromList substitutions) (b,cs) in
  AnnType (Map.difference p' p) b' cs'

-- |Defines concatenation of annotation types.
concatAnnType :: ( CSL.ConstraintSetLike e c
                 , CSL.ConstraintSetLikePromotable ConstraintSet c
                 , Transform ReplaceVariables c)
              => AnnType c -> AnnType c
              -> Either AnnotationConcatenationError (AnnType c)
concatAnnType (AnnType p1 b1 cs1) ann2@(AnnType p2 _ _) = do
    let (AnnType p2' b2' cs2') = instantiateAnnotation p1 ann2
    unless (Map.null p2') $ Left $
      IncompatibleTypeParameters p1 p2
    (b3,cs3) <- concatAnnBody b1 b2'
    return $ AnnType p1 b3 $ CSL.unions [cs1,cs2',CSL.promote cs3]
    
-- |Defines concatenation over numerous annotation types.
concatAnnTypes :: ( CSL.ConstraintSetLike e c
                  , CSL.ConstraintSetLikePromotable ConstraintSet c
                  , Transform ReplaceVariables c, FreshVarI m)
               => [AnnType c]
               -> m (Either AnnotationConcatenationError (AnnType c))
concatAnnTypes typs = do
  emptyAnn <- emptyAnnType
  return $ foldM concatAnnType emptyAnn typs

-- |Defines a fresh empty annotation type.
emptyAnnType :: ( CSL.ConstraintSetLike e c
                , CSL.ConstraintSetLikePromotable ConstraintSet c
                , FreshVarI m)
             => m (AnnType c)
emptyAnnType = do
  a_C <- freshUVar TVarEmptyAnnotationOrigin
  a_F <- freshUVar TVarEmptyAnnotationOrigin
  a_S <- freshUVar TVarEmptyAnnotationOrigin
  return $ AnnType (Map.fromList
            [ (TEnvIdContent, a_C)
            , (TEnvIdFinal, a_F)
            , (TEnvIdSelf, a_S) ]) (AnnBodyType [] []) CSL.empty

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

-- |Defines depolarization of annotation members.  If depolarization is not
--  defined (e.g. because multiple annotations positively define the same
--  identifier), then an appropriate error is returned instead.
depolarize :: [AnnMemType]
           -> Either DepolarizationError (ShallowType, ConstraintSet)
depolarize ms = do
  let ids = Set.toList $ Set.fromList $ map idOf ms -- dedup the list
  pairs <- mapM depolarizePart ids
  let (mt,cs) = (recordConcat *** csUnions) $ unzip pairs
  case mt of
    Right t -> return (t,cs)
    Left (RecordIdentifierOverlap is) ->
      Left $ MultipleProvisions is
    Left (RecordOpaqueOverlap oas) ->
      Left $ MultipleOpaques oas
    Left (NonRecordType t) ->
      error $ "depolarize received non-record type complaint for " ++
              show t ++ ", which should never happen"
  where
    idOf :: AnnMemType -> Identifier
    idOf (AnnMemType i _ _) = i
    depolarizePart :: Identifier
                   -> Either DepolarizationError (ShallowType, ConstraintSet)
    depolarizePart i =
      let (posqas,negqas) = mconcat $ map extract ms in
      case (Set.size posqas, Set.null negqas) of
        (0,True) -> return (STop, csEmpty)
        (0,False) -> return ( SRecord
                                (Map.singleton i $ Set.findMin negqas)
                                Set.empty
                            , csFromList [ qa1 <: qa2
                                         | qa1 <- Set.toList negqas
                                         , qa2 <- Set.toList negqas ])
        (1,_) ->
          let posqa = Set.findMin posqas in
          return ( SRecord (Map.singleton i posqa) Set.empty
                 , csFromList [ posqa <: qa
                              | qa <- Set.toList negqas ])
        (_,_) -> Left $ MultipleProvisions $ Set.singleton i
      where
        extract :: AnnMemType -> (Set QVar, Set QVar)
        extract (AnnMemType i' p qa) =
          case p of
            _ | i /= i' -> (Set.empty,Set.empty)
            Positive -> (Set.singleton qa,Set.empty)
            Negative -> (Set.empty,Set.singleton qa)

-- |A convenience function to get the special type parameters from an annotation
--  parameter environment.  If this cannot be done, a type error is raised.
readAnnotationSpecialParameters :: (Monad m, TypeErrorI m)
                                => TParamEnv -> m (UVar, UVar, UVar)
readAnnotationSpecialParameters p = do
  a_c' <- readParameter TEnvIdContent
  a_f' <- readParameter TEnvIdFinal
  a_s' <- readParameter TEnvIdSelf
  return (a_c', a_f', a_s')
  where
    readParameter envId =
      maybe (typeError $ MissingAnnotationTypeParameter envId) return $
        Map.lookup envId p

-- |A convenience function to raise a type error if depolarization fails.
depolarizeOrError :: (Monad m, TypeErrorI m)
                  => UID -> [AnnMemType] -> m (ShallowType, ConstraintSet)
depolarizeOrError u =
  either (typeError . AnnotationDepolarizationFailure u) return . depolarize
