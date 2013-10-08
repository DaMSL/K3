{-# LANGUAGE TupleSections, ScopedTypeVariables, FlexibleContexts, ConstraintKinds, DataKinds, TemplateHaskell #-}
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
import Control.Monad
import Data.List
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Monoid
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Common
import Language.K3.Utils.TemplateHaskell.Reduce
import Language.K3.Utils.TemplateHaskell.Transform
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Annotations.Error
import Language.K3.TypeSystem.Annotations.Within
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

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
                 , WithinAlignable e, Ord e, Pretty c
                 , Transform ReplaceVariables c)
              => AnnType c -> AnnType c
              -> Either AnnotationConcatenationError (AnnType c)
concatAnnType (AnnType p1 b1 cs1) ann2@(AnnType p2 _ _) = do
    let (AnnType p2' b2' cs2') = instantiateAnnotation p1 ann2
    unless (Map.null p2') $ Left $
      IncompatibleTypeParameters p1 p2
    b3 <- concatAnnBody b1 b2'
    return $ AnnType p1 b3 $ cs1 `CSL.union` cs2'
    
-- |Defines concatenation over numerous annotation types.
concatAnnTypes :: ( CSL.ConstraintSetLike e c
                  , CSL.ConstraintSetLikePromotable ConstraintSet c
                  , Transform ReplaceVariables c, FreshVarI m, Pretty c
                  , WithinAlignable e, Ord e)
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
concatAnnBody :: forall c el.
                 ( CSL.ConstraintSetLike el c
                 , CSL.ConstraintSetLikePromotable ConstraintSet c
                 , WithinAlignable el, Ord el, Pretty c)
              => AnnBodyType c -> AnnBodyType c
              -> Either AnnotationConcatenationError (AnnBodyType c)
concatAnnBody (AnnBodyType ms1 ms2) (AnnBodyType ms1' ms2') =
  AnnBodyType <$> concatAnnMembers ms1 ms1' <*> concatAnnMembers ms2 ms2'

-- |Defines concatenation over numerous annotation body types.
concatAnnBodies :: forall c el.
                   ( CSL.ConstraintSetLike el c
                   , CSL.ConstraintSetLikePromotable ConstraintSet c
                   , WithinAlignable el, Ord el, Pretty c)
                => [AnnBodyType c]
                -> Either AnnotationConcatenationError (AnnBodyType c)
concatAnnBodies = foldM concatAnnBody $ AnnBodyType [] []
  
-- |Defines concatenation over (lists of) annotation member types.
concatAnnMembers :: forall c el.
                    ( CSL.ConstraintSetLike el c
                    , CSL.ConstraintSetLikePromotable ConstraintSet c
                    , WithinAlignable el, Ord el, Pretty c)
                 => [AnnMemType c] -> [AnnMemType c]
                 -> Either AnnotationConcatenationError [AnnMemType c]
concatAnnMembers ms1 ms2 =
  let memsById =
        foldl (\acc mem -> Map.insertWith (++) (idOf mem) [mem] acc) Map.empty $
            ms1 ++ ms2 in
  mapM concatConstr $ Map.elems memsById
  where
    polOf :: AnnMemType c -> TPolarity
    polOf (AnnMemType _ pol _ _ _) = pol
    idOf :: AnnMemType c -> Identifier
    idOf (AnnMemType i _ _ _ _) = i
    -- |Implements the concatConstr function from the specification.  This
    --  function must never be passed an empty list; @concatAnnMembers@ ensures
    --  that this never happens.  This function assumes that all member types
    --  have the same identifier; this property is not checked.
    concatConstr :: [AnnMemType c]
                 -> Either AnnotationConcatenationError (AnnMemType c)
    concatConstr mems = do
      _debug $ boxToString $
        ["Calculating concatenation of: [ "] %+
          (vconcats $ map prettyLines mems) +% ["]"]
      let arities = nub $ map (\(AnnMemType _ _ ar _ _) -> ar) mems
      let positives = filter ((== Positive) . polOf) mems
      case (length positives, arities) of
        (_,_:_:_) ->
          Left $ DifferentMorphicArities $ idOf $ head mems
        (_,[]) ->
          error "concatConstr provided empty list of members!"
        (0,[MonoArity]) ->
          let (AnnMemType i1 _ _ qa1 cs1) = head mems in
          Right $ monoRepresentative i1 qa1 cs1 Negative $ tail mems
        (1,[MonoArity]) ->
          let (AnnMemType i _ _ qa' cs') = head positives in
          Right $ monoRepresentative i qa' cs' Positive mems
        (0,[PolyArity]) -> do
          let (AnnMemType _ _ _ qa1 cs1) = head mems
          mconcat <$> mapM (polyCheckEquiv (qa1,cs1) . typeOfMem) (tail mems)
          Right $ head mems
        (1,[PolyArity]) -> do
          let (AnnMemType _ _ _ qa' cs') = head positives
          mconcat <$> mapM (polyCheckEquiv (qa',cs') . typeOfMem) mems
          Right $ head positives
        (_,[_]) ->
          Left $ OverlappingPositiveMember $ idOf $ head positives
      where
        typeOfMem :: AnnMemType c -> (QVar, c)
        typeOfMem (AnnMemType _ _ _ qa cs) = (qa,cs)
        memTypesToCs :: (QVar -> c) -> [(QVar,c)] -> c
        memTypesToCs f = CSL.unions . map (\(qa,cs) -> CSL.union cs $ f qa)
        monoRepresentative :: Identifier -> QVar -> c -> TPolarity
                           -> [AnnMemType c] -> AnnMemType c
        monoRepresentative i qa cs pol mems' =
          let cs' = memTypesToCs (CSL.promote . csSing . (qa <:)) $
                      map typeOfMem mems' in
          AnnMemType i pol MonoArity qa $ CSL.union cs cs'
        polyCheckEquiv :: (QVar,c) -> (QVar,c)
                       -> Either AnnotationConcatenationError ()
        polyCheckEquiv x y =
          if isWithin x y && isWithin y x then Right ()
            else Left $ PolymorphicArityMembersNotEquivalent $ idOf $ head mems
            
-- |Defines depolarization of annotation members.  If depolarization is not
--  defined (e.g. because multiple annotations positively define the same
--  identifier), then an appropriate error is returned instead.
depolarize :: forall c el.
              ( CSL.ConstraintSetLike el c
              , CSL.ConstraintSetLikePromotable ConstraintSet c
              , WithinAlignable el, Ord el, Pretty c)
           => [AnnMemType c] -> Either DepolarizationError (ShallowType, c)
depolarize ms = do
  ms' <- either (Left . DepolarizationConcatenationError) Right $
            concatAnnMembers ms []
  let (ts,css) = unzip $ map depolarizeSingle ms'
  case recordConcat ts of
    Left (RecordIdentifierOverlap is) ->
      Left $ MultipleProvisions $ Set.singleton $ Set.findMin is
    Left (RecordOpaqueOverlap os) -> Left $ MultipleOpaques os
    Left (NonRecordType _) ->
      error "Depolarization got non-record type from depolarizeSingle!"
    Right t -> return (t, CSL.unions css)
  where
    depolarizeSingle :: AnnMemType c -> (ShallowType, c)
    depolarizeSingle (AnnMemType i _ _ qa cs) =
      (SRecord (Map.singleton i qa) Set.empty, cs)

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
depolarizeOrError :: ( Monad m, TypeErrorI m, CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , WithinAlignable el, Ord el, Pretty c)
                  => UID -> [AnnMemType c] -> m (ShallowType, c)
depolarizeOrError u =
  either (typeError . AnnotationDepolarizationFailure u) return . depolarize
