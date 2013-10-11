{-# LANGUAGE GeneralizedNewtypeDeriving, TypeSynonymInstances, FlexibleInstances #-}

{-|
  This module provides a routine which manifests a type given a constraint set
  and a variable appearing within that constraint set.  The manifested form can
  be either the upper bound or the lower bound of the given type variable.
-}

module Language.K3.TypeSystem.Manifestation
( manifestType
, upperBound
, lowerBound
) where

import Control.Applicative
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Annotation
import qualified Language.K3.Core.Constructor.Type as TC
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.Data
import Language.K3.TypeSystem.Manifestation.Monad

-- |A general top-level function for manifesting a type from a constraint set
--  and a manifestable entity.
manifestType :: (Manifestable a) => BoundType -> ConstraintSet -> a -> K3 Type
manifestType bt cs x = runManifestM bt cs $ manifestTypeFrom $ Set.singleton x

-- |A typeclass for entities from which types can be manifested.
class Manifestable a where
  -- |Given a list of entities to be combined, produces a manifested type.  This
  --  list will be combined in a way dictated by the bound type.
  manifestTypeFrom :: Set a -> ManifestM (K3 Type)

instance Manifestable UVar where
  manifestTypeFrom = doVariableManifestation getConcreteUVarBounds

instance Manifestable QVar where
  manifestTypeFrom qas = do
    typ <- doVariableManifestation getConcreteQVarBounds qas
    qualQuery <- getConcreteQVarQualifiers <$> askBoundType
    qss <- concat <$> mapM (envQuery . qualQuery) (Set.toList qas)
    qualMergeOp <- getQualifierOperation <$> askBoundType
    let qs = qualMergeOp qss
    return $ foldr addQual typ $ Set.toList qs
    where
      addQual :: TQual -> K3 Type -> K3 Type
      addQual q typ = case q of
        TMut -> typ @+ TMutable
        TImmut -> typ @+ TImmutable

doVariableManifestation :: (BoundType ->
                              TVar q -> ConstraintSetQuery ShallowType)
                        -> Set (TVar q)
                        -> ManifestM (K3 Type)
doVariableManifestation queryGetter vars = do
  -- TODO: use the monad's state to address mu-recursion
  query <- queryGetter <$> askBoundType
  bounds <- mapM (envQuery . query) $ Set.toList vars
  manifestTypeFrom $ Set.fromList $ map shallowToDelayed $ concat bounds

instance Manifestable DelayedType where
  manifestTypeFrom ts = do
    mergeOp <- getDelayedOperation <$> askBoundType
    let t = mergeOp $ Set.toList ts
    case t of
      DFunction as as' -> do
        typ <- dualizeBoundType $ manifestTypeFrom as
        typ' <- manifestTypeFrom as'
        return $ TC.function typ typ'
      DTrigger as -> TC.trigger <$> dualizeBoundType (manifestTypeFrom as)
      DBool -> return TC.bool
      DInt -> return TC.int
      DReal -> return TC.real
      DString -> return TC.string
      DOption qas -> TC.option <$> manifestTypeFrom qas
      DIndirection qas -> TC.indirection <$> manifestTypeFrom qas
      DTuple qass -> TC.tuple <$> mapM manifestTypeFrom qass
      DRecord m oas -> do
        let (is,qass) = unzip $ Map.toList m
        typs <- mapM manifestTypeFrom qass
        if Set.null oas
          then
            return $ TC.record $ zip is typs
          else
            undefined -- TODO
      DTop -> return TC.top
      DBottom -> return TC.bottom
      DOpaque oas ->
        undefined -- TODO

