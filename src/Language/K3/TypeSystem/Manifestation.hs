{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

{-|
  This module provides a routine which manifests a type given a constraint set
  and a variable appearing within that constraint set.  The manifested form can
  be either the upper bound or the lower bound of the given type variable.
-}

module Language.K3.TypeSystem.Manifestation
( manifestType
, Manifestable
, upperBound
, lowerBound
) where

import Control.Applicative
import Data.List
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Type as TC
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.Data
import Language.K3.TypeSystem.Manifestation.Monad

-- |A general top-level function for manifesting a type from a constraint set
--  and a manifestable entity.
manifestType :: (Manifestable a) => BoundType -> ConstraintSet -> a -> K3 Type
manifestType bt cs x =
  runManifestM bt cs $ declareOpaques $ manifestTypeFrom $ Set.singleton x
  
-- |A helper routine which will create declarations for opaque variables as
--  necessary.
declareOpaques :: ManifestM (K3 Type) -> ManifestM (K3 Type)
declareOpaques xM = do
  x <- xM
  namedOpaques <- Map.toList <$> grabNamedOpaques
  cs <- askConstraints
  return $ if null namedOpaques
    then x
    else TC.forAll (map (bindOpaque cs) namedOpaques) x
  where
    bindOpaque :: ConstraintSet -> (OpaqueVar, Identifier) -> TypeVarDecl
    bindOpaque cs (oa,i) =
      let bounds = csQuery cs $ QueryOpaqueBounds oa in
      case length bounds of
        1 ->
          let (t_L,t_U) = head bounds in
          let typL = manifestFromTypeOrVar lowerBound t_L in
          let typU = manifestFromTypeOrVar upperBound t_U in
          TypeVarDecl i (Just typL) (Just typU)
        0 -> error $ "Missing opaque bound for " ++ show oa
        _ -> error $ "Multile opaque bounds for " ++ show oa
      where
        manifestFromTypeOrVar bt tov = case tov of
          CLeft x -> manifestType bt cs $ shallowToDelayed x
          CRight x -> manifestType bt cs x

-- |A typeclass for entities from which types can be manifested.
class Manifestable a where
  -- |Given a list of entities to be combined, produces a manifested type.  This
  --  list will be combined in a way dictated by the bound type.
  manifestTypeFrom :: Set a -> ManifestM (K3 Type)

instance Manifestable UVar where
  manifestTypeFrom as =
    considerMuType as $ doVariableManifestation getConcreteUVarBounds as

instance Manifestable QVar where
  manifestTypeFrom qas = considerMuType qas $ do
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

considerMuType :: Set (TVar q) -> ManifestM (K3 Type) -> ManifestM (K3 Type)
considerMuType vars computation = do
  let sas = Set.map someVar vars
  sig <- VariableSignature sas <$> getDelayedOperationTag <$> askBoundType
  tryDeclSig sig alreadyDefined (justDefined sig)
  where
    -- |Used when this point has already been witnessed before in the same
    --  branch of tree.
    alreadyDefined :: Identifier -> ManifestM (K3 Type)
    alreadyDefined i = return $ TC.declaredVar i
    -- |Used when this point is new, which is the most common case.
    justDefined :: VariableSignature -> ManifestM (K3 Type)
    justDefined sig = do
      (typ, usedName) <- catchSigUse sig computation
      return $ case usedName of
        Just name -> TC.mu name typ
        Nothing -> typ

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
          else do
            oids <- mapM nameOpaque $ Set.toList oas
            return $ TC.recordExtension (zip is typs) oids
      DTop -> return TC.top
      DBottom -> return TC.bottom
      DOpaque oas -> do
        is <- sort <$> mapM nameOpaque (Set.toList oas)
        TC.declaredVarOp is <$> getTyVarOp <$> askBoundType

