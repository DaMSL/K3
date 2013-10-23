{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TemplateHaskell #-}

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
import Data.Foldable (Foldable)
import qualified Data.Foldable as Foldable
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
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- |A general top-level function for manifesting a type from a constraint set
--  and a manifestable entity.
manifestType :: (Manifestable a, Pretty a)
             => BoundType -> ConstraintSet -> a -> K3 Type
manifestType bt cs x =
  let name = getBoundTypeName bt in
  _debugI (boxToString
    (["Manifesting " ++ name ++ " bound type for "] %+ prettyLines x %+
      ["\\{"] %+ prettyLines cs +% ["}"])) $
  let t = runManifestM bt cs $
            declareOpaques $ manifestTypeFrom $ Set.singleton x in
  _debugI (boxToString
    (["Manifested " ++ name ++ " bound type for "] %+ prettyLines x %+
      prettyLines cs %$ indent 2 (["Result: "] %+ prettyLines t)))
  t
  
-- |A helper routine which will create declarations for opaque variables as
--  necessary.
declareOpaques :: ManifestM (K3 Type) -> ManifestM (K3 Type)
declareOpaques xM = do
  x <- xM
  namedOpaques <- Map.toList <$> getNamedOpaques
  if null namedOpaques
    then return x
    else do
          ans <- (`TC.externallyBound` x) <$> mapM bindOpaque namedOpaques
          clearNamedOpaques
          return ans
  where
    bindOpaque :: (OpaqueVar, Identifier) -> ManifestM TypeVarDecl
    bindOpaque (oa,i) = do
      cs <- askConstraints
      let bounds = csQuery cs $ QueryOpaqueBounds oa
      case length bounds of
        1 -> do
          let (t_L,t_U) = head bounds
          typL <- manifestFromTypeOrVar lowerBound t_L
          typU <- manifestFromTypeOrVar upperBound t_U
          return $ TypeVarDecl i (Just typL) (Just typU)
        0 -> error $ "Missing opaque bound for " ++ show oa
        _ -> error $ "Multile opaque bounds for " ++ show oa
      where
        manifestFromTypeOrVar bt tov =
          case tov of
            CLeft x ->
              usingBoundType bt $ manifestTypeFrom $ Set.singleton $
                shallowToDelayed x  
            CRight x ->
              usingBoundType bt $ manifestTypeFrom $ Set.singleton x

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
  logManifestPrefix vars
  query <- queryGetter <$> askBoundType
  bounds <- mapM (envQuery . query) $ Set.toList vars
  t <- manifestTypeFrom $ Set.fromList $ map shallowToDelayed $ concat bounds
  logManifestSuffix vars t
  return t  

instance Manifestable DelayedType where
  manifestTypeFrom ts = do
    mergeOp <- getDelayedOperation <$> askBoundType
    logManifestPrefix ts
    let t = mergeOp $ Set.toList ts
    t' <-
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
        DAddress -> return TC.address
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
          if length is == 1
            then return $ TC.declaredVar $ head is
            else TC.declaredVarOp is <$> getTyVarOp <$> askBoundType
    logManifestSuffix ts t'
    return t'

logManifestPrefix :: (Pretty a, Foldable f) => f a -> ManifestM ()
logManifestPrefix xs = do
  pfxBox <- logTopMsg "Manifesting" xs
  _debug $ boxToString pfxBox

logManifestSuffix :: (Pretty a, Foldable f) => f a -> K3 Type -> ManifestM ()
logManifestSuffix xs t = do
  pfxBox <- logTopMsg "Manifested" xs
  _debug $ boxToString $ pfxBox %$
    indent 2 (["Result: "] %+ prettyLines t)

logTopMsg :: (Pretty a, Foldable f) => String -> f a -> ManifestM [String]
logTopMsg verb xs = do
  name <- getBoundTypeName <$> askBoundType
  return $ [verb ++ " " ++ name ++ " bound type for ["] %+
    intersperseBoxes [","] (map prettyLines $ Foldable.toList xs) +% ["]"]
