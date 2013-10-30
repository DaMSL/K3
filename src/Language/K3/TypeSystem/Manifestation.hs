{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TemplateHaskell, FlexibleContexts #-}

{-|
  This module provides a routine which manifests a type given a constraint set
  and a variable appearing within that constraint set.  The manifested form can
  be either the upper bound or the lower bound of the given type variable.
-}

module Language.K3.TypeSystem.Manifestation
( manifestType
--, Manifestable
--, upperBound
--, lowerBound
) where

import Control.Applicative
import Control.Monad
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
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Simplification
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- |A general top-level function for manifesting a type from a constraint set
--  and a type variable.  This function preserves work in a partial application
--  closure; a caller providing just a constraint set can use the resulting
--  function numerous times to compute different bounds without repeating work.
manifestType :: ConstraintSet -> BoundType -> AnyTVar -> K3 Type
manifestType cs =
  {-
    This routine proceeds as follows:
      1. Simplify the constraint set.  This will prevent a lot of redundant
         work.  It involves unifying equivalent variables, eliminating
         unnecessary constraints , and so forth.  The resulting set is not
         equivalent, but it is suitable for manifestation's use.
      2. Eliminate branching from the constraint set.  This is achieved by
         introducing fresh variables for each bounding type and then merging
         the shallow bounds recursively.  This process is technically lossy,
         but the kinds of types available in K3 as of this writing (2013-10-29)
         are unaffected by it.
      3. Simplify again.  The branching elimination often introduces structures
         that turn out to be equivalent.
      4. Construct from the resulting non-branching constraint set a dictionary
         from each variable to its corresponding bounds.
      5. Using this dictionary, expand the provided type.
    In order to prevent duplicate work, this function performs all but the last
    step once a constraint set is applied; this allows the resulting function to
    hold the bound dictionary in its closure.
  -}
  undefined -- TODO

{-




-- |A general top-level function for manifesting a type from a constraint set
--  and a manifestable entity.
_manifestType :: ( Manifestable a, Pretty a, VariableExtractable a)
             => BoundType -> ConstraintSet -> a -> K3 Type
_manifestType bt cs x =
  let name = getBoundTypeName bt in
  _debugI (boxToString
    (["Manifesting " ++ name ++ " bound type for constrained type "] %+
      prettyLines x %+ ["\\"] %+ prettyLines simpCs)) $
  let t = runManifestM bt simpCs $
            declareOpaques $ manifestTypeFrom $ Set.singleton x in
  _debugI (boxToString
    (["Manifested " ++ name ++ " bound type for constrained type "] %+
      prettyLines x %+ ["\\"] %+ prettyLines simpCs %$
      indent 2 (["Result: "] %+ prettyLines t)))
  t
  where
    -- |Simplified constraints.  This must be done on a per-manifestation-target
    --  basis for variable preservation reasons.
    simpCs :: ConstraintSet
    simpCs =
      let config = SimplificationConfig { preserveVars = extractVariables x } in
      let cs1 = runSimplifyM config $
                  simplifyByConstraintEquivalenceUnification cs in
      let cs2 = keepOnlyStrictlyNecessary cs1 in
      let cs3 = runSimplifyM config $ simplifyByGarbageCollection cs2 in
      leastFixedPoint (simplifier config) cs3
      where
        simplifier :: SimplificationConfig -> ConstraintSet -> ConstraintSet
        simplifier config =
          runSimplifyM config .
            (simplifyByBoundEquivalenceUnification >=>
             simplifyByStructuralEquivalenceUnification)
        -- |Eliminates all constraints that do not provide immediate bounding
        --  information.  This only makes sense for a closed constraint set and the
        --  resulting set should never be extended or subjected to closure.
        keepOnlyStrictlyNecessary :: ConstraintSet -> ConstraintSet
        keepOnlyStrictlyNecessary =
          csFromList . filter strictlyNecessary . csToList
          where
            strictlyNecessary :: Constraint -> Bool
            strictlyNecessary c = case c of
              IntermediateConstraint (CLeft _) (CLeft _) -> False
              IntermediateConstraint (CLeft _) (CRight _) -> True
              IntermediateConstraint (CRight _) (CLeft _) -> True
              IntermediateConstraint (CRight _) (CRight _) -> False
              QualifiedLowerConstraint (CLeft _) _ -> True
              QualifiedLowerConstraint (CRight _) _ -> False
              QualifiedUpperConstraint _ (CLeft _) -> True
              QualifiedUpperConstraint _ (CRight _) -> False
              QualifiedIntermediateConstraint (CLeft _) (CLeft _) -> False
              QualifiedIntermediateConstraint (CLeft _) (CRight _) -> True
              QualifiedIntermediateConstraint (CRight _) (CLeft _) -> True
              QualifiedIntermediateConstraint (CRight _) (CRight _) -> False
              MonomorphicQualifiedUpperConstraint _ _ -> False
              PolyinstantiationLineageConstraint _ _ -> False
              OpaqueBoundConstraint _ _ _ -> True

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
        DNumber -> return TC.number
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
-}
