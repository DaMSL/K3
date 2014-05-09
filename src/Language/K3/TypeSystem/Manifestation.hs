{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, TemplateHaskell, FlexibleContexts, TupleSections #-}

{-|
  This module provides a routine which manifests a type given a constraint set
  and a variable appearing within that constraint set.  The manifested form can
  be either the upper bound or the lower bound of the given type variable.
-}

module Language.K3.TypeSystem.Manifestation
( manifestType
-- re-export
, BoundType(..)
) where

import Control.Applicative
import Control.Monad
import Control.Monad.Writer
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Type as TC
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.BranchSemiElimination
import Language.K3.TypeSystem.Manifestation.Data
import Language.K3.TypeSystem.Manifestation.Monad
import Language.K3.TypeSystem.Simplification
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

--------------------------------------------------------------------------------
-- * Manifestation routine

-- |A general top-level function for manifesting a type from a constraint set
--  and a type variable.  This function preserves work in a partial application
--  closure; a caller providing just a constraint set can use the resulting
--  function numerous times to compute different bounds without repeating work.
manifestType :: ConstraintSet -> BoundType -> AnyTVar -> K3 Type
manifestType csIn =
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
  _debugI (boxToString
      (["Preparing type manifestation for constraints:"] %$
        indent 2 (prettyLines csIn))) $
  -- Perform all of the simplification steps in the following monadic context
  let (simpCs,result) =
        runWriter $ do
          cs1 <- bracketLogM _debugI
                    (boxToString $
                      ["Performing pre-elimination simplification for:"] %$
                      indent 2 (prettyLines csIn))
                    (\cs -> boxToString $
                      ["Performed pre-elimination simplification:"] %$
                      indent 2 (prettyLines cs))
                    $ doSimplify firstSimplifier csIn
          let cs2 =
                bracketLog _debugI
                  (boxToString $
                    ["Performing branch elimination for:"] %$
                    indent 2 (prettyLines cs1))
                  (\cs -> boxToString $
                    ["Performed branch elimination:"] %$
                    indent 2 (prettyLines cs))
                  $ semiEliminateBranches cs1
          bracketLogM _debugI
            (boxToString $
              ["Performing post-elimination simplification for:"] %$
              indent 2 (prettyLines cs2))
            (\cs -> boxToString $
              ["Performed post-elimination simplification:"] %$
              indent 2 (prettyLines cs))
            $ doSimplify secondSimplifier cs2
  in
  -- Calculate the bounding dictionary and substitutions
  let dictionary = computeBoundDictionary simpCs in
  let substitutions = simplificationVarMap result in
  _debugI (boxToString
      (["Completed preparations for manifestation of constraints:"] %$
        indent 2 (
          ["Constraints:   "] %+ prettyLines simpCs %$
          ["Bounds:        "] %+ prettyLines dictionary %$
          ["Substitutions: "] %+ prettySubstitution "~=" substitutions))) $
  -- Return a function with these values embedded in context
  performLookup dictionary substitutions
  where
    performLookup :: BoundDictionary -> VariableSubstitution
                  -> BoundType -> AnyTVar -> K3 Type
    performLookup dict substs bt sa =
      case sa of
        SomeUVar a -> performLookup' a
        SomeQVar qa -> performLookup' qa
      where
        performLookup' var =
          runManifestM bt dict $ declareOpaques $ manifestTypeFrom $
              substitutionLookup var substs

    computeBoundDictionary :: ConstraintSet -> BoundDictionary
    computeBoundDictionary cs =
      let bd = mconcat $ map constraintToBounds $ csToList cs in
      -- Give top and bottom to any variable bounded only by opaques
      bd `mappend` extremeBounds bd
      where
        constraintToBounds :: Constraint -> BoundDictionary
        constraintToBounds c = case c of
          IntermediateConstraint (CLeft _) (CLeft _) -> mempty
          IntermediateConstraint (CLeft t) (CRight a) ->
            singletonUVarDict a LowerBound t
          IntermediateConstraint (CRight a) (CLeft t) ->
            singletonUVarDict a UpperBound t
          IntermediateConstraint (CRight _) (CRight _) -> mempty
          QualifiedLowerConstraint (CLeft t) qa ->
            singletonQVarDict qa LowerBound t
          QualifiedLowerConstraint (CRight _) _ -> mempty
          QualifiedUpperConstraint qa (CLeft t) ->
            singletonQVarDict qa UpperBound t
          QualifiedUpperConstraint _ (CRight _) -> mempty
          QualifiedIntermediateConstraint (CLeft _) (CLeft _) -> mempty
          QualifiedIntermediateConstraint (CLeft qs) (CRight qa) ->
            mempty { qvarQualDict = Map.singleton (qa, LowerBound) qs }
          QualifiedIntermediateConstraint (CRight qa) (CLeft qs) ->
            mempty { qvarQualDict = Map.singleton (qa, UpperBound) qs }
          QualifiedIntermediateConstraint (CRight _) (CRight _) -> mempty
          MonomorphicQualifiedUpperConstraint _ _ -> mempty
          PolyinstantiationLineageConstraint _ _ -> mempty
          OpaqueBoundConstraint oa lb ub ->
            mempty { ovarRangeDict = Map.fromList
                        [ ((oa,LowerBound),lb) , ((oa,UpperBound),ub) ] }
          where
            singletonUVarDict :: UVar -> BoundType -> ShallowType
                              -> BoundDictionary
            singletonUVarDict a bt t =
              case t of
                SOpaque oa ->
                  mempty { uvarOpaqueBoundDict =
                            Map.singleton (a, bt) $ Set.singleton oa }
                _ -> mempty { uvarBoundDict = Map.singleton (a, bt) t }
            singletonQVarDict :: QVar -> BoundType -> ShallowType
                              -> BoundDictionary
            singletonQVarDict qa bt t =
              case t of
                SOpaque oa ->
                  mempty { qvarOpaqueBoundDict =
                            Map.singleton (qa, bt) $ Set.singleton oa }
                _ -> mempty { qvarBoundDict = Map.singleton (qa, bt) t }
        extremeBounds :: BoundDictionary -> BoundDictionary
        extremeBounds bd =
          let as = map fst $
                      Map.keys (uvarBoundDict bd) ++
                      Map.keys (uvarOpaqueBoundDict bd) in
          let qas = map fst $
                      Map.keys (qvarBoundDict bd) ++
                      Map.keys (qvarOpaqueBoundDict bd) ++
                      Map.keys (qvarQualDict bd) in
          mempty { uvarBoundDict = Map.union
                      (Map.fromList $ map ((,STop) . (,UpperBound)) as)
                      (Map.fromList $ map ((,SBottom) . (,LowerBound)) as)
                 , qvarBoundDict = Map.union
                      (Map.fromList $ map ((,STop) . (,UpperBound)) qas)
                      (Map.fromList $ map ((,SBottom) . (,LowerBound)) qas)
                 }

    simplificationConfig :: SimplificationConfig
    simplificationConfig = SimplificationConfig { preserveVars = Set.empty }

    doSimplify :: (ConstraintSet -> SimplifyM ConstraintSet)
               -> ConstraintSet -> Writer SimplificationResult ConstraintSet
    doSimplify simplifier cs =
      runConfigSimplifyM simplificationConfig $ simplifier cs

    firstSimplifier :: ConstraintSet -> SimplifyM ConstraintSet
    firstSimplifier =
      simplifyByConstraintEquivalenceUnification >=>
      return . keepOnlyStrictlyNecessary >=>
      leastFixedPointM
        (leastFixedPointM simplifyByBoundEquivalenceUnification {- >=>
         simplifyByStructuralEquivalenceUnification -} )

    secondSimplifier :: ConstraintSet -> SimplifyM ConstraintSet
    secondSimplifier =
      leastFixedPointM
        (leastFixedPointM simplifyByBoundEquivalenceUnification {- >=>
         simplifyByStructuralEquivalenceUnification -} )

--------------------------------------------------------------------------------
-- * Manifestation type class instances

class Manifestable a where
  manifestTypeFrom :: a -> ManifestM (K3 Type)

instance Manifestable AnyTVar where
  manifestTypeFrom (SomeUVar a) = manifestTypeFrom a
  manifestTypeFrom (SomeQVar qa) = manifestTypeFrom qa

instance Manifestable TypeOrVar where
  manifestTypeFrom (CLeft t) = manifestTypeFrom (t, Set.empty :: Set OpaqueVar)
  manifestTypeFrom (CRight a) = manifestTypeFrom a

instance Manifestable UVar where
  manifestTypeFrom a = considerMuType a $ do
    logManifestPrefix a
    dict <- askDictionary
    bt <- askBoundType
    t' <- case ( Map.lookup (a, bt) $ uvarBoundDict dict
               , Map.findWithDefault Set.empty (a, bt) $
                  uvarOpaqueBoundDict dict ) of
      (Nothing, _) ->
        error $ "Manifestation of " ++ pretty bt ++ " of " ++ pretty a ++
                "failed: no binding in bound dictionary"
      (Just t, oas) -> manifestTypeFrom (t, oas)
    logManifestSuffix a t'
    return t'

instance Manifestable QVar where
  manifestTypeFrom qa = considerMuType qa $ do
    logManifestPrefix qa
    dict <- askDictionary
    bt <- askBoundType
    t' <- case ( Map.lookup (qa, bt) $ qvarBoundDict dict
               , Map.findWithDefault Set.empty (qa, bt) $
                    qvarOpaqueBoundDict dict
               , Map.lookup (qa, bt) $ qvarQualDict dict ) of
      (Nothing, _, _) ->
        error $ "Manifestation of " ++ getBoundTypeName bt ++ " of " ++
                  pretty qa ++ "failed: no binding in bound dictionary"
      (Just _, _, Nothing) ->
        error $ "Manifestation of " ++ getBoundTypeName bt ++ " of " ++
                  pretty qa ++ "failed: no binding in qualifier dictionary"
      (Just t, oas, Just qs) -> do
        typ <- manifestTypeFrom (t, oas)
        return $ foldr addQual typ $ Set.toList qs
    logManifestSuffix qa t'
    return t'
    where
      addQual :: TQual -> K3 Type -> K3 Type
      addQual q typ = case q of
        TMut -> typ @+ TMutable
        TImmut -> typ @+ TImmutable

instance Manifestable (ShallowType, Set OpaqueVar) where
  manifestTypeFrom (t, withOas) = attachOpaques $ do
    logManifestPrefix t
    t' <-
      case t of
        SFunction a a' -> do
          typ <- dualizeBoundType $ manifestTypeFrom a
          typ' <- manifestTypeFrom a'
          return $ TC.function typ typ'
        STrigger a -> TC.trigger <$> dualizeBoundType (manifestTypeFrom a)
        SBool -> return TC.bool
        SInt -> return TC.int
        SReal -> return TC.real
        SNumber -> return TC.number
        SString -> return TC.string
        SAddress -> return TC.address
        SOption qas -> TC.option <$> manifestTypeFrom qas
        SIndirection qas -> TC.indirection <$> manifestTypeFrom qas
        STuple qass -> TC.tuple <$> mapM manifestTypeFrom qass
        SRecord m oas -> do
          let (is,qass) = unzip $ Map.toList m
          typs <- mapM manifestTypeFrom qass
          if Set.null oas
            then
              return $ TC.record $ zip is typs
            else do
              oids <- mapM nameOpaque $ Set.toList oas
              return $ TC.recordExtension (zip is typs) oids
        STop -> return TC.top
        SBottom -> return TC.bottom
        SOpaque oa -> do
          i <- nameOpaque oa
          return $ TC.declaredVar i
    logManifestSuffix t t'
    return t'
    where
      attachOpaques :: ManifestM (K3 Type) -> ManifestM (K3 Type)
      attachOpaques tM =
        if Set.null withOas then tM else do
          t' <- getBoundDefaultType <$> askBoundType
          if t == t' && Set.size withOas == 1
            then manifestTypeFrom ( SOpaque $ Set.findMin withOas
                                  , Set.empty :: Set OpaqueVar)
            else
              TC.declaredVarOp <$>
                mapM nameOpaque (Set.toList withOas) <*>
                (getTyVarOp <$> askBoundType) <*>
                tM

-- ** Supporting routines

considerMuType :: TVar q -> ManifestM (K3 Type) -> ManifestM (K3 Type)
considerMuType var computation =
  tryVisitVar (someVar var) alreadyDefined (justDefined $ someVar var)
  where
    -- |Used when this point has already been witnessed before in the same
    --  branch of tree.
    alreadyDefined :: Identifier -> ManifestM (K3 Type)
    alreadyDefined i = return $ TC.declaredVar i
    -- |Used when this point is new, which is the most common case.
    justDefined :: AnyTVar -> ManifestM (K3 Type)
    justDefined sa = do
      (typ, usedName) <- catchVarUse sa computation
      return $ case usedName of
        Just name -> TC.mu name typ
        Nothing -> typ

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
      dict <- askDictionary
      let tov_L = _fromJustD $ Map.lookup (oa, LowerBound) $ ovarRangeDict dict
      let tov_U = _fromJustD $ Map.lookup (oa, UpperBound) $ ovarRangeDict dict
      typL <- manifestFromTypeOrVar LowerBound tov_L
      typU <- manifestFromTypeOrVar UpperBound tov_U
      return $ TypeVarDecl i (Just typL) (Just typU)
      where
        _fromJustD (Just x) = x
        _fromJustD Nothing =
          error "Opaque does not have appropriate bounds in manifestation dictionary!"
        manifestFromTypeOrVar bt = usingBoundType bt . manifestTypeFrom

-- |A routine to remove all constraints from a constraint set which are not
--  necessary for manifestation.  The removed constraints are of the variety
--  which propagate information but carry no immediate meaning after closure is
--  complete.
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

--------------------------------------------------------------------------------
-- * Logging tools

logManifestPrefix :: (Pretty a) => a -> ManifestM ()
logManifestPrefix x = do
  pfxBox <- logManifestTopMsg "Manifesting" x
  _debug $ boxToString pfxBox

logManifestSuffix :: (Pretty a) => a -> K3 Type -> ManifestM ()
logManifestSuffix x t = do
  pfxBox <- logManifestTopMsg "Manifested" x
  _debug $ boxToString $ pfxBox %$
    indent 2 (["Result: "] %+ prettyLines t)

logManifestTopMsg :: (Pretty a) => String -> a -> ManifestM [String]
logManifestTopMsg verb x = do
  name <- getBoundTypeName <$> askBoundType
  return $ [verb ++ " " ++ name ++ " bound type for "] %+ prettyLines x
