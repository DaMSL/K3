{-# LANGUAGE TupleSections, FlexibleContexts, TemplateHaskell #-}

module Language.K3.TypeSystem.TypeDecision.StubReplacement 
( calculateStubs
, aEnvStubSubstitute
, rEnvStubSubstitute
) where

import Control.Arrow
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import qualified Data.Set as Set
import qualified Data.Traversable as Trav

import Language.K3.Pretty
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.TypeDecision.AnnotationInlining
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |A type alias for maps containing the information and reduction associated
--  with stubs.
type StubInfoMap c = Map Stub (StubInfo, (QVar, c))

-- |Given a skeletal environment and a mapping of constructed stubs, calculates
--  non-stubbed constraint sets for each stub mapping.  This is accomplished by
--  constructing stubbed types for each sub in the provided map and then closing
--  substitution over the entire set of stubs.
calculateStubs :: TSkelAliasEnv -> Map Stub StubInfo
               -> TypeDecideM (StubInfoMap ConstraintSet)
calculateStubs aEnv stubInfoMap = do
  -- Calculate the appropriate initial values for the stub map
  stubInitialMap <- Trav.mapM stubInfoEntry stubInfoMap
  _debug $ boxToString $
    ["Initial stub map:"] %+ indent 2 (
        sequenceBoxes maxWidth ", " $ prettyStubs $ Map.toList stubInitialMap
      )
  -- Now close over stub substitution for each stub.
  let result = Map.map (second $ second $ closeStubSubstitution stubInitialMap)
              stubInitialMap
  _debug $ boxToString $
    ["Closed stub map:"] %+ indent 2 (
        sequenceBoxes maxWidth ", " $ prettyStubs $ Map.toList result
      )
  return result
  where
    prettyStubs :: (Pretty c) => [(Stub, (StubInfo, (QVar, c)))] -> [[String]]
    prettyStubs = map (\(k,(_,(qa,cs))) -> prettyLines k %+ [" → "] %+
                        prettyLines qa %+ ["\\"] %+ prettyLines cs)
    stubInfoEntry :: StubInfo
                  -> TypeDecideM (StubInfo, (QVar, StubbedConstraintSet))
    stubInfoEntry x = do
      -- Calculate the result by extending the environment to include the
      -- type parameters from the context in the stub info.
      let cxt = typeParamCxt $ stubMemRepr x
      let aEnv' = Map.mapKeys TEnvIdentifier $
                    Map.map (\(a,qa,_) ->
                      QuantAlias $ QuantType Set.empty qa $ qa ~= a) cxt
      -- Now calculate the correct type for the stub.
      (qa,cs) <- deriveQualifiedTypeExpression
                  (envMerge aEnv aEnv') (stubTypeExpr x)
      return (x,(qa,cs))

-- |Closes over stub substitution for a given set of constraints.  Then,
--  replaces each stub with an appropriate type variable bound.
closeStubSubstitution
    :: (CSL.ConstraintSetLikePromotable c StubbedConstraintSet)
    => StubInfoMap c -> StubbedConstraintSet -> ConstraintSet
closeStubSubstitution m scs =
  -- First, do transitive closure over stubs
  -- PERF: optimize this closure process by preventing duplicate work
  let scs' = closeInner scs in
  -- Next, replace stubs with constraints
  constraintsOf scs' `csUnion`
    csFromList (map constraintForStub $ Set.toList $ stubsOf scs')
  where
    closeInner :: StubbedConstraintSet -> StubbedConstraintSet
    closeInner scs' =
      let scs'' = stubSubstitution m scs' in
      if scs'' /= scs' then closeInner scs'' else scs'
    constraintForStub :: Stub -> Constraint
    constraintForStub stub =
      let (info,(qa,_)) = stubLookup stub m in
      qa <: stubVar info

-- |Performs a single pass of stub substitution.
stubSubstitution
    :: (CSL.ConstraintSetLikePromotable c StubbedConstraintSet)
    => StubInfoMap c -> StubbedConstraintSet -> StubbedConstraintSet
stubSubstitution m scs =
  -- Just add all of the constraints from the stubs in this set.  Don't remove
  -- those stubs; doing so could cause an infinite loop.
  CSL.union scs $
    CSL.unions $ map (CSL.promote . snd . snd . (`stubLookup` m)) $
      Set.toList $ stubsOf scs

-- |Performs lookup on a stub.  It must be present or an error occurs.
stubLookup :: Stub -> Map Stub a -> a
stubLookup stub m =
  fromMaybe (error $ "Undefined stub " ++ show stub ++
                     " in context of substitution!")
          $ Map.lookup stub m

-- |Performs stub substitution for annotation body types.
closeBodyStubSubstitution
    :: (CSL.ConstraintSetLikePromotable c StubbedConstraintSet)
    => StubInfoMap c -> AnnBodyType StubbedConstraintSet -> NormalAnnBodyType
closeBodyStubSubstitution m (AnnBodyType ms1 ms2) =
  let ms1' = map closeMemberStubSubstitution ms1 in
  let ms2' = map closeMemberStubSubstitution ms2 in
  AnnBodyType ms1' ms2'
  where
    closeMemberStubSubstitution (AnnMemType i pol qa cs) =
      AnnMemType i pol qa $ closeStubSubstitution m cs

-- |Performs stub substitution on a skeletal environment, yielding a complete
--  environment.
aEnvStubSubstitute :: StubInfoMap ConstraintSet
                   -> TSkelAliasEnv -> TAliasEnv
aEnvStubSubstitute m = Map.map replEntry
  where
    replEntry :: TypeAliasEntry StubbedConstraintSet
              -> TypeAliasEntry ConstraintSet
    replEntry entry = case entry of
      AnnAlias (AnnType p b scs) ->
        let cs = closeStubSubstitution m scs in
        let b' = closeBodyStubSubstitution m b in
        AnnAlias $ AnnType p b' cs
      QuantAlias _ ->
        error $ "Quantified alias " ++ show entry ++
                " unexpected in skeletal environment!"

-- |Performs stub substitution on a skeletal environment, yielding a complete
--  environment.
rEnvStubSubstitute :: StubInfoMap ConstraintSet
                   -> TSkelGlobalQuantEnv -> TGlobalQuantEnv
rEnvStubSubstitute m = Map.map replEntry
  where
    replEntry :: TSkelQuantEnv -> TQuantEnv
    replEntry = Map.map replEntry'
    replEntry' :: TQuantEnvValue StubbedConstraintSet
               -> TQuantEnvValue ConstraintSet
    replEntry' (a,t_L,t_U,scs) =
      (a,t_L,t_U, closeStubSubstitution m scs)
