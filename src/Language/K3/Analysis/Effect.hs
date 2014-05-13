{-# LANGUAGE ViewPatterns #-}

module Language.K3.Analysis.Effect where

import Control.Arrow
import Control.Monad

import Data.List 
import Data.Map ( Map )
import Data.Tree
import qualified Data.Map as Map

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

type Property   = (Identifier, Maybe (K3 Literal))
type NamedEnv a = Map Identifier a

type NamedEffectEnv = NamedEnv [[Property]]
data AnnEffectEnv   = AnnEffectEnv { definitions  :: NamedEnv (NamedEnv [Property])
                                   , realizations :: NamedEnv (NamedEnv [Property]) }

-- | An effect environment supporting an open set of effect types as properties.
data EffectEnv = EffectEnv { bindingE    :: NamedEffectEnv
                           , annotationE :: AnnEffectEnv }

{- Helpers -}
annotationComboId :: [Identifier] -> Identifier
annotationComboId annIds = intercalate ";" annIds

annotationComboIdT :: [Annotation Type] -> Maybe Identifier
annotationComboIdT (namedTAnnotations -> [])  = Nothing
annotationComboIdT (namedTAnnotations -> ids) = Just $ annotationComboId ids

annotationComboIdE :: [Annotation Expression] -> Maybe Identifier
annotationComboIdE (namedEAnnotations -> [])  = Nothing
annotationComboIdE (namedEAnnotations -> ids) = Just $ annotationComboId ids

emptyEnv :: EffectEnv
emptyEnv = EffectEnv Map.empty (AnnEffectEnv Map.empty Map.empty)

insertBinding :: Identifier -> [[Property]] -> EffectEnv -> EffectEnv
insertBinding n props env = flip EffectEnv (annotationE env) $ Map.insert n props (bindingE env)

insertAnnDef :: Identifier -> NamedEnv [Property] -> EffectEnv -> EffectEnv
insertAnnDef n memEnv env =
  EffectEnv (bindingE env) $ 
    annEnvWithDefinitions (annotationE env) $ Map.insert n memEnv (definitions $ annotationE env)

envWithBindings :: EffectEnv -> NamedEffectEnv -> EffectEnv
envWithBindings (EffectEnv _ aeEnv) bEnv = EffectEnv bEnv aeEnv

envWithAnnotations :: EffectEnv -> AnnEffectEnv -> EffectEnv 
envWithAnnotations (EffectEnv bEnv _) aeEnv = EffectEnv bEnv aeEnv

annEnvWithDefinitions :: AnnEffectEnv -> NamedEnv (NamedEnv [Property]) -> AnnEffectEnv
annEnvWithDefinitions annEnv defns = AnnEffectEnv defns (realizations annEnv) 

annEnvWithRealizations :: AnnEffectEnv -> NamedEnv (NamedEnv [Property]) -> AnnEffectEnv
annEnvWithRealizations annEnv reals = AnnEffectEnv (definitions annEnv) reals

unionAnnEnv :: AnnEffectEnv -> AnnEffectEnv -> AnnEffectEnv
unionAnnEnv a b = AnnEffectEnv (Map.union (definitions a)  $ definitions b)
                               (Map.union (realizations a) $ realizations b)

-- | Effect constants
pureEffect :: Property
pureEffect = ("Pure", Nothing)

isDPure :: K3 Declaration -> Bool
isDPure (annotations -> anns) = any isPureDProperty anns

isEPure :: K3 Expression -> Bool
isEPure (annotations -> anns) = any isPureEProperty anns

isPureDProperty :: Annotation Declaration -> Bool
isPureDProperty (DProperty a b) = isPureProperty (a,b)
isPureDProperty _               = False

isPureEProperty :: Annotation Expression -> Bool
isPureEProperty (EProperty a b) = isPureProperty (a,b)
isPureEProperty _               = False

isPureProperty :: Property -> Bool
isPureProperty (a,b) = (a,b) == pureEffect

addDEffects :: [Annotation Declaration] -> [Property] -> [Annotation Declaration]
addDEffects anns props = anns ++ map (uncurry DProperty) props

addEEffects :: [Annotation Expression] -> [Property] -> [Annotation Expression]
addEEffects anns props = anns ++ map (uncurry EProperty) props

addPureDEffect :: [Annotation Declaration] -> [Annotation Declaration]
addPureDEffect anns = anns ++ [uncurry DProperty pureEffect]

addPureEEffect :: [Annotation Expression] -> [Annotation Expression]
addPureEEffect anns = anns ++ [uncurry EProperty pureEffect]

-- | A simple effect analysis that adds @:Pure properties to both
--   declarations and expressions.
--   The input program must be typechecked by the caller beforehand.
analyzeEffects :: K3 Declaration -> Either String (K3 Declaration)
analyzeEffects prog = do
    initEnv <- initialEffectEnv
    either Left (Right . snd) $ foldRebuildTree analyzeDeclEffect initEnv prog
  where
    initialEffectEnv :: Either String EffectEnv
    initialEffectEnv = foldTree initialDeclEffect emptyEnv prog


-- | Assumes all global function declarations with initializers are pure, to
--   bootstrap mutually recursive global functions.
--   For declarations without initializers (i.e., externals), take their effect
--   from any explicitly specified properties.
initialDeclEffect :: EffectEnv -> K3 Declaration -> Either String EffectEnv
initialDeclEffect env (tag &&& annotations -> (DGlobal n t eOpt, anns)) =
    if not $ isTFunction t
      then return env
      else return $ maybe addIfAnnotated addAsPure eOpt
  where
    addIfAnnotated   = if any isPureDProperty anns then insertBinding n [[pureEffect]] env else env
    addAsPure      _ = insertBinding n [[pureEffect]] env

initialDeclEffect env (tag -> DTrigger n _ _) = return $ insertBinding n [[]] env
initialDeclEffect env _ = return $ env


analyzeDeclEffect :: EffectEnv -> [K3 Declaration] -> K3 Declaration
                  -> Either String (EffectEnv, K3 Declaration)

-- | Add an effect binding for the global, ensuring that any collection realization
--   is appropriately constucted in the annotation effect environment. 
analyzeDeclEffect env ch (tag &&& annotations -> (DGlobal n t eOpt, anns)) = do
    ntEnv <- ensureGlobalCollectionRealization env t eOpt
    ((naEnv, nEffects), neOpt) <- maybe (effectFromDType ntEnv t anns) (effectFromDExpr ntEnv) eOpt
    let nEnv = insertBinding n [nEffects] $ envWithAnnotations ntEnv naEnv
    return (nEnv, Node (DGlobal n t neOpt :@: (addDEffects anns nEffects)) ch)

  where
    effectFromDType eEnv dt dAnns = do
      props <- analyzeTypeEffect dt dAnns
      return ((annotationE $ eEnv, props), Nothing)
    
    effectFromDExpr eEnv e = do
      ((naEnv, props), ne) <- analyzeExprEffect eEnv e
      return ((naEnv, props), Just ne)

    ensureGlobalCollectionRealization eEnv _ (Just _) = return eEnv
    ensureGlobalCollectionRealization eEnv dt Nothing  = case details dt of
      (TCollection, _, tAnns) -> ensureTRealization eEnv tAnns
      _ -> return eEnv

-- | While triggers must be impure, accumulate any other effects from the trigger.
--   Terminate the analysis if we find a pure trigger.
analyzeDeclEffect env ch (tag &&& annotations -> (DTrigger n t e, anns)) = do
  ((naEnv, nEffects), ne) <- analyzeExprEffect env e
  case find (== pureEffect) nEffects of
    Just _  -> Left $ "Invalid pure trigger " ++ n
    Nothing -> let nEnv = insertBinding n [nEffects] $ envWithAnnotations env naEnv
               in Right (nEnv, Node (DTrigger n t ne :@: (addDEffects anns nEffects)) ch)

-- | Build an effect environment for annotation members, and add it to the
--   accumulating effect environment.
analyzeDeclEffect env ch (tag &&& annotations -> (DAnnotation n tVars mems, anns)) = do
  initMemEnv             <- return $ initialAnnotationEnv mems
  (naEnv, memEnv, nMems) <- analyzeMembersEffects env initMemEnv mems
  nEnv                   <- return $ insertAnnDef n memEnv $ envWithAnnotations env naEnv
  return (nEnv, Node (DAnnotation n tVars nMems :@: anns) ch)

analyzeDeclEffect env ch (Node n _) = return (env, Node n ch)


analyzeExprEffect :: EffectEnv -> K3 Expression
                  -> Either String ((AnnEffectEnv, [Property]), K3 Expression)
analyzeExprEffect env e = do
  (nEnv, ne) <- foldIn1RebuildTree preMkEffect postMkEffect mergeEffectInRebuild propagateEffect env e
  props      <- extractProperties ne
  return $ ((annotationE nEnv, props), ne)
  where
    extractProperties expr = return $ concatMap extractProperty $ annotations expr
    extractProperty (EProperty p vOpt) = [(p, vOpt)]
    extractProperty _                  = []

    propagateEnvFlags :: [K3 Expression] -> EffectEnv -> Either String (EffectEnv, [Bool])
    propagateEnvFlags ch eEnv = Right $ (eEnv, replicate (length $ tail ch) True)

    preMkEffect :: EffectEnv -> K3 Expression -> K3 Expression -> Either String EffectEnv
    preMkEffect eEnv _ (tag -> ELambda i) = Right $ addDirectBindingEffect eEnv i True
    preMkEffect eEnv _ _ = Right $ eEnv

    postMkEffect :: EffectEnv -> K3 Expression -> K3 Expression -> Either String (EffectEnv, [Bool])
    postMkEffect eEnv src (tag &&& children -> (ELetIn  i, ch)) =
      propagateEnvFlags ch $ addBindingEffect eEnv i src
    
    postMkEffect eEnv src (tag &&& children -> (EBindAs b, ch)) =
      propagateEnvFlags ch $ foldl (\acc i -> addBindingEffect acc i src) eEnv $ bindingVariables b

    -- We do not propagate i's effect binding to the None branch in foldIn1RebuildTree.
    postMkEffect eEnv src (tag -> ECaseOf i) =
      Right $ (addBindingEffect eEnv i src, [True, False])
    
    postMkEffect eEnv _ (children -> ch) = propagateEnvFlags ch eEnv

    -- | Effect environment merging
    mergeEffectInRebuild :: EffectEnv -> EffectEnv -> Either String EffectEnv
    mergeEffectInRebuild a b = Right $ envWithAnnotations a $ unionAnnEnv (annotationE a) (annotationE b)

    -- | Effect propagation.
    propagateEffect :: EffectEnv -> [K3 Expression] -> K3 Expression
                    -> Either String (EffectEnv, K3 Expression)
    propagateEffect eEnv ch n@(tag -> EAssign _)     = Right (eEnv, Node (tag n :@: annotations n) ch) 
    propagateEffect eEnv ch n@(tag -> EOperate OSnd) = Right (eEnv, Node (tag n :@: annotations n) ch) 

    -- TODO: since empty collections may involve allocation, reconsider whether these are pure in K3.
    propagateEffect eEnv ch (tag &&& annotations -> (EConstant c, anns)) = do
      nEnv <- case c of 
                CEmpty _ -> ensureERealization eEnv anns
                _        -> return eEnv
      return (nEnv, Node (EConstant c :@: addPureEEffect anns) ch)

    propagateEffect eEnv ch (tag &&& annotations -> (EVariable i,  anns)) =
      case Map.lookup i $ bindingE eEnv of 
        Nothing -> Left $ "Unbound effect for variable " ++ i
        Just [] -> Left $ "Invalid empty effect for variable " ++ i
        Just el -> let nAnns = if any isPureProperty (head el) then addPureEEffect anns else anns
                   in Right $ (eEnv, Node (EVariable i :@: nAnns) ch)

    propagateEffect eEnv ch n@(chainEffect . tag -> True) = propagateIfAllPure eEnv ch n

    propagateEffect eEnv ch n@(tag -> EProject i) =
      let srcAnns = annotations $ head ch in
      case maybe Nothing (Just . details) $ typeOf $ srcAnns of
        Just (TRecord   _, _, _)     -> propagateIfAllPure eEnv ch n
        Just (TCollection, _, tAnns) -> propagateProjectMemberEffect eEnv ch tAnns srcAnns i

        Just _  -> Left $ "Invalid source type in projection expression, neither collection nor record: " ++ (show $ typeOf srcAnns)
        Nothing -> Left "Unknown source type in projection expression."


    propagateEffect _ _ (tag -> ESelf) = Left "Self expression not handled in effect analysis."
    propagateEffect eEnv ch (Node n _) = Right (eEnv, (Node n ch))

    -- | Extract realization id from the type, and check member annotations
    --   for i in the realization env.
    propagateProjectMemberEffect eEnv ch tAnns eAnns i =
      flip (maybe unannotatedProjError) (annotationComboIdT tAnns) $ \cid ->
      flip (maybe $ unknownRealizationError cid) (Map.lookup cid $ realizations $ annotationE eEnv) $ \mEnv ->
      flip (maybe $ unknownMemberError cid i) (Map.lookup i mEnv) $ \el ->
        let nAnns = if any isPureProperty el then addPureEEffect eAnns else eAnns
        in Right (eEnv, Node (EProject i :@: nAnns) ch)

    chainEffect (ESome       ) = True
    chainEffect (ETuple      ) = True
    chainEffect (ERecord    _) = True
    chainEffect (EIndirect   ) = True
      -- ^ TODO: indirections involve allocation, revisit if this should pure in terms of K3.

    chainEffect (EOperate   _) = True
    chainEffect (ELambda    _) = True
    chainEffect (ELetIn     _) = True
    chainEffect (EBindAs    _) = True
    chainEffect (ECaseOf    _) = True
    chainEffect (EIfThenElse ) = True
    chainEffect (EAddress    ) = True
    chainEffect _              = False

    propagateIfAllPure eEnv ch (Node (t :@: a) _) =
      Right $ (eEnv, Node (t :@: (if all isEPure ch then addPureEEffect a else a)) ch)

    addBindingEffect eEnv i src = addDirectBindingEffect eEnv i $ isEPure src

    addDirectBindingEffect eEnv i pure =
      envWithBindings eEnv $ Map.insertWith (++) i (if pure then [[pureEffect]] else [[]]) (bindingE eEnv)

    typeOf anns = maybe Nothing extractType $ find isEExactType anns
    isEExactType (EType _) = True
    isEExactType _         = False
    extractType (EType t)  = Just t
    extractType _          = Nothing

    unannotatedProjError        = Left "Projection on unannotated collection"
    unknownRealizationError cid = Left $ "Unknown realization type: " ++ cid
    unknownMemberError    cid i = Left $ "Unknown member " ++ i ++ " in realization " ++ cid


-- | Type-based effect property determination.
--   This applies to declarations that have no initializer expressions,
--   including value declarations with default values, and external functions.
--   For external functions, effects must be explicitly specified, while all
--   other values are assumed to be pure (ignoring allocation effects for now).
analyzeTypeEffect :: K3 Type -> [Annotation Declaration] -> Either String [Property]
analyzeTypeEffect t anns =
    if not $ isTFunction t then return [pureEffect] else ifAnnotated
  where
    ifAnnotated = return $ if any isPureDProperty anns then [pureEffect] else []


-- | Returns a member effect environment which assumes all members with 
--   valid bodies (i.e., non-externals) are pure to support cyclic definitions.
--   Externals adopt their purity properties through explicit specification.
initialAnnotationEnv :: [AnnMemDecl] -> NamedEnv [Property]
initialAnnotationEnv mems = foldl initMemberEffect Map.empty mems
  where initMemberEffect acc (Lifted      Provides n _ (Just _) _)     = addMemberEffect acc n
        initMemberEffect acc (Attribute   Provides n _ (Just _) _)     = addMemberEffect acc n
        initMemberEffect acc (Lifted      Provides n _ Nothing  dAnns) = addIfAnnotated acc n dAnns
        initMemberEffect acc (Attribute   Provides n _ Nothing  dAnns) = addIfAnnotated acc n dAnns
        initMemberEffect acc _ = acc

        addIfAnnotated acc n dAnns = if any isPureDProperty dAnns then addMemberEffect acc n else acc
        addMemberEffect acc n = Map.insert n [pureEffect] acc


-- | Returns a member effect environment as well as members annotated with purity properties.
analyzeMembersEffects :: EffectEnv -> NamedEnv [Property] -> [AnnMemDecl]
                      -> Either String (AnnEffectEnv, NamedEnv [Property], [AnnMemDecl])
analyzeMembersEffects env initMemEnv mems = foldM analyzeInit (annotationE env, Map.empty, []) mems
  where
    analyzeInit acc (Lifted Provides n t (Just e) dAnns) =
      fromExpr acc n e dAnns $ \ne ndAnns -> Lifted Provides n t (Just ne) ndAnns

    analyzeInit acc (Attribute Provides n t (Just e) dAnns) =
      fromExpr acc n e dAnns $ \ne ndAnns -> Attribute Provides n t (Just ne) ndAnns
    
    analyzeInit acc mem@(Lifted    Provides n _ Nothing _) = fromInitEnv acc n mem
    analyzeInit acc mem@(Attribute Provides n _ Nothing _) = fromInitEnv acc n mem

    analyzeInit (annEnvAcc, defAcc, memAcc) mem = return (annEnvAcc, defAcc, memAcc ++ [mem])

    fromExpr (annEnvAcc, defAcc, memAcc) n e dAnns memCtor = do
      ((naEnv, props), ne) <- analyzeExprEffect (envWithAnnotations env annEnvAcc) e
      return (naEnv, Map.insert n props defAcc, memAcc ++ [memCtor ne $ addPureDEffect dAnns])

    fromInitEnv (annEnvAcc, defAcc, memAcc) n mem = do
      nDefAcc <- maybe (undefinedExternalError n) (\props -> Right $ Map.insert n props defAcc)
                    $ Map.lookup n initMemEnv
      return (annEnvAcc, nDefAcc, memAcc ++ [mem])

    undefinedExternalError n = Left $ "Undefined effect for external member " ++ n

ensureRealization :: EffectEnv -> [Identifier] -> Either String EffectEnv
ensureRealization env [] = Right $ env
ensureRealization env annIds@(annotationComboId -> cid) =
  maybe (buildRealization env annIds)
        (const $ Right env)
        (Map.lookup cid $ realizations $ annotationE env)

buildRealization :: EffectEnv -> [Identifier] -> Either String EffectEnv
buildRealization env [] = Right $ env
buildRealization env annIds@(annotationComboId -> cid) =
    let defns   = definitions $ annotationE env
        cDefns  = map (\n -> (n, Map.lookup n defns)) annIds
        accDefn = \acc (n, defnOpt) ->
                    maybe (definitionError n) (\mEnv -> Right $ Map.union acc mEnv) defnOpt
    in do
      nRel <- foldM accDefn Map.empty cDefns
      return $ envWithAnnotations env
                $ annEnvWithRealizations (annotationE env )
                $ Map.insert cid nRel $ realizations $ annotationE env
  where
    definitionError n = Left $ "No annotation definition found for " ++ n


ensureTRealization :: EffectEnv -> [Annotation Type] -> Either String EffectEnv
ensureTRealization env tAnns = ensureRealization env $ namedTAnnotations tAnns

ensureERealization :: EffectEnv -> [Annotation Expression] -> Either String EffectEnv
ensureERealization env eAnns = ensureRealization env $ namedEAnnotations eAnns
