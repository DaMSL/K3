{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Metaprogram.Evaluation where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.IO.Class

import Data.Functor.Identity
import Data.List
import qualified Data.Map as Map
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Metaprogram
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC
import qualified Language.K3.Core.Constructor.Literal     as LC

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.MetaHK3 hiding ( localLog, localLogAction )

import Language.K3.Parser.ProgramBuilder ( defaultRoleName )
import Language.K3.Parser.DataTypes

import Language.K3.Analysis.HMTypes.Inference hiding ( localLog, localLogAction )

import Language.K3.Utils.Pretty

traceLogging :: Bool
traceLogging = True

localLog :: (Functor m, Monad m) => String -> m ()
localLog = logVoid traceLogging

localLogAction :: (Functor m, Monad m) => (Maybe a -> Maybe String) -> m a -> m a
localLogAction = logAction traceLogging


{- Top-level AST transformations -}
evalMetaprogram :: Maybe MPEvalOptions
                -> Maybe (K3 Declaration -> GeneratorM (K3 Declaration))
                -> Maybe (K3 Declaration -> GeneratorM (K3 Declaration))
                -> K3 Declaration -> IO (Either String (K3 Declaration))
evalMetaprogram evalOpts analyzeFOpt repairFOpt mp =
    runGeneratorM initGState synthesizedProg >>= return . fst
  where
    synthesizedProg = do
      localLog $ generatorInput mp
      pWithDataAnns  <- mpGenerators mp
      pWithMDataAnns <- applyDAnnGens pWithDataAnns
      pWithDADecls   <- modifyGDeclsF $ \gd -> addDecls gd pWithMDataAnns
      analyzedP      <- analyzeF pWithDADecls
      localLog $ debugAnalysis analyzedP
      pWithMCtrlAnns <- applyCAnnGens analyzedP
      pWithCADecls   <- modifyGDeclsF $ \gd -> addDecls gd pWithMCtrlAnns
      pRepaired      <- repairF pWithCADecls
      if pRepaired == mp then return pRepaired
                         else rcr    pRepaired -- Tail recursive fixpoint

    initGState = maybe emptyGeneratorState mkGeneratorState evalOpts
    analyzeF   = maybe defaultMetaAnalysis id analyzeFOpt
    repairF    = maybe defaultMetaRepair   id repairFOpt

    rcr p = (liftIO $ evalMetaprogram evalOpts analyzeFOpt repairFOpt p) >>= either throwG return

    addDecls genDecls p@(tag -> DRole n)
      | n == defaultRoleName =
          let (dd, cd) = generatorDeclsToList genDecls
          in return $ (emptyGeneratorDecls, Node (DRole n :@: annotations p) $ children p ++ dd ++ cd)

    addDecls _ p = Left . boxToString $ [addErrMsg] %$ prettyLines p

    generatorInput = metalog "Evaluating metaprogram "
    debugAnalysis  = metalog "Analyzed metaprogram "
    metalog msg p  = boxToString $ [msg] %$ (indent 2 $ prettyLines p)

    addErrMsg = "Invalid top-level role resulting from metaprogram evaluation"

defaultMetaAnalysis :: K3 Declaration -> GeneratorM (K3 Declaration)
defaultMetaAnalysis p = do
    strippedP <- mapExpression removeTypes p
    liftError (liftError return . translateProgramTypes) $ inferProgramTypes strippedP

  where
    -- | Match any type annotation except pattern types which are user-defined in patterns.
    removeTypes e = return $ stripExprAnnotations (\a -> isETypeOrBound a || isEQType a) (const True) e
    liftError = either throwG

defaultMetaRepair :: K3 Declaration -> GeneratorM (K3 Declaration)
defaultMetaRepair p = return $ repairProgram p

nullMetaAnalysis :: K3 Declaration -> GeneratorM (K3 Declaration)
nullMetaAnalysis p = return p

mpGenerators :: K3 Declaration -> GeneratorM (K3 Declaration)
mpGenerators mp = mapTree evalMPDecl mp
  where
    evalMPDecl :: [K3 Declaration] -> K3 Declaration -> GeneratorM (K3 Declaration)
    evalMPDecl ch d@(tag -> DGenerator (MPDataAnnotation n [] tvars mems)) =
      rebuildNode (DC.dataAnnotation n tvars mems) (annotations d) ch

    evalMPDecl ch d@(tag -> DGenerator mpd@(MPDataAnnotation n svars tvars mems)) =
      let extendGen genEnv =
            case lookupDGenE n genEnv of
              Nothing -> Right $ addDGenE n (annotationSplicer n svars tvars mems) genEnv
              Just _  -> Left $ unwords ["Duplicate metaprogrammed data annotation for", n]
      in modifyGEnvF_ extendGen >> rebuildNode (DC.generator mpd) (annotations d) ch

    evalMPDecl ch d@(tag -> DGenerator mpd@(MPCtrlAnnotation n svars rewriteRules extensions)) =
      let extendGen genEnv =
            case lookupCGenE n genEnv of
              Nothing -> Right $ addCGenE n (exprPatternMatcher svars rewriteRules extensions) genEnv
              Just _  -> Left $ unwords ["Duplicate metaprogrammed control annotation for", n]
      in modifyGEnvF_ extendGen >> rebuildNode (DC.generator mpd) (annotations d) ch

    evalMPDecl ch (tag &&& annotations -> (t,anns)) = return $ Node (t :@: anns) ch

    rebuildNode (Node (t :@: anns) _) nanns ch = return $ Node (t :@: (nub $ anns ++ nanns)) ch


applyDAnnGens :: K3 Declaration -> GeneratorM (K3 Declaration)
applyDAnnGens mp = mapProgram applyDAnnDecl applyDAnnMemDecl applyDAnnExprTree (Just applyDAnnTypeTree) mp
  where
    applyDAnnExprTree e = mapTree applyDAnnExpr e
    applyDAnnTypeTree t = mapTree applyDAnnType t
    applyDAnnLitTree  l = mapTree applyDAnnLiteral l

    applyDAnnDecl d = mapM dApplyAnn (annotations d) >>= rebuildNodeWithAnns d

    applyDAnnMemDecl (Lifted      p n t eOpt anns) = mapM dApplyAnn anns >>= return . Lifted    p n t eOpt
    applyDAnnMemDecl (Attribute   p n t eOpt anns) = mapM dApplyAnn anns >>= return . Attribute p n t eOpt
    applyDAnnMemDecl (MAnnotation p n anns)        = mapM dApplyAnn anns >>= return . MAnnotation p n

    applyDAnnExpr ch n@(tag -> EConstant (CEmpty t)) = do
      nt    <- applyDAnnTypeTree t
      nanns <- mapM eApplyAnn $ annotations n
      rebuildNode (EC.constant $ CEmpty nt) (Just nanns) ch

    applyDAnnExpr ch n = rebuildNode n Nothing ch

    applyDAnnType ch n@(tag -> TCollection) = do
      nanns <- mapM tApplyAnn $ annotations n
      rebuildNode (TC.collection $ head $ children n) (Just nanns) ch

    applyDAnnType ch n = rebuildNode n Nothing ch

    applyDAnnLiteral ch n@(tag -> LEmpty t) = do
      nt    <- applyDAnnTypeTree t
      nanns <- mapM lApplyAnn $ annotations n
      rebuildNode (LC.empty nt) (Just nanns) ch

    applyDAnnLiteral ch n@(tag -> LCollection t) = do
      nt    <- applyDAnnTypeTree t
      nanns <- mapM lApplyAnn $ annotations n
      rebuildNode (LC.collection nt $ children n) (Just nanns) ch

    applyDAnnLiteral ch n = rebuildNode n Nothing ch

    dApplyAnn (DProperty n (Just l)) = applyDAnnLitTree l >>= return . DProperty n . Just
    dApplyAnn x = return x

    eApplyAnn (EApplyGen False n senv) = applyDAnnotation EAnnotation n senv
    eApplyAnn (EProperty n (Just l)) = applyDAnnLitTree l >>= return . EProperty n . Just
    eApplyAnn x = return x

    tApplyAnn (TApplyGen n senv) = applyDAnnotation TAnnotation n senv
    tApplyAnn x = return x

    lApplyAnn (LApplyGen n senv) = applyDAnnotation LAnnotation n senv
    lApplyAnn x = return x

    rebuildNode (Node (t :@: anns) _) Nothing      ch = return $ Node (t :@: anns) ch
    rebuildNode (Node (t :@: anns) _) (Just nanns) ch = return $ Node (t :@: (nub $ anns ++ nanns)) ch

    rebuildNodeWithAnns (Node (t :@: _) ch) anns = return $ Node (t :@: anns) ch

applyDAnnotation :: AnnotationCtor a -> Identifier -> SpliceEnv -> GeneratorM (Annotation a)
applyDAnnotation aCtor annId sEnv = generatorWithGEnv $ \gEnv ->
    maybe (spliceLookupErr annId) (expectSpliceAnnotation . ($ sEnv)) $ lookupDSPGenE annId gEnv

  where expectSpliceAnnotation (SRDecl p) = do
          decl <- p
          case tag decl of
            DDataAnnotation n _ _ -> modifyGDeclsF_ (Right . addDGenDecl annId decl) >> return (aCtor n)
            _ -> throwG $ boxToString $ ["Invalid data annotation splice"] %+ prettyLines decl

        expectSpliceAnnotation _ = throwG "Invalid data annotation splice"

        spliceLookupErr n = throwG $ unwords ["Could not find data macro", n]


applyCAnnGens :: K3 Declaration -> GeneratorM (K3 Declaration)
applyCAnnGens mp = mapProgram applyCAnnDecl applyCAnnMemDecl applyCAnnExprTree Nothing mp
  where
    applyCAnnDecl      d = return d
    applyCAnnMemDecl mem = return mem
    applyCAnnExprTree  e = mapTree applyCAnnExpr e

    -- TODO: think about propagation of annotations between rewrites.
    -- Currently we preserve all non-control annotations, but clearly this is rewrite-dependent.
    applyCAnnExpr ch (Node (t :@: anns) _) =
      let (appAnns, rest) = partition isEApplyGen anns
      in foldM (eApplyAnn rest) (Node (t :@: rest) ch) appAnns

    eApplyAnn ncAnns e (EApplyGen True n senv) =
      applyCAnnotation e n senv >>= \ne -> return (foldl (@+) ne ncAnns)
    eApplyAnn _ e _ = return e


applyCAnnotation :: K3 Expression -> Identifier -> SpliceEnv -> ExprGenerator
applyCAnnotation targetE cAnnId sEnv = do
   localLog $ "Applying control annotation " ++ cAnnId
   generatorWithGEnv $ \gEnv ->
     maybe (spliceLookupErr cAnnId) (\g -> injectRewrite $ g targetE sEnv) $ lookupERWGenE cAnnId gEnv

  where
    injectRewrite (SRExpr p) = localLog debugPassThru >> p

    injectRewrite (SRRewrite p) = do
      (rewriteE, decls) <- p
      localLog (debugRewrite rewriteE)
      modifyGDeclsF_ (Right . addCGenDecls cAnnId decls) >> return rewriteE

    injectRewrite _ = throwG "Invalid control annotation rewrite"

    debugPassThru   = unwords ["Passed on generator", cAnnId]
    debugRewrite  e = boxToString $ [unwords ["Generator", cAnnId, "rewrote as "]] %+ prettyLines e

    spliceLookupErr n = throwG $ unwords ["Could not find control macro", n]



{- Splice-checking -}
-- TODO: match splice parameter types (e.g., types vs label-types vs exprs.)
validateSplice :: [TypedSpliceVar] -> SpliceEnv -> SpliceEnv
validateSplice spliceParams spliceEnv =
  let paramIds = map snd spliceParams
  in Map.filterWithKey (\k _ -> k `elem` paramIds) spliceEnv

{- Splicer construction -}
globalSplicer :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3Generator
globalSplicer n t eOpt = Splicer $ \spliceEnv -> SRDecl $ do
  nt <- generateInSpliceEnv spliceEnv $ spliceType t
  neOpt <- maybe (return Nothing) (\e -> generateInSpliceEnv spliceEnv (spliceExpression e) >>= return . Just) eOpt
  return $ DC.global n nt neOpt

annotationSplicer :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> K3Generator
annotationSplicer n spliceParams typeParams mems = Splicer $ \spliceEnv -> SRDecl $ do
  let vspliceEnv = validateSplice spliceParams spliceEnv
  nmems <- generateInSpliceEnv vspliceEnv $ mapM spliceAnnMem mems
  withGUID $ \i -> DC.dataAnnotation (concat [n, "_", show i]) typeParams nmems

exprSplicer :: K3 Expression -> K3Generator
exprSplicer e = Splicer $ \spliceEnv -> SRExpr $ generateInSpliceEnv spliceEnv $ spliceExpression e

typeSplicer :: K3 Type -> K3Generator
typeSplicer t = Splicer $ \spliceEnv -> SRType $ generateInSpliceEnv spliceEnv $ spliceType t

{- Splice evaluation -}
spliceDeclaration :: K3 Declaration -> DeclGenerator
spliceDeclaration = mapProgram doSplice spliceAnnMem spliceExpression (Just spliceType)
  where
    doSplice d@(tag -> DGlobal n t eOpt) = do
      ((nn, nt, neOpt), nanns) <- spliceDeclParts n t eOpt >>= newAnns d
      return $ Node (DGlobal nn nt neOpt :@: nanns) $ children d

    doSplice d@(tag -> DTrigger n t e) = do
      ((nn, nt, Just ne), nanns) <- spliceDeclParts n t (Just e) >>= newAnns d
      return $ Node (DTrigger nn nt ne :@: nanns) $ children d

    doSplice d@(tag -> DDataAnnotation n tvars mems) =
      mapM spliceAnnMem mems >>= newAnns d >>= \(nmems, nanns) ->
        return $ Node (DDataAnnotation n tvars nmems :@: nanns) $ children d

    doSplice d@(tag -> DTypeDef n t) =
      spliceType t >>= newAnns d >>= \(nt, nanns) -> return $ Node (DTypeDef n nt :@: nanns) $ children d

    doSplice d@(Node (tg :@: _) ch) =
      newAnns d () >>= \(_,nanns) -> return $ Node (tg :@: nanns) ch

    newAnns d v = mapM spliceDAnnotation (annotations d) >>= return . (v,)

spliceAnnMem :: AnnMemDecl -> AnnMemGenerator
spliceAnnMem = \case
    Lifted      p n t eOpt anns -> spliceDeclParts n t eOpt >>= newAnns anns >>= \((sn, st, seOpt), nanns) -> return $ Lifted    p sn st seOpt nanns
    Attribute   p n t eOpt anns -> spliceDeclParts n t eOpt >>= newAnns anns >>= \((sn, st, seOpt), nanns) -> return $ Attribute p sn st seOpt nanns
    MAnnotation p n anns -> newAnns anns () >>= \(_,nanns) -> return $ MAnnotation p n nanns

  where newAnns anns v = mapM spliceDAnnotation anns >>= return . (v,)

spliceDeclParts :: Identifier -> K3 Type -> Maybe (K3 Expression) -> GeneratorM (Identifier, K3 Type, Maybe (K3 Expression))
spliceDeclParts n t eOpt = do
  sn    <- spliceIdentifier n
  st    <- spliceType t
  seOpt <- maybe (return Nothing) (\e -> spliceExpression e >>= return . Just) eOpt
  return (sn, st, seOpt)

spliceExpression :: K3 Expression -> ExprGenerator
spliceExpression = mapTree doSplice
  where
    doSplice [] e@(tag -> EVariable i)           = expectExprSplicer i      >>= newAnns e >>= \(ne, nanns)   -> return $ foldl (@+) ne nanns
    doSplice ch e@(tag -> ERecord ids)           = mapM expectIdSplicer ids >>= newAnns e >>= \(nids, nanns) -> return $ Node (ERecord  nids :@: nanns) ch
    doSplice ch e@(tag -> EProject i)            = expectIdSplicer i        >>= newAnns e >>= \(nid, nanns)  -> return $ Node (EProject nid  :@: nanns) ch
    doSplice ch e@(tag -> EAssign i)             = expectIdSplicer i        >>= newAnns e >>= \(nid, nanns)  -> return $ Node (EAssign  nid  :@: nanns) ch
    doSplice ch e@(tag -> EConstant (CEmpty ct)) = spliceType ct            >>= newAnns e >>= \(nct, nanns)  -> return $ Node (EConstant (CEmpty nct) :@: nanns) ch
    doSplice ch e@(Node (tg :@: _) _) = newAnns e () >>= \(_,nanns) -> return $ Node (tg :@: nanns) ch

    newAnns e v = mapM spliceEAnnotation (annotations e) >>= return . (v,)

spliceType :: K3 Type -> TypeGenerator
spliceType = mapTree doSplice
  where
    doSplice [] t@(tag -> TDeclaredVar i) = expectTypeSplicer i       >>= \nt   -> return $ foldl (@+) nt $ annotations t
    doSplice ch t@(tag -> TRecord ids)    = mapM spliceIdentifier ids >>= \nids -> return $ Node (TRecord nids :@: annotations t) ch
    doSplice ch (Node tg _) = return $ Node tg ch

spliceLiteral :: K3 Literal -> LiteralGenerator
spliceLiteral = mapTree doSplice
  where doSplice [] l@(tag -> LString s)      = expectLiteralSplicer s   >>= \ns   -> return $ foldl (@+) ns $ annotations l
        doSplice ch l@(tag -> LRecord ids)    = mapM expectIdSplicer ids >>= \nids -> return $ Node (LRecord nids    :@: annotations l) ch
        doSplice ch l@(tag -> LEmpty ct)      = spliceType ct            >>= \nct  -> return $ Node (LEmpty nct      :@: annotations l) ch
        doSplice ch l@(tag -> LCollection ct) = spliceType ct            >>= \nct  -> return $ Node (LCollection nct :@: annotations l) ch
        doSplice ch (Node tg _) = return $ Node tg ch

spliceIdentifier :: Identifier -> GeneratorM Identifier
spliceIdentifier i = expectIdSplicer i

spliceDAnnotation :: Annotation Declaration -> DeclAnnGenerator
spliceDAnnotation (DProperty n (Just l)) = spliceLiteral l >>= return . DProperty n . Just
spliceDAnnotation da = return da

spliceEAnnotation :: Annotation Expression  -> ExprAnnGenerator
spliceEAnnotation (EProperty n (Just l)) = spliceLiteral l >>= return . EProperty n . Just
spliceEAnnotation ea = return ea


expectIdSplicer :: Identifier -> GeneratorM Identifier
expectIdSplicer   i = generatorWithSCtxt $ \sctxt -> liftParser i idFromParts >>= evalIdPartsSplice sctxt

expectTypeSplicer :: Identifier -> TypeGenerator
expectTypeSplicer i = generatorWithSCtxt $ \sctxt -> liftParser i typeEmbedding >>= evalTypeSplice sctxt

expectExprSplicer :: Identifier -> ExprGenerator
expectExprSplicer i = generatorWithSCtxt $ \sctxt -> liftParser i exprEmbedding >>= evalExprSplice sctxt

expectLiteralSplicer :: String -> LiteralGenerator
expectLiteralSplicer i = generatorWithSCtxt $ \sctxt -> liftParser i literalEmbedding >>= evalLiteralSplice sctxt

evalIdPartsSplice :: SpliceContext -> Either [MPEmbedding] Identifier -> GeneratorM Identifier
evalIdPartsSplice sctxt (Left l)  = return . concat =<< mapM (evalIdSplice sctxt) l
evalIdPartsSplice _ (Right i) = return i

evalIdSplice :: SpliceContext -> MPEmbedding -> GeneratorM Identifier
evalIdSplice sctxt m = evalEmbedding sctxt m >>= \case
  SLabel i -> return i
  _ -> spliceFail "Invalid splice identifier embedding"

evalTypeSplice :: SpliceContext -> Either MPEmbedding (K3 Type) -> TypeGenerator
evalTypeSplice sctxt (Left m) = evalEmbedding sctxt m >>= \case
    SType t -> return t
    _ -> spliceFail "Invalid splice type value"

evalTypeSplice _ (Right t) = return t

evalExprSplice :: SpliceContext -> Either MPEmbedding (K3 Expression) -> ExprGenerator
evalExprSplice sctxt (Left m) = evalEmbedding sctxt m >>= \case
    SExpr e -> return e
    _ -> spliceFail "Invalid splice expression value"

evalExprSplice _ (Right e) = return e

evalLiteralSplice :: SpliceContext -> Either MPEmbedding (K3 Literal) -> LiteralGenerator
evalLiteralSplice sctxt (Left m) = evalEmbedding sctxt m >>= \case
    SLiteral l -> return l
    _ -> spliceFail "Invalid splice literal value"

evalLiteralSplice _ (Right l) = return l

evalEmbedding :: SpliceContext -> MPEmbedding -> GeneratorM SpliceValue
evalEmbedding _ (MPENull i) = return $ SLabel i

evalEmbedding sctxt (MPEPath var path) = maybe evalErr (flip matchPath path) $ lookupSCtxt var sctxt
  where matchPath v [] = return v
        matchPath v (h:t) = maybe evalErr (flip matchPath t) $ spliceRecordField v h
        evalErr = spliceIdPathFail var path "lookup failed"

evalEmbedding sctxt (MPEHProg expr) = evalHaskellProg sctxt expr

spliceIdPathFail :: Identifier -> [Identifier] -> String -> GeneratorM a
spliceIdPathFail i path msg = throwG $ unwords ["Failed to splice", (intercalate "." $ [i]++path), ":", msg]

spliceFail :: String -> GeneratorM a
spliceFail msg = throwG $ unwords ["Splice failed:", msg]


{- Pattern matching -}
isPatternVariable :: Identifier -> Bool
isPatternVariable i = isPrefixOf "?" i

patternVariable :: Identifier -> Maybe Identifier
patternVariable i = stripPrefix "?" i

matchTree :: (Monad m) => (b -> K3 a -> K3 a -> m (Bool, b)) -> K3 a -> K3 a -> b -> m b
matchTree matchF t1 t2 z = matchF z t1 t2 >>= \(stop, acc) ->
  if stop then return acc
  else let (ch1, ch2) = (children t1, children t2) in
       if length ch1 == length ch2
         then foldM rcr acc $ zip ch1 ch2
         else fail "Mismatched children during matchTree"
       where rcr z' (t1',t2') = matchTree matchF t1' t2' z'

-- | Matches the first expression to the second, returning a splice environment
--   of pattern variables present in the second expression.
matchExpr :: K3 Expression -> K3 Expression -> Maybe SpliceEnv
matchExpr e patE = matchTree matchTag e patE emptySpliceEnv
  where
    matchTag sEnv e1 e2@(tag -> EVariable i)
      | isPatternVariable i =
          let nrEnv = spliceRecord $ (maybe [] typeRepr $ e1 @~ isEType) ++ [(spliceVESym, SExpr e1)]
              nsEnv = maybe sEnv (\n -> if null n then sEnv else addSpliceE n nrEnv sEnv) $ patternVariable i
          in do
              localLog $ debugMatchPVar i
              matchTypesAndAnnotations (annotations e1) (annotations e2) nsEnv >>= return . (True,)

    matchTag sEnv e1@(tag -> x) e2@(tag -> y)
      | x == y    = matchTypesAndAnnotations (annotations e1) (annotations e2) sEnv >>= return . (False,)
      | otherwise = Nothing

    matchTypesAndAnnotations :: [Annotation Expression] -> [Annotation Expression] -> SpliceEnv
                             -> Maybe SpliceEnv
    matchTypesAndAnnotations anns1 anns2 sEnv = case (find isEType anns1, find isEPType anns2) of
      (Just (EType ty), Just (EPType pty)) ->
          if   matchAnnotations ignoreUIDSpan anns1 anns2
          then matchType ty pty >>= return . mergeSpliceEnv sEnv
          else Nothing

      (_, _) -> if matchAnnotations ignoreUIDSpan anns1 anns2 then Just sEnv else Nothing

    typeRepr (EType ty) = [(spliceVTSym, SType ty)]
    typeRepr _ = []

    ignoreUIDSpan a = not (isEUID a || isESpan a)

    debugMatchPVar i =
      unwords ["isPatternVariable", show i, ":", show $ isPatternVariable i]


-- | Match two types, returning any pattern variables bound in the second argument.
matchType :: K3 Type -> K3 Type -> Maybe SpliceEnv
matchType t patT = matchTree matchTag t patT emptySpliceEnv
  where matchTag sEnv t1 (tag -> TDeclaredVar i)
          | isPatternVariable i =
              let extend n =
                    if null n then Nothing
                    else Just . (True,) $ addSpliceE n (spliceRecord [(spliceVTSym, SType t1)]) sEnv
              in maybe Nothing extend $ patternVariable i

        matchTag sEnv t1@(tag -> x) t2@(tag -> y)
          | x == y && matchMutability t1 t2 = Just (False, sEnv)
          | otherwise = Nothing

        matchMutability t1 t2 = (t1 @~ isTQualified) == (t2 @~ isTQualified)


-- | Match two annotation sets. For now this does not introduce any bindings,
--   rather it ensures that the second set of annotations are a subset of the first.
--   Thus matching acts as a constraint on the presence of annotation and properties
--   in any rewrite rules fired.
matchAnnotations :: (Eq (Annotation a)) => (Annotation a -> Bool) -> [Annotation a] -> [Annotation a] -> Bool
matchAnnotations a2FilterF a1 a2 = all (`elem` a1) $ filter a2FilterF a2


exprPatternMatcher :: [TypedSpliceVar] -> [PatternRewriteRule] -> [K3 Declaration] -> K3Generator
exprPatternMatcher spliceParams rules extensions = ExprRewriter $ \expr spliceEnv ->
    let vspliceEnv  = validateSplice spliceParams spliceEnv
        matchResult = foldl (tryMatch expr) Nothing rules
    in logValue (debugMatchResult matchResult)
         $ maybe (inputSR expr) (exprDeclSR vspliceEnv) matchResult

  where
    logValue msg v = runIdentity (localLog msg >> return v)

    inputSR expr = SRExpr $ return expr
    exprDeclSR spliceEnv (sEnv, rewriteE, ruleExts) =
      SRRewrite $ generateInSpliceEnv (mergeSpliceEnv spliceEnv sEnv) $
        (,) <$> spliceExpression rewriteE <*> mapM spliceNonAnnotationTree (extensions ++ ruleExts)

    tryMatch _ acc@(Just _) _ = acc
    tryMatch expr Nothing (pat, rewrite, ruleExts) =
      (localLogAction (tryMatchLogger expr pat) $ matchExpr expr pat) >>= return . (, rewrite, ruleExts)

    tryMatchLogger expr pat = maybe (Just $ debugMatchStep expr pat) (Just . debugMatchStepResult expr pat)

    debugMatchStep expr pat = boxToString $
          ["Trying match step "] %+ prettyLines pat %+ [" on "] %+ prettyLines expr

    debugMatchStepResult expr pat r = boxToString $
          ["Match step result "] %+ prettyLines pat %+ [" on "] %+ prettyLines expr
      %$ (["Result "] %+ [show r])

    debugMatchResult opt = unwords ["Match result", show opt]

    spliceNonAnnotationTree d = mapTree spliceNonAnnotationDecl d

    spliceNonAnnotationDecl ch d@(tag -> DGlobal  n t eOpt) =
      spliceDeclParts n t eOpt >>= \(nn, nt, neOpt) ->
        return (overrideChildren ch $ foldl (@+) (DC.global nn nt neOpt) $ annotations d)

    spliceNonAnnotationDecl ch d@(tag -> DTrigger n t e) =
      spliceDeclParts n t (Just e) >>= \(nn, nt, neOpt) ->
        case neOpt of
          Nothing -> throwG "Invalid trigger body resulting from pattern splicing"
          Just ne -> return (overrideChildren ch $ foldl (@+) (DC.trigger nn nt ne) $ annotations d)

    spliceNonAnnotationDecl _ _ = throwG "Invalid declaration in control annotation extensions"

    overrideChildren ch (Node n _) = Node n ch
