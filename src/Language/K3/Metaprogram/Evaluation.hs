{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Metaprogram.Evaluation where

import Control.Applicative
import Control.Arrow
import Control.Monad

import Data.List
import qualified Data.Map as Map
import Data.Tree

import qualified Text.Parsec as P
import Text.Parser.Combinators

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
import Language.K3.Parser.DataTypes
import Language.K3.Utils.Pretty

{- Top-level AST transformations -}
evalMetaprogram :: K3 Declaration -> (Either String (K3 Declaration), GeneratorState)
evalMetaprogram mp = runGeneratorM emptyGeneratorState synthesizedProg
  where synthesizedProg = do
          pWithDataAnns  <- mpGenerators mp
          pWithMDataAnns <- applyDAnnGens pWithDataAnns
          -- TODO: typecheck
          applyCAnnGens pWithMDataAnns

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
            DDataAnnotation n _ _ -> modifyGDeclsF_ (Right . (++[decl])) >> return (aCtor n)
            _ -> throwE $ boxToString $ ["Invalid data annotation splice"] %+ prettyLines decl

        expectSpliceAnnotation _ = throwE "Invalid data annotation splice"

        spliceLookupErr n = throwE $ unwords ["Could not find macro", n]


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

    eApplyAnn ncAnns e (EApplyGen True n senv) = applyCAnnotation e n senv >>= \ne -> return (foldl (@+) ne ncAnns)
    eApplyAnn _ e _ = return e


applyCAnnotation :: K3 Expression -> Identifier -> SpliceEnv -> ExprGenerator
applyCAnnotation targetE cAnnId sEnv = generatorWithGEnv $ \gEnv ->
    maybe (spliceLookupErr cAnnId) (\g -> injectRewrite $ g targetE sEnv) $ lookupERWGenE cAnnId gEnv

  where
    injectRewrite (SRExpr p) = p
    injectRewrite (SRRewrite p) = p >>= \(rewriteE, decls) -> modifyGDeclsF_ (Right . (++ decls)) >> return rewriteE
    injectRewrite _ = throwE "Invalid control annotation rewrite"

    spliceLookupErr n = throwE $ unwords ["Could not find macro", n]




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
  nmems <- generateInSpliceEnv vspliceEnv $ spliceAnnMems mems
  withGUID $ \i -> DC.dataAnnotation (concat [n, "_", show i]) typeParams nmems

exprSplicer :: K3 Expression -> K3Generator
exprSplicer e = Splicer $ \spliceEnv -> SRExpr $ generateInSpliceEnv spliceEnv $ spliceExpression e

typeSplicer :: K3 Type -> K3Generator
typeSplicer t = Splicer $ \spliceEnv -> SRType $ generateInSpliceEnv spliceEnv $ spliceType t

{- Splice evaluation -}
spliceAnnMems :: [AnnMemDecl] -> GeneratorM [AnnMemDecl]
spliceAnnMems mems = flip mapM mems $ \case
  Lifted    p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Lifted    p sn st seOpt mAnns
  Attribute p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Attribute p sn st seOpt mAnns
  m -> return m

spliceDeclParts :: Identifier -> K3 Type -> Maybe (K3 Expression) -> GeneratorM (Identifier, K3 Type, Maybe (K3 Expression))
spliceDeclParts n t eOpt = do
  sn <- spliceIdentifier n
  st <- spliceType t
  seOpt <- maybe (return Nothing) (\e -> spliceExpression e >>= return . Just) eOpt
  return (sn, st, seOpt)

spliceIdentifier :: Identifier -> GeneratorM Identifier
spliceIdentifier i = expectIdSplicer i

spliceType :: K3 Type -> TypeGenerator
spliceType = mapTree doSplice
  where
    doSplice [] t@(tag -> TDeclaredVar i) = expectTypeSplicer i >>= \nt -> return $ foldl (@+) nt $ annotations t
    doSplice ch t@(tag -> TRecord ids) = mapM spliceIdentifier ids >>= \nids -> return $ Node (TRecord nids :@: annotations t) ch
    doSplice ch (Node tg _) = return $ Node tg ch

spliceExpression :: K3 Expression -> ExprGenerator
spliceExpression = mapTree doSplice
  where
    doSplice [] e@(tag -> EVariable i)           = expectExprSplicer i      >>= \ne   -> return $ foldl (@+) ne $ annotations e
    doSplice ch e@(tag -> ERecord ids)           = mapM expectIdSplicer ids >>= \nids -> return $ Node (ERecord nids :@: annotations e) ch
    doSplice ch e@(tag -> EProject i)            = expectIdSplicer i        >>= \nid  -> return $ Node (EProject nid :@: annotations e) ch
    doSplice ch e@(tag -> EAssign i)             = expectIdSplicer i        >>= \nid  -> return $ Node (EAssign nid :@: annotations e) ch
    doSplice ch e@(tag -> EConstant (CEmpty ct)) = spliceType ct            >>= \nct  -> return $ Node (EConstant (CEmpty nct) :@: annotations e) ch
    doSplice ch (Node tg _) = return $ Node tg ch

expectIdSplicer :: Identifier -> GeneratorM Identifier
expectIdSplicer   i = generatorWithSCtxt $ \sctxt -> parseSplice i $ choice [try (idFromParts sctxt), identifier]

expectTypeSplicer :: Identifier -> TypeGenerator
expectTypeSplicer i = generatorWithSCtxt $ \sctxt -> parseSplice i $ choice [try (typeFromParts sctxt), identifier >>= return . TC.declaredVar]

expectExprSplicer :: Identifier -> ExprGenerator
expectExprSplicer i = generatorWithSCtxt $ \sctxt -> parseSplice i $ choice [try (exprFromParts sctxt), identifier >>= return . EC.variable]

parseSplice :: String -> K3Parser a -> GeneratorM a
parseSplice s p = either throwE return $ stringifyError $ runK3Parser Nothing p s

idFromParts :: SpliceContext -> K3Parser Identifier
idFromParts sctxt = evalIdSplice sctxt =<< identParts

typeFromParts :: SpliceContext -> TypeParser
typeFromParts sctxt = evalTypeSplice sctxt =<< identParts

exprFromParts :: SpliceContext -> ExpressionParser
exprFromParts sctxt = evalExprSplice sctxt =<< identParts

evalIdSplice :: SpliceContext -> [Either String TypedSpliceVar] -> K3Parser Identifier
evalIdSplice sctxt l = return . concat =<< (flip mapM l $ \case
  Left n -> return n
  Right (STLabel, i) -> evalSplice sctxt i spliceVIdSym $ \case { SLabel n -> return n; _ -> spliceTypeFail i spliceVIdSym }
  Right (_, i) -> spliceTypeFail i spliceVIdSym)

evalTypeSplice :: SpliceContext -> [Either String TypedSpliceVar] -> TypeParser
evalTypeSplice sctxt = \case
  [Right (STType, i)] -> evalSplice sctxt i spliceVTSym $ \case { SType  t -> return t; _ -> spliceTypeFail i spliceVTSym }
  l -> spliceTypeFail (partsAsString l) spliceVTSym

evalExprSplice :: SpliceContext -> [Either String TypedSpliceVar] -> ExpressionParser
evalExprSplice sctxt = \case
  [Right (STExpr, i)] -> evalSplice sctxt i spliceVESym $ \case { SExpr  e -> return e; _ -> spliceTypeFail i spliceVESym }
  l -> spliceTypeFail (partsAsString l) spliceVESym

evalSplice :: SpliceContext -> Identifier -> String -> (SpliceValue -> K3Parser a) -> K3Parser a
evalSplice sctxt i kind f = maybe (spliceFail i kind "lookup failed") f $ lookupSCtxt i kind sctxt

partsAsString :: [Either String TypedSpliceVar] -> String
partsAsString l = concat $ flip map l $ \case
  Left n -> n
  Right (STLabel, i)     -> "#["   ++ i ++ "]"
  Right (STType,  i)     -> "::["  ++ i ++ "]"
  Right (STExpr,  i)     -> "$["   ++ i ++ "]"
  Right (STLabelType, i) -> "#::[" ++ i ++ "]"
  Right (STDecl, i)      -> "$d["  ++ i ++ "]"

spliceTypeFail :: Identifier -> String -> K3Parser a
spliceTypeFail i kind = spliceFail i kind "invalid type"

spliceFail :: Identifier -> String -> String -> K3Parser a
spliceFail n kind msg = P.parserFail $ unwords ["Failed to splice a", kind , "symbol", n, ":", msg]


{- Pattern matching -}
isPatternVariable :: Identifier -> Bool
isPatternVariable i = isPrefixOf "?" i

patternVariable :: Identifier -> Maybe Identifier
patternVariable i = stripPrefix "?" i

matchTree :: (Monad m) => (b -> K3 a -> K3 a -> m b) -> K3 a -> K3 a -> b -> m b
matchTree matchF t1 t2 z = matchF z t1 t2 >>= \acc ->
  let (ch1, ch2) = (children t1, children t2) in
  if length ch1 == length ch2
    then foldM rcr acc $ zip ch1 ch2
    else fail "Mismatched children during matchTree"
  where rcr z' (t1',t2') = matchTree matchF t1' t2' z'

-- | Matches the first expression to the second, returning a splice environment
--   of pattern variables present in the second expression.
matchExpr :: K3 Expression -> K3 Expression -> Maybe SpliceEnv
matchExpr e patE = matchTree matchTag e patE emptySpliceEnv
  where matchTag sEnv e1 e2@(tag -> EVariable i)
          | isPatternVariable i =
              let nrEnv = mkSpliceReprEnv $ (maybe [] typeRepr $ e1 @~ isEType) ++ [(spliceVESym, SExpr e1)]
                  nsEnv = maybe sEnv (\n -> if null n then sEnv else addSpliceE n nrEnv sEnv) $ patternVariable i
              in matchTypesAndAnnotations (annotations e1) (annotations e2) nsEnv

        matchTag sEnv e1@(tag -> x) e2@(tag -> y)
          | x == y    = matchTypesAndAnnotations (annotations e1) (annotations e2) sEnv
          | otherwise = Nothing

        matchTypesAndAnnotations :: [Annotation Expression] -> [Annotation Expression] -> SpliceEnv -> Maybe SpliceEnv
        matchTypesAndAnnotations anns1 anns2 sEnv = case (find isEType anns1, find isEPType anns2) of
          (Just (EType ty), Just (EPType pty)) ->
              if matchAnnotations ignoreUIDSpan anns1 anns2
              then matchType ty pty >>= return . mergeSpliceEnv sEnv else Nothing

          (_, _) -> if matchAnnotations ignoreUIDSpan anns1 anns2 then Just sEnv else Nothing

        typeRepr (EType ty) = [(spliceVTSym, SType ty)]
        typeRepr _ = []

        ignoreUIDSpan a = not (isEUID a || isESpan a)

-- | Match two types, returning any pattern variables bound in the second argument.
matchType :: K3 Type -> K3 Type -> Maybe SpliceEnv
matchType t patT = matchTree matchTag t patT emptySpliceEnv
  where matchTag sEnv t1 (tag -> TDeclaredVar i)
          | isPatternVariable i =
              let extend n =
                    if null n then Nothing
                    else Just $ addSpliceE n (mkSpliceReprEnv [(spliceVTSym, SType t1)]) sEnv
              in maybe Nothing extend $ patternVariable i

        matchTag sEnv t1@(tag -> x) t2@(tag -> y)
          | x == y && matchMutability t1 t2 = Just sEnv
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
    let vspliceEnv = validateSplice spliceParams spliceEnv
    in maybe (inputSR expr) (exprDeclSR vspliceEnv) $ foldl (tryMatch expr) Nothing rules
  where
    inputSR expr = SRExpr $ return expr
    exprDeclSR spliceEnv (sEnv, rewriteE, ruleExts) =
      SRRewrite $ generateInSpliceEnv (mergeSpliceEnv spliceEnv sEnv) $
        (,) <$> spliceExpression rewriteE <*> mapM spliceNonAnnotationTree (extensions ++ ruleExts)

    tryMatch _ acc@(Just _) _ = acc
    tryMatch expr Nothing (pat, rewrite, ruleExts) = matchExpr expr pat >>= return . (, rewrite, ruleExts)

    spliceNonAnnotationTree d = mapTree spliceNonAnnotationDecl d

    spliceNonAnnotationDecl ch d@(tag -> DGlobal  n t eOpt) =
      spliceDeclParts n t eOpt >>= \(nn, nt, neOpt) ->
        return (overrideChildren ch $ foldl (@+) (DC.global nn nt neOpt) $ annotations d)

    spliceNonAnnotationDecl ch d@(tag -> DTrigger n t e) =
      spliceDeclParts n t (Just e) >>= \(nn, nt, neOpt) ->
        case neOpt of
          Nothing -> throwE "Invalid trigger body resulting from pattern splicing"
          Just ne -> return (overrideChildren ch $ foldl (@+) (DC.trigger nn nt ne) $ annotations d)

    spliceNonAnnotationDecl _ _ = throwE "Invalid declaration in control annotation extensions"

    overrideChildren ch (Node n _) = Node n ch
