{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Parser.Metaprogram.Evaluation where

import Control.Applicative
import Control.Arrow
import Control.Monad

import Data.List
import qualified Data.Map as Map
import Data.Tree

import qualified Text.Parsec as P
import Text.Parser.Combinators
import Text.Parser.Token

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser.Metaprogram.DataTypes
import Language.K3.Parser.DataTypes

import Language.K3.Utils.Pretty


{- Basic parsers -}
spliceParameters :: K3Parser [TypedSpliceVar]
spliceParameters = brackets (commaSep spliceParameter)

spliceParameter :: K3Parser TypedSpliceVar
spliceParameter  = choice $ map try [labelSParam, typeSParam, exprSParam, declSParam, labelTypeSParam]
  where labelSParam      = (STLabel,)     <$> (keyword "label" *> identifier)
        typeSParam       = (STType,)      <$> (keyword "type"  *> identifier)
        exprSParam       = (STExpr,)      <$> (keyword "expr"  *> identifier)
        declSParam       = (STDecl,)      <$> (keyword "decl"  *> identifier)
        labelTypeSParam  = (STLabelType,) <$> identifier

{- Top-level AST transformations -}
evalMetaprogram :: K3 MPDeclaration -> K3Parser (Maybe (K3 Declaration))
evalMetaprogram mp = foldMapTree evalMPDecl [] mp >>= \case
    []  -> return Nothing
    [x] -> return (Just x)
    l   -> evalErrL l $ unwords ["Invalid metaprogram evaluation (", show (length l), "results):"]

  where
    evalMPDecl :: [[K3 Declaration]] -> K3 MPDeclaration -> K3Parser [K3 Declaration]
    evalMPDecl ch (tag &&& annotations -> (Staged (MDataAnnotation n [] tvars mems), anns)) =
      return $ rwNode (DC.dataAnnotation n tvars mems) (rwAnns anns) ch

    evalMPDecl ch (tag -> Staged (MDataAnnotation n svars tvars mems))
      | null (concat ch) =
        let extendGen genEnv =
              case lookupDGenE n genEnv of
                Nothing -> Right $ addDGenE n (annotationSplicer n svars tvars mems) genEnv
                Just _  -> Left $ unwords ["Duplicate metaprogrammed data annotation for", n]
        in modifyGEnvF_ extendGen >> return []

      | otherwise = evalErrLL ch
                      $ unwords ["Invalid data annotation generator", n, "with non-empty children"]

    evalMPDecl ch (tag &&& annotations -> (Staged (MCtrlAnnotation n svars rewriteRules extensions), anns)) =
      let extendGen genEnv =
            case lookupCGenE n genEnv of
              Nothing -> Right $ addCGenE n (exprPatternMatcher svars rewriteRules extensions) genEnv
              Just _  -> Left $ unwords ["Duplicate metaprogrammed control annotation for", n]
      in modifyGEnvF_ extendGen >> return (rwNode (DC.ctrlAnnotation n svars rewriteRules extensions) (rwAnns anns) ch)

    evalMPDecl ch (tag &&& annotations -> (Unstaged d, anns)) = return $ mkNode d (rwAnns anns) ch

    evalMPDecl _ n = evalErrL [n] $ unwords ["Invalid metaprogram node"]

    mkNode :: Declaration -> [Annotation Declaration] -> [[K3 Declaration]] -> [K3 Declaration]
    mkNode t anns ch = [Node (t :@: anns) $ concat ch]

    rwNode :: K3 Declaration -> [Annotation Declaration] -> [[K3 Declaration]] -> [K3 Declaration]
    rwNode (Node (t :@: anns) _) nanns ch = [Node (t :@: (anns ++ nanns)) $ concat ch]

    rwAnns :: [Annotation MPDeclaration] -> [Annotation Declaration]
    rwAnns l = map (\case { MPSpan sp -> DSpan sp }) l

    evalErrL  ch msg = P.parserFail $ boxToString $ [msg] ++ concatMap prettyLines ch
    evalErrLL ch msg = P.parserFail $ boxToString $ [msg] ++ concatMap (concatMap prettyLines) ch


{- Splice-checking -}
-- TODO: match splice parameter types (e.g., types vs label-types vs exprs.)
validateSplice :: [TypedSpliceVar] -> SpliceEnv -> SpliceEnv
validateSplice spliceParams spliceEnv =
  let paramIds = map snd spliceParams
  in Map.filterWithKey (\k _ -> k `elem` paramIds) spliceEnv

{- Splicer construction -}
globalSplicer :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3Generator
globalSplicer n t eOpt = Splicer $ \spliceEnv -> SRDecl $ do
  nt <- parseInSpliceEnv spliceEnv $ spliceType t
  neOpt <- maybe (return Nothing) (\e -> parseInSpliceEnv spliceEnv (spliceExpression e) >>= return . Just) eOpt
  return $ DC.global n nt neOpt

annotationSplicer :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> K3Generator
annotationSplicer n spliceParams typeParams mems = Splicer $ \spliceEnv -> SRDecl $ do
  let vspliceEnv = validateSplice spliceParams spliceEnv
  nmems <- parseInSpliceEnv vspliceEnv $ spliceAnnMems mems
  withGUID $ \i -> DC.dataAnnotation (concat [n, "_", show i]) typeParams nmems

exprSplicer :: K3 Expression -> K3Generator
exprSplicer e = Splicer $ \spliceEnv -> SRExpr $ parseInSpliceEnv spliceEnv $ spliceExpression e

typeSplicer :: K3 Type -> K3Generator
typeSplicer t = Splicer $ \spliceEnv -> SRType $ parseInSpliceEnv spliceEnv $ spliceType t

{- Splice evaluation -}
spliceAnnMems :: [AnnMemDecl] -> K3Parser [AnnMemDecl]
spliceAnnMems mems = flip mapM mems $ \case
  Lifted    p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Lifted    p sn st seOpt mAnns
  Attribute p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Attribute p sn st seOpt mAnns
  m -> return m

spliceDeclParts :: Identifier -> K3 Type -> Maybe (K3 Expression) -> K3Parser (Identifier, K3 Type, Maybe (K3 Expression))
spliceDeclParts n t eOpt = do
  sn <- spliceIdentifier n
  st <- spliceType t
  seOpt <- maybe (return Nothing) (\e -> spliceExpression e >>= return . Just) eOpt
  return (sn, st, seOpt)

spliceIdentifier :: Identifier -> K3Parser Identifier
spliceIdentifier i = expectIdSplicer i

spliceType :: K3 Type -> TypeParser
spliceType = mapTree doSplice
  where
    doSplice [] t@(tag -> TDeclaredVar i) = expectTypeSplicer i >>= \nt -> return $ foldl (@+) nt $ annotations t
    doSplice ch t@(tag -> TRecord ids) = mapM spliceIdentifier ids >>= \nids -> return $ Node (TRecord nids :@: annotations t) ch
    doSplice ch (Node tg _) = return $ Node tg ch

spliceExpression :: K3 Expression -> ExpressionParser
spliceExpression = mapTree doSplice
  where
    doSplice [] e@(tag -> EVariable i)           = expectExprSplicer i      >>= \ne   -> return $ foldl (@+) ne $ annotations e
    doSplice ch e@(tag -> ERecord ids)           = mapM expectIdSplicer ids >>= \nids -> return $ Node (ERecord nids :@: annotations e) ch
    doSplice ch e@(tag -> EProject i)            = expectIdSplicer i        >>= \nid  -> return $ Node (EProject nid :@: annotations e) ch
    doSplice ch e@(tag -> EAssign i)             = expectIdSplicer i        >>= \nid  -> return $ Node (EAssign nid :@: annotations e) ch
    doSplice ch e@(tag -> EConstant (CEmpty ct)) = spliceType ct            >>= \nct  -> return $ Node (EConstant (CEmpty nct) :@: annotations e) ch
    doSplice ch (Node tg _) = return $ Node tg ch

expectIdSplicer :: Identifier -> K3Parser Identifier
expectIdSplicer   i = parseSplice i $ choice [try idFromParts, identifier]

expectTypeSplicer :: Identifier -> TypeParser
expectTypeSplicer i = parseSplice i $ choice [try typeFromParts, identifier >>= return . TC.declaredVar]

expectExprSplicer :: Identifier -> ExpressionParser
expectExprSplicer i = parseSplice i $ choice [try exprFromParts, identifier >>= return . EC.variable]

idFromParts :: K3Parser Identifier
idFromParts = evalIdSplice =<< identParts

typeFromParts :: TypeParser
typeFromParts = evalTypeSplice =<< identParts

exprFromParts :: ExpressionParser
exprFromParts = evalExprSplice =<< identParts

evalIdSplice :: [Either String TypedSpliceVar] -> K3Parser Identifier
evalIdSplice l = return . concat =<< (flip mapM l $ \case
  Left n -> return n
  Right (STLabel, i) -> evalSplice i spliceVIdSym $ \case { SLabel n -> return n; _ -> spliceTypeFail i spliceVIdSym }
  Right (_, i) -> spliceTypeFail i spliceVIdSym)

evalTypeSplice :: [Either String TypedSpliceVar] -> TypeParser
evalTypeSplice = \case
  [Right (STType, i)] -> evalSplice i spliceVTSym $ \case { SType  t -> return t; _ -> spliceTypeFail i spliceVTSym }
  l -> spliceTypeFail (partsAsString l) spliceVTSym

evalExprSplice :: [Either String TypedSpliceVar] -> ExpressionParser
evalExprSplice = \case
  [Right (STExpr, i)] -> evalSplice i spliceVESym $ \case { SExpr  e -> return e; _ -> spliceTypeFail i spliceVESym }
  l -> spliceTypeFail (partsAsString l) spliceVESym

parseSplice :: String -> K3Parser a -> K3Parser a
parseSplice s p = P.getState >>= \st -> either P.parserFail return $ stringifyError $ runK3Parser (Just st) p s

evalSplice :: Identifier -> String -> (SpliceValue -> K3Parser a) -> K3Parser a
evalSplice i kind f = parserWithSCtxt $ \ctxt -> maybe (spliceFail i kind "lookup failed") f $ lookupSCtxt i kind ctxt

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
      SRRewrite $ parseInSpliceEnv (mergeSpliceEnv spliceEnv sEnv) $
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
          Nothing -> P.parserFail "Invalid trigger body resulting from pattern splicing"
          Just ne -> return (overrideChildren ch $ foldl (@+) (DC.trigger nn nt ne) $ annotations d)

    spliceNonAnnotationDecl _ _ = P.parserFail "Invalid declaration in control annotation extensions"

    overrideChildren ch (Node n _) = Node n ch
