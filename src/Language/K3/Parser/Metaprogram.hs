{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Parser.Metaprogram where

import Control.Applicative

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

import Language.K3.Parser.DataTypes

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
makeDataAnnGenerators :: K3 Declaration -> DeclParser
makeDataAnnGenerators prog = undefined
--genOrCtor (n, [], (tp, mems)) = return . Just $ DC.dataAnnotation n [] tp mems
--genOrCtor (n, sp, (tp, mems)) =
--  let extendGen genEnv =
--        case lookupGenE n genEnv of
--          Nothing -> Right $ addGenE n (annotationSplicer (n,sp,(tp,mems))) genEnv
--          Just _  -> Left $ unwords ["Duplicate macro annotation for", n]
--  in modifyGEnvF_ extendGen >> return Nothing

applyDataAnnGenerators :: K3 Declaration -> DeclParser
applyDataAnnGenerators prog = undefined

makeCtrlAnnGenerators :: K3 Declaration -> DeclParser
makeCtrlAnnGenerators prog = undefined

applyCtrlAnnGenerators :: K3 Declaration -> DeclParser
applyCtrlAnnGenerators prog = undefined

{- Splice-checking -}
-- TODO: match splice parameter types (e.g., types vs label-types vs exprs.)
validateSplice :: [Identifier] -> SpliceEnv -> SpliceEnv
validateSplice spliceParams spliceEnv = Map.filterWithKey (\k _ -> k `elem` spliceParams) spliceEnv

{- Generator and splicer construction -}
globalSplicer :: Identifier -> K3 Type -> Maybe (K3 Expression) -> SpliceEnv -> SpliceResult
globalSplicer n t eOpt spliceEnv = SRDecl $ do
  nt <- parseInSpliceEnv spliceEnv $ spliceType t
  neOpt <- maybe (return Nothing) (\e -> parseInSpliceEnv spliceEnv (spliceExpression e) >>= return . Just) eOpt
  return $ DC.global n nt neOpt

annotationSplicer :: (Identifier, [Identifier], ([TypeVarDecl], [AnnMemDecl]))
                  -> SpliceEnv -> SpliceResult
annotationSplicer (n, spliceParams, (typeParams, mems)) spliceEnv = SRDecl $ do
  let vspliceEnv = validateSplice spliceParams spliceEnv
  nmems <- parseInSpliceEnv vspliceEnv $ spliceAnnMems mems
  withGUID $ \i -> DC.dataAnnotation (concat [n, "_", show i]) [] typeParams nmems

exprSplicer :: K3 Expression -> SpliceEnv -> SpliceResult
exprSplicer e spliceEnv = SRExpr $ parseInSpliceEnv spliceEnv $ spliceExpression e

typeSplicer :: K3 Type -> SpliceEnv -> SpliceResult
typeSplicer t spliceEnv = SRType $ parseInSpliceEnv spliceEnv $ spliceType t

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
