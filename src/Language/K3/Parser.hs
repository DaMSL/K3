{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoMonoLocalBinds #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ViewPatterns #-}
-- | K3 Parser.
module Language.K3.Parser {-(
  K3Parser,
  qualifiedTypeExpr,
  typeExpr,
  qualifiedLiteral,
  literal,
  qualifiedExpr,
  expr,
  declaration,
  program,
  parseType,
  parseLiteral,
  parseExpression,
  parseDeclaration,
  parseSimpleK3,
  parseK3,
  stitchK3,
  ensureUIDs
)-} where

import Control.Applicative
import Control.Arrow
import Control.Monad

import Data.Maybe
import Data.Tree

import Debug.Trace

import System.FilePath
import qualified Text.Parsec as P
import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Token

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Metaprogram
import Language.K3.Core.Type
import Language.K3.Core.Utils

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Literal     as LC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser.DataTypes
import Language.K3.Parser.Operator
import Language.K3.Parser.ProgramBuilder
import Language.K3.Parser.Preprocessor

import qualified Language.K3.Analysis.Provenance.Core         as P
import qualified Language.K3.Analysis.Provenance.Constructors as PC
import qualified Language.K3.Analysis.SEffects.Core           as FS
import qualified Language.K3.Analysis.SEffects.Constructors   as FSC

import qualified Language.K3.Utils.Pretty.Syntax as S

{- Main parsing functions -}
parseType :: String -> Maybe (K3 Type)
parseType = maybeParser typeExpr

parseLiteral :: String -> Maybe (K3 Literal)
parseLiteral = maybeParser literal

parseExpression :: String -> Maybe (K3 Expression)
parseExpression = maybeParser expr

parseDeclaration :: String -> Maybe (K3 Declaration)
parseDeclaration s = either (const Nothing) mkRole $ runK3Parser Nothing (head <$> endBy1 declaration eof) s
  where mkRole l = Just $ DC.role defaultRoleName l

parseSimpleK3 :: String -> Maybe (K3 Declaration)
parseSimpleK3 s = either (const Nothing) Just $ runK3Parser Nothing (program True) s

stitchK3 :: [FilePath] -> String -> IO [String]
stitchK3 includePaths s = do
  searchPaths   <- if null includePaths then getSearchPath else return includePaths
  subFiles      <- processIncludes searchPaths (lines s) []
  subFileCtnts  <- trace (unwords ["subfiles:", show subFiles]) $ mapM readFile subFiles
  return $ subFileCtnts ++ [s]

parseK3 :: Bool -> [FilePath] -> String -> IO (Either String (K3 Declaration))
parseK3 noFeed includePaths s = do
  searchPaths   <- if null includePaths then getSearchPath else return includePaths
  subFiles      <- processIncludes searchPaths (defaultIncludes ++ lines s) []
  subFileCtnts  <- trace (unwords ["subfiles:", show subFiles]) $ mapM readFile subFiles
  let fileContents = map (False,) subFileCtnts ++ [(True,s)]
  parseK3WithIncludes noFeed fileContents $ DC.role defaultRoleName []

parseK3WithIncludes :: Bool -> [(Bool, String)] -> K3 Declaration -> IO (Either String (K3 Declaration))
parseK3WithIncludes noFeed fileContents initProg = do
  let parseE = foldl chainValidParse (return (initProg, Nothing)) fileContents
  case parseE of
    Left msg -> return $ Left msg
    Right (prog, _) -> return $ Right prog
  where
    chainValidParse parse (asDriver, src) = parse >>= parseAndCompose src asDriver

    parseAndCompose src asDriver (prog, parseEnvOpt) = do
      (prog', nEnv) <- parseAtLevel asDriver parseEnvOpt src
      nprog <- concatProgram prog prog'
      return (nprog, Just $ nEnv)

    parseAtLevel asDriver parseEnvOpt src =
      stringifyError $ flip (runK3Parser parseEnvOpt) src $ do
        decl <- program $ not asDriver || noFeed
        env  <- P.getState
        return (decl, env)

stitchK3Includes :: Bool -> [FilePath] -> [String] -> K3 Declaration -> IO (Either String (K3 Declaration))
stitchK3Includes noFeed includePaths includes prog = do
  searchPaths   <- if null includePaths then getSearchPath else return includePaths
  subFiles      <- processIncludes searchPaths (defaultIncludes ++ includes) []
  subFileCtnts  <- trace (unwords ["subfiles:", show subFiles]) $ mapM readFile subFiles
  let fileContents = map (False,) subFileCtnts
  iprogE <- parseK3WithIncludes noFeed fileContents $ DC.role defaultRoleName []
  return (iprogE >>= \p -> concatProgram p $ declareBuiltins prog)


{- K3 grammar parsers -}

-- TODO: inline testing
program :: Bool -> DeclParser
program noDriver = DSpan <-> (rule >>= selfContainedProgram)
  where rule = (DC.role defaultRoleName) . concat <$> (spaces *> endBy1 (roleBody noDriver) eof)

        selfContainedProgram d =
          if noDriver then return d
          else (mkBuilderDecls d >>= mkEntryPoints >>= mkBuiltins)

        mkBuilderDecls d
          | DRole n <- tag d, n == defaultRoleName =
              withBuilderDecls $ \decls -> Node (tag d :@: annotations d) (children d ++ decls)
          | otherwise = return d

        mkEntryPoints d = withEnv $ processInitsAndRoles d . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins


roleBody :: Bool -> K3Parser [K3 Declaration]
roleBody noDriver = do
    pushFrame
    decls <- rule
    frame <- popFrame
    if noDriver then return decls else postProcessRole decls frame
  where rule = some declaration >>= return . concat


{- Declarations -}
declaration :: K3Parser [K3 Declaration]
declaration = (//) attachComment <$> comment False <*>
              choice [ignores >> return [], k3Decls, edgeDecls, driverDecls >> return []]

  where k3Decls     = choice $ map normalizeDeclAsList
                        [Right dGlobal, Right dTrigger,
                         Right dDataAnnotation, Right dControlAnnotation,
                         Left dTypeAlias]

        edgeDecls   = mapM ((DUID #) . return) =<< choice [dSource, dSink]
        driverDecls = dFeed
        ignores     = pInclude >> return ()

        props    p = DUID # withProperties "" True dProperties p
        optProps p = uidOfOpt =<< optWithProperties "" True dProperties p
        uidOfOpt Nothing  = return Nothing
        uidOfOpt (Just v) = (DUID # (return v)) >>= return . Just

        normalizeDeclAsList (Right p) = (:[]) <$> props p
        normalizeDeclAsList (Left  p) = try (optProps p) >>= return . maybe [] (:[])

        attachComment [] _      = []
        attachComment (h:t) cmt = (h @+ DSyntax cmt):t

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (mkGlobal <$>)
  where
    rule x = x <* colon <*> polymorphicTypeExpr <*> optional equateExpr
                                                <*> optional (effectSignature False)

    mkGlobal n qte eOpt eSigOpt =
      let glob = DC.global n qte (propagateQualifier qte eOpt) in
      case (eOpt, eSigOpt) of
        (Just _, Just _) -> glob
        (_, _) -> maybe glob (foldl (@+) glob) $ eSigOpt

dTrigger :: DeclParser
dTrigger = namedDecl "trigger" "trigger" $ rule . (DC.trigger <$>)
  where rule x = x <* colon <*> typeExpr <*> equateExpr

dEndpoint :: String -> String -> Bool -> K3Parser [K3 Declaration]
dEndpoint kind name isSource =
  attachFirst (DSpan <->) =<< (namedIdentifier kind name $ join . rule . (mkEndpoint <$>))
  where rule x      = ruleError =<< (x <*> (colon *> typeExpr) <*> (symbol "=" *> (endpoint isSource)))
        ruleError x = either unexpected pure x

        mkEndpoint n t endpointCstr = either Left (Right . mkDecls n t) $ endpointCstr n t
        mkEndpointT t = qualifyT $ if isSource then TC.source t else TC.sink t

        mkDecls n t (spec, eOpt, subdecls) = do
          epDecl <- trackEndpoint spec $ DC.endpoint n (mkEndpointT t) eOpt []
          return $ epDecl:subdecls

        qualifyT t = if null $ filter isTQualified $ annotations t then t @+ TImmutable else t

        attachFirst _ []    = return []
        attachFirst f (h:t) = f (return h) >>= return . (:t)

dSource :: K3Parser [K3 Declaration]
dSource = dEndpoint "source" "source" True

dSink :: K3Parser [K3 Declaration]
dSink = dEndpoint "sink" "sink" False

dFeed :: K3Parser ()
dFeed = track $ mkFeed <$> (feedSym *> identifier) <*> bidirectional <*> identifier
  where feedSym             = choice [keyword "feed", void $ symbol "~~"]
        bidirectional       = choice [symbol "|>" >> return True, symbol "<|" >> return False]
        mkFeed id1 lSrc id2 = if lSrc then (id1, id2) else (id2, id1)
        track p             = (trackBindings =<<) $ declError "feed" $ p

-- | Data annotation parsing.
--   This covers metaprogramming for data annotations when an annotation defines splice parameters.
dDataAnnotation :: DeclParser
dDataAnnotation = namedIdentifier "data annotation" "annotation" rule
  where rule x = mkAnnotation <$> x <*> option [] spliceParameterDecls <*> typesParamsAndMembers
        typesParamsAndMembers = parseInMode Splice $ protectedVarDecls typeParameters $ braces (some member)
        typeParameters = keyword "given" *> keyword "type" *> typeVarDecls <|> return []
        member = try (Left <$> mpAnnotationMember) <|> try (Right <$> annotationMember)
        mkAnnotation n sp (tp, mems) = DC.generator $ mpDataAnnotation n sp tp mems


{- Annotation declaration members -}
annotationMember :: K3Parser AnnMemDecl
annotationMember = memberError $ mkMember <$> annotatedRule
  where
    rule          = (,,) <$> polarity <*> (choice $ map uidOver [liftedOrAttribute, subAnnotation])
                                      <*> optional (effectSignature True)
    annotatedRule = wrapInComments $ spanned $ flip (,) <$> optionalProperties "" dProperties <*> rule

    liftedOrAttribute = mkLA  <$> optional (keyword "lifted")
                              <*> identifier <* colon
                              <*> qualifiedTypeExpr
                              <*> optional equateExpr

    subAnnotation     = mkSub <$> (keyword "annotation" *> identifier)

    mkMember ((((p, Left (Just _,  n, qte, eOpt, uid), eSigOpt), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props) (effectAnnot eSigOpt eOpt)
        $ Lifted p n qte (propagateQualifier qte eOpt)

    mkMember ((((p, Left (Nothing, n, qte, eOpt, uid), eSigOpt), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props) (effectAnnot eSigOpt eOpt)
        $ Attribute p n qte (propagateQualifier qte eOpt)

    mkMember ((((p, Right (n, uid), _), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props) [] $ MAnnotation p n

    mkLA kOpt n qte eOpt uid = Left (kOpt, n, qte, eOpt, uid)
    mkSub n uid              = Right (n, uid)

    attachMemAnnots uid spn cmts props effectAnns memCtor =
      memCtor $ (DUID uid):(DSpan spn):(effectAnns ++ props ++ map DSyntax cmts)

    wrapInComments p = (\a b c -> (b, a ++ c)) <$> comment False <*> p <*> comment True

    memberError = parseError "annotation" "member"

    effectAnnot (Just _) (Just _) = []
    effectAnnot eSigOpt _ = maybe [] id eSigOpt

polarity :: K3Parser Polarity
polarity = choice [keyword "provides" >> return Provides,
                   keyword "requires" >> return Requires]

mpAnnotationMember :: K3Parser MPAnnMemDecl
mpAnnotationMember = mpAnnMemDecl <$> (keyword "for" *> parseInMode Normal identifier)
                                  <*> (keyword "in" *> svTerm <* colon)
                                  <*> some annotationMember


-- | Control annotation parsing
dControlAnnotation :: DeclParser
dControlAnnotation = namedIdentifier "control annotation" "control" $ rule
  where rule x = mkCtrlAnn <$> x <*> option [] spliceParameterDecls <*> braces body
        body           = (,) <$> some cpattern <*> (extensions $ keyword "shared")
        cpattern       = (,,) <$> patternE <*> (symbol "=>" *> rewriteE) <*> (extensions $ symbol "+>")

        extensions :: K3Parser a -> K3Parser [K3 Declaration]
        extensions pfx = concat <$> option [] (try $ pfx *> braces (many extensionD))
        rewriteE       = cleanExpr =<< parseInMode Splice expr
        patternE       = cleanExpr =<< parseInMode SourcePattern expr
        extensionD     = mapM cleanDecl =<< parseInMode Splice declaration

        mkCtrlAnn n svars (rw, exts) = DC.generator $ mpCtrlAnnotation n svars rw exts

        cleanExpr e = return $ stripEUIDSpan e
        cleanDecl d = return $ stripDUIDSpan d


dTypeAlias :: K3Parser (Maybe (K3 Declaration))
dTypeAlias = namedIdentifier "typedef" "typedef" rule
  where rule x = mkTypeAlias =<< ((,) <$> x <*> equateTypeExpr)
        mkTypeAlias (n, t) = modifyTAEnvF_ (Right . appendTAliasE n (stripTUIDSpan t)) >> return Nothing

pInclude :: K3Parser String
pInclude = keyword "include" >> stringLiteral


{- Types -}
typeExpr :: TypeParser
typeExpr = typeError "expression" $ TUID # withProperties ":" False tProperties tTermOrFun

qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = typeExprError "qualified" $ flip (@+) <$> (option TImmutable typeQualifier) <*> typeExpr

polymorphicTypeExpr :: TypeParser
polymorphicTypeExpr =
  typeExprError "polymorphic" $ (TUID # forAllParser) <|> qualifiedTypeExpr
  where
    forAllParser = (uncurry TC.forAll) <$> (keyword "forall" *> protectedVarDecls typeVarDecls (symbol "." *> qualifiedTypeExpr))

typeVarDecls :: K3Parser [TypeVarDecl]
typeVarDecls = sepBy typeVarDecl (symbol ",")

-- | Parses a set of type variable declarations and subsequently applies a parser
--   in a type alias environment where the new declarations are protected (i.e., shadow)
--   any existing matching aliases.
protectedVarDecls :: K3Parser [TypeVarDecl] -> K3Parser a -> K3Parser ([TypeVarDecl], a)
protectedVarDecls declP p = do
  tvdecls <- declP
  void $ modifyTAEnvF_ (Right . (pushTAliasE $ map (\(TypeVarDecl i _ _) -> (i, TC.declaredVar i)) tvdecls))
  r <- p
  void $ modifyTAEnvF_ $ Right . popTAliasE
  return (tvdecls, r)

typeVarDecl :: K3Parser TypeVarDecl
typeVarDecl = TypeVarDecl <$> identifier <*> pure Nothing <*> option Nothing (Just <$ symbol "<=" <*> typeExpr)

typeQualifier :: K3Parser (Annotation Type)
typeQualifier = typeError "qualifier" $ choice [keyword "immut" >> return TImmutable,
                                                keyword "mut"   >> return TMutable]

{- Type terms -}
tTerm :: TypeParser
tTerm = TSpan <-> (//) attachComment <$> comment False
              <*> choice [ tPrimitive, tOption, tIndirection, tTrigger,
                           tTupleOrNested, tRecord, tCollection,
                           tBuiltIn, tDeclared ]
  where attachComment t cmt = t @+ (TSyntax cmt)

tTermOrFun :: TypeParser
tTermOrFun = TSpan <-> mkTermOrFun <$> (TUID # tTerm) <*> optional (symbol "->" *> typeExpr)
  where mkTermOrFun t (Just rt) = TC.function t rt
        mkTermOrFun t Nothing   = clean t
        clean t                 = stripAnnot isTSpan $ stripAnnot isTUID t
        stripAnnot f t          = maybe t (t @-) $ t @~ f
tPrimitive :: TypeParser
tPrimitive = tPrimError $ choice $ map tConst [ ("bool",    TC.bool)
                                              , ("int",     TC.int)
                                              , ("real",    TC.real)
                                              , ("string",  TC.string)
                                              , ("address", TC.address) ]
  where tConst (x, f) = keyword x >> return f
        tPrimError    = typeExprError "primitive"

tQNested :: (K3 Type -> K3 Type) -> String -> TypeParser
tQNested f k = f <$> (keyword k *> qualifiedTypeExpr)

tOption :: TypeParser
tOption = typeExprError "option" $ tQNested TC.option "option"

tIndirection :: TypeParser
tIndirection = typeExprError "indirection" $ tQNested TC.indirection "ind"

tTrigger :: TypeParser
tTrigger = typeExprError "trigger" $ TC.trigger <$> (keyword "trigger" *> typeExpr)

tTupleOrNested :: TypeParser
tTupleOrNested = choice [try unit, try (parens $ nestErr $ clean <$> typeExpr), parens $ tupErr tTuple]
  where unit             = typeExprError "unit" $ symbol "(" *> symbol ")" >> return (TC.unit)
        tTuple           = mkTypeTuple <$> qualifiedTypeExpr <* comma <*> commaSep1 qualifiedTypeExpr
        mkTypeTuple t tl = TC.tuple $ t:tl

        clean t          = stripAnnot isTSpan $ stripAnnot isTUID t
        stripAnnot f t   = maybe t (t @-) $ t @~ f
        nestErr          = typeExprError "nested"
        tupErr           = typeExprError "tuple"

tRecord :: TypeParser
tRecord = typeExprError "record" $ ( TUID # ) $
            TC.record <$> (braces . commaSep) idQType

tCollection :: TypeParser
tCollection = cErr $ TUID # mkCollectionType
                  <$> ((keyword "collection" *> choice [tRecord, tDeclared]) >>= attachAnnotations)
  where mkCollectionType (t, a) = foldl (@+) (TC.collection t) a
        cErr = typeExprError "collection"

        attachAnnotations t@(tag -> TRecord ids) = do
          a <- withAnnotations (tAnnotations $ Just $ mkRecordSpliceEnv ids $ children $ stripTUIDSpan t)
          return (t,a)

        attachAnnotations t = withAnnotations (tAnnotations Nothing) >>= return . (t,)

tBuiltIn :: TypeParser
tBuiltIn = typeExprError "builtin" $ choice $ map (\(kw,bi) -> keyword kw >> return (TC.builtIn bi))
              [ ("self",TSelf)
              , ("structure",TStructure)
              , ("horizon",THorizon)
              , ("content",TContent) ]

tDeclared :: TypeParser
tDeclared = typeExprError "declared" $ TUID # ( aliasOrDecl =<< identifier )
  where aliasOrDecl i = withTAEnv $ \tae -> maybe (TC.declaredVar i) id $ lookupTAliasE i tae


{- Expressions -}

expr :: ExpressionParser
expr = parseError "expression" "k3"
        $ withProperties "" False eProperties $ buildExpressionParser fullOpTable eApp

nonSeqExpr :: ExpressionParser
nonSeqExpr = buildExpressionParser nonSeqOpTable eApp

qualifiedExpr :: ExpressionParser
qualifiedExpr = exprError "qualified" $ flip (@+) <$> (option EImmutable exprQualifier) <*> expr

exprQualifier :: K3Parser (Annotation Expression)
exprQualifier = suffixError "expression" "qualifier" $
      keyword "immut" *> return EImmutable
  <|> keyword "mut"   *> return EMutable

exprNoneQualifier :: K3Parser NoneMutability
exprNoneQualifier = suffixError "expression" "option qualifier" $
      keyword "immut" *> return NoneImmut
  <|> keyword "mut"   *> return NoneMut

eTerm :: ExpressionParser
eTerm = do
  e  <- EUID # (ESpan <-> rawTerm)
  mi <- many (spanned eProject)
  case mi of
    [] -> return e
    l  -> foldM attachProjection e l
  where
    rawTerm = wrapInComments $ eWithProperties $ eWithAnnotations $ asSourcePattern attachPType id $
        choice [ (try eAssign),
                 (try eAddress),
                 eLiterals,
                 eLambda,
                 eCondition,
                 eLet,
                 eCase,
                 eBind,
                 eSelf ]

    eProject :: K3Parser ((String, Maybe (K3 Type)), [Annotation Expression])
    eProject = prjWithAnnotations $ dot *> identifier

    -- attachProjection :: _
    attachProjection e (((i, tOpt), anns), sp) =
      EUID # (return $ foldl (@+) (EC.project i e) $ [ESpan sp] ++ maybe [] ((:[]) . EPType) tOpt ++ anns)

    eWithProperties    p = withProperties "" False eProperties p

    eWithAnnotations :: K3Parser (K3 Expression) -> K3Parser (K3 Expression)
    eWithAnnotations   p = foldl (@+) <$> p <*> withAnnotations eCAnnotations

    prjWithAnnotations p = (,) <$> asSourcePattern (,) (,Nothing) p <*> withAnnotations eCAnnotations

    asSourcePattern :: (a -> Maybe (K3 Type) -> b) -> (a -> b) -> K3Parser a -> K3Parser b
    asSourcePattern pctor ctor p = parserWithPMode $ \case
      SourcePattern -> pctor <$> p <*> optional (try $ colon *> typeExpr)
      _ -> ctor <$> p

    attachPType :: K3 Expression -> Maybe (K3 Type) -> K3 Expression
    attachPType e tOpt = maybe e ((e @+) . EPType) tOpt

    wrapInComments p = (\c1 e c2 -> (//) attachComment (c1++c2) e) <$> comment False <*> p <*> comment True
    attachComment e cmt = e @+ (ESyntax cmt)


{- Expression literals -}
eLiterals :: ExpressionParser
eLiterals = choice [
    try eCollection,
    eTerminal,
    eOption,
    eIndirection,
    eTuplePrefix,
    eRecord,
    eEmpty ]

{- Terminals -}
eTerminal :: ExpressionParser
eTerminal = choice [eConstant,
                    eVariable]

eConstant :: ExpressionParser
eConstant = exprError "constant" $ choice [eCBool, try eCNumber, eCString]

eCBool :: ExpressionParser
eCBool = (EC.constant . CBool) <$>
  choice [keyword "true" >> return True, keyword "false" >> return False]

eCNumber :: ExpressionParser
eCNumber = mkNumber <$> naturalOrDouble
  where mkNumber x = case x of
                      Left i  -> EC.constant . CInt . fromIntegral $ i
                      Right d -> EC.constant . CReal $ d

eCString :: ExpressionParser
eCString = EC.constant . CString <$> stringLiteral

eVariable :: ExpressionParser
eVariable = exprError "variable" $ parserWithPMode $ \mode -> mkVar mode <$> identifier
  where mkVar Normal ('\'':t) = EC.applyMany resolveFn [EC.constant $ CString t]
        mkVar _      i = EC.variable i

{- Complex literals -}
eOption :: ExpressionParser
eOption = exprError "option" $
  choice [EC.some <$> (keyword "Some" *> qualifiedExpr),
          keyword "None" *> (EC.constant . CNone <$> exprNoneQualifier)]

eIndirection :: ExpressionParser
eIndirection = exprError "indirection" $ EC.indirect <$> (keyword "ind" *> qualifiedExpr)

eTuplePrefix :: ExpressionParser
eTuplePrefix = choice [try unit, try eNested, eTupleOrSend]
  where unit          = symbol "(" *> symbol ")" >> return (EC.tuple [])
        eNested       = stripSpan  <$> parens expr
        eTupleOrSend  = do
                          elements <- parens $ commaSep1 qualifiedExpr
                          msuffix <- optional sendSuffix
                          mkTupOrSend elements msuffix

        sendSuffix :: K3Parser (K3 Expression, Maybe (Either (K3 Literal) (Either (K3 Literal) (K3 Literal))))
        sendSuffix = (,) <$> (symbol "<-" *> nonSeqExpr) <*> optional delay

        delay :: K3Parser (Either (K3 Literal) (Either (K3 Literal) (K3 Literal)))
        delay = choice $ map try [delayOverrideEdge, delayOverride, delayOnly]

        delayOnly :: K3Parser (Either (K3 Literal) (Either (K3 Literal) (K3 Literal)))
        delayOnly = Left <$> (keyword "delay" *> delayValue)

        delayOverride :: K3Parser (Either (K3 Literal) (Either (K3 Literal) (K3 Literal)))
        delayOverride = Right . Left  <$> ((keyword "delay" *> keyword "override") *> delayValue)

        delayOverrideEdge :: K3Parser (Either (K3 Literal) (Either (K3 Literal) (K3 Literal)))
        delayOverrideEdge = Right . Right <$> ((keyword "delay" *> keyword "override" *> keyword "edge") *> delayValue)

        mkTupOrSend [e] Nothing              = return $ stripSpan <$> e
        mkTupOrSend [e] (Just (arg, delayO)) = mkDelay (EC.binop OSnd e arg) delayO
        mkTupOrSend l Nothing                = return $ EC.tuple l
        mkTupOrSend l (Just (arg, delayO))   = (EC.binop OSnd <$> (EUID # return (EC.tuple l)) <*> pure arg) >>= \e -> mkDelay e delayO

        delayValue :: K3Parser (K3 Literal)
        delayValue = mkNumber <$> integerOrDouble <*> (choice $ map try $ [symbol "s", symbol "ms", symbol "us"])
          where mkNumber x units = case x of
                                     Left i  -> LC.int $ scale units $ fromIntegral i
                                     Right d -> LC.int $ scale units $ round d
                scale u i = case u of
                              "s" -> i * 1000000
                              "ms" -> i * 1000
                              _ -> i

        mkDelay :: K3 Expression -> Maybe (Either (K3 Literal) (Either (K3 Literal) (K3 Literal))) -> ExpressionParser
        mkDelay e Nothing = return e
        mkDelay e (Just (Left l@(tag -> LInt _)))          = return (e @+ (EProperty $ Left ("Delay", Just l)))
        mkDelay e (Just (Right (Left l@(tag -> LInt _))))  = return (e @+ (EProperty $ Left ("DelayOverride", Just l)))
        mkDelay e (Just (Right (Right l@(tag -> LInt _)))) = return (e @+ (EProperty $ Left ("DelayOverrideEdge", Just l)))
        mkDelay _ (Just _)                                 = fail "Invalid send delay constant"

        stripSpan e = maybe e (e @-) $ e @~ isESpan

eRecord :: ExpressionParser
eRecord = exprError "record" $ EC.record <$> braces idQExprList

eEmpty :: ExpressionParser
eEmpty = exprError "empty" $ mkEmpty <$> (typedEmpty >>= attachAnnotations)
  where mkEmpty (e, a) = foldl (@+) e a
        typedEmpty = EC.empty <$> (keyword "empty" *> choice [tRecord, tDeclared])

        attachAnnotations e@(tag -> EConstant (CEmpty t@(tag -> TRecord ids))) = do
          a <- withAnnotations (eAnnotations $ Just $ mkRecordSpliceEnv ids $ children $ stripTUIDSpan t)
          return (e,a)

        attachAnnotations e = withAnnotations (eAnnotations Nothing) >>= return . (e,)

eLambda :: ExpressionParser
eLambda = exprError "lambda" $ EC.lambda <$> choice [iArrow "fun", iArrowS "\\"] <*> nonSeqExpr

eApp :: ExpressionParser
eApp = do
  eTerms <- some (try eTerm) -- Have m [a], need [m a], ambivalent to effects
  try $ foldl1 mkApp $ map return eTerms
  where
    mkApp :: ExpressionParser -> ExpressionParser -> ExpressionParser
    mkApp x y = EUID # binOpSpan (EC.binop OApp) <$> x <*> y

eCondition :: ExpressionParser
eCondition = exprError "conditional" $
  EC.ifThenElse <$> (nsPrefix "if") <*> (ePrefix "then") <*> (ePrefix "else")

eAssign :: ExpressionParser
eAssign = exprError "assign" $ mkAssign <$> sepBy1 identifier dot <*> equateNSExpr
  where mkAssign [] _  = error "Invalid identifier list"
        mkAssign [n] e = EC.assign n e
        mkAssign l e   =
          let syms     = snd $ foldl pairSym (0::Int,[]) l
              wSyms    = zip (init syms) (tail syms)
              bindSyms = ((\((_,pn), (s,n)) -> (s,pn,n)) $ head wSyms)
                          : (map (\((ps,_),(s,n)) -> (s,ps,n)) . tail $ wSyms)
          in
          foldr (\(sym, prevSym, x) e' ->
                    EC.bindAs (EC.variable prevSym) (BRecord [(x, sym)]) e')
                (EC.assign ((\(x,_,_) -> x) $ last bindSyms) e) bindSyms

        pairSym (cnt,acc) n = (cnt+1,acc++[("__"++n++"_asn"++(show cnt), n)])

eLet :: ExpressionParser
eLet = exprError "let" $ EC.letIn <$> iPrefix "let" <*> equateQExpr <*> (keyword "in" *> expr)

eCase :: ExpressionParser
eCase = exprError "case" $ mkCase <$> ((nsPrefix "case") <* keyword "of")
                                  <*> (braces eCaseSome) <*> (braces eCaseNone)
  where eCaseSome = (,) <$> (iArrow "Some") <*> expr
        eCaseNone = (keyword "None" >> symbol "->") *> expr
        mkCase e (x, s) n = EC.caseOf e x s n

eBind :: ExpressionParser
eBind = exprError "bind" $ EC.bindAs <$> (nsPrefix "bind")
                                     <*> (keyword "as" *> eBinder) <*> (ePrefix "in")

eBinder :: BinderParser
eBinder = exprError "binder" $ choice [bindInd, bindTup, bindRec]

bindInd :: K3Parser Binder
bindInd = BIndirection <$> iPrefix "ind"

bindTup :: K3Parser Binder
bindTup = BTuple <$> parens idList

bindRec :: K3Parser Binder
bindRec = BRecord <$> braces idPairList

eAddress :: ExpressionParser
eAddress = exprError "address" $ EC.address <$> ipAddress <* colon <*> port
  where ipAddress = EUID # EC.constant . CString <$> (some $ choice [alphaNum, oneOf "."])
        port = EUID # EC.constant . CInt . fromIntegral <$> natural

eSelf :: ExpressionParser
eSelf = exprError "self" $ keyword "self" >> return EC.self

-- TODO: treating collection literals as immutable will fail when initializing mutable collections.
eCollection :: ExpressionParser
eCollection = exprError "collection" $
              mkCollection <$> (braces (choice [try singleField, multiField]) >>= attachAnnotations)
  where
        singleField =     (symbol "|" *> idQType <* symbol "|")
                      >>= mkSingletonRecord (commaSep1 expr <* symbol "|")

        multiField  = (\a b c -> ((a:b), c))
                          <$> (symbol "|" *> idQType <* comma)
                          <*> commaSep1 idQType
                          <*> (symbol "|" *> commaSep1 expr <* symbol "|")

        mkCollection ((tyl, el), a) = EC.letIn cId (emptyC tyl a) $ EC.binop OSeq (mkInserts el) cVar

        mkInserts el = foldl (\acc e -> EC.binop OSeq acc $ mkInsert e) (mkInsert $ head el) (tail el)
        mkInsert     = EC.binop OApp (EC.project "insert" cVar)
        emptyC tyl a = foldl (@+) ((EC.empty $ TC.record tyl) @+ EImmutable) a
        (cId, cVar)  = ("__collection", EC.variable "__collection")

        mkSingletonRecord p (n,t) =
          p >>= return . ([(n,t)],) . map (EC.record . (:[]) . (n,) . (@+ EImmutable))

        attachAnnotations (tyl, el) = do
          a <- withAnnotations (eAnnotations $ Just $ uncurry mkRecordSpliceEnv $ second (map stripTUIDSpan) $ unzip tyl)
          return ((tyl,el),a)


{- Literal values -}

literal :: LiteralParser
literal = parseError "literal" "k3" $ choice [
    try (lCollection),
    try (lAddress),
    lTerminal,
    lOption,
    lIndirection,
    lTuple,
    lRecord,
    lEmpty ]

qualifiedLiteral :: LiteralParser
qualifiedLiteral = litError "qualified" $ flip (@+) <$> (option LImmutable litQualifier) <*> literal

litQualifier :: K3Parser (Annotation Literal)
litQualifier = suffixError "literal" "qualifier" $
      keyword "immut" *> return LImmutable
  <|> keyword "mut"   *> return LMutable

lTerminal :: LiteralParser
lTerminal = litError "constant" $ choice [lBool, try lNumber, lString]

lBool :: LiteralParser
lBool = LC.bool <$> choice [keyword "true" >> return True, keyword "false" >> return False]

lNumber :: LiteralParser
lNumber = mkNumber <$> integerOrDouble
  where mkNumber x = case x of
                      Left i  -> LC.int . fromIntegral $ i
                      Right d -> LC.real $ d

lString :: LiteralParser
lString = LC.string <$> stringLiteral

lOption :: LiteralParser
lOption = litError "option" $ choice [
            LC.some <$> (keyword "Some" *> qualifiedLiteral),
            keyword "None" *> (LC.none <$> exprNoneQualifier)]

lIndirection :: LiteralParser
lIndirection = litError "indirection" $ LC.indirect <$> (keyword "ind" *> qualifiedLiteral)

lTuple :: LiteralParser
lTuple = choice [try unit, try lNested, litTuple]
  where unit       = symbol "(" *> symbol ")" >> return (LC.tuple [])
        lNested    = stripSpan <$> parens literal
        litTuple   = mkLTuple  <$> (parens $ commaSep1 qualifiedLiteral)

        mkLTuple [x] = stripSpan <$> x
        mkLTuple l   = LC.tuple l
        stripSpan l = maybe l (l @-) $ l @~ isLSpan

lRecord :: LiteralParser
lRecord = litError "record" $ LC.record <$> braces idQLitList

lEmpty :: LiteralParser
lEmpty = litError "empty" $ mkEmpty <$> (typedEmpty >>= attachAnnotations)
  where mkEmpty (l, a) = foldl (@+) l a
        typedEmpty = LC.empty <$> (keyword "empty" *> tRecord)

        attachAnnotations l@(tag -> LEmpty t@(tag -> TRecord ids)) = do
          a <- withAnnotations (lAnnotations $ Just $ mkRecordSpliceEnv ids $ children $ stripTUIDSpan t)
          return (l,a)

        attachAnnotations l = withAnnotations (lAnnotations Nothing) >>= return . (l,)

lCollection :: LiteralParser
lCollection = litError "collection" $
              mkCollection <$> (braces (choice [try singleField, multiField]) >>= attachAnnotations)
  where
    singleField =     (symbol "|" *> idQType <* symbol "|")
                  >>= mkSingletonRecord (commaSep1 literal <* symbol "|")

    multiField  = (\a b c -> ((a:b), c))
                    <$> (symbol "|" *> idQType <* comma)
                    <*> commaSep1 idQType
                    <*> (symbol "|" *> commaSep1 literal <* symbol "|")

    mkCollection ((tyl, el), a) = foldl (@+) ((LC.collection (TC.record tyl) el) @+ LImmutable) a

    mkSingletonRecord p (n,t) =
      p >>= return . ([(n,t)],) . map (LC.record . (:[]) . (n,) . (@+ LImmutable))

    attachAnnotations (tyl,el) = do
      a <- withAnnotations (lAnnotations $ Just $ uncurry mkRecordSpliceEnv $ second (map stripTUIDSpan) $ unzip tyl)
      return ((tyl,el),a)

lAddress :: LiteralParser
lAddress = litError "address" $ LC.address <$> ipAddress <* colon <*> port
  where ipAddress = LC.string <$> (some $ choice [alphaNum, oneOf "."])
        port = LC.int . fromIntegral <$> natural


{- Attachments -}
withAnnotations :: K3Parser [Annotation a] -> K3Parser [Annotation a]
withAnnotations p = option [] (try $ symbol "@" *> p)

tAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Type]
tAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = dAnnotationUse sEnv TAnnotation TApplyGen

eAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Expression]
eAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = dAnnotationUse sEnv EAnnotation $ EApplyGen False

lAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Literal]
lAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = dAnnotationUse sEnv LAnnotation LApplyGen

dAnnotationUse :: Maybe SpliceEnv -> AnnotationCtor a -> ApplyAnnCtor a -> K3Parser (Annotation a)
dAnnotationUse sEnvOpt aCtor apCtor = try mpOrAnn
  where mpOrAnn   = mkMpOrAnn =<< ((,) <$> identifier <*> annParams)
        annParams = option [] (parens $ commaSep1 $ contextualizedSpliceParameter sEnvOpt)
        mkMpOrAnn (n, [])    = return $ aCtor n
        mkMpOrAnn (n,params) = return $ apCtor n $ mkSpliceEnv $ catMaybes params

eCAnnotations :: K3Parser [Annotation Expression]
eCAnnotations = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = cAnnotationUse (EApplyGen True)

cAnnotationUse :: ApplyAnnCtor Expression -> K3Parser (Annotation Expression)
cAnnotationUse aCtor = mkSEnv <$> identifier <*> option [] (parens $ commaSep1 spliceParameter)
  where mkSEnv a b = aCtor a $ mkSpliceEnv $ catMaybes b

withPropertiesF :: (Eq (Annotation a))
                => String -> Bool -> K3Parser [Annotation a] -> K3Parser b -> (b -> [Annotation a] -> b) -> K3Parser b
withPropertiesF sfx asPrefix prop tree attachF =
    if asPrefix then (flip propertize) <$> optionalProperties sfx prop <*> tree
                else propertize <$> tree <*> optionalProperties sfx prop
  where propertize a (Just b) = attachF a b
        propertize a Nothing  = a

withProperties :: (Eq (Annotation a))
               => String -> Bool -> K3Parser [Annotation a] -> K3Parser (K3 a) -> K3Parser (K3 a)
withProperties sfx asPrefix prop tree = withPropertiesF sfx asPrefix prop tree $ foldl (@+)

optWithProperties :: (Eq (Annotation a))
                  => String -> Bool -> K3Parser [Annotation a] -> K3Parser (Maybe (K3 a)) -> K3Parser (Maybe (K3 a))
optWithProperties sfx asPrefix prop tree = withPropertiesF sfx asPrefix prop tree attachOpt
  where attachOpt treeOpt anns = maybe Nothing (\t -> Just $ foldl (@+) t anns) treeOpt

optionalProperties :: String -> K3Parser [Annotation a] -> K3Parser (Maybe [Annotation a])
optionalProperties sfx p = optional (try $ symbol ("@:" ++ sfx) *> p)

properties :: (Identifier -> Maybe (K3 Literal) -> Annotation a) -> K3Parser [Annotation a]
properties ctor = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = ctor <$> identifier <*> optional literal

nproperties :: (Identifier -> Annotation a) -> K3Parser [Annotation a]
nproperties ctor = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = ctor <$> identifier

dProperties :: K3Parser [Annotation Declaration]
dProperties = properties (\n lopt -> DProperty $ Left (n,lopt))

eProperties :: K3Parser [Annotation Expression]
eProperties = properties (\n lopt -> EProperty $ Left (n,lopt))

tProperties :: K3Parser [Annotation Type]
tProperties = nproperties $ TProperty . Left

{- Metaprogramming -}
stTerm :: K3Parser SpliceType
stTerm = choice $ map try [ stLabel, stType, stExpr, stDecl, stLiteral
                          , stLabelType, stLabelExpr, stLabelLit, stLTL
                          , stRecord, stList ]
  where
    stLabel     = STLabel     <$ keyword "label"
    stType      = STType      <$ keyword "type"
    stExpr      = STExpr      <$ keyword "expr"
    stDecl      = STDecl      <$ keyword "decl"
    stLiteral   = STLiteral   <$ keyword "literal"
    stLabelType = mkLabelType <$ keyword "labeltype"
    stLabelExpr = mkLabelExpr <$ keyword "labelexpr"
    stLabelLit  = mkLabelLit  <$ keyword "labellit"
    stLTL       = mkLTL       <$ keyword "labeltylit"
    stList      = spliceListT   <$> brackets stTerm
    stRecord    = spliceRecordT <$> braces (commaSep1 stField)
    stField     = (,) <$> identifier <* colon <*> stTerm
    mkLabelType = spliceRecordT [(spliceVIdSym, STLabel), (spliceVTSym, STType)]
    mkLabelExpr = spliceRecordT [(spliceVIdSym, STLabel), (spliceVESym, STExpr)]
    mkLabelLit  = spliceRecordT [(spliceVIdSym, STLabel), (spliceVLSym, STLiteral)]
    mkLTL       = spliceRecordT [(spliceVIdSym, STLabel), (spliceVTSym, STType), (spliceVLSym, STLiteral)]

stVar :: K3Parser TypedSpliceVar
stVar = try (flip (,) <$> identifier <* colon <*> stTerm)

spliceParameterDecls :: K3Parser [TypedSpliceVar]
spliceParameterDecls = brackets (commaSep stVar)

-- TODO: strip literal uid/span
svTerm :: K3Parser SpliceValue
svTerm = choice $ map try [ sVar
                          , svTypeDict, svExprDict, svLiteralDict, svTylitDict
                          , svNamedTypeDict, svNamedExprDict, svNamedLiteralDict, svNamedTermDict
                          , sLabel, sType, sExpr, sDecl, sLiteral, sLabelType, sRecord, sList, sLitRange ]
  where
    sVar        = SVar                  <$> identifier
    sLabel      = SLabel                <$> wrap "[#"  "]"  identifier
    sType       = SType . stripTUIDSpan <$> wrap "[:"  "]"  typeExpr
    sExpr       = SExpr . stripEUIDSpan <$> wrap "[$"  "]"  expr
    sLiteral    = SLiteral              <$> wrap "[$#" "]"  literal
    sDecl       = mkDecl                =<< wrap "[$^" "]"  declaration
    sLabelType  = mkLabelType           <$> wrap "[&"  "]"  ((,) <$> identifier <* colon <*> typeExpr)
    sRecord     = spliceRecord          <$> wrap "[%"  "]"  (commaSep1 ((,) <$> identifier <* colon <*> svTerm))
    sList       = spliceList            <$> wrap "[*"  "]"  (commaSep1 svTerm)
    sLitRange   = mkLitRange            =<< wrap "[~"  "]"  ((,) <$> literal <* symbol ".." <*> literal)

    mkLabelType (n,st) = spliceRecord [(spliceVIdSym, SLabel n), (spliceVTSym, SType $ stripTUIDSpan st)]

    mkDecl [x] = return $ SDecl $ stripDUIDSpan x
    mkDecl _   = P.parserFail "Invalid splice declaration"

    mkLitRange ((tag -> LInt i), (tag -> LInt j)) | j > i = return $ spliceList [SLiteral (LC.int x) | x <- [i..j]]
    mkLitRange _ = P.parserFail "Invalid splice range"

    wrap l r p = between (symbol l) (symbol r) p

svDict :: String -> Identifier -> K3Parser SpliceValue -> K3Parser SpliceValue
svDict bracketSuffix valRecId valParser =
  mkDict <$> wrap ("[" ++ bracketSuffix) "]"
    (commaSep1 ((,) <$> identifier <* symbol "=>" <*> valParser))

  where mkDict nl = SList $ map recCtor nl
        recCtor (n, v) = spliceRecord [(spliceVIdSym, SLabel n), (valRecId, v)]
        wrap l r p = between (symbol l) (symbol r) p

svDictP :: String -> K3Parser (Identifier, [(Identifier, SpliceValue)]) -> K3Parser SpliceValue
svDictP bracketSuffix idvalParser =
  mkDict <$> wrap ("[" ++ bracketSuffix) "]" (commaSep1 idvalParser)

  where mkDict nvl = SList $ map recCtor nvl
        recCtor (n, vl) = spliceRecord $ [(spliceVIdSym, SLabel n)] ++ vl
        wrap l r p = between (symbol l) (symbol r) p

svNamedDict :: String -> K3Parser SpliceValue -> K3Parser SpliceValue
svNamedDict nameSuffix valParser =
  mkDict <$> wrap "[" "]"
         ((,) <$> (identifier <* symbol nameSuffix)
              <*> (commaSep1 ((,) <$> identifier <* symbol "=>" <*> valParser)))

  where mkDict (recId, nl) = SList $ map (recCtor recId) nl
        recCtor i (n, v) = spliceRecord [(spliceVIdSym, SLabel n), (i, v)]
        wrap l r p = between (symbol l) (symbol r) p

svTypeDict :: K3Parser SpliceValue
svTypeDict = svDict ":>" spliceVTSym (SType . stripTUIDSpan <$> typeExpr)

svExprDict :: K3Parser SpliceValue
svExprDict = svDict "$>" spliceVESym (SExpr . stripEUIDSpan <$> expr)

svLiteralDict :: K3Parser SpliceValue
svLiteralDict = svDict "$#>" spliceVLSym (SLiteral <$> literal)

svTylitDict :: K3Parser SpliceValue
svTylitDict = svDictP ":#>" ((,) <$> identifier <* symbol "=>" <*> tylitP)
  where tylitP = mkTyLit <$> (SType . stripTUIDSpan <$> typeExpr) <* colon <*> (SLiteral <$> literal)
        mkTyLit a b = [(spliceVTSym, a), (spliceVLSym, b)]

svNamedTypeDict :: K3Parser SpliceValue
svNamedTypeDict = svNamedDict ":>" (SType . stripTUIDSpan <$> typeExpr)

svNamedExprDict :: K3Parser SpliceValue
svNamedExprDict = svNamedDict "$>" (SExpr . stripEUIDSpan <$> expr)

svNamedLiteralDict :: K3Parser SpliceValue
svNamedLiteralDict = svNamedDict "$#>" (SLiteral <$> literal)

svNamedTermDict :: K3Parser SpliceValue
svNamedTermDict = svNamedDict ">" svTerm

spliceParameter :: K3Parser (Maybe (Identifier, SpliceValue))
spliceParameter = try ((\a b -> Just (a,b)) <$> identifier <* symbol "=" <*> parseInMode Splice svTerm)

contextualizedSpliceParameter :: Maybe SpliceEnv -> K3Parser (Maybe (Identifier, SpliceValue))
contextualizedSpliceParameter sEnvOpt = choice [try fromContext, spliceParameter]
  where
    fromContext = mkCtxtVal sEnvOpt <$> identifier <*> optional (symbol "=" *> contextParams)
    contextParams = try (Left <$> identifier) <|> try (Right <$> (brackets $ commaSep1 identifier))
      -- ^ TODO: this prevents us from matching against an SVar. Generalize.

    mkCtxtVal Nothing _ _                     = Nothing
    mkCtxtVal (Just sEnv) a Nothing           = mkCtxtLt sEnv a >>= return . (a,)
    mkCtxtVal (Just sEnv) a (Just (Left b))   = mkCtxtLt sEnv b >>= return . (a,)
    mkCtxtVal (Just sEnv) a (Just (Right bs)) = mapM (mkCtxtLt sEnv) bs >>= return . (a,) . SList

    mkCtxtLt sEnv sn = do
      lv <- lookupSpliceE sn sEnv >>= ltLabel
      tv <- lookupSpliceE sn sEnv >>= ltType
      return $ spliceRecord [(spliceVIdSym, lv), (spliceVTSym, tv)]


{- Effect signatures -}
effectSignature :: Bool -> K3Parser [Annotation Declaration]
effectSignature asAttrMem = mkSigAnn =<< (keyword "with" *> keyword "effects" *> effSig)
  where
    effSig    = (,) <$> effTerm asAttrMem <*> optional returnSig
    returnSig = keyword "return" *> provTerm
    mkSigAnn (f, rOpt) = return $
      [DEffect $ Left f] ++ maybe [DProvenance $ Left $ provOfEffect [] f] (\p -> [DProvenance $ Left p]) rOpt

    provOfEffect args (tnc -> (FS.FLambda i, [_, _, sf])) = PC.plambda i [] $ provOfEffect (args++[i]) sf
    provOfEffect args _ = PC.pderived $ map (\i -> PC.pfvar i Nothing) args

effTerm :: Bool -> K3Parser (K3 FS.Effect)
effTerm asAttrMem = effApply fTerm <?> "effect term"
  where
    fTerm     = choice (map try [effExec, effStruc, fNest])
    fNest     = parens $ effTerm asAttrMem
    fTermE    = choice $ map try [effExec >>= return . Left, effStruc >>= return . Right, fNestE]
    fNestE    = parens fTermE

    effApply p = foldl1 FSC.fapplyExt <$> some (try p)
    effExec    = choice $ map try [effIO, effRead, effWrite, effSeq, effLoop]
    effStruc   = choice $ map try [effNone, effVar, effLambda]

    effIO     = const FSC.fio <$> keyword "io"
    effRead   = FSC.fseq . map FSC.fread  <$> varList "R" provTerm
    effWrite  = FSC.fseq . map FSC.fwrite <$> varList "W" provTerm
    effSeq    = FSC.fseq <$> brackets (semiSep1 $ effTerm asAttrMem)
    effLoop   = FSC.floop <$> (parens $ effTerm asAttrMem) <* symbol "*"

    effNone   = const FSC.fnone <$> keyword "none"
    effLambda = mkLambda <$> choice [iArrow "fun", iArrowS "\\"] <*> lambdaBody
    effVar    = FSC.ffvar <$> (identifier <|> (keyword "self"    >> return "self")
                                          <|> (keyword "content" >> return "content"))
                          <?> "effect symbol"

    lambdaBody   = choice $ map try [lamApp fTermE, fTermE]
    lamApp     p = mkLamApp =<< some (try p)
    mkLamApp []  = P.parserFail "Invalid provenance lambda body"
    mkLamApp [p] = return p
    mkLamApp l   = return $ Right $ foldl1 FSC.fapplyExt $ map (either id id) l

    mkLambda i (Left  ef) = FSC.flambda i FSC.fnone ef FSC.fnone
    mkLambda i (Right rf) = FSC.flambda i FSC.fnone FSC.fnone rf

    varList pfx parser = string pfx *> brackets (commaSep1 parser)

provTerm :: K3Parser (K3 P.Provenance)
provTerm =  pApply pTerm <?> "provenance term"
  where
    pTerm    = mkTerm <$> choice [pVar, pInd, pDerived, pLambda, pNest] <*> optional pSuffix
    pNest    = parens provTerm
    pSuffix  = choice [pPrj, pRec, pTup]
    pApply p = foldl1 PC.papplyExt <$> some (try p)

    pVar  = (mkVar =<< (identifier <|> (keyword "self"    >> return "self")
                                   <|> (keyword "content" >> return "content")
                                   <|> (keyword "fresh"   >> return "fresh")))
              <?> "return symbol"

    pInd = mkInd <$> (symbol "!" *> provTerm)
    pDerived = PC.pderived <$> (symbol "~" *> choice [provTerm >>= return . (:[]), parens (commaSep1 provTerm)])
    pLambda  = (\a b -> PC.plambda a [] b) <$> choice [iArrow "fun", iArrowS "\\"] <*> provTerm

    pPrj = flip mkPrj <$> (dot        *> identifier)
    pRec = flip mkRec <$> (colon      *> identifier)
    pTup = flip mkTup <$> (symbol "#" *> integer)

    mkTerm p psfxOpt = maybe p ($ p) psfxOpt

    mkVar "fresh" = withEffectID $ \eid -> PC.pfvar ("fresh"++ show eid) Nothing
    mkVar       i = return $ PC.pfvar i Nothing

    mkPrj    p n = PC.pproject n p Nothing
    mkRec    p n = PC.precord n p
    mkTup    p n = PC.ptuple (fromInteger n) p
    mkInd      p = PC.pindirect p


{- Identifiers and their list forms -}

idList :: K3Parser [Identifier]
idList = commaSep identifier

idPairList :: K3Parser [(Identifier, Identifier)]
idPairList = commaSep idPair

idQExprList :: K3Parser [(Identifier, K3 Expression)]
idQExprList = commaSep idQExpr

idQLitList :: K3Parser [(Identifier, K3 Literal)]
idQLitList = commaSep idQLit

{- Note: unused
idQTypeList :: K3Parser [(Identifier, K3 Type)]
idQTypeList = commaSep idQType
-}

idPair :: K3Parser (Identifier, Identifier)
idPair = (,) <$> identifier <*> (colon *> identifier)

idQExpr :: K3Parser (Identifier, K3 Expression)
idQExpr = (,) <$> identifier <*> (colon *> qualifiedExpr)

idQLit :: K3Parser (Identifier, K3 Literal)
idQLit = (,) <$> identifier <*> (colon *> qualifiedLiteral)

idQType :: K3Parser (Identifier, K3 Type)
idQType = (,) <$> identifier <*> (colon *> qualifiedTypeExpr)


{- Expression helpers -}
nsPrefix :: String -> ExpressionParser
nsPrefix k = keyword k *> nonSeqExpr

ePrefix :: String -> ExpressionParser
ePrefix k  = keyword k *> expr

iPrefix :: String -> K3Parser Identifier
iPrefix k = keyword k *> identifier

iArrow :: String -> K3Parser Identifier
iArrow k = iPrefix k <* symbol "->"

iArrowS :: String -> K3Parser Identifier
iArrowS s = symbol s *> identifier <* symbol "->"

equateTypeExpr :: TypeParser
equateTypeExpr = symbol "=" *> typeExpr

equateExpr :: ExpressionParser
equateExpr = symbol "=" *> expr

equateNSExpr :: ExpressionParser
equateNSExpr = symbol "=" *> nonSeqExpr

equateQExpr :: ExpressionParser
equateQExpr = symbol "=" *> qualifiedExpr


{- Endpoints -}

endpoint :: Bool -> K3Parser EndpointBuilder
endpoint isSource = if isSource
                      then choice $ [ value
                                    , try filemux
                                    , try $ file True "fileseq" FileSeqEP eVariable
                                    , try polyfile
                                    ] ++ common
                      else choice common
  where common = [builtin isSource, file isSource "file" FileEP eTerminal, network isSource]

value :: K3Parser EndpointBuilder
value = mkValueStream <$> (symbol "value" *> expr)
  where mkValueStream e _ _ = Right (ValueEP, Just e, [])

builtin :: Bool -> K3Parser EndpointBuilder
builtin isSource = mkBuiltin <$> builtinChannels <*> format
  where mkBuiltin idE formatE n t =
          builtinSpec idE formatE >>= \s -> return $ endpointMethods isSource s idE formatE n t
        builtinSpec idE formatE = BuiltinEP <$> S.symbolS idE <*> S.symbolS formatE

file :: Bool -> String -> (String -> Bool -> String -> EndpointSpec) -> ExpressionParser
     -> K3Parser EndpointBuilder
file isSource sym ctor prsr = mkFileSrc <$> (symbol sym *> prsr) <*> textOrBinary <*> format
  where mkFileSrc argE asTxt formatE n t = do
          s <- spec argE asTxt formatE
          return $ endpointMethods isSource s argE formatE n t

        spec argE asTxt formatE = (\a f -> ctor a asTxt f) <$> S.exprS argE <*> S.symbolS formatE
        textOrBinary = (symbol "text" *> return True) <|> (symbol "binary" *> return False)

filemux :: K3Parser EndpointBuilder
filemux = mkFMuxSrc <$> syms ["filemxsq", "filemux"] <*> eVariable <*> textOrBinary <*> format
  where
    textOrBinary = (symbol "text" *> return True) <|> (symbol "binary" *> return False)
    syms l = choice $ map (try . symbol) l

    mkFMuxSrc sym argE asTxt formatE n t = do
      s <- fMuxSpec (ctorOfSym sym) argE asTxt formatE
      return $ endpointMethods True s argE formatE n t

    fMuxSpec ctor argE asTxt formatE = (\a f -> ctor a asTxt f) <$> S.exprS argE <*> S.symbolS formatE

    ctorOfSym s =
      if s == "filemux" then FileMuxEP
      else if s == "filemxsq" then FileMuxseqEP
      else fail "Invalid file mux kind"

polyfile :: K3Parser EndpointBuilder
polyfile = mkPFSrc <$> syms ["polyfileseq", "polyfile"] <*> eVariable <*> textOrBinary <*> format <*> eTerminal <*> eVariable
  where
    textOrBinary = (symbol "text" *> return True) <|> (symbol "binary" *> return False)
    syms l = choice $ map (try . symbol) l

    mkPFSrc sym argE asTxt formatE orderE rbsizeE n t = do
      s <- (\a f o sv -> (ctorOfSym sym) a asTxt f o sv)
              <$> S.exprS argE <*> S.symbolS formatE <*> S.exprS orderE <*> S.exprS rbsizeE
      return $ endpointMethods True s argE formatE n t

    ctorOfSym s =
      if s == "polyfile" then PolyFileMuxEP
      else if s == "polyfileseq" then PolyFileMuxSeqEP
      else fail "Invalid poly file kind"


network :: Bool -> K3Parser EndpointBuilder
network isSource = mkNetwork <$> (symbol "network" *> eTerminal) <*> textOrBinary <*> format
  where textOrBinary = (symbol "text" *> return True) <|> (symbol "binary" *> return False)
        mkNetwork addrE asText formatE n t = do
          s <- networkSpec addrE asText formatE
          return $ endpointMethods isSource s addrE formatE n t

        networkSpec addrE asText formatE = (\a f -> NetworkEP a asText f) <$> S.exprS addrE <*> S.symbolS formatE

builtinChannels :: ExpressionParser
builtinChannels = choice [ch "stdin", ch "stdout", ch "stderr"]
  where ch s = try (symbol s >> return (EC.constant $ CString s))

format :: ExpressionParser
format = choice [fmt "k3", fmt "k3b", fmt "k3yb", fmt "k3ybt", fmt "csv", fmt "psv", fmt "k3x", fmt "raw"]
  where fmt s = try (symbol s >> return (EC.constant $ CString s))

{- Declaration helpers -}
namedIdentifier :: String -> String -> (K3Parser Identifier -> K3Parser a) -> K3Parser a
namedIdentifier nameKind name namedCstr =
  declError nameKind $ namedCstr $ keyword name *> identifier

namedDecl :: String -> String -> (K3Parser Identifier -> DeclParser) -> DeclParser
namedDecl k n c = (DSpan <->) $ namedIdentifier k n c

{- Note: unused
namedBraceDecl :: String -> String -> K3Parser a -> (Identifier -> a -> K3 Declaration)
               -> DeclParser
namedBraceDecl k n rule cstr =
  namedDecl k n $ braceRule . (cstr <$>)
  where braceRule x = x <*> (braces rule)
-}

chainedNamedBraceDecl :: String -> String
                      -> (Identifier -> K3Parser a) -> (Identifier -> a -> K3 Declaration)
                      -> DeclParser
chainedNamedBraceDecl k n namedRule cstr =
  namedDecl k n $ join . (passName <$>)
  where passName x = (cstr x) <$> (braces $ namedRule x)


-- | Records source bindings in a K3 parsing environment
--   A parsing error is raised on an attempt to bind to anything other than a source.
trackBindings :: (Identifier, Identifier) -> K3Parser ()
trackBindings (src, dest) = modifyEnvF_ $ updateBindings
  where updateBindings (safePopFrame -> (s, env)) =
          case lookup src s of
            Just (es, Just b, q, g) -> Right $ (replaceAssoc s src (es, Just (dest:b), q, g)):env
            Just (_, Nothing, _, _) -> Left  $ "Invalid binding for endpoint " ++ src
            Nothing                 -> Left  $ "Invalid binding, no source " ++ src


-- | Records endpoint identifiers and initializer expressions in a K3 parsing environment
trackEndpoint :: EndpointSpec -> K3 Declaration -> DeclParser
trackEndpoint eSpec d
  | DGlobal n t eOpt <- tag d, TSource <- tag t = track True  n eOpt >> return d
  | DGlobal n t eOpt <- tag d, TSink   <- tag t = track False n eOpt >> return d
  | otherwise = return d

  where
    track isSource n eOpt = modifyEnvF_ $ addEndpointGoExpr isSource n eOpt

    addEndpointGoExpr isSource n eOpt (safePopFrame -> (fs, env)) =
      case (eOpt, isSource) of
        (Just _, True)   -> Right $ refresh n fs env (Just [], n, Nothing)
        (Nothing, True)  -> Right $ refresh n fs env (Just [], n, Just $ mkRunSourceE n)
        (Just _, False)  -> Right $ refresh n fs env (Nothing, n, Just $ mkRunSinkE n)
        (_,_)            -> Left  $ "Invalid endpoint initializer"

    refresh n fs env (a,b,c) = (replaceAssoc fs n (eSpec, a, b, c)):env


-- | Completes any stateful processing needed for the role.
--   This includes handling 'feed' clauses, and checking and qualifying role defaults.
postProcessRole :: [K3 Declaration] -> EnvFrame -> K3Parser [K3 Declaration]
postProcessRole decls frame =
  mergeFrame frame >> processEndpoints frame

  where processEndpoints s = addBuilderDecls $ map (annotateEndpoint s . attachSource s) decls

        addBuilderDecls dAndExtras =
          let (ndl, extrasl) = unzip dAndExtras
          in modifyBuilderDeclsF_ (Right . ((concat extrasl) ++)) >> return ndl

        attachSource s = bindSource (sourceEndpointSpecs s) $ sourceBindings s
        annotateEndpoint s (d, extraDecls)
          | DGlobal en t _ <- tag d, TSource <- tag t = (maybe d (d @+) $ syntaxAnnotation en s, extraDecls)
          | DGlobal en t _ <- tag d, TSink   <- tag t = (maybe d (d @+) $ syntaxAnnotation en s, extraDecls)
          | otherwise = (d, extraDecls)

        syntaxAnnotation en s = do
          (enSpec,bindingsOpt,_,_) <- lookup en s
          return . DSyntax . EndpointDeclaration enSpec $ maybe [] id bindingsOpt


-- | Adds UIDs to nodes that do not already have one.
--   This ensures every AST node has a UID after parsing, including builtins.
ensureUIDs :: K3 Declaration -> K3Parser (K3 Declaration)
ensureUIDs p = traverse (parserWithUID . annotateDecl) p
  where
    annotateDecl d@(dt :@: _) uid =
      case dt of
        DGlobal n t eOpt -> do
          t'    <- annotateType t
          eOpt' <- maybe (return Nothing) (\e -> annotateExpr e >>= return . Just) eOpt
          rebuildDecl d uid $ DGlobal n t' eOpt'

        DTrigger n t e -> do
          t' <- annotateType t
          e' <- annotateExpr e
          rebuildDecl d uid $ DTrigger n t' e'

        DRole n -> rebuildDecl d uid $ DRole n

        DDataAnnotation n tis mems -> rebuildDecl d uid $ DDataAnnotation n tis mems
          --  TODO: recur through members (e.g., attributes w/ initializers)
          --  and ensure they have a uid

        DGenerator mp -> rebuildDecl d uid $ DGenerator mp
          -- TODO: recur on all subexpressions in metaprogram annotations to ensure they have a uid

        DTypeDef _ _ -> fail "Invalid type alias in AST"

    rebuildDecl (_ :@: as) uid tg =
      let d' = tg :@: as in
      return $ unlessAnnotated (any isDUID) d' (d' @+ (DUID $ UID uid))

    annotateNode test anns node = return $ unlessAnnotated test node (foldl (@+) node anns)

    annotateExpr :: (Eq (Annotation Expression)) => K3 Expression -> K3Parser (K3 Expression)
    annotateExpr = traverse (\e -> parserWithUID (\uid -> annotateNode (any isEUID) [EUID $ UID uid] e))

    annotateType :: (Eq (Annotation Type)) => K3 Type -> K3Parser (K3 Type)
    annotateType = traverse (\t -> parserWithUID (\uid -> annotateNode (any isTUID) [TUID $ UID uid] t))

    unlessAnnotated test n@(_ :@: as) n' = if test as then n else n'


-- | Propagates a mutability qualifier from a type to an expression.
--   This is used in desugaring non-function initializers and annotation members.
propagateQualifier :: K3 Type -> Maybe (K3 Expression) -> Maybe (K3 Expression)
propagateQualifier _ Nothing  = Nothing
propagateQualifier (tag &&& annotations -> (ttag, tas)) (Just e@(annotations -> eas))
  | any isEQualified eas || inApplicable ttag = Just e
  | otherwise = Just $ if any isTImmutable tas then (e @+ EImmutable) else (e @+ EMutable)
  where inApplicable = flip elem [TTrigger, TSink, TSource]
