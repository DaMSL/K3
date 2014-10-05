{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | K3 Parser.
module Language.K3.Parser (
  K3Parser,
  identifier,
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
  runK3Parser,
  ensureUIDs
) where

import Control.Applicative
import Control.Arrow
import Control.Monad

import Data.Function
import Data.List
import Data.Maybe
import Data.Traversable hiding ( mapM )
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

import Language.K3.Metaprogram.DataTypes
import Language.K3.Metaprogram.Evaluation

import Language.K3.Parser.DataTypes
import Language.K3.Parser.Operator
import Language.K3.Parser.ProgramBuilder
import Language.K3.Parser.Preprocessor

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

parseK3 :: Bool -> [FilePath] -> String -> IO (Either String (K3 Declaration))
parseK3 noFeed includePaths s = do
  searchPaths   <- if null includePaths then getSearchPath else return includePaths
  subFiles      <- processIncludes searchPaths (lines s) []
  fileContents  <- (trace (unwords ["subfiles:", show subFiles]) $ mapM readFile subFiles) >>= return . (++ [s])
  return $ fst <$> foldr chainValidParse (Right $ (DC.role defaultRoleName [], True)) fileContents
  where
    chainValidParse c parse = parse >>= parseAndCompose c
    parseAndCompose src (p, asTopLevel) =
      parseAtLevel asTopLevel src >>= return . flip ((,) `on` (tag &&& children)) p >>= \case
        ((DRole n, ch), (DRole n2, ch2))
          | n == defaultRoleName && n == n2 -> return (DC.role n $ ch++ch2, False || noFeed)
          | otherwise                       -> programError
        _                                   -> programError
    parseAtLevel asTopLevel = stringifyError . runK3Parser Nothing (program $ not asTopLevel || noFeed)
    programError = Left "Invalid program, expected top-level role."


{- K3 grammar parsers -}

-- TODO: inline testing
program :: Bool -> DeclParser
program noDriver = DSpan <-> (rule >>= selfContainedProgram)
  where rule = (DC.role defaultRoleName) . concat <$> (spaces *> endBy1 (roleBody noDriver "") eof)

        addDecls gd p@(tag -> DRole n)
          | n == defaultRoleName =
              let (dd, cd) = generatorDeclsToList gd
              in return $ Node (DRole n :@: annotations p) $ children p ++ dd ++ cd

        addDecls _ _ = P.parserFail "Invalid top-level role resulting from metaprogram evaluation"

        selfContainedProgram d =
          if noDriver then return d
          else (mkBuilderDecls d >>= mkEntryPoints >>= mkBuiltins >>= runMP)

        mkBuilderDecls d
          | DRole n <- tag d, n == defaultRoleName =
              withBuilderDecls $ \decls -> Node (tag d :@: annotations d) (children d ++ decls)
          | otherwise = return d

        mkEntryPoints d = withEnv $ (uncurry $ processInitsAndRoles d) . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins

        runMP mp =
          let (prgE, genSt) = evalMetaprogram defaultMetaAnalysis mp
          in either P.parserFail (addDecls $ getGeneratedDecls genSt) prgE


roleBody :: Bool -> Identifier -> K3Parser [K3 Declaration]
roleBody noDriver n =
    pushBindings >> rule >>= popBindings
      >>= \df -> if noDriver then return $ fst df else postProcessRole n df
  where rule = some declaration >>= return . concat
        pushBindings = modifyEnv_ addFrame
        popBindings dl = modifyEnv (\env -> (removeFrame env, (dl, currentFrame env)))


{- Declarations -}
declaration :: K3Parser [K3 Declaration]
declaration = (//) attachComment <$> comment False <*>
              choice [ignores >> return [], k3Decls, edgeDecls, driverDecls >> return []]

  where k3Decls     = choice $ map normalizeDeclAsList
                        [Right dGlobal, Right dTrigger, Right dRole,
                         Right dDataAnnotation, Right dControlAnnotation,
                         Left dTypeAlias]

        edgeDecls   = mapM ((DUID #) . return) =<< choice [dSource, dSink]
        driverDecls = choice [dSelector, dFeed]
        ignores     = pInclude >> return ()

        props    p = DUID # withProperties True dProperties p
        optProps p = uidOfOpt =<< optWithProperties True dProperties p
        uidOfOpt Nothing  = return Nothing
        uidOfOpt (Just v) = (DUID # (return v)) >>= return . Just

        normalizeDeclAsList (Right p) = (:[]) <$> props p
        normalizeDeclAsList (Left  p) = try (optProps p) >>= return . maybe [] (:[])

        attachComment [] _      = []
        attachComment (h:t) cmt = (h @+ DSyntax cmt):t

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (mkGlobal <$>)
  where
    rule x = x <* colon <*> polymorphicTypeExpr <*> (optional equateExpr)
    mkGlobal n qte eOpt = DC.global n qte (propagateQualifier qte eOpt)

dTrigger :: DeclParser
dTrigger = namedDecl "trigger" "trigger" $ rule . (DC.trigger <$>)
  where rule x = x <* colon <*> typeExpr <*> equateExpr

dEndpoint :: String -> String -> Bool -> K3Parser [K3 Declaration]
dEndpoint kind name isSource =
  attachFirst (DSpan <->) =<< (namedIdentifier kind name $ join . rule . (mkEndpoint <$>))
  where rule x      = ruleError =<< (x <*> (colon *> typeExpr) <*> (symbol "=" *> (endpoint isSource)))
        ruleError x = either unexpected pure x

        (typeCstr, stateModifier) =
          (if isSource then TC.source else TC.sink, trackEndpoint)

        mkEndpoint n t endpointCstr = either Left (Right . mkDecls n t) $ endpointCstr n t

        mkDecls n t (spec, eOpt, subDecls) = do
          epDecl <- stateModifier spec $ DC.endpoint n (qualifyT $ typeCstr t) eOpt []
          return $ epDecl:subDecls

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

dRole :: DeclParser
dRole = chainedNamedBraceDecl n n (roleBody False) DC.role
  where n = "role"

dSelector :: K3Parser ()
dSelector = namedIdentifier "selector" "default" (id <$>) >>= trackDefault

-- | Data annotation parsing.
--   This covers metaprogramming for data annotations when an annotation defines splice parameters.
dDataAnnotation :: DeclParser
dDataAnnotation = namedIdentifier "data annotation" "annotation" rule
  where rule x = mkAnnotation <$> x <*> option [] spliceParameterDecls <*> typesParamsAndMembers
        typesParamsAndMembers = parseInMode Splice $ protectedVarDecls typeParameters $ braces (some annotationMember)
        typeParameters = keyword "given" *> keyword "type" *> typeVarDecls <|> return []
        mkAnnotation n sp (tp, mems) = DC.generator $ mpDataAnnotation n sp tp mems


{- Annotation declaration members -}
annotationMember :: K3Parser AnnMemDecl
annotationMember = memberError $ mkMember <$> annotatedRule
  where
    rule          = (,) <$> polarity <*> (choice $ map uidOver [liftedOrAttribute, subAnnotation])
    annotatedRule = wrapInComments $ spanned $ flip (,) <$> optionalProperties dProperties <*> rule

    liftedOrAttribute = mkLA  <$> optional (keyword "lifted")
                              <*> identifier <* colon
                              <*> qualifiedTypeExpr
                              <*> optional equateExpr

    subAnnotation     = mkSub <$> (keyword "annotation" *> identifier)

    mkMember ((((p, Left (Just _,  n, qte, eOpt, uid)), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props)
        $ Lifted p n qte (propagateQualifier qte eOpt)

    mkMember ((((p, Left (Nothing, n, qte, eOpt, uid)), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props)
        $ Attribute p n qte (propagateQualifier qte eOpt)

    mkMember ((((p, Right (n, uid)), props), spn), cmts) =
      attachMemAnnots uid spn cmts (maybe [] id props) $ MAnnotation p n

    mkLA kOpt n qte eOpt uid = Left (kOpt, n, qte, eOpt, uid)
    mkSub n uid              = Right (n, uid)

    attachMemAnnots uid spn cmts props memCtor = memCtor $ (DUID uid):(DSpan spn):(props ++ map DSyntax cmts)
    wrapInComments p = (\a b c -> (b, a ++ c)) <$> comment False <*> p <*> comment True

    memberError = parseError "annotation" "member"

polarity :: K3Parser Polarity
polarity = choice [keyword "provides" >> return Provides,
                   keyword "requires" >> return Requires]

-- | Control annotation parsing
dControlAnnotation :: DeclParser
dControlAnnotation = namedIdentifier "control annotation" "control" $ rule
  where rule x = mkCtrlAnn <$> x <*> option [] spliceParameterDecls
                                 <*> some pattern <*> (extensions $ keyword "shared")
        pattern        = (,,) <$> patternE <*> (symbol "=>" *> rewriteE) <*> (extensions $ symbol "+>")
        extensions pfx = concat <$> option [] (pfx *> braces (many extensionD))
        rewriteE       = cleanExpr =<< parseInMode Splice expr
        patternE       = cleanExpr =<< parseInMode SourcePattern expr
        extensionD     = mapM cleanDecl =<< parseInMode Splice declaration

        mkCtrlAnn n svars rw exts = DC.generator $ mpCtrlAnnotation n svars rw exts

        cleanExpr e = return $ stripExprAnnotations eAnnFilter tAnnFilter e
        cleanDecl d = return $ stripDeclAnnotations dAnnFilter eAnnFilter tAnnFilter d

        dAnnFilter a = isDUID a || isDSpan a
        eAnnFilter a = isEUID a || isESpan a
        tAnnFilter a = isTUID a || isTSpan a


dTypeAlias :: K3Parser (Maybe (K3 Declaration))
dTypeAlias = namedIdentifier "typedef" "typedef" rule
  where rule x = mkTypeAlias =<< ((,) <$> x <*> equateTypeExpr)
        mkTypeAlias (n, t) = modifyTAEnvF_ (Right . appendTAliasE n t) >> return Nothing

pInclude :: K3Parser String
pInclude = keyword "include" >> stringLiteral


{- Types -}
typeExpr :: TypeParser
typeExpr = typeError "expression" $ TUID # tTermOrFun

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
              <*> choice [ tPrimitive, tOption, tIndirection,
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
        attachAnnotations t@(tag -> TRecord ids) = withAnnotations (tAnnotations $ Just $ mkRecordSpliceEnv ids $ children t) >>= return . (t,)
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
        $ withProperties False eProperties $ buildExpressionParser fullOpTable eApp

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

    eProject = prjWithAnnotations $ dot *> identifier

    attachProjection e (((i, tOpt), anns), sp) =
      EUID # (return $ foldl (@+) (EC.project i e) $ [ESpan sp] ++ maybe [] ((:[]) . EPType) tOpt ++ anns)

    eWithProperties    p = withProperties False eProperties p
    eWithAnnotations   p = foldl (@+) <$> p <*> withAnnotations eCAnnotations
    prjWithAnnotations p = (,) <$> asSourcePattern (,) (,Nothing) p <*> withAnnotations eCAnnotations

    asSourcePattern pctor ctor p = parserWithPMode $ \case
      SourcePattern -> pctor <$> p <*> optional (colon *> typeExpr)
      _ -> ctor <$> p

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
eVariable = exprError "variable" $ EC.variable <$> identifier

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
        sendSuffix    = symbol "<-" *> nonSeqExpr

        mkTupOrSend [e] Nothing    = return $ stripSpan <$> e
        mkTupOrSend [e] (Just arg) = return $ EC.binop OSnd e arg
        mkTupOrSend l Nothing      = return $ EC.tuple l
        mkTupOrSend l (Just arg)   = EC.binop OSnd <$> (EUID # return (EC.tuple l)) <*> pure arg

        stripSpan e               = maybe e (e @-) $ e @~ isESpan

eRecord :: ExpressionParser
eRecord = exprError "record" $ EC.record <$> braces idQExprList

eEmpty :: ExpressionParser
eEmpty = exprError "empty" $ mkEmpty <$> (typedEmpty >>= attachAnnotations)
  where mkEmpty (e, a) = foldl (@+) e a
        typedEmpty = EC.empty <$> (keyword "empty" *> tRecord)
        attachAnnotations e@(tag -> EConstant (CEmpty t@(tag -> TRecord ids))) =
          withAnnotations (eAnnotations $ Just $ mkRecordSpliceEnv ids $ children t) >>= return . (e,)
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

        attachAnnotations (tyl, el) =
          withAnnotations (eAnnotations $ Just $ uncurry mkRecordSpliceEnv $ unzip tyl) >>= return . ((tyl,el),)


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
        attachAnnotations l@(tag -> LEmpty t@(tag -> TRecord ids)) =
          withAnnotations (lAnnotations $ Just $ mkRecordSpliceEnv ids $ children t) >>= return . (l,)
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

    attachAnnotations (tyl,el) =
      withAnnotations (lAnnotations $ Just $ uncurry mkRecordSpliceEnv $ unzip tyl) >>= return . ((tyl,el),)

lAddress :: LiteralParser
lAddress = litError "address" $ LC.address <$> ipAddress <* colon <*> port
  where ipAddress = LC.string <$> (some $ choice [alphaNum, oneOf "."])
        port = LC.int . fromIntegral <$> natural


{- Attachments -}
withAnnotations :: K3Parser [Annotation a] -> K3Parser [Annotation a]
withAnnotations p = try $ option [] (symbol "@" *> p)

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
  where mpOrAnn = mkMpOrAnn =<< ((,) <$> identifier <*> option [] (parens $ commaSep1 $ contextualizedSpliceParameter sEnvOpt))
        mkMpOrAnn (n, [])    = return $ aCtor n
        mkMpOrAnn (n,params) = return $ apCtor n $ mkSpliceEnv $ catMaybes params

eCAnnotations :: K3Parser [Annotation Expression]
eCAnnotations = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = cAnnotationUse (EApplyGen True)

cAnnotationUse :: ApplyAnnCtor Expression -> K3Parser (Annotation Expression)
cAnnotationUse aCtor = mkSEnv <$> identifier <*> option [] (parens $ commaSep1 spliceParameter)
  where mkSEnv a b = aCtor a $ mkSpliceEnv $ catMaybes b

withPropertiesF :: (Eq (Annotation a))
                => Bool -> K3Parser [Annotation a] -> K3Parser b -> (b -> [Annotation a] -> b) -> K3Parser b
withPropertiesF asPrefix prop tree attachF =
    if asPrefix then (flip propertize) <$> optionalProperties prop <*> tree
                else propertize <$> tree <*> optionalProperties prop
  where propertize a (Just b) = attachF a b
        propertize a Nothing  = a

withProperties :: (Eq (Annotation a))
               => Bool -> K3Parser [Annotation a] -> K3Parser (K3 a) -> K3Parser (K3 a)
withProperties asPrefix prop tree = withPropertiesF asPrefix prop tree $ foldl (@+)

optWithProperties :: (Eq (Annotation a))
                  => Bool -> K3Parser [Annotation a] -> K3Parser (Maybe (K3 a)) -> K3Parser (Maybe (K3 a))
optWithProperties asPrefix prop tree = withPropertiesF asPrefix prop tree attachOpt
  where attachOpt treeOpt anns = maybe Nothing (\t -> Just $ foldl (@+) t anns) treeOpt

optionalProperties :: K3Parser [Annotation a] -> K3Parser (Maybe [Annotation a])
optionalProperties p = try $ optional (symbol "@:" *> p)

properties :: (Identifier -> Maybe (K3 Literal) -> Annotation a) -> K3Parser [Annotation a]
properties ctor = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = ctor <$> identifier <*> optional literal

dProperties :: K3Parser [Annotation Declaration]
dProperties = properties DProperty

eProperties :: K3Parser [Annotation Expression]
eProperties = properties EProperty

{- Metaprogramming -}
stTerm :: K3Parser SpliceType
stTerm = choice $ map try [stLabel, stType, stExpr, stDecl, stLiteral, stLabelType, stRecord, stList]
  where
    stLabel     = STLabel     <$ keyword "label"
    stType      = STType      <$ keyword "type"
    stExpr      = STExpr      <$ keyword "expr"
    stDecl      = STDecl      <$ keyword "decl"
    stLiteral   = STLiteral   <$ keyword "literal"
    stLabelType = mkLabelType <$ keyword "labeltype"
    stList      = spliceListT   <$> brackets stTerm
    stRecord    = spliceRecordT <$> braces (commaSep1 stField)
    stField     = (,) <$> identifier <* colon <*> stTerm
    mkLabelType = spliceRecordT [(spliceVIdSym, STLabel), (spliceVTSym, STType)]

stVar :: K3Parser TypedSpliceVar
stVar = try (flip (,) <$> identifier <* colon <*> stTerm)

spliceParameterDecls :: K3Parser [TypedSpliceVar]
spliceParameterDecls = brackets (commaSep stVar)

svTerm :: K3Parser SpliceValue
svTerm = choice $ map try [sLabel, sType, sExpr, sDecl, sLiteral, sLabelType, sRecord, sList]
  where
    sLabel      = SLabel   <$> wrap "[#"  "]" identifier
    sType       = SType    <$> wrap "[:"  "]" typeExpr
    sExpr       = SExpr    <$> wrap "[$"  "]" expr
    sLiteral    = SLiteral <$> wrap "[$#" "]" literal
    sDecl       = mkDecl   =<< wrap "[$^" "]" declaration
    sLabelType  = mkLabelType  <$> wrap "[&" "]" ((,) <$> identifier <* colon <*> typeExpr)
    sRecord     = spliceRecord <$> wrap "[%" "]" (commaSep1 ((,) <$> identifier <* colon <*> svTerm))
    sList       = spliceList   <$> wrap "[*" "]" (commaSep1 svTerm)

    mkLabelType (n,st) = spliceRecord [(spliceVIdSym, SLabel n), (spliceVTSym, SType st)]

    mkDecl [x] = return $ SDecl x
    mkDecl _   = P.parserFail "Invalid splice declaration"

    wrap l r p = between (symbol l) (symbol r) p

spliceParameter :: K3Parser (Maybe (Identifier, SpliceValue))
spliceParameter = try ((\a b -> Just (a,b)) <$> identifier <* symbol "=" <*> svTerm)

contextualizedSpliceParameter :: Maybe SpliceEnv -> K3Parser (Maybe (Identifier, SpliceValue))
contextualizedSpliceParameter sEnvOpt = choice [spliceParameter, try fromContext]
  where
    fromContext = mkCtxtVal sEnvOpt <$> identifier <*> optional (symbol "=" *> contextParams)
    contextParams = try (Left <$> identifier) <|> try (Right <$> (brackets $ commaSep1 identifier))

    mkCtxtVal Nothing _ _                     = Nothing
    mkCtxtVal (Just sEnv) a Nothing           = mkCtxtLt sEnv a >>= return . (a,)
    mkCtxtVal (Just sEnv) a (Just (Left b))   = mkCtxtLt sEnv b >>= return . (a,)
    mkCtxtVal (Just sEnv) a (Just (Right bs)) = mapM (mkCtxtLt sEnv) bs >>= return . (a,) . SList

    mkCtxtLt sEnv sn = do
      lv <- lookupSpliceE sn sEnv >>= ltLabel
      tv <- lookupSpliceE sn sEnv >>= ltType
      return $ spliceRecord [(spliceVIdSym, lv), (spliceVTSym, tv)]


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
endpoint isSource = if isSource then choice $ [value]++common else choice common
  where common = [builtin isSource, file isSource, network isSource]

value :: K3Parser EndpointBuilder
value = mkValueStream <$> (symbol "value" *> expr)
  where mkValueStream e _ _ = Right (ValueEP, Just e, [])

builtin :: Bool -> K3Parser EndpointBuilder
builtin isSource = mkBuiltin <$> builtinChannels <*> format
  where mkBuiltin idE formatE n t =
          builtinSpec idE formatE >>= \s -> return $ endpointMethods isSource s idE formatE n t
        builtinSpec idE formatE = BuiltinEP <$> S.symbolS idE <*> S.symbolS formatE

file :: Bool -> K3Parser EndpointBuilder
file isSource = mkFile <$> (symbol "file" *> eCString) <*> format
  where mkFile argE formatE n t =
          fileSpec argE formatE >>= \s -> return $ endpointMethods isSource s argE formatE n t
        fileSpec argE formatE = FileEP <$> S.exprS argE <*> S.symbolS formatE

network :: Bool -> K3Parser EndpointBuilder
network isSource = mkNetwork <$> (symbol "network" *> eAddress) <*> format
  where mkNetwork addrE formatE n t =
          networkSpec addrE formatE >>= \s -> return $ endpointMethods isSource s addrE formatE n t
        networkSpec addrE formatE = NetworkEP <$> S.exprS addrE <*> S.symbolS formatE

builtinChannels :: ExpressionParser
builtinChannels = choice [ch "stdin", ch "stdout", ch "stderr"]
  where ch s = try (symbol s >> return (EC.constant $ CString s))

format :: ExpressionParser
format = choice [fmt "k3"]
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
  where updateBindings (safePopFrame -> ((s,d), env)) =
          case lookup src s of
            Just (es, Just b, q, g) -> Right $ (replaceAssoc s src (es, Just (dest:b), q, g), d):env
            Just (_, Nothing, _, _) -> Left  $ "Invalid binding for endpoint " ++ src
            Nothing                 -> Left  $ "Invalid binding, no source " ++ src


-- | Records endpoint identifiers and initializer expressions in a K3 parsing environment
trackEndpoint :: EndpointSpec -> K3 Declaration -> DeclParser
trackEndpoint eSpec d
  | DGlobal n t eOpt <- tag d, TSource <- tag t = track True n eOpt >> return d
  | DGlobal n t eOpt <- tag d, TSink <- tag t   = track False n eOpt >> return d
  | otherwise = return d

  where
    track isSource n eOpt = modifyEnvF_ $ addEndpointGoExpr isSource n eOpt

    addEndpointGoExpr isSource n eOpt (safePopFrame -> ((fs,fd), env)) =
      case (eOpt, isSource) of
        (Just _, True)   -> Right $ refresh n fs fd env (Just [], n, Nothing)
        (Nothing, True)  -> Right $ refresh n fs fd env (Just [], n, Just $ mkRunSourceE n)
        (Just _, False)  -> Right $ refresh n fs fd env (Nothing, n, Just $ mkRunSinkE n)
        (_,_)            -> Left  $ "Invalid endpoint initializer"

    refresh n fs fd env (a,b,c) = (replaceAssoc fs n (eSpec, a, b, c), fd):env


-- | Records defaults in a K3 parsing environment
trackDefault :: Identifier -> K3Parser ()
trackDefault n = modifyEnv_ $ updateState
  where updateState (safePopFrame -> ((s,d), env)) = (s,replaceAssoc d "" n):env


-- | Completes any stateful processing needed for the role.
--   This includes handling 'feed' clauses, and checking and qualifying role defaults.
postProcessRole :: Identifier -> ([K3 Declaration], EnvFrame) -> K3Parser [K3 Declaration]
postProcessRole n (dl, frame) =
  modifyEnvF_ (ensureQualified frame) >> processEndpoints frame

  where processEndpoints (s,_) = addBuilderDecls $ map (annotateEndpoint s . attachSource s) dl

        addBuilderDecls dAndExtras =
          let (ndl, extrasl) = unzip dAndExtras
          in modifyBuilderDeclsF_ (Right . ((concat extrasl) ++)) >> return ndl

        attachSource s = bindSource $ sourceBindings s
        annotateEndpoint s (d, extraDecls)
          | DGlobal en t _ <- tag d, TSource <- tag t = (maybe d (d @+) $ syntaxAnnotation en s, extraDecls)
          | DGlobal en t _ <- tag d, TSink   <- tag t = (maybe d (d @+) $ syntaxAnnotation en s, extraDecls)
          | otherwise = (d, extraDecls)

        syntaxAnnotation en s =
          lookup en s
            >>= (\(enSpec,bindingsOpt,_,_) -> return (enSpec, maybe [] id bindingsOpt))
            >>= return . DSyntax . uncurry EndpointDeclaration

        ensureQualified poppedFrame (safePopFrame -> (frame', env)) =
          case validateDefaults poppedFrame of
            (_, [])     -> Right $ (qualifyRole' poppedFrame frame'):env
            (_, failed) -> Left  $ "Invalid defaults\n" ++ qualifyError poppedFrame failed

        validateDefaults (s,d)     = partition ((flip elem $ qualifiedSources s) . snd) d
        qualifyRole' (s,d) (s2,d2) = (map qualifySource s ++ s2, qualifyDefaults d ++ d2)

        qualifySource (eid, (es, Just b, q, g)) = (eid, (es, Just b, prefix' "." n q, g))
        qualifySource x = x

        qualifyDefaults = map $ uncurry $ flip (,) . prefix' "." n

        qualifyError frame' failed = "Frame: " ++ show frame' ++ "\nFailed: " ++ show failed

        prefix' sep x z = if x == "" then z else x ++ sep ++ z


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
    annotateExpr = traverse (\e -> parserWithUID (\uid -> annotateNode (any isEUID) [EUID $ UID uid] e))
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
