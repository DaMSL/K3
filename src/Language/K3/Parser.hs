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
import qualified Data.Map as Map
import Data.Maybe
import Data.Traversable hiding ( mapM )
import Data.Tree

import Debug.Trace

import System.FilePath
import qualified Text.Parsec          as P
import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Token

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Literal     as LC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser.DataTypes
import Language.K3.Parser.Operator
import Language.K3.Parser.ProgramBuilder
import Language.K3.Parser.Preprocessor
import Language.K3.Utils.Pretty
import qualified Language.K3.Utils.Pretty.Syntax as S


{- Main parsing functions -}
stringifyError :: Either P.ParseError a -> Either String a
stringifyError = either (Left . show) Right

runK3Parser :: Maybe ParserState -> K3Parser a -> String -> Either P.ParseError a
runK3Parser Nothing   p s = P.runParser p emptyParserState "" s
runK3Parser (Just st) p s = P.runParser p st "" s

maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (const Nothing) Just $ runK3Parser Nothing (head <$> endBy1 p eof) s

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
  where rule = mkProgram =<< (spaces *> endBy1 (roleBody noDriver "") eof)
        mkProgram l = P.getState >>= return . DC.role defaultRoleName . ((concat l) ++) . getGeneratedDecls

        selfContainedProgram d = if noDriver then return d else (mkEntryPoints d >>= mkBuiltins)
        mkEntryPoints d = withEnv $ (uncurry $ processInitsAndRoles d) . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins

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
                        [Right dGlobal, Right dTrigger, Right dRole, Left dMacroAnnotation, Right dAnnotation]
        edgeDecls   = mapM ((DUID #) . return) =<< choice [dSource, dSink]
        driverDecls = choice [dSelector, dFeed]
        ignores     = pInclude >> return ()

        props p = DUID # withProperties True dProperties p

        normalizeDeclAsList (Right p) = (:[]) <$> props p
        normalizeDeclAsList (Left  p) = try p >>= return . maybe [] (:[])

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

dMacroAnnotation :: K3Parser (Maybe (K3 Declaration))
dMacroAnnotation = namedIdentifier "macro annotation" "annotation" rule
  where rule x = mkGenerator =<< ((,,,) <$> x <*> spliceParameters <*> typeParameters <*> braces (some annotationMember))

        typeParameters   = keyword "given" *> keyword "type" *> typeVarDecls <|> return []
        spliceParameters = brackets (commaSep spliceParameter)
        spliceParameter  = choice [typeSParam, labelTypeSParam]
        typeSParam       = keyword "type" *> identifier
        labelTypeSParam  = identifier

        mkGenerator (n, [], tp, mems) = spliceAnnMems mems >>= return . Just . DC.annotation n tp
        mkGenerator (n, sp, tp, mems) =
          -- TODO: match splice parameter types (e.g., types vs label-types vs exprs.)
          let validateSplice env = Map.filterWithKey (\k _ -> k `elem` sp) env
              splicer spliceEnv  = SRDecl $ do
                                     modifySCtxtF_ $ \ctxt -> Right $ pushSCtxt (validateSplice spliceEnv) ctxt
                                     nmems <- spliceAnnMems mems
                                     modifySCtxtF_ $ Right . popSCtxt
                                     withGUID $ \i -> DC.annotation (concat [n, "_", show i]) tp nmems

              extendGen genEnv   = maybe (Right $ addGenE n splicer genEnv) duplicateSpliceErr $ lookupGenE n genEnv
              duplicateSpliceErr = const $ Left $ unwords ["Duplicate macro annotation for", n]
          in modifyGEnvF_ extendGen >> return Nothing

        spliceAnnMems mems = flip mapM mems $ \case
          Lifted    p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Lifted    p sn st seOpt mAnns
          Attribute p n t eOpt mAnns -> spliceDeclParts n t eOpt >>= \(sn, st, seOpt) -> return $ Attribute p sn st seOpt mAnns
          m -> return m

        spliceDeclParts n t eOpt = do
          sn <- spliceIdentifier n
          st <- spliceType t
          seOpt <- maybe (return Nothing) (\e -> spliceExpression e >>= return . Just) eOpt
          return (sn, st, seOpt)

        spliceIdentifier i = expectIdSplicer i

        spliceType = mapTree doSplice
          where
            doSplice [] t@(tag -> TDeclaredVar i) = expectTypeSplicer i >>= \nt -> return $ foldl (@+) nt $ annotations t
            doSplice ch t@(tag -> TRecord ids) = mapM spliceIdentifier ids >>= \nids -> return $ Node (TRecord nids :@: annotations t) ch
            doSplice ch (Node tg _) = return $ Node tg ch

        spliceExpression = mapTree doSplice
          where
            doSplice [] e@(tag -> EVariable i)           = expectExprSplicer i      >>= \ne   -> return $ foldl (@+) ne $ annotations e
            doSplice ch e@(tag -> ERecord ids)           = mapM expectIdSplicer ids >>= \nids -> return $ Node (ERecord nids :@: annotations e) ch
            doSplice ch e@(tag -> EProject i)            = expectIdSplicer i        >>= \nid  -> return $ Node (EProject nid :@: annotations e) ch
            doSplice ch e@(tag -> EAssign i)             = expectIdSplicer i        >>= \nid  -> return $ Node (EAssign nid :@: annotations e) ch
            doSplice ch e@(tag -> EConstant (CEmpty ct)) = spliceType ct            >>= \nct  -> return $ Node (EConstant (CEmpty nct) :@: annotations e) ch
            doSplice ch (Node tg _) = return $ Node tg ch

        expectIdSplicer   i = parseSplice i $ choice [try idFromParts, identifier]
        expectTypeSplicer i = parseSplice i $ choice [try typeFromParts, identifier >>= return . TC.declaredVar]
        expectExprSplicer i = parseSplice i $ choice [try exprFromParts, identifier >>= return . EC.variable]

        idFromParts   = evalIdSplice   =<< identParts
        typeFromParts = evalTypeSplice =<< identParts
        exprFromParts = evalExprSplice =<< identParts

        evalIdSplice l = return . concat =<< (flip mapM l $ \case
          Left n -> return n
          Right (SVLabel, i) -> evalSplice i spliceVIdSym $ \case { SLabel n -> return n; _ -> spliceTypeFail i spliceVIdSym }
          Right (_, i) -> spliceTypeFail i spliceVIdSym)

        evalTypeSplice = \case
          [Right (SVType, i)] -> evalSplice i spliceVTSym $ \case { SType  t -> return t; _ -> spliceTypeFail i spliceVTSym }
          l -> spliceTypeFail (partsAsString l) spliceVTSym

        evalExprSplice = \case
          [Right (SVExpr, i)] -> evalSplice i spliceVESym $ \case { SExpr  e -> return e; _ -> spliceTypeFail i spliceVESym }
          l -> spliceTypeFail (partsAsString l) spliceVESym

        evalSplice i kind f = parserWithSCtxt $ \ctxt -> maybe (spliceFail i kind "lookup failed") f $ lookupSCtxt i kind ctxt

        parseSplice s p = P.getState >>= \st -> either P.parserFail return $ stringifyError $ runK3Parser (Just st) p s

        partsAsString l = concat $ flip map l $ \case
          Left n -> n
          Right (SVLabel, i) -> "#["  ++ i ++ "]"
          Right (SVType,  i) -> "::[" ++ i ++ "]"
          Right (SVExpr,  i) -> "$["  ++ i ++ "]"


        spliceTypeFail i kind = spliceFail i kind "invalid type"
        spliceFail n kind msg = P.parserFail $ unwords ["Failed to splice a", kind , "symbol", n, ":", msg]


dAnnotation :: DeclParser
dAnnotation = namedDecl "annotation" "annotation" $ rule . (DC.annotation <$>)
  where rule x = x <*> typeParameters <*> braces (some annotationMember)
        typeParameters = keyword "given" *> keyword "type" *> typeVarDecls <|> return []

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

uidOver :: K3Parser (UID -> a) -> K3Parser a
uidOver parser = parserWithUID $ ap (fmap (. UID) parser) . return

pInclude :: K3Parser String
pInclude = keyword "include" >> stringLiteral


{- Types -}
typeExpr :: TypeParser
typeExpr = typeError "expression" $ TUID # tTermOrFun

qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = typeExprError "qualified" $ flip (@+) <$> (option TImmutable typeQualifier) <*> typeExpr

polymorphicTypeExpr :: TypeParser
polymorphicTypeExpr =
  typeExprError "polymorphic" $
        (TUID # TC.forAll <$ keyword "forall" <*> typeVarDecls <*
            symbol "." <*> qualifiedTypeExpr)
    <|> qualifiedTypeExpr

typeVarDecls :: K3Parser [TypeVarDecl]
typeVarDecls = sepBy typeVarDecl (symbol ",")

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
tDeclared = typeExprError "declared" $ ( TUID # ) $ TC.declaredVar <$> identifier


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
    l  -> foldM (\accE (i, sp) -> EUID # (return $ (EC.project i accE) @+ ESpan sp)) e l
  where
    rawTerm = wrapInComments $ withProperties False eProperties $
        choice [ (try eAssign),
                 (try eAddress),
                 eLiterals,
                 eLambda,
                 eCondition,
                 eLet,
                 eCase,
                 eBind,
                 eSelf ]

    eProject = dot *> identifier

    wrapInComments p =
      (\c1 e c2 -> (//) attachComment (c1++c2) e) <$> comment False <*> p <*> comment True

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
        litTuple   = mkTuple   <$> (parens $ commaSep1 qualifiedLiteral)

        mkTuple [x] = stripSpan <$> x
        mkTuple l   = LC.tuple l
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
withAnnotations p = option [] (symbol "@" *> p)

tAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Type]
tAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = annotationUse sEnv TAnnotation

eAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Expression]
eAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = annotationUse sEnv EAnnotation

lAnnotations :: Maybe SpliceEnv -> K3Parser [Annotation Literal]
lAnnotations sEnv = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = annotationUse sEnv LAnnotation

annotationUse :: Maybe SpliceEnv -> (Identifier -> Annotation a) -> K3Parser (Annotation a)
annotationUse sEnv aCtor = try macroOrAnn
  where macroOrAnn = mkMacroOrAnn =<< ((,) <$> identifier <*> option [] (parens $ commaSep1 spliceParam))
        mkMacroOrAnn (n, []) = return $ aCtor n
        mkMacroOrAnn np = mkAnnDecl np

        mkAnnDecl (n, params) = parserWithGEnv $ \gEnv ->
          let nsenv = mkSpliceEnv $ catMaybes params
          in maybe (spliceLookupErr n) (expectSpliceAnnotation . ($ nsenv)) $ Map.lookup n gEnv

        expectSpliceAnnotation (SRDecl p) = do
          decl <- p
          case tag decl of
            DAnnotation n _ _ -> modifyGDeclsF_ (Right . (++[decl])) >> return (aCtor n)
            _ -> P.parserFail $ boxToString $ ["Invalid annotation splice"] %+ prettyLines decl

        expectSpliceAnnotation _ = P.parserFail "Invalid annotation splice"

        spliceParam = choice [sLabel, sType, sExpr, sLabelType]
        sLabel      = (\a b -> mkSRepr spliceVIdSym a $ SLabel b) <$> (symbol "label" *> identifier) <*> (symbol "=" *> identifier)
        sType       = (\a b -> mkSRepr spliceVESym  a $ SType  b) <$> (symbol "type"  *> identifier) <*> (symbol "=" *> typeExpr)
        sExpr       = (\a b -> mkSRepr spliceVTSym  a $ SExpr  b) <$> (symbol "expr"  *> identifier) <*> (symbol "=" *> expr)
        sLabelType  = mkLabelTypeSRepr sEnv <$> identifier <*> optional (symbol "=" *> identifier)

        mkSRepr sym n v = Just (n, mkSpliceReprEnv [(sym, v)])

        mkLabelTypeSRepr Nothing _ _ = Nothing
        mkLabelTypeSRepr (Just sEnv') a bOpt =
          let lvOpt = lookupSpliceE (maybe a id bOpt) spliceVIdSym sEnv'
              tvOpt = lookupSpliceE (maybe a id bOpt) spliceVTSym  sEnv'
          in case (lvOpt, tvOpt) of
               (Just lv, Just tv) -> Just (a, mkSpliceReprEnv [(spliceVIdSym, lv), (spliceVTSym, tv)])
               (_, _) -> Nothing

        spliceLookupErr n = P.parserFail $ unwords ["Could not find macro", n]

mkRecordSpliceEnv :: [Identifier] -> [K3 Type] -> SpliceEnv
mkRecordSpliceEnv ids tl = mkSpliceEnv $ map mkSpliceEnvEntry $ zip ids tl
  where mkSpliceEnvEntry (i,t) = (i, mkSpliceReprEnv [(spliceVIdSym, SLabel i), (spliceVTSym, SType t)])

withProperties :: (Eq (Annotation a))
               =>  Bool -> K3Parser [Annotation a] -> K3Parser (K3 a) -> K3Parser (K3 a)
withProperties asPrefix prop tree =
    if asPrefix then (flip propertize) <$> optionalProperties prop <*> tree
                else propertize <$> tree <*> optionalProperties prop
  where propertize a (Just b) = foldl (@+) a b
        propertize a Nothing  = a

optionalProperties :: K3Parser [Annotation a] -> K3Parser (Maybe [Annotation a])
optionalProperties p = optional (symbol "@:" *> p)

properties :: (Identifier -> Maybe (K3 Literal) -> Annotation a) -> K3Parser [Annotation a]
properties ctor = try ((:[]) <$> p) <|> try (braces $ commaSep1 p)
  where p = ctor <$> identifier <*> optional literal

dProperties :: K3Parser [Annotation Declaration]
dProperties = properties DProperty

eProperties :: K3Parser [Annotation Expression]
eProperties = properties EProperty


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

  where processEndpoints (s,_) = return . flip concatMap dl $ annotateEndpoint s . attachSource s
        attachSource s         = bindSource $ sourceBindings s

        annotateEndpoint _ [] = []
        annotateEndpoint s (d:drest)
          | DGlobal en t _ <- tag d, TSource <- tag t = (maybe d (d @+) $ syntaxAnnotation en s):drest
          | DGlobal en t _ <- tag d, TSink   <- tag t = (maybe d (d @+) $ syntaxAnnotation en s):drest
          | otherwise = d:drest

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
            DAnnotation n tis mems -> rebuildDecl d uid $ DAnnotation n tis mems
              --  TODO: recur through members (e.g., attributes w/ initializers)
              --  and ensure they have a uid

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
