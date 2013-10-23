{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | K3 Parser.
module Language.K3.Parser (
  K3Parser,
  identifier,
  declaration,
  qualifiedTypeExpr,
  typeExpr,
  qualifiedLiteral,
  literal,
  qualifiedExpr,
  expr,
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
import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.HashSet (HashSet)
import Data.List
import Data.String
import Data.Traversable hiding ( mapM )

import System.FilePath

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP

import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Expression
import Text.Parser.Token
import Text.Parser.Token.Style

import Text.Parser.Parsec()

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

import Language.K3.Parser.ProgramBuilder
import Language.K3.Parser.Preprocessor
import qualified Language.K3.Utils.Pretty.Syntax as S

{- Note: debugging helper
import Debug.Trace

myTrace :: String -> K3Parser a -> K3Parser a
myTrace s p = PP.getInput >>= (\i -> trace (s++" "++i) p)
-}

{- Type synonyms for parser return types -}
{-| Parser environment type.
    This includes two scoped frames, one for source metadata, and another as a
    list of K3 program entry points as role-qualified sources that can be consumed.    
-}

-- endpoint name => endpoint spec, bound triggers, qualified name, initializer expression
type EndpointInfo      = (EndpointSpec, Maybe [Identifier], Identifier, Maybe (K3 Expression))
type EndpointsBQG      = [(Identifier, EndpointInfo)]

-- role name, default name
type DefaultEntries    = [(Identifier, Identifier)] 

type EnvironmentFrame  = (EndpointsBQG, DefaultEntries)
type ParserEnvironment = [EnvironmentFrame]
type ParserState       = (Int, ParserEnvironment)

-- | Parser type synonyms
type K3Parser          = PP.ParsecT String ParserState Identity
type TypeParser        = K3Parser (K3 Type)
type LiteralParser     = K3Parser (K3 Literal)
type ExpressionParser  = K3Parser (K3 Expression)
type BinderParser      = K3Parser Binder
type DeclParser        = K3Parser (K3 Declaration)

-- | Additional parsing type synonyms.
type EndpointMethods   = (EndpointSpec, Maybe (K3 Expression), [K3 Declaration])
type EndpointBuilder   = Identifier -> K3 Type -> Either String EndpointMethods

type K3Operator           = Language.K3.Core.Expression.Operator
type ParserOperator       = Text.Parser.Expression.Operator K3Parser
type K3BinaryOperator     = K3 Expression -> K3 Expression -> K3 Expression
type K3UnaryOperator      = K3 Expression -> K3 Expression

{- Helpers -}
set :: [String] -> HashSet String
set = HashSet.fromList

parseError :: Parsing m => String -> String -> m a -> m a
parseError rule i m = m <?> (i ++ " " ++ rule)

suffixError :: Parsing m => String -> String -> m a -> m a
suffixError k x = parseError x k

declError :: Parsing m => String -> m a -> m a
declError x = parseError "declaration" x

typeError :: Parsing m => String -> m a -> m a
typeError x = parseError x "type"

typeExprError :: Parsing m => String -> m a -> m a
typeExprError x = parseError "type expression" x

litError :: Parsing m => String -> m a -> m a
litError x = parseError "literal" x

exprError :: Parsing m => String -> m a -> m a
exprError x = parseError "expression" x


{- Span and UID helpers -}
emptySpan :: Span 
emptySpan = Span "<dummy>" 0 0 0 0

-- | Span computation.
--   TODO: what if source names do not match?
(<->) :: (AContainer a, Eq (IElement a)) => (Span -> IElement a) -> K3Parser a -> K3Parser a
(<->) cstr parser = annotate <$> PP.getPosition <*> parser <*> PP.getPosition
  where annotate start x end = x @+ (cstr $ mkSpan start end)

mkSpan :: P.SourcePos -> P.SourcePos -> Span
mkSpan s e = Span (P.sourceName s) (P.sourceLine s) (P.sourceColumn s)
                                   (P.sourceLine e) (P.sourceColumn e)

spanned :: K3Parser a -> K3Parser (a, Span)
spanned parser = do
  start <- PP.getPosition
  result <- parser
  end <- PP.getPosition
  return (result, mkSpan start end)

infixl 1 <->
infixl 1 #

getESpan :: [Annotation Expression] -> Span
getESpan l = maybe emptySpan extractSpan $ find isESpan l
  where extractSpan (ESpan s) = s
        extractSpan _         = emptySpan

binOpSpan :: K3BinaryOperator -> K3 Expression -> K3 Expression -> K3 Expression
binOpSpan cstr l@(annotations -> la) r@(annotations -> ra) =
  (cstr l r) @+ (ESpan $ coverSpans (getESpan la) (getESpan ra))

unOpSpan :: String -> K3UnaryOperator -> K3 Expression -> K3 Expression
unOpSpan opName cstr e@(annotations -> al) =
  (cstr e) @+ (ESpan $ (prefixSpan $ length opName) $ getESpan al)


-- Some code for UID annotation.  Abusing typeclasses into ad-hoc polymorphism.
-- |Ensures that the provided parser generates an element with a UID.
ensureUID :: (Eq (IElement a), AContainer a)
          => (IElement a -> Bool) -> (UID -> IElement a) -> K3Parser a
          -> K3Parser a
ensureUID matcher constructor parser = do
  x <- parser
  case x @~ matcher of
    Just _ -> return x
    Nothing -> (\uid -> x @+ constructor uid) <$> nextUID

class (Eq (IElement a), AContainer a) => UIDAttachable a where
  (#) :: (UID -> IElement a) -> K3Parser a -> K3Parser a
instance UIDAttachable (K3 Expression) where
  (#) = ensureUID isEUID
instance UIDAttachable (K3 Type) where
  (#) = ensureUID isTUID
instance UIDAttachable (K3 Declaration) where
  (#) = ensureUID isDUID


{- Parsing state helpers -}
withEnvironment :: (ParserEnvironment -> a) -> K3Parser a
withEnvironment f = PP.getState >>= return . f . snd

modifyEnvironment :: (ParserEnvironment -> (ParserEnvironment, a)) -> K3Parser a
modifyEnvironment f = modifyEnvironmentF $ Right . f

modifyEnvironment_ :: (ParserEnvironment -> ParserEnvironment) -> K3Parser ()
modifyEnvironment_ f = modifyEnvironment $ (,()) . f

modifyEnvironmentF :: (ParserEnvironment -> Either String (ParserEnvironment, a)) -> K3Parser a
modifyEnvironmentF f = PP.getState >>= (\(x,y) -> case f y of
  Left errMsg    -> PP.parserFail errMsg
  Right (nenv,r) -> PP.putState (x, nenv) >> return r)

modifyEnvironmentF_ :: (ParserEnvironment -> Either String ParserEnvironment) -> K3Parser ()
modifyEnvironmentF_ f = modifyEnvironmentF $ (>>= Right . (,())) . f

parserWithUID :: (Int -> K3Parser a) -> K3Parser a
parserWithUID f = PP.getState >>= (\(x,y) -> PP.putState (x+1, y) >> f x)

withUID :: (Int -> a) -> K3Parser a
withUID f = parserWithUID $ return . f

{- Note: unused
modifyUID :: (Int -> (Int,a)) -> K3Parser a
modifyUID f = PP.getState >>= (\(old, env) -> let (new, r) = f old in PP.putState (new, env) >> return r)

modifyUID_ :: (Int -> Int) -> K3Parser ()
modifyUID_ f = modifyUID $ (,()) . f
-}

nextUID :: K3Parser UID
nextUID = withUID UID


{- Language definition constants -}
k3Operators :: [[Char]]
k3Operators = [
    "+", "-", "*", "/",
    "==", "!=", "<>", "<", ">", ">=", "<=", ";"
  ]

k3Keywords :: [[Char]]
k3Keywords = [
    {- Types -}
    "int", "bool", "real", "string",
    "immut", "mut", "witness", "option", "ind" , "collection",

    {- Declarations -}
    "declare", "fun", "trigger", "source", "sink", "feed",

    {- Expressions -}
    "let", "in", "if", "then", "else", "case", "of", "bind", "as",
    "and", "or", "not",

    {- Values -}
    "true", "false", "ind", "Some", "None", "empty",

    {- Annotation declarations -}
    "annotation", "lifted", "provides", "requires",

    {- Annotation keywords -}
    "self", "structure", "horizon", "content", "forall"
  ]

{- Style definitions for parsers library -}

k3Ops :: TokenParsing m => IdentifierStyle m
k3Ops = emptyOps { _styleReserved = set k3Operators }

k3Idents :: TokenParsing m => IdentifierStyle m
k3Idents = emptyIdents { _styleReserved = set k3Keywords }

operator :: (TokenParsing m, Monad m) => String -> m ()
operator = reserve k3Ops

identifier :: (TokenParsing m, Monad m, IsString s) => m s
identifier = ident k3Idents

keyword :: (TokenParsing m, Monad m) => String -> m ()
keyword = reserve k3Idents


{- Comments -}

mkComment :: Bool -> P.SourcePos -> String -> P.SourcePos -> SyntaxAnnotation
mkComment multi start contents end = SourceComment multi (mkSpan start end) contents

multiComment :: K3Parser SyntaxAnnotation
multiComment = (mkComment True 
                 <$> PP.getPosition 
                 <*> (symbol "/*" *> manyTill anyChar (try $ symbol "*/") <* spaces)
                 <*> PP.getPosition) <?> "multi-line comment"

singleComment :: K3Parser SyntaxAnnotation
singleComment = (mkComment False
                 <$> PP.getPosition
                 <*> (symbol "//" *> manyTill anyChar (try newline) <* spaces)
                 <*> PP.getPosition) <?> "single line comment"

comment :: K3Parser [SyntaxAnnotation]
comment = many (choice [try multiComment, singleComment])

-- | Helper to attach comment annotations
(//) :: (a -> SyntaxAnnotation -> a) -> [SyntaxAnnotation] -> a -> a
(//) attachF l x = foldl attachF x l


{- Main parsing functions -}
stringifyError :: Either P.ParseError a -> Either String a
stringifyError = either (Left . show) Right

emptyParserState :: ParserState
emptyParserState = (0,[])

runK3Parser :: K3Parser a -> String -> Either P.ParseError a
runK3Parser p s = P.runParser p emptyParserState "" s

maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (const Nothing) Just $ runK3Parser p s

parseType :: String -> Maybe (K3 Type)
parseType = maybeParser typeExpr

parseLiteral :: String -> Maybe (K3 Literal)
parseLiteral = maybeParser literal

parseExpression :: String -> Maybe (K3 Expression)
parseExpression = maybeParser expr

parseDeclaration :: String -> Maybe (K3 Declaration)
parseDeclaration s = either (const Nothing) mkRole $ runK3Parser declaration s
  where mkRole l = Just $ DC.role defaultRoleName l

parseSimpleK3 :: String -> Maybe (K3 Declaration)
parseSimpleK3 s = either (const Nothing) Just $ runK3Parser (program False) s

parseK3 :: [FilePath] -> String -> IO (Either String (K3 Declaration))
parseK3 includePaths s = do
  searchPaths   <- if null includePaths then getSearchPath else return includePaths
  subFiles      <- processIncludes searchPaths (lines s) []
  fileContents  <- mapM readFile subFiles >>= return . (++ [s])
  return $ fst <$> foldr chainValidParse (Right $ (DC.role defaultRoleName [], True)) fileContents
  where
    chainValidParse c parse = parse >>= parseAndCompose c
    parseAndCompose src (p, asTopLevel) =
      parseAtLevel asTopLevel src >>= return . flip ((,) `on` (tag &&& children)) p >>= \case
        ((DRole n, ch), (DRole n2, ch2))
          | n == defaultRoleName && n == n2 -> return (DC.role n $ ch++ch2, False)
          | otherwise                       -> programError
        _                                   -> programError
    parseAtLevel asTopLevel = stringifyError . runK3Parser (program $ not asTopLevel)
    programError = Left "Invalid program, expected top-level role."


{- K3 grammar parsers -}

-- TODO: inline testing
program :: Bool -> DeclParser
program asInclude = DSpan <-> (rule >>= selfContainedProgram)
  where rule = mkProgram <$> endBy (roleBody "") eof
        mkProgram l = DC.role defaultRoleName $ concat l
        
        selfContainedProgram d = if asInclude then return d else (mkEntryPoints d >>= mkBuiltins)
        mkEntryPoints d = withEnvironment $ (uncurry $ processInitsAndRoles d) . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins

roleBody :: Identifier -> K3Parser [K3 Declaration]
roleBody n = pushBindings >> rule >>= popBindings >>= postProcessRole n
  where rule = some declaration >>= return . concat
        pushBindings = modifyEnvironment_ addFrame
        popBindings dl = modifyEnvironment (\env -> (removeFrame env, (dl, currentFrame env)))
        

{- Declarations -}
declaration :: K3Parser [K3 Declaration]
declaration = (//) attachComment <$> comment <*> 
              choice [ignores >> return [], singleDecls, multiDecls, sugaredDecls >> return []]

  where singleDecls  = (:[]) <$> (DUID # choice [dGlobal, dTrigger, dRole, dAnnotation])
        multiDecls   = mapM ((DUID #) . return) =<< choice [dSource, dSink]
        sugaredDecls = choice [dSelector, dFeed]
        ignores      = pInclude >> return ()

        attachComment [] _      = []
        attachComment (h:t) cmt = (h @+ DSyntax cmt):t

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (mkGlobal <$>)
  where rule x = x <* colon <*> polymorphicTypeExpr <*> (optional equateExpr)
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
dRole = chainedNamedBraceDecl n n roleBody DC.role
  where n = "role"

dSelector :: K3Parser ()
dSelector = namedIdentifier "selector" "default" (id <$>) >>= trackDefault

dAnnotation :: DeclParser
dAnnotation = namedDecl "annotation" "annotation" $ rule . (DC.annotation <$>)
  where rule x = x <*> annotationTypeParametersParser
                   <*> braces (some annotationMember)
        annotationTypeParametersParser =
              keyword "given" *> keyword "type" *> typeVarDecls
          <|> return []

{- Annotation declaration members -}
annotationMember :: K3Parser AnnMemDecl
annotationMember = 
  memberError $ mkMember <$> polarity <*> (choice $ map uidOver [liftedOrAttribute, subAnnotation])
  where 
        liftedOrAttribute = mkLA  <$> optional (keyword "lifted") <*> identifier <* colon
                                  <*> qualifiedTypeExpr <*> optional equateExpr
        
        subAnnotation     = mkSub <$> (keyword "annotation" *> identifier)
        
        mkMember p (Left (Just _,  n, qte, eOpt, uid)) = Lifted p n qte (propagateQualifier qte eOpt) uid
        mkMember p (Left (Nothing, n, qte, eOpt, uid)) = Attribute p n qte (propagateQualifier qte eOpt) uid
        mkMember p (Right (n, uid))                    = MAnnotation p n uid
        
        mkLA kOpt n qte eOpt uid = Left (kOpt, n, qte, eOpt, uid)
        mkSub n uid              = Right (n, uid)

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
typeVarDecl = TypeVarDecl <$> identifier <*> pure Nothing <*>
                  option Nothing (Just <$ symbol "<=" <*> typeExpr) 
    

{- Parenthesized version of qualified types.
qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = typeExprError "qualified" $ 
  choice [parens $ qualifiedTypeExpr
         , flip (@+) <$> typeQualifier <*> typeExpr]
-}

typeQualifier :: K3Parser (Annotation Type)
typeQualifier = typeError "qualifier" $ choice [keyword "immut" >> return TImmutable,
                                                keyword "mut"   >> return TMutable]

{- Type terms -}
tTerm :: TypeParser
tTerm = TSpan <-> (//) attachComment <$> comment 
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
tCollection = cErr $ TUID #
                 mkCollectionType <$> (keyword "collection" *> tRecord)
                                  <*> (option [] (symbol "@" *> tAnnotations))
  where mkCollectionType t a = foldl (@+) (TC.collection t) a
        cErr = typeExprError "collection"

tBuiltIn :: TypeParser
tBuiltIn = typeExprError "builtin" $ choice $ map (\(kw,bi) -> keyword kw >> return (TC.builtIn bi))
              [ ("self",TSelf)
              , ("structure",TStructure)
              , ("horizon",THorizon)
              , ("content",TContent) ]

tDeclared :: TypeParser
tDeclared = typeExprError "declared" $ ( TUID # ) $ TC.declaredVar <$> identifier

tAnnotations :: K3Parser [Annotation Type]
tAnnotations = braces $ commaSep1 (mkTAnnotation <$> identifier)
  where mkTAnnotation x = TAnnotation x

{- Expressions -}

expr :: ExpressionParser
expr = parseError "expression" "k3" $ buildExpressionParser fullOpTable eApp

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
  e <- EUID # (ESpan <-> rawTerm)
  mi <- optional (spanned eProject)
  case mi of
    Nothing -> return e
    Just (i, sp) -> EUID # (return $ (EC.project i e) @+ ESpan sp)
  where
    rawTerm = (//) attachComment <$> comment <*> 
      choice [ (try eAssign),
               (try eAddress),
               eLiterals,
               eLambda,
               eCondition,
               eLet,
               eCase,
               eBind,
               eSelf  ]
    eProject = dot *> identifier
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
eCNumber = mkNumber <$> integerOrDouble
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
eEmpty = exprError "empty" $ mkEmpty <$> typedEmpty <*> (option [] (symbol "@" *> eAnnotations))
  where mkEmpty e a = foldl (@+) e a 
        typedEmpty = EC.empty <$> (keyword "empty" *> tRecord) 

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
              mkCollection <$> braces (choice [try singleField, multiField])
                           <*> (option [] (symbol "@" *> eAnnotations))
  where 
        singleField =     (symbol "|" *> idQType <* symbol "|")
                      >>= mkSingletonRecord (commaSep1 expr <* symbol "|")
        
        multiField  = (\a b c -> ((a:b), c))
                          <$> (symbol "|" *> idQType <* comma)
                          <*> commaSep1 idQType
                          <*> (symbol "|" *> commaSep1 expr <* symbol "|")
        
        mkCollection (tyl, el) a = EC.letIn cId (emptyC tyl a) $ EC.binop OSeq (mkInserts el) cVar

        mkInserts el = foldl (\acc e -> EC.binop OSeq acc $ mkInsert e) (mkInsert $ head el) (tail el)
        mkInsert     = EC.binop OApp (EC.project "insert" cVar)
        emptyC tyl a = foldl (@+) ((EC.empty $ TC.record tyl) @+ EImmutable) a
        (cId, cVar)  = ("__collection", EC.variable "__collection")

        mkSingletonRecord p (n,t) =
          p >>= return . ([(n,t)],) . map (EC.record . (:[]) . (n,) . (@+ EImmutable))

eAnnotations :: K3Parser [Annotation Expression]
eAnnotations = braces $ commaSep1 (mkEAnnotation <$> identifier)
  where mkEAnnotation x = EAnnotation x
  

{- Operators -}
uidTagBinOp :: K3Parser (a -> b -> K3 Expression)
            -> K3Parser (a -> b -> K3 Expression)
uidTagBinOp mg = (\g uid x y -> g x y @+ EUID uid) <$> mg <*> nextUID

uidTagUnOp :: K3Parser (a -> K3 Expression)
           -> K3Parser (a -> K3 Expression)
uidTagUnOp mg = (\g uid x -> g x @+ EUID uid) <$> mg <*> nextUID

binary :: String -> K3BinaryOperator -> Assoc -> (String -> K3Parser ())
       -> ParserOperator (K3 Expression)
binary op cstr assoc parser = Infix (uidTagBinOp $ (pure cstr) <* parser op) assoc

prefix :: String -> K3UnaryOperator -> (String -> K3Parser ())
       -> ParserOperator (K3 Expression)
prefix op cstr parser = Prefix (uidTagUnOp  $ (pure cstr) <* parser op)

{- Note: unused
postfix :: String -> K3UnaryOperator -> (String -> K3Parser ())
        -> ParserOperator (K3 Expression)
postfix op cstr parser = Postfix (uidTagUnOp  $ (pure cstr) <* parser op)
-}

binaryParseOp :: (String, K3Operator)
              -> ((String -> K3Parser ()) -> ParserOperator (K3 Expression))
binaryParseOp (opName, opTag) = binary opName (binOpSpan $ EC.binop opTag) AssocLeft

unaryParseOp :: (String, K3Operator)
             -> ((String -> K3Parser ()) -> ParserOperator (K3 Expression))
unaryParseOp (opName, opTag) = prefix opName (unOpSpan opName $ EC.unop opTag)

mkBinOp :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkBinOp x = binaryParseOp x operator

mkBinOpK :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkBinOpK x = binaryParseOp x keyword

{- Note: unused
mkUnOp :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkUnOp x = unaryParseOp x operator
-}

mkUnOpK :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkUnOpK x = unaryParseOp x keyword

nonSeqOpTable :: OperatorTable K3Parser (K3 Expression)
nonSeqOpTable =
  [   map mkBinOp  [("*",   OMul), ("/",  ODiv)],
      map mkBinOp  [("+",   OAdd), ("-",  OSub)],
      map mkBinOp  [("++",  OConcat)],
      map mkBinOp  [("<",   OLth), ("<=", OLeq), (">",  OGth), (">=", OGeq) ],
      map mkBinOp  [("==",  OEqu), ("!=", ONeq), ("<>", ONeq)],
      map mkUnOpK  [("not", ONot)],
      map mkBinOpK [("and", OAnd)],
      map mkBinOpK [("or",  OOr)]
  ]

fullOpTable :: OperatorTable K3Parser (K3 Expression)
fullOpTable = nonSeqOpTable ++
  [   map mkBinOp  [(";",   OSeq)]
  ]

{- Terms -}
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


{- Literal values -}

literal :: LiteralParser
literal = parseError "literal" "k3" $ choice [ 
    lTerminal,
    lOption,
    lIndirection,
    lTuple,
    lRecord,
    lEmpty,
    lCollection,
    lAddress ]

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
lEmpty = litError "empty" $ mkEmpty <$> typedEmpty <*> (option [] (symbol "@" *> lAnnotations))
  where mkEmpty l a = foldl (@+) l a 
        typedEmpty = LC.empty <$> (keyword "empty" *> tRecord) 

lCollection :: LiteralParser
lCollection = litError "collection" $
              mkCollection <$> braces (choice [try singleField, multiField])
                           <*> (option [] (symbol "@" *> lAnnotations))
  where 
    singleField =     (symbol "|" *> idQType <* symbol "|")
                  >>= mkSingletonRecord (commaSep1 literal <* symbol "|")
        
    multiField  = (\a b c -> ((a:b), c))
                    <$> (symbol "|" *> idQType <* comma)
                    <*> commaSep1 idQType
                    <*> (symbol "|" *> commaSep1 literal <* symbol "|")
        
    mkCollection (tyl, el) a = foldl (@+) ((LC.collection (TC.record tyl) el) @+ LImmutable) a

    mkSingletonRecord p (n,t) =
      p >>= return . ([(n,t)],) . map (LC.record . (:[]) . (n,) . (@+ LImmutable))

lAnnotations :: K3Parser [Annotation Literal]
lAnnotations = braces $ commaSep1 (mkLAnnotation <$> identifier)
  where mkLAnnotation x = LAnnotation x

lAddress :: LiteralParser
lAddress = litError "address" $ LC.address <$> ipAddress <* colon <*> port
  where ipAddress = LC.string <$> (some $ choice [alphaNum, oneOf "."])
        port = LC.int . fromIntegral <$> natural


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

{- Misc -}
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


{- Environment maintenance helpers -}

{- Note: unused
sourceState :: EnvironmentFrame -> EndpointsBQG
sourceState = fst

defaultEntries :: EnvironmentFrame -> DefaultEntries
defaultEntries = snd
-}

sourceBindings :: EndpointsBQG -> [(Identifier, Identifier)]
sourceBindings s = concatMap extractBindings s
  where extractBindings (x,(_,Just b,_,_))  = map ((,) x) b
        extractBindings (_,(_,Nothing,_,_)) = []

qualifiedSources :: EndpointsBQG -> [Identifier]
qualifiedSources s = concatMap (qualifiedSourceName . snd) s
  where qualifiedSourceName (_, Just _, x, _)  = [x]
        qualifiedSourceName (_, Nothing, _, _) = []

addFrame :: ParserEnvironment -> ParserEnvironment
addFrame env = ([],[]):env

removeFrame :: ParserEnvironment -> ParserEnvironment
removeFrame = tail

currentFrame :: ParserEnvironment -> EnvironmentFrame
currentFrame = head

safePopFrame :: ParserEnvironment -> (EnvironmentFrame, ParserEnvironment)
safePopFrame [] = (([],[]),[])
safePopFrame (h:t) = (h,t)

-- | Records source bindings in a K3 parsing environment
--   A parsing error is raised on an attempt to bind to anything other than a source.
trackBindings :: (Identifier, Identifier) -> K3Parser ()
trackBindings (src, dest) = modifyEnvironmentF_ $ updateBindings
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
    track isSource n eOpt = modifyEnvironmentF_ $ addEndpointGoExpr isSource n eOpt

    addEndpointGoExpr isSource n eOpt (safePopFrame -> ((fs,fd), env)) =
      case (eOpt, isSource) of
        (Just _, True)   -> Right $ refresh n fs fd env (Just [], n, Nothing)
        (Nothing, True)  -> Right $ refresh n fs fd env (Just [], n, Just $ mkRunSourceE n)
        (Just _, False)  -> Right $ refresh n fs fd env (Nothing, n, Just $ mkRunSinkE n)
        (_,_)            -> Left  $ "Invalid endpoint initializer"

    refresh n fs fd env (a,b,c) = (replaceAssoc fs n (eSpec, a, b, c), fd):env


-- | Records defaults in a K3 parsing environment
trackDefault :: Identifier -> K3Parser ()
trackDefault n = modifyEnvironment_ $ updateState
  where updateState (safePopFrame -> ((s,d), env)) = (s,replaceAssoc d "" n):env


-- | Completes any stateful processing needed for the role.
--   This includes handling 'feed' clauses, and checking and qualifying role defaults.
postProcessRole :: Identifier -> ([K3 Declaration], EnvironmentFrame) -> K3Parser [K3 Declaration]
postProcessRole n (dl, frame) = 
  modifyEnvironmentF_ (ensureQualified frame) >> processEndpoints frame
  
  where processEndpoints (s,_) = return . flip concatMap dl $ annotateEndpoint s . attachSource s
        attachSource s         = bindSource $ sourceBindings s
        
        annotateEndpoint _ [] = []
        annotateEndpoint s (d:drest)
          | DGlobal en t _ <- tag d, TSource <- tag t = (maybe d (d @+) (syntaxAnnotation en s)):drest
          | DGlobal en t _ <- tag d, TSink   <- tag t = (maybe d (d @+) (syntaxAnnotation en s)):drest
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
