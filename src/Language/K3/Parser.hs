{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | K3 Parser.
module Language.K3.Parser (
  declaration,
  qualifiedTypeExpr,
  typeExpr,
  qualifiedExpr,
  expr,
  parseType,
  parseExpression,
  parseDeclaration,
  parseK3,
  K3Parser,
  ensureUIDs
) where

import Control.Applicative
import Control.Arrow
import Control.Monad

import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.HashSet (HashSet)
import Data.List
import Data.String
import Data.Traversable
import Data.Tree

import Debug.Trace

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP

import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Expression
import Text.Parser.Token
import Text.Parser.Token.Style

import Text.Parser.Parsec

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Pretty
import Language.K3.Parser.ProgramBuilder

{- Type synonyms for parser return types -}
{-| Parser environment type.
    This includes two scoped frames, one for source metadata, and another as a
    list of K3 program entry points as role-qualified sources that can be consumed.    
-}

-- endpoint name => bound triggers, qualified name, initializer expression
type EndpointsBQG        = [(Identifier, (Maybe [Identifier], Identifier, Maybe (K3 Expression)))]

-- role name, default name
type DefaultEntries    = [(Identifier, Identifier)] 

type EnvironmentFrame  = (EndpointsBQG, DefaultEntries)
type ParserEnvironment = [EnvironmentFrame]
type ParserState       = (Int, ParserEnvironment)

type K3Parser a        = PP.ParsecT String ParserState Identity a
type TypeParser        = K3Parser (K3 Type)
type ExpressionParser  = K3Parser (K3 Expression)
type BinderParser      = K3Parser Binder
type DeclParser        = K3Parser (K3 Declaration)

{- Helpers -}
optAsList :: Maybe a -> [a]
optAsList (Just a) = [a]
optAsList Nothing = []

set :: [String] -> HashSet String
set = HashSet.fromList

filterOptions :: [Maybe a] -> [a]
filterOptions = concat . map optAsList

parseError :: Parsing m => String -> String -> m a -> m a
parseError rule i m = m <?> (i ++ " " ++ rule)

declError :: Parsing m => String -> m a -> m a
declError x = parseError "declaration" x

typeError :: Parsing m => String -> m a -> m a
typeError x = parseError "type expression" x

exprError :: Parsing m => String -> m a -> m a
exprError x = parseError "expression" x

-- | Span computation.
--   TODO: what if source names do not match?
(<->) cstr parser = annotate <$> PP.getPosition <*> parser <*> PP.getPosition
  where annotate start x end = x @+ (cstr $ mkSpan start end)

mkSpan s e = Span (P.sourceName s) (P.sourceLine s) (P.sourceColumn s)
                                   (P.sourceLine e) (P.sourceColumn e)
                                   
spanned parser = do
  start <- PP.getPosition
  result <- parser
  end <- PP.getPosition
  return (result, mkSpan start end)

infixl 1 <->
infixl 1 #

-- | UID annotation.
(#) :: (Eq (IElement a), AContainer a)
    => (UID -> IElement a) -> K3Parser a -> K3Parser a
(#) cstr parser = parserWithUID (\uid -> (@+ (cstr $ UID uid)) <$> parser)

{- Language definition constants -}
k3Operators = [
    "+", "-", "*", "/",
    "==", "!=", "<>", "<", ">", ">=", "<=", ";"
  ]

k3Keywords = [
    {- Types -}
    "int", "bool", "real", "string",
    "immut", "mut", "witness", "option", "ind" , "collection",
    "self", "structure", "horizon", "content",
    {- Declarations -}
    "declare", "trigger", "source", "fun",
    {- Expressions -}
    "let", "in", "if", "then", "else", "case", "of", "bind", "as",
    "and", "or", "not",
    {- Values -}
    "true", "false", "ind", "Some", "None", "empty",
    {- Annotation declarations -}
    "annotation", "lifted", "provides", "requires"
  ]

{- Style definitions for parsers library -}
k3CommentStyle :: CommentStyle
k3CommentStyle = javaCommentStyle

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


{- Main parsing functions -}
emptyParserState :: ParserState
emptyParserState = (0,[])

maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (const Nothing) Just $ P.runParser p emptyParserState "" s

parseType :: String -> Maybe (K3 Type)
parseType = maybeParser typeExpr

parseExpression :: String -> Maybe (K3 Expression)
parseExpression = maybeParser expr

parseDeclaration :: String -> Maybe (K3 Declaration)
parseDeclaration s = either (const Nothing) id $ P.runParser declaration emptyParserState "" s

parseK3 :: String -> Either String (K3 Declaration)
parseK3 s = either (Left . show) Right $ P.runParser program emptyParserState "" s


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

modifyUID :: (Int -> (Int,a)) -> K3Parser a
modifyUID f = PP.getState >>= (\(old, env) -> let (new, r) = f old in PP.putState (new, env) >> return r)

modifyUID_ :: (Int -> Int) -> K3Parser ()
modifyUID_ f = modifyUID $ (,()) . f

nextUID :: K3Parser UID
nextUID = withUID UID

{- K3 grammar parsers -}

-- TODO: inline testing
program :: DeclParser
program = DSpan <-> (rule >>= mkEntryPoints >>= mkBuiltins)
  where rule = mkProgram <$> endBy (roleBody "") eof
        mkProgram l = DC.role defaultRoleName $ concat l
        mkEntryPoints d = withEnvironment $ (uncurry $ desugarRoleEntries d) . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins

roleBody :: Identifier -> K3Parser [K3 Declaration]
roleBody n = pushBindings >> rule >>= popBindings >>= desugarRole n
  where rule = some declaration >>= return . filterOptions
        pushBindings = modifyEnvironment_ addFrame
        popBindings dl = modifyEnvironment (\env -> (removeFrame env, (dl, currentFrame env)))
        

{- Declarations -}
declaration :: K3Parser (Maybe (K3 Declaration))
declaration = choice [decls >>= return . Just, sugaredDecls >> return Nothing]
  where decls        = DUID # choice [dGlobal, dTrigger, dSource, dSink, dRole, dAnnotation]
        sugaredDecls =        choice [dSelector, dBind]

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (mkGlobal <$>)
  where rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateNSExpr)
        mkGlobal n qte eOpt = DC.global n qte (propagateQualifier qte eOpt)

dTrigger :: DeclParser
dTrigger = namedDecl "trigger" "trigger" $ rule . (DC.trigger <$>)
  where rule x = x <* colon <*> typeExpr <*> equateExpr

dEndpoint :: String -> String -> Bool -> DeclParser
dEndpoint kind name isSource = 
  namedDecl kind name $ join . rule . (mkEndpoint <$>)
  where rule x = x <*> (colon *> typeExpr) <*> (symbol "=" *> (channel isSource))
        
        (typeCstr, stateModifier) = 
          (if isSource then TC.source else TC.sink, trackEndpoint)
        
        mkEndpoint n t channelCstr =
          stateModifier $ uncurry (DC.endpoint n (typeCstr t)) (channelCstr n t)

dSource :: DeclParser
dSource = dEndpoint "source" "source" True

dSink :: DeclParser
dSink = dEndpoint "sink" "sink" False

dBind :: K3Parser ()
dBind = (trackBindings =<<) $ mkBind <$> (symbol "~~" *> identifier) <*> bidirectional <*> identifier
  where mkBind id1 lSrc id2 = if lSrc then (id1, id2) else (id2, id1)
        bidirectional = choice [symbol "|>" >> return True,
                                symbol "<|" >> return False]

dRole :: DeclParser
dRole = chainedNamedBraceDecl n n roleBody DC.role
  where n = "role"

dSelector :: K3Parser ()
dSelector = namedIdentifier "selector" "default" (id <$>) >>= trackDefault

dAnnotation :: DeclParser
dAnnotation = namedBraceDecl n n (some annotationMember) DC.annotation
  where n = "annotation"

{- Annotation declaration members -}
annotationMember :: K3Parser AnnMemDecl
annotationMember =
  choice $ map uidOver [annLifted, annAttribute, subAnnotation]

polarity :: K3Parser Polarity
polarity = choice [keyword "provides" >> return Provides,
                   keyword "requires" >> return Requires]

annLifted :: K3Parser (UID -> AnnMemDecl)
annLifted = mkLifted <$> polarity <*  keyword "lifted" <*> identifier <* colon
                     <*> qualifiedTypeExpr <*> optional equateNSExpr <* semi
  where mkLifted p n qte eOpt = Lifted p n qte (propagateQualifier qte eOpt)

annAttribute :: K3Parser (UID -> AnnMemDecl)
annAttribute = mkAttribute <$> polarity <*> identifier <*  colon
                           <*> qualifiedTypeExpr <*> optional equateNSExpr <* semi
  where mkAttribute p n qte eOpt = Attribute p n qte (propagateQualifier qte eOpt)

subAnnotation :: K3Parser (UID -> AnnMemDecl)
subAnnotation = MAnnotation <$> polarity <* keyword "annotation" <*> identifier

uidOver :: K3Parser (UID -> a) -> K3Parser a
uidOver parser =
  parserWithUID $ ap (fmap (. UID) parser) . return


{- Types -}
typeExpr :: TypeParser
typeExpr = parseError "type" "expression" $ TUID # tTermOrFun

qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = flip (@+) <$> typeQualifier <*> typeExpr

typeQualifier :: K3Parser (Annotation Type)
typeQualifier = parseError "type" "qualifier" $
                  choice [keyword "immut" >> return TImmutable,
                          keyword "mut" >> return TMutable]

{- Type terms -}
tTerm :: TypeParser
tTerm = TSpan <-> choice [ tPrimitive, tOption, tIndirection,
                           tTupleOrNested, tRecord, tCollection,
                           tBuiltIn ]

tTermOrFun :: TypeParser
tTermOrFun = 
  choice [try (TSpan <-> TC.function <$> (TUID # tTerm) <* symbol "->" <*> typeExpr), tTerm]

tPrimitive :: TypeParser
tPrimitive = choice $ map tConst ["bool", "int", "real", "string"]
  where tConst x   = keyword x >> return (typeCstr x)
        typeCstr x = case x of
                      "bool"    -> TC.bool
                      "int"     -> TC.int
                      "real"    -> TC.real
                      "string"  -> TC.string

tQNested f k = f <$> (keyword k *> qualifiedTypeExpr)

tOption :: TypeParser
tOption = tQNested TC.option "option"

tIndirection :: TypeParser
tIndirection = tQNested TC.indirection "ind"

tTupleOrNested :: TypeParser
tTupleOrNested = choice [try unit, parens $ choice [try (stripMetas <$> typeExpr), tTuple]]
  where unit = symbol "(" *> symbol ")" >> return (TC.unit)
        tTuple = commaSep1 qualifiedTypeExpr >>= return . TC.tuple
        stripMetas t = stripMetaOf isTSpan $ stripMetaOf isTUID t
        stripMetaOf f t = maybe t (t @-) $ t @~ f

tRecord :: TypeParser
tRecord = TC.record <$> (braces . semiSep1) idQType

tCollection :: TypeParser
tCollection = mkCollectionType <$> (keyword "collection" *> tRecord)
                               <*> (option [] (symbol "@" *> tAnnotations))
  where mkCollectionType t a = foldl (@+) (TC.collection t) a

tBuiltIn :: TypeParser
tBuiltIn = choice $ map (\(kw,bi) -> keyword kw >> return (TC.builtIn bi))
              [ ("self",TSelf)
              , ("structure",TStructure)
              , ("horizon",THorizon)
              , ("content",TContent) ]

tAnnotations :: K3Parser [Annotation Type]
tAnnotations = braces $ commaSep1 (mkTAnnotation <$> identifier)
  where mkTAnnotation x = TAnnotation x

{- Expressions -}
myTrace :: String -> K3Parser a -> K3Parser a
myTrace s p = PP.getInput >>= (\i -> trace (s++" "++i) p)

expr :: ExpressionParser
expr = parseError "k3" "expression" $ EUID # mkSeq <$> sepBy1 nonSeqExpr (operator ";")
  where mkSeq = foldl1 (EC.binop OSeq)

nonSeqExpr :: ExpressionParser
nonSeqExpr = buildExpressionParser opTable eApp

qualifiedExpr :: ExpressionParser
qualifiedExpr = flip (@+) <$> exprQualifier <*> expr

exprQualifier :: K3Parser (Annotation Expression)
exprQualifier = choice [keyword "immut" >> return EImmutable,
                        keyword "mut" >> return EMutable]

exprNoneQualifier :: K3Parser NoneMutability
exprNoneQualifier =
      keyword "immut" *> return NoneImmut
  <|> keyword "mut" *> return NoneMut

eTerm :: ExpressionParser
eTerm = EUID # (ESpan <-> mkTerm <$> choice [
    (try eAssign),
    (try eAddress),
    eLiterals,
    eLambda,
    eCondition,
    eLet,
    eCase,
    eBind,
    eSelf  ] <*> optional eProject)
  where eProject          = dot *> identifier
        mkTerm e Nothing  = e
        mkTerm e (Just x) = EC.project x e


{- Literals -}
eLiterals :: ExpressionParser
eLiterals = choice [ 
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
eConstant = choice [eCBool,
                    try eCNumber,
                    eCString]

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
eVariable = EC.variable <$> identifier

{- Complex literals -}
eOption :: ExpressionParser
eOption = choice [EC.some <$> (keyword "Some" *> qualifiedExpr),
                  keyword "None" *> (EC.constant . CNone <$> exprNoneQualifier)]

eIndirection :: ExpressionParser
eIndirection = EC.indirect <$> (keyword "ind" *> qualifiedExpr)

eTuplePrefix :: ExpressionParser
eTuplePrefix = choice [try unit, eTupleOrSnd]
  where 
        unit          = symbol "(" *> symbol ")" >> return (EC.tuple [])
        eTupleOrSnd   = mkTupOrSnd <$> (parens $ commaSep1 $ choice [try expr, qualifiedExpr]) <*> optional sendSuffix        
        sendSuffix    = symbol "<-" *> nonSeqExpr

        mkTupOrSnd [e] Nothing    = stripSpan <$> e
        mkTupOrSnd [e] (Just arg) = EC.binop OSnd e arg
        mkTupOrSnd l Nothing      = EC.tuple $ map promoteToImmutable l
        mkTupOrSnd l (Just arg)   = EC.binop OSnd (EC.tuple $ map promoteToImmutable l) arg

        stripSpan e          = maybe e (e @-) $ e @~ isESpan
        promoteToImmutable e = maybe (e @+ EImmutable) (const e) $ e @~ isEQualified

eRecord :: ExpressionParser
eRecord = EC.record <$> braces idQExprList

eEmpty :: ExpressionParser
eEmpty = mkEmpty <$> typedEmpty <*> (option [] (symbol "@" *> eAnnotations))
  where mkEmpty e a = foldl (@+) e a 
        typedEmpty = EC.empty <$> (keyword "empty" *> tRecord) 

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

binary  op cstr assoc parser =
  Infix (uidTagBinOp $ (pure cstr) <* parser op) assoc
prefix  op cstr parser       =
  Prefix  (uidTagUnOp $ (pure cstr) <* parser op)
postfix op cstr parser       =
  Postfix (uidTagUnOp $ (pure cstr) <* parser op)

-- Temporary hack to annotate operator spans
-- TODO: clean up
getAnnotations (Node (_ :@: al) _) = al

getSpan l = case find isESpan l of
              Just (ESpan s) -> s
              Nothing -> Span "<dummy>" 0 0 0 0

binOpSpan cstr l r = (cstr l r) @+ (ESpan $ coverSpans (getSpan la) (getSpan ra))
  where la = getAnnotations l
        ra = getAnnotations r
        coverSpans (Span n l1 c1 _ _) (Span _ _ _ l2 c2) = Span n l1 c1 l2 c2

unOpSpan opName cstr e = (cstr e) @+ (ESpan $ prefixSpan $ getSpan al)
  where al = getAnnotations e
        prefixSpan (Span n l1 c1 l2 c2) = Span n l1 (c1-(length opName)) l2 c2

binaryParseOp (opName, opTag) = binary opName (binOpSpan $ EC.binop opTag) AssocLeft
mkBinOp  x = binaryParseOp x operator
mkBinOpK x = binaryParseOp x keyword

unaryParseOp (opName, opTag) = prefix opName (unOpSpan opName $ EC.unop opTag)
mkUnOp  x = unaryParseOp x operator
mkUnOpK x = unaryParseOp x keyword

opTable = [   map mkBinOp  [("*",   OMul), ("/",  ODiv)],
              map mkBinOp  [("+",   OAdd), ("-",  OSub)],
              map mkBinOp  [("<",   OLth), ("<=", OLeq), (">",  OGth), (">=", OGeq) ],
              map mkBinOp  [("==",  OEqu), ("!=", ONeq), ("<>", ONeq)],
              map mkUnOpK  [("not", ONot)],
              map mkBinOpK [("and", OAnd)],
              map mkBinOpK [("or",  OOr)]
          ]

{- Terms -}
nsPrefix k = keyword k *> nonSeqExpr
ePrefix k  = keyword k *> expr
iPrefix k  = keyword k *> identifier
iArrow k   = iPrefix k <* symbol "->"
iArrowS s  = symbol s *> identifier <* symbol "->"

eLambda :: ExpressionParser
eLambda = EC.lambda <$> choice [iArrow "fun", iArrowS "\\"] <*> nonSeqExpr

eApp :: ExpressionParser
eApp = try $ mkApp <$> (some $ try eTerm)
  where mkApp = foldl1 (EC.binop OApp)

eCondition :: ExpressionParser
eCondition = EC.ifThenElse <$> (nsPrefix "if") <*> (ePrefix "then") <*> (ePrefix "else")

eAssign :: ExpressionParser
eAssign = EC.assign <$> identifier <*> equateNSExpr

eLet :: ExpressionParser
eLet = EC.letIn <$> iPrefix "let" <*> equateQExpr <*> (keyword "in" *> expr)

eCase :: ExpressionParser
eCase = mkCase <$> ((nsPrefix "case") <* keyword "of")
               <*> (braces eCaseSome) <*> (braces eCaseNone)
  where eCaseSome = (,) <$> (iArrow "Some") <*> expr
        eCaseNone = (keyword "None" >> symbol "->") *> expr
        mkCase e (x, s) n = EC.caseOf e x s n

eBind :: ExpressionParser
eBind = EC.bindAs <$> (nsPrefix "bind") <*> (keyword "as" *> eBinder) <*> (ePrefix "in")

eBinder :: BinderParser
eBinder = choice [bindInd, bindTup, bindRec]

bindInd = BIndirection <$> iPrefix "ind"
bindTup = BTuple <$> parens idList
bindRec = BRecord <$> braces idPairList

eAddress :: ExpressionParser
eAddress = EC.address <$> ipAddress <* colon <*> port
  where ipAddress = EC.constant . CString <$> (some $ choice [alphaNum, oneOf "."])
        port = EC.constant . CInt . fromIntegral <$> natural

eSelf :: ExpressionParser
eSelf = keyword "self" >> return EC.self

{- Identifiers and their list forms -}
idList      = commaSep identifier
idPairList  = commaSep idPair
idQExprList = commaSep idQExpr
idQTypeList = commaSep idQType
idPair      = (,) <$> identifier <*> (colon *> identifier)
idQExpr     = (,) <$> identifier <*> (colon *> qualifiedExpr)
idQType     = (,) <$> identifier <*> (colon *> qualifiedTypeExpr)

{- Misc -}
equateExpr :: ExpressionParser
equateExpr = symbol "=" *> expr

equateNSExpr :: ExpressionParser
equateNSExpr = symbol "=" *> nonSeqExpr

equateQExpr :: ExpressionParser
equateQExpr = symbol "=" *> qualifiedExpr

{- Channels -}
-- TODO: read/write modes for file/network sinks vs sources.

channel :: Bool -> K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
channel isSource = if isSource then choice $ [value]++common else choice common
  where common = [file isSource, network isSource]

value :: K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
value = mkValueStream <$> (symbol "value" *> expr)
  where mkValueStream e n t = (Just e, [])

file :: Bool -> K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
file isSource = mkFile <$> (symbol "file" *> eCString) <*> format
  where mkFile argE formatE n t = channelMethods isSource True argE formatE n t

network :: Bool -> K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
network isSource = mkNetwork <$> (symbol "network" *> eAddress) <*> format
  where mkNetwork argE formatE n t =  channelMethods isSource False argE formatE n t


format :: ExpressionParser
format = choice [symbol "csv" >> return (EC.constant $ CString "csv"),
                 symbol "txt" >> return (EC.constant $ CString "txt"),
                 symbol "bin" >> return (EC.constant $ CString "bin")]



{- Declaration helpers -}
namedIdentifier nameKind name namedCstr =
  declError nameKind $ namedCstr $ keyword name *> identifier

namedDecl k n c = (DSpan <->) $ namedIdentifier k n c

namedBraceDecl k n rule cstr =
  namedDecl k n $ braceRule . (cstr <$>)
  where braceRule x = x <*> (braces rule)

chainedNamedBraceDecl k n namedRule cstr =
  namedDecl k n $ join . (passName <$>)
  where passName x = (cstr x) <$> (braces $ namedRule x)


{- Environment maintenance helpers -}
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

sourceState :: EnvironmentFrame -> EndpointsBQG
sourceState = fst

defaultEntries :: EnvironmentFrame -> DefaultEntries
defaultEntries = snd

sourceBindings :: EndpointsBQG -> [(Identifier, Identifier)]
sourceBindings s = concatMap extractBindings s
  where extractBindings (x,(Just b,_,_))  = map ((,) x) b
        extractBindings (x,(Nothing,_,_)) = []

qualifiedSources :: EndpointsBQG -> [Identifier]
qualifiedSources s = concatMap (qualifiedSourceName . snd) s
  where qualifiedSourceName (Just _, x, _)  = [x]
        qualifiedSourceName (Nothing, _, _) = []

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
trackBindings (src, dest) = modifyEnvironmentF_ $ updateBindings src dest
  where updateBindings src dest (safePopFrame -> ((s,d), env)) =
          case lookup src s of
            Just (Just b, q, g)  -> Right $ (replaceAssoc s src (Just (dest:b), q, g), d):env
            Just (Nothing, q, g) -> Left  $ "Invalid binding for endpoint " ++ src
            Nothing              -> Left  $ "Invalid binding, no source " ++ src

-- | Records endpoint identifiers and initializer expressions in a K3 parsing environment
trackEndpoint :: K3 Declaration -> DeclParser
trackEndpoint d
  | DGlobal n t eOpt <- tag d, TSource <- tag t = track True n eOpt >> return d
  | DGlobal n t eOpt <- tag d, TSink <- tag t   = track False n eOpt >> return d
  | otherwise = return d

  where 
    track isSource n eOpt = modifyEnvironmentF_ $ addEndpointGoExpr isSource n eOpt
    addEndpointGoExpr isSource n eOpt (safePopFrame -> ((s,d), env)) =
          case (eOpt, isSource) of
            (Just e, True)   -> Right $ (replaceAssoc s n (Just [], n, Nothing), d):env
            (Nothing, True)  -> Right $ (replaceAssoc s n (Just [], n, Just $ mkRunSourceE n), d):env
            (Just e, False)  -> Right $ (replaceAssoc s n (Nothing, n, Just $ mkRunSinkE n),   d):env
            (_,_)            -> Left  $ "Invalid endpoint initializer"

-- | Records defaults in a K3 parsing environment
trackDefault :: Identifier -> K3Parser ()
trackDefault n = modifyEnvironment_ $ updateState n 
  where updateState n (safePopFrame -> ((s,d), env)) = (s,replaceAssoc d "" n):env


{- Desugaring methods -}
desugarRole :: Identifier -> ([K3 Declaration], EnvironmentFrame) -> K3Parser [K3 Declaration]
desugarRole n (dl, frame) = 
  modifyEnvironmentF_ (qualify frame) >> (return $ desugarSourceImpl dl frame)
  
  where desugarSourceImpl dl (s,_) = flip map dl $ rewriteSource $ sourceBindings s
        
        qualify poppedFrame (safePopFrame -> (frame, env)) =
          case validateSources poppedFrame of
            (ndIds, []) -> Right $ (concatWithPrefix poppedFrame frame):env
            (_, failed) -> Left  $ "Invalid defaults\n" ++ qualifyError poppedFrame failed

        validateSources (s,d) = partition ((flip elem $ qualifiedSources s) . snd) d
        concatWithPrefix (s,d) (s2,d2) = (map qualifySource s ++ s2, qualifyDefaults d ++ d2)

        qualifySource (eid, (Just b, q, g)) = (eid, (Just b, prefix "." n q, g))
        qualifySource x = x

        qualifyDefaults = map $ uncurry $ flip (,) . prefix "." n

        qualifyError frame failed = "Frame: " ++ show frame ++ "\nFailed: " ++ show failed
        
        prefix sep x z = if x == "" then z else x ++ sep ++ z
        

-- | Adds UIDs to nodes that do not already have one. 
--   This ensures every AST node has a UID after parsing, including builtins.
ensureUIDs :: K3 Declaration -> K3Parser (K3 Declaration)
ensureUIDs p = traverse (parserWithUID . annotateDecl) p
  where 
        annotateDecl d@(dt :@: as) uid =
          case dt of
            DGlobal n t eOpt -> do
              t'    <- annotateType t
              eOpt' <- maybe (return Nothing) (\e -> annotateExpr e >>= return . Just) eOpt
              rebuildDecl d uid $ DGlobal n t' eOpt'

            DTrigger n t e -> do
              t' <- annotateType t
              e' <- annotateExpr e
              rebuildDecl d uid $ DTrigger n t' e'

            DRole       n      -> rebuildDecl d uid $ DRole n
            DAnnotation n mems -> rebuildDecl d uid $ DAnnotation n mems

        rebuildDecl d@(_ :@: as) uid =
          return . unlessAnnotated (any isDUID) d . flip (@+) (DUID $ UID uid) . ( :@: as) 

        annotateNode test anns node = return $ unlessAnnotated test node (foldl (@+) node anns) 
        annotateExpr = traverse (\e -> parserWithUID (\uid -> annotateNode (any isEUID) [EUID $ UID uid] e))
        annotateType = traverse (\t -> parserWithUID (\uid -> annotateNode (any isTUID) [TUID $ UID uid] t))

        unlessAnnotated test n@(_ :@: as) n' = if test as then n else n'


-- | Propagates a mutability qualifier from a type to an expression.
--   This is used in desugaring non-function initializers and annotation members.
propagateQualifier :: K3 Type -> Maybe (K3 Expression) -> Maybe (K3 Expression)
propagateQualifier qte Nothing  = Nothing
propagateQualifier qte@(tag &&& annotations -> (ttag, tas)) (Just e@(annotations -> eas))
  | any isEQualified eas || inApplicable ttag = Just e
  | otherwise = Just $ if any isTImmutable tas then (e @+ EImmutable) else (e @+ EMutable)
  where inApplicable = flip elem [TFunction, TTrigger, TSink, TSource]
