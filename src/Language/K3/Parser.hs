{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PatternGuards #-}
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
  parseExpressionCSV,
  parseK3,
  K3Parser
) where

import Control.Applicative
import Control.Monad

import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.HashSet (HashSet)
import Data.List
import Data.String
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

-- source name => bound triggers, qualified source name, go expression
type SourcesBQG        = [(Identifier, ([Identifier], Identifier, Maybe (K3 Expression)))]

-- role name, default name
type DefaultEntries    = [(Identifier, Identifier)] 

type EnvironmentFrame  = (SourcesBQG, DefaultEntries)
type ParserEnvironment = [EnvironmentFrame]

type K3Parser a        = PP.ParsecT String ParserEnvironment Identity a
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

-- Span computation
-- TODO: what if source names do not match?
(<->) cstr parser = annotate <$> PP.getPosition <*> parser <*> PP.getPosition
  where annotate start x end = x @+ (cstr $ mkSpan start end)
        mkSpan s e = Span (P.sourceName s) (P.sourceLine s) (P.sourceColumn s)
                                           (P.sourceLine e) (P.sourceColumn e)

infixl 1 <->

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
maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (\_ -> Nothing) Just $ P.runParser p [] "" s

parseType :: String -> Maybe (K3 Type)
parseType = maybeParser typeExpr

parseExpression :: String -> Maybe (K3 Expression)
parseExpression = maybeParser expr

parseExpressionCSV :: String -> Maybe (K3 Expression)
parseExpressionCSV = maybeParser (mkTuple <$> commaSep1 expr)
  where mkTuple [x] = x
        mkTuple x = EC.tuple x

parseK3 :: String -> Either String (K3 Declaration)
parseK3 s = either (Left . show) Right $ P.runParser program [] "" s


{- K3 grammar parsers -}

-- TODO: inline testing
program :: DeclParser
program = DSpan <-> (rule >>= mkEntryPoints >>= return . declareBuiltins)
  where rule = mkProgram <$> endBy (roleBody "") eof
        mkProgram l = DC.role defaultRoleName $ concat l
        mkEntryPoints d = PP.getState >>= return . (uncurry $ desugarRoleEntries d) . fst . safePopFrame

roleBody :: Identifier -> K3Parser [K3 Declaration]
roleBody n = pushBindings >> rule >>= popBindings >>= desugarRole n
  where rule = some declaration >>= return . filterOptions
        pushBindings = PP.getState >>= PP.putState . addFrame
        popBindings dl = PP.getState >>= (\env ->
          (PP.putState . removeFrame) env >> return (dl, currentFrame env))
        

{- Declarations -}
declaration :: K3Parser (Maybe (K3 Declaration))
declaration = choice [decls >>= return . Just, sugaredDecls >> return Nothing]
  where decls = choice [dGlobal, dTrigger, dSource, dSink, dRole, dAnnotation]
        sugaredDecls = choice [dSelector, dBind]

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (DC.global <$>)
  where rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateNSExpr)

-- syntax: "trigger" id ":" type "=" expr ";"
dTrigger :: DeclParser
dTrigger = declError "trigger" $ DSpan <->
              DC.trigger <$ keyword "trigger" <*> identifier
                         <* colon <*> typeExpr
                         <* symbol "=" <*> expr
                         <* semi

dEndpoint :: String -> String -> (K3 Type -> K3 Type)
              -> (K3 Declaration -> DeclParser) -> DeclParser
dEndpoint kind name typeCstr stateModifier = 
  namedDecl kind name $ join . rule . (mkEndpoint <$>)
  where rule x = x <*> (colon *> typeExpr) <*> (symbol "=" *> channel)
        mkEndpoint n t channelCstr =
          stateModifier $ uncurry (DC.endpoint n (typeCstr t)) (channelCstr n t)

dSource :: DeclParser
dSource = dEndpoint "source" "source" TC.source trackSource

dSink :: DeclParser
dSink = dEndpoint "sink" "sink" TC.sink return

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
annotationMember = chain <$> polarity <*> member
  where chain p f = f p
        member    = choice [annMethod, annLifted, annAttribute, subAnnotation]

polarity :: K3Parser Polarity
polarity = choice [keyword "provides" >> return Provides,
                   keyword "requires" >> return Requires]

annMethod :: K3Parser (Polarity -> AnnMemDecl)
annMethod = mkMethod <$> identifier <*> parens idQTypeList
                     <*> (colon *> typeExpr)
                     <*> choice [Just <$> braces expr, semi >> return Nothing]
  where mkMethod n args ret body_opt pol = Method n pol args ret body_opt

annLifted :: K3Parser (Polarity -> AnnMemDecl)
annLifted = mkLifted <$> (keyword "lifted" *> identifier)
                     <*> (colon *> qualifiedTypeExpr)
                     <*> ((optional equateNSExpr) <* semi)
  where mkLifted n t e_opt pol = Lifted n pol t e_opt

annAttribute :: K3Parser (Polarity -> AnnMemDecl)
annAttribute = mkAttribute <$> identifier <*> (colon *> qualifiedTypeExpr)
                                          <*> ((optional equateNSExpr) <* semi)
  where mkAttribute n t e_opt pol = Attribute n pol t e_opt

subAnnotation :: K3Parser (Polarity -> AnnMemDecl)
subAnnotation = MAnnotation <$> (keyword "annotation" *> identifier)


{- Types -}
typeExpr :: TypeParser
typeExpr = parseError "type" "expression" $ tTermOrFun

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
  choice [try (TSpan <-> TC.function <$> tTerm <* symbol "->" <*> typeExpr), tTerm]

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
tTupleOrNested = choice [try unit, parens $ choice [try (stripSpan <$> typeExpr), tTuple]]
  where unit = symbol "(" *> symbol ")" >> return (TC.unit)
        tTuple = commaSep qualifiedTypeExpr >>= return . TC.tuple
        stripSpan t = maybe t (t @-) $ t @~ isSpan
        isSpan (TSpan _) = True
        isSpan _ = False

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
expr = parseError "k3" "expression" $ mkSeq <$> sepBy1 nonSeqExpr (operator ";")
  where mkSeq = foldl1 (EC.binop OSeq)

nonSeqExpr :: ExpressionParser
nonSeqExpr = myTrace "EOPS" $ buildExpressionParser opTable eApp

qualifiedExpr :: ExpressionParser
qualifiedExpr = flip (@+) <$> exprQualifier <*> expr

exprQualifier :: K3Parser (Annotation Expression)
exprQualifier = choice [keyword "immut" >> return EImmutable,
                        keyword "mut" >> return EMutable]

eTerm :: ExpressionParser
eTerm = ESpan <-> mkTerm <$> choice [
    myTrace "EASN" (try eAssign),
    myTrace "ESND" (try eSend),
    myTrace "ELIT" eLiterals,
    myTrace "ELAM" eLambda,
    myTrace "ECND" eCondition,
    myTrace "ELET" eLet,
    myTrace "ECAS" eCase,
    myTrace "EBND" eBind,
    myTrace "EADR" eAddress,
    myTrace "ESLF" eSelf           ] <*> optional eSuffix
  where eSuffix  = choice [eAddr >>= return . Left, eProject >>= return . Right]
        eAddr    = colon *> nonSeqExpr  
        eProject = dot *> identifier
        mkTerm e Nothing = e
        mkTerm e (Just (Left e2)) = EC.address e e2
        mkTerm e (Just (Right x)) = EC.project x e


{- Literals -}
eLiterals :: ExpressionParser
eLiterals = choice [ 
    myTrace "ETRM" eTerminal,
    myTrace "EOPT" eOption,
    myTrace "EIND" eIndirection,
    myTrace "ETON" eTupleOrNested,
    myTrace "EREC" eRecord,
    myTrace "EMPT" eEmpty ]

{- Terminals -}
eTerminal :: ExpressionParser
eTerminal = choice [myTrace "ECST" eConstant,
                    myTrace "EVAR" eVariable]

eConstant :: ExpressionParser
eConstant = choice [myTrace "EBOL" eCBool,
                    myTrace "ENUM" $ try eCNumber,
                    myTrace "ESTR" eCString]

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
                  (@+) (EC.constant CNone) <$> (keyword "None" *> exprQualifier)]

eIndirection :: ExpressionParser
eIndirection = EC.indirect <$> (keyword "ind" *> qualifiedExpr)

eTupleOrNested :: ExpressionParser
eTupleOrNested = choice [try unit, parens $ choice [try (stripSpan <$> expr), eTuple]]
  where unit = symbol "(" *> symbol ")" >> return (EC.tuple [])
        eTuple = commaSep qualifiedExpr >>= return . EC.tuple
        stripSpan e = maybe e (e @-) $ e @~ isSpan
        isSpan (ESpan _) = True
        isSpan _ = False

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
binary  op cstr assoc parser = Infix   ((pure cstr) <* parser op) assoc
prefix  op cstr parser       = Prefix  ((pure cstr) <* parser op)
postfix op cstr parser       = Postfix ((pure cstr) <* parser op)

-- Temporary hack to annotate operator spans
-- TODO: clean up
getAnnotations (Node (_ :@: al) _) = al

getSpan l = case find isSpan l of
              Just (ESpan s) -> s
              Nothing -> Span "<dummy>" 0 0 0 0
isSpan x = case x of
            ESpan _ -> True
            _ -> False

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
              --map mkBinOp  [(";",   OSeq)]
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
eApp = myTrace "EAPP" $ try $ mkApp <$> (some $ try eTerm)
  where mkApp = foldl1 (EC.binop OApp)

eSend :: ExpressionParser
eSend = mkSend <$> eVariable <* comma <*> eAddress <* symbol "<-" <*> nonSeqExpr
  where mkSend t addr arg = EC.binop OSnd (EC.tuple [t, addr]) arg

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
channel :: K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
channel = choice [value, file, network]

value :: K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
value = mkValueStream <$> (symbol "value" *> expr)
  where mkValueStream e n t = (Just e, [])

file :: K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
file = mkFile <$> (symbol "file" *> eCString) <*> format
  where mkFile argE formatE n t = (,) Nothing $ channelMethods True argE formatE n t

network :: K3Parser (Identifier -> K3 Type -> (Maybe (K3 Expression), [K3 Declaration]))
network = mkNetwork <$> (symbol "network" *> eAddress) <*> format
  where mkNetwork argE formatE n t = (,) Nothing $ channelMethods False argE formatE n t


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

sourceState :: EnvironmentFrame -> SourcesBQG
sourceState = fst

defaultEntries :: EnvironmentFrame -> DefaultEntries
defaultEntries = snd

sourceBindings :: SourcesBQG -> [(Identifier, Identifier)]
sourceBindings s = concat $ map extractBindings s
  where extractBindings (x,(b,_,_)) = map ((,) x) b

qualifiedSources :: SourcesBQG -> [Identifier]
qualifiedSources s = map (second . snd) s
  where second (_,x,_) = x

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
trackBindings :: (Identifier, Identifier) -> K3Parser ()
trackBindings (src, dest) = PP.getState >>= update src dest
  where update src dest (safePopFrame -> ((s,d), env)) =
          case lookup src s of
            Just (b,q,g) -> PP.putState $ (replaceAssoc s src (dest:b, q, g), d):env
            Nothing      -> PP.parserFail $ "invalid binding, no source " ++ src

-- | Records declaration identifiers in a K3 parsing environment
trackSource :: K3 Declaration -> DeclParser
trackSource d
  | DGlobal n _ eOpt <- tag d
  = PP.getState >>= PP.putState . addSourceId n eOpt >> return d
  | otherwise = return d
  where addSourceId n eOpt (safePopFrame -> ((s,d), env)) =
          case eOpt of 
            Just e -> (replaceAssoc s n ([], n, Nothing), d):env
            Nothing -> (replaceAssoc s n ([], n, Just $ mkRunSourceE n), d):env

-- | Records defaults in a K3 parsing environment
trackDefault :: Identifier -> K3Parser ()
trackDefault n = PP.getState >>= PP.putState . updateState n 
  where updateState n (safePopFrame -> ((s,d), env)) = (s,replaceAssoc d "" n):env


{- Desugaring methods -}
desugarRole :: Identifier -> ([K3 Declaration], EnvironmentFrame) -> K3Parser [K3 Declaration]
desugarRole n (dl, frame) = 
  PP.getState >>= qualify frame >> (return $ desugarSourceImpl dl frame)

  where desugarSourceImpl dl (s,_) = flip map dl $ rewriteSource $ sourceBindings s
        
        qualify poppedFrame (safePopFrame -> (frame, env)) =
          case validateSources poppedFrame of
            (ndIds, []) -> PP.putState . (:env) $ concatWithPrefix poppedFrame frame
            (_, failed) -> PP.parserFail $ "invalid defaults " ++ show poppedFrame

        validateSources (s,d) = partition ((flip elem $ qualifiedSources s) . snd) d
        concatWithPrefix (s,d) (s2,d2) = (qualifySources s ++ s2, qualifyDefaults d ++ d2)
        
        qualifySources  = map (\(src,(b,q,g)) -> (src, (b, prefix "." n q, g))) 
        qualifyDefaults = map $ uncurry $ flip (,) . prefix "." n
        
        prefix sep x z = if x == "" then z else x ++ sep ++ z
