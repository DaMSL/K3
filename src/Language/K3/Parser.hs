{-# LANGUAGE FlexibleContexts #-}

-- | K3 Parser.
module Language.K3.Parser (
  declaration,
  qualifiedTypeExpr,
  typeExpr,
  qualifiedExpr,
  expr,
  parseK3
) where

import Control.Applicative

import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.HashSet (HashSet)
import Data.List
import Data.String
import Data.Tree
import GHC.Float

import Debug.Trace

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP
import qualified Text.Parsec.Language as PL
import qualified Text.Parsec.Token    as PT

import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Expression
import Text.Parser.Token
import Text.Parser.Token.Style

import Text.Parser.Parsec

import Language.K3.Core.Type
import Language.K3.Core.Expression
import Language.K3.Core.Declaration
import Language.K3.Core.Annotation

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

{- Type synonyms for parser return types -}
type K3Parser a       = PP.ParsecT String () Identity a
type TypeParser       = K3Parser (K3 Type)
type ExpressionParser = K3Parser (K3 Expression)
type BinderParser     = K3Parser Binder
type DeclParser       = K3Parser (K3 Declaration)

{- Helpers -}
set :: [String] -> HashSet String
set = HashSet.fromList

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
    "==", "!=", "<>", "<", ">", ">=", "<=",
    ";", "<-"
  ]

k3Keywords = [
    {- Types -}
    "int", "bool", "real", "string",
    "immut", "mut", "witness", "option", "ind" , "collection",
    {- Declarations -}
    "declare", "trigger", "source", "consume", "fun",
    {- Expressions -}
    "let", "in", "if", "then", "else", "case", "of", "bind", "as",
    "and", "or", "not",
    {- Values -}
    "true", "false", "ind", "Some", "None", "empty",
    {- Annotation declarations -}
    "annotation", "lifted", "provides", "requires"
  ]

{- K3 LanguageDef for Parsec -}
k3Style = PL.javaStyle {
  PT.opStart = oneOf "+-*/=!<>;",
  PT.opLetter = oneOf "+-*/=!<>;",
  PT.reservedOpNames = k3Operators,
  PT.reservedNames = k3Keywords
}

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


{- K3 grammar parsers -}

-- TODO: inline testing
program :: DeclParser
program = DSpan <-> mkProgram <$> endBy roleBody eof
  where mkProgram l = DC.role "__global" $ concat l

roleBody :: K3Parser [K3 Declaration]
roleBody = some declaration

{- Declaration helpers -}
namedDecl nameKind name namedCstr =
  (<->) DSpan $ declError nameKind $ namedCstr $ keyword name *> identifier

namedBraceDecl k n rule cstr =
  namedDecl k n $ braceRule . (cstr <$>)
  where braceRule x = x <*> (braces rule)

{- Declarations -}
declaration :: DeclParser
declaration = choice [dGlobal, dTrigger, dSource, dSink,
                      dBind, dRole, dSelector, dAnnotation]

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (DC.global <$>)
  where rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateExpr)

dTrigger :: DeclParser
dTrigger = namedDecl "trigger" "trigger" $ rule . (mkTrig <$>)
  where
    rule x = x <*> (parens idQTypeList) <*> equateExpr
    mkTrig name args body = DC.global name (TC.trigger args) (Just body)

dEndpoint :: String -> String -> (K3 Type -> K3 Type) -> DeclParser
dEndpoint kind name typeCstr = namedDecl kind name $ rule . (mkEndpoint <$>)
  where rule x = x <*> (colon *> typeExpr) <*> (symbol "=" *> channel)
        mkEndpoint n t channelCstr = DC.endpoint n (typeCstr t) (channelCstr n)

dSource :: DeclParser
dSource = dEndpoint "source" "source" TC.source

dSink :: DeclParser
dSink = dEndpoint "sink" "sink" TC.sink

dBind :: DeclParser
dBind = DSpan <-> mkBind <$> identifier <*> bidirectional <*> identifier
  where mkBind id1 lSrc id2 = if lSrc then DC.bind id1 id2 else DC.bind id2 id1
        bidirectional = choice [symbol "|>" >> return True,
                                symbol "<|" >> return False]

dRole :: DeclParser
dRole = namedBraceDecl n n roleBody DC.role
  where n = "role"

dSelector :: DeclParser
dSelector = namedDecl "selector" "default" $ (DC.selector <$>)

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
                     <*> ((optional equateExpr) <* semi)
  where mkLifted n t e_opt pol = Lifted n pol t e_opt

annAttribute :: K3Parser (Polarity -> AnnMemDecl)
annAttribute = mkAttribute <$> identifier <*> (colon *> qualifiedTypeExpr)
                                          <*> ((optional equateExpr) <* semi)
  where mkAttribute n t e_opt pol = Attribute n pol t e_opt

subAnnotation :: K3Parser (Polarity -> AnnMemDecl)
subAnnotation = MAnnotation <$> (keyword "annotation" *> identifier)


{- Types -}
typeExpr :: TypeParser
typeExpr = (<->) TSpan $ parseError "type" "expression" $ tTermOrFun

qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = flip (@+) <$> typeQualifier <*> typeExpr

typeQualifier :: K3Parser (Annotation Type)
typeQualifier = parseError "type" "qualifier" $
                  choice [keyword "immut" >> return TImmutable,
                          keyword "mut" >> return TMutable]

{- Type terms -}
tTerm :: TypeParser
tTerm = choice [ tPrimitive, tOption, tIndirection,
                 tTuple, tRecord, tCollection ]

tTermOrFun :: TypeParser
tTermOrFun = mkFun <$> tTerm <*> (optional $ symbol "->" *> typeExpr)
  where mkFun l Nothing = l
        mkFun l (Just r) = TC.function l r

tPrimitive :: TypeParser
tPrimitive = choice $ map tConst ["bool", "int", "real", "string"]
  where tConst x   = keyword x >> return (typeCstr x)
        typeCstr x = case x of
                      "bool"    -> TC.bool
                      "int"     -> TC.int
                      "real"    -> TC.float
                      "string"  -> TC.string

tQNested f k = f <$> (keyword k *> qualifiedTypeExpr)

tOption :: TypeParser
tOption = tQNested TC.option "option"

tIndirection :: TypeParser
tIndirection = tQNested TC.indirection "ind"

tTuple :: TypeParser
tTuple = mkTuple <$> (parens . commaSep) qualifiedTypeExpr
  where mkTuple [x] = x
        mkTuple l = TC.tuple l

tRecord :: TypeParser
tRecord = TC.record <$> (braces . semiSep1) idQType

tCollection :: TypeParser
tCollection = mkCollectionType <$> (keyword "collection" *> tRecord)
                               <*> (option [] (symbol "@" *> tAnnotations))
  where mkCollectionType t a = foldl (@+) (TC.collection t) a

tAnnotations :: K3Parser [Annotation Type]
tAnnotations = braces $ commaSep1 (mkTAnnotation <$> identifier)
  where mkTAnnotation x = TAnnotation x

{- Expressions -}
expr :: ExpressionParser
expr = (<->) ESpan $ parseError "k3" "expression" $ buildExpressionParser opTable eTerm

qualifiedExpr :: ExpressionParser
qualifiedExpr = flip (@+) <$> exprQualifier <*> expr

exprQualifier :: K3Parser (Annotation Expression)
exprQualifier = choice [keyword "immut" >> return EImmutable,
                        keyword "mut" >> return EMutable]

eTerm :: ExpressionParser
eTerm = ESpan <-> choice [
    eLiterals, eSend,
    eProject, eLambda, eCondition, 
    eAssign, eLet, eCase, eBind
  ]

{- Literals -}
eLiterals :: ExpressionParser
eLiterals = choice [eTerminal, eOption, eIndirection,
                    eTuple, eRecord, eEmpty]

{- Terminals -}
eTerminal :: ExpressionParser
eTerminal = choice [eConstant, eVariable]

eConstant :: ExpressionParser
eConstant = choice [eCBool, eCNumber, eCString]

eCBool :: ExpressionParser
eCBool = (EC.constant . CBool) <$>
  choice [keyword "true" >> return True, keyword "false" >> return False]

eCNumber :: ExpressionParser
eCNumber = mkNumber <$> integerOrDouble
  where mkNumber x = case x of
                      Left i  -> EC.constant . CInt . fromIntegral $ i
                      Right d -> EC.constant . CFloat . double2Float $ d

eCString :: ExpressionParser
eCString = EC.constant . CString <$> stringLiteral

eVariable :: ExpressionParser
eVariable = EC.variable <$> identifier

eAddress :: ExpressionParser
eAddress = mkAddress <$> eCString <* comma <*> eCNumber
  where mkAddress a b = EC.tuple [a,b]

{- Complex literals -}
eOption :: ExpressionParser
eOption = choice [EC.some <$> (keyword "Some" *> qualifiedExpr),
                  (@+) (EC.constant CNone) <$> (keyword "None" *> exprQualifier)]

eIndirection :: ExpressionParser
eIndirection = EC.indirect <$> (keyword "ind" *> qualifiedExpr)

eTuple :: ExpressionParser
eTuple = mkTuple <$> (parens . commaSep) qualifiedExpr
  where mkTuple [x] = x
        mkTuple l = EC.tuple l

eRecord :: ExpressionParser
eRecord = EC.record <$> braces idQExprList

eEmpty :: ExpressionParser
eEmpty = mkEmpty <$> typedEmpty <*> (option [] (symbol "@" *> eAnnotations))
  where mkEmpty e a = foldl (@+) e a 
        typedEmpty = ((@+) EC.empty) . EType <$> (keyword "empty" *> tRecord)

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

opTable = [ [Infix ((pure $ EC.binop OApp) <* someSpace) AssocLeft],
              map mkBinOp  [("*",   OMul), ("/",  ODiv)],
              map mkBinOp  [("+",   OAdd), ("-",  OSub)],
              map mkBinOp  [("==",  OEqu), ("!=", ONeq), ("<>", ONeq),
                            ("<",   OLth), ("<=", OLeq),
                            (">",   OGth), (">=", OGeq) ],
            ((map mkBinOpK [("and", OAnd), ("or", OOr)])
              ++ [mkUnOpK   ("not", ONot)]),
              map mkBinOp  [(";",   OSeq)]
          ]

{- Terms -}
ePrefix k = keyword k *> expr
iPrefix k = keyword k *> identifier
iArrow k  = iPrefix k <* symbol "->" 

eSend :: ExpressionParser
eSend = mkSend <$> eVariable <* comma <*> eAddress <* symbol "<-" <*> expr
  where mkSend t addr arg = EC.binop OSnd (EC.tuple [t, addr]) arg

eProject :: ExpressionParser
eProject = (flip EC.project) <$> expr <*> (dot *> identifier)

eLambda :: ExpressionParser
eLambda = EC.lambda <$> (iArrow "fun") <*> expr

eCondition :: ExpressionParser
eCondition = EC.ifThenElse <$> (ePrefix "if") <*> (ePrefix "then") <*> (ePrefix "else")

eAssign :: ExpressionParser
eAssign = EC.assign <$> identifier <*> equateExpr

eLet :: ExpressionParser
eLet = EC.letIn <$> identifier <*> equateQExpr <*> (keyword "in" *> expr)

eCase :: ExpressionParser
eCase = mkCase <$> ((ePrefix "case") <* keyword "of")
               <*> (braces eCaseSome) <*> (braces eCaseNone)
  where eCaseSome = (,) <$> (iArrow "Some") <*> expr
        eCaseNone = (keyword "None" >> symbol "->") *> expr
        mkCase e (x, s) n = EC.caseOf e x s n


eBind :: ExpressionParser
eBind = EC.bindAs <$> (ePrefix "bind") <*> (keyword "as" *> eBinder) <*> (ePrefix "in")

eBinder :: BinderParser
eBinder = choice [bindInd, bindTup, bindRec]

bindInd = BIndirection <$> iPrefix "ind"
bindTup = BTuple <$> parens idList
bindRec = BRecord <$> braces idPairList


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

equateQExpr :: ExpressionParser
equateQExpr = symbol "=" *> qualifiedExpr

{- Channels -}
channel :: K3Parser (Identifier -> (K3 Expression, K3 Expression, K3 Expression))
channel = choice [file, network]

file :: K3Parser (Identifier -> (K3 Expression, K3 Expression, K3 Expression))
file = mkFile <$> (symbol "file" *> eCString) <*> format
  where mkFile path_e format_e n = (fileInit path_e format_e n, fileProcess, fileFinal n)
        fileInit path_e format_e n = EC.applyMany (EC.variable "fileOpen") [string n, path_e, format_e]
        fileFinal n = EC.applyMany (EC.variable "fileClose") [string n]
        fileProcess = EC.lambda "_" (EC.tuple [])
        string n = EC.constant $ CString n

network :: K3Parser (Identifier -> (K3 Expression, K3 Expression, K3 Expression))
network = mkNetwork <$> (symbol "network" *> eAddress) <*> format
  where mkNetwork addr_e format_e n = (netInit addr_e format_e n, netProcess, netFinal n)
        netInit addr_e format_e n = EC.applyMany (EC.variable "netOpen") [string n, addr_e, format_e]
        netFinal n = EC.applyMany (EC.variable "netClose") [string n]
        netProcess = EC.lambda "_" (EC.tuple [])
        string n = EC.constant $ CString n

format :: ExpressionParser
format = choice [symbol "csv" >> return (EC.constant $ CString "csv"),
                 symbol "txt" >> return (EC.constant $ CString "txt")]

{- Main parsing functions -}
parseK3 :: String -> String
parseK3 progStr = case P.parse program "" progStr of
  -- TODO: better parse error handling
  Left err -> "No match " ++ show err
  Right val -> "Found val" ++ show val
