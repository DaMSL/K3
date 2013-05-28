-- | K3 Parser.
module Language.K3.Parser.Parser (
  parseK3
) where

-- TODO: selector statements
-- TODO: annotations (bodies, attachment to collections and expressions)
-- TODO: qualifier attachment
-- TODO: qualified types and expressions in grammar

import Control.Applicative

import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.HashSet (HashSet)
import Data.String
import Data.Tree
import GHC.Float

import qualified Text.Parsec as P
import qualified Text.Parsec.Prim as PP
import qualified Text.Parsec.Language as PL
import qualified Text.Parsec.Token as PT

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

import qualified Language.K3.Core.Constructor.Type as TC
import qualified Language.K3.Core.Constructor.Expression as EC
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

mkPosition :: P.SourcePos -> Position
mkPosition p =
  Position (P.sourceName p) (P.sourceLine p) (P.sourceColumn p)

parseError :: Parsing m => String -> String -> m a -> m a
parseError rule i m = m <?> (i ++ " " ++ rule)

declError :: Parsing m => String -> m a -> m a
declError x = parseError "declaration" x

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
    "true", "false", "ind", "Some", "None", "empty"
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
k3Ops = emptyOps
  { _styleReserved = set k3Operators }

k3Idents :: TokenParsing m => IdentifierStyle m
k3Idents = emptyIdents
  { _styleReserved = set k3Keywords }

operator :: (TokenParsing m, Monad m) => String -> m ()
operator = reserve k3Ops

identifier :: (TokenParsing m, Monad m, IsString s) => m s
identifier = ident k3Idents

keyword :: (TokenParsing m, Monad m) => String -> m ()
keyword = reserve k3Idents


{- K3 grammar parsers -}

-- TODO: inline testing
program :: DeclParser
program = mkProgram <$> endBy roleBody eof
  where mkProgram l = DC.role "__global" $ concat l

roleBody :: K3Parser [K3 Declaration]
roleBody = some declaration


{- Declaration helpers -}
namedDecl nameKind name namedCstr =
  declError nameKind $ namedCstr $ keyword name *> identifier

namedBraceDecl k n rule cstr =
  namedDecl k n $ braceRule . (cstr <$>)
  where braceRule x = x <*> (braces rule)

{- Declarations -}
declaration :: DeclParser
declaration = choice [dGlobal, dTrigger, dSource, dSink,
                      dBind, dRole, dSelector, dAnnotation]

dGlobal :: DeclParser
dGlobal = namedDecl "state" "declare" $ rule . (mkState <$>)
  where
    rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateExpr)
    mkState name (qual,t) e = DC.global name t e

dTrigger :: DeclParser
dTrigger = namedDecl "trigger" "trigger" $ rule . (mkTrig <$>)
  where
    rule x = x <*> (parens idTypeList) <*> equateExpr
    mkTrig name args body = DC.global name (TC.trigger args) (Just body)

dEndpoint :: String -> String -> (K3 Type -> K3 Type) -> DeclParser
dEndpoint kind name typeCstr = namedDecl kind name $ rule . (mkEndpoint <$>)
  where rule x = x <*> (colon *> typeExpr) <*> equateExpr
        mkEndpoint n t e = DC.global n (typeCstr t) (Just e)

dSource :: DeclParser
dSource = dEndpoint "source" "source" TC.source

dSink :: DeclParser
dSink = dEndpoint "sink" "sink" TC.sink

dBind :: DeclParser
dBind = mkBind <$> identifier <*> bidirectional <*> identifier
  where mkBind id1 lSrc id2 = if lSrc then DC.bind id1 id2 else DC.bind id2 id1
        bidirectional = choice [symbol "|>" >> return True,
                                symbol "<|" >> return False]

dRole :: DeclParser
dRole = namedBraceDecl n n roleBody DC.role
  where n = "role"

dSelector :: DeclParser
dSelector = namedDecl "selector" "default" $ (DC.selector <$>)

dAnnotation :: DeclParser
dAnnotation = namedBraceDecl n n (some annotationBody) mkAnnotation
  where n = "annotation"
        mkAnnotation n body = DC.annotation $ n ++ (foldl (++) "" body)

-- TODO: requires, provides, methods, attributes, etc
annotationBody = identifier

{- Types -}
typeExpr :: TypeParser
typeExpr = parseError "type" "expression" $ choice [
    tPrimitive, tOption, tIndirection,
    tTuple, tRecord,
    tCollection, tFunction
  ]

qualifiedTypeExpr :: K3Parser (String, K3 Type)
qualifiedTypeExpr = 
  (,) <$> choice [tNamed "immut", tNamed "mut"] <*> typeExpr


-- TODO: qualified type subcomponents
tNamed x    = keyword x >> return x
tNested f k = f <$> (keyword k *> typeExpr)

tPrimitive :: TypeParser
tPrimitive = choice $ map tConst ["bool", "int", "real", "string"]
  where tConst x   = keyword x >> return (typeCstr x)
        typeCstr x = case x of
                      "bool"    -> TC.bool
                      "int"     -> TC.int
                      "real"    -> TC.float
                      "string"  -> TC.string

tOption :: TypeParser
tOption = tNested TC.option "option"

tIndirection :: TypeParser
tIndirection = tNested TC.indirection "ind"

tTuple :: TypeParser
tTuple = TC.tuple <$> (parens . commaSep1) typeExpr

tRecord :: TypeParser
tRecord = TC.record <$> (braces . semiSep1) idType

-- TODO: annotations
tCollection :: TypeParser
tCollection = tNested TC.collection "collection"

tFunction :: TypeParser
tFunction = TC.function <$> (typeExpr <* symbol "->") <*> typeExpr


{- Expressions -}
-- TODO: qualified expressions as subcomponents
expr :: ExpressionParser
expr = parseError "k3" "expression" $ buildExpressionParser opTable eTerm

qualifiedExpr :: K3Parser (String, K3 Expression)
qualifiedExpr = 
  (,) <$> choice [tNamed "immut", tNamed "mut"] <*> expr

eTerm :: ExpressionParser
eTerm = choice [
    eLiterals,
    eProject, eLambda, eCondition, 
    eAssign, eLet, eCase, eBind 
  ]

{- Literals -}
eLiterals :: ExpressionParser
eLiterals = choice [eTerminal, eOption, eIndirection,
                    eTuple, eRecord, eCollection]

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

{- Complex literals -}
eOption :: ExpressionParser
eOption = choice [EC.some <$> (keyword "Some" *> expr),
                  keyword "None" >> return (EC.constant CNone)]

eIndirection :: ExpressionParser
eIndirection = EC.indirect <$> (keyword "ind" *> expr)

eTuple :: ExpressionParser
eTuple = EC.tuple <$> commaSep expr

eRecord :: ExpressionParser
eRecord = EC.record <$> idExprList

-- TODO: annotations
eCollection :: ExpressionParser
eCollection = mkEmpty <$> (keyword "empty" *> tRecord)
  where mkEmpty t = EC.empty @+ (EType t)

{- Operators -}
binary  op cstr assoc parser = Infix   ((pure cstr) <* parser op) assoc
prefix  op cstr parser       = Prefix  ((pure cstr) <* parser op)
postfix op cstr parser       = Postfix ((pure cstr) <* parser op)

mkBinOp  (opName, opTag) = binary opName (EC.binop opTag) AssocLeft operator
mkBinOpK (opName, opTag) = binary opName (EC.binop opTag) AssocLeft keyword
mkUnOp   (opName, opTag) = prefix opName (EC.unop opTag) operator
mkUnOpK  (opName, opTag) = prefix opName (EC.unop opTag) keyword

opTable = [ [Infix ((pure $ EC.binop OApp) <* someSpace) AssocLeft],
              map mkBinOp  [("*",   OMul), ("/",  ODiv)],
              map mkBinOp  [("+",   OAdd), ("-",  OSub)],
              map mkBinOp  [("==",  OEqu), ("!=", ONeq), ("<>", ONeq),
                            ("<",   OLth), ("<=", OLeq),
                            (">",   OGth), (">=", OGeq) ],
            ((map mkBinOpK [("and", OAnd), ("or", OOr)])
              ++ [mkUnOpK   ("not", ONot)]),
              map mkBinOp  [("<-",  OSnd)],
              map mkBinOp  [(";",   OSeq)]
          ]

{- Terms -}
ePrefix k = keyword k *> expr
iPrefix k = keyword k *> identifier
iArrow k  = iPrefix k <* symbol "->" 

eProject :: ExpressionParser
eProject = (flip EC.project) <$> expr <*> (dot *> identifier)

eLambda :: ExpressionParser
eLambda = EC.lambda <$> (iArrow "fun") <*> expr

eCondition :: ExpressionParser
eCondition = EC.ifThenElse <$> (ePrefix "if") <*> (ePrefix "then") <*> (ePrefix "else")

eAssign :: ExpressionParser
eAssign = EC.assign <$> identifier <*> equateExpr

eLet :: ExpressionParser
eLet = EC.letIn <$> identifier <*> ((symbol "=" *> expr) <* keyword "in") <*> expr

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
idList     = commaSep identifier
idPairList = commaSep idPair
idExprList = commaSep idExpr
idTypeList = commaSep idType
idPair     = (,) <$> identifier <*> (colon *> identifier)
idExpr     = (,) <$> identifier <*> (colon *> expr)
idType     = (,) <$> identifier <*> (colon *> typeExpr)

{- Misc -}
equateExpr :: ExpressionParser
equateExpr = symbol "=" *> expr

{- Main parsing functions -}
parseK3 :: String -> String
parseK3 progStr = case P.parse program "" progStr of
  -- TODO: better parse error handling
  Left err -> "No match " ++ show err
  Right val -> "Found val" ++ show val
