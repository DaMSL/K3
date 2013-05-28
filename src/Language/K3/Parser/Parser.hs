-- | K3 Parser.
module Language.K3.Parser.Parser (
  parseK3
) where

-- TODO: structured top-level declaration construction
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
import Language.K3.Core.Annotation

{- Type synonyms for parser return types -}
type K3Type = K3 Type
type K3Expr = K3 Expression

type K3TypeParser = PP.ParsecT String () Identity (Tree Type)

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

{- Expression helpers -}
mkExpr n ch = Node n ch
mkConst x   = mkExpr (EConstant x) []
mkVar x     = mkExpr (EVariable x) []
mkOp op l r = mkExpr (EOperate op) [l, r]
mkUOp op e  = mkExpr (EOperate op) [e]

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
program  = endBy roleBody eof
roleBody = some declaration


{- Declarations -}
namedDecl nameKind name namedCstr =
  declError nameKind $ namedCstr $ keyword name *> identifier

namedBraceDecl k n rule cstr =
  namedDecl k n $ braceRule . (cstr <$>)
  where braceRule x = x <*> (braces rule)

declaration = choice [state, sink, source, instruction, role, annotation]

state = namedDecl "state" "declare" $ rule . (mkState <$>)
  where
    rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateExpr)
    mkState name (qual,t) initExpr = name ++ qual ++ (show t) ++ (show initExpr)

sink = namedDecl "trigger" "trigger" $ rule . (mkSink <$>)
  where
    f (x,y) = x ++ (show y)
    g x y = f x ++ show y
    locals = semiSep $ g <$> idType <*> (optional equateExpr)
    rule x = x <*> (parens idTypeList) <*> (braces locals) <*> equateExpr
    mkSink name args lDecls body =
      name ++ (foldl (++) "" (map f args)) ++ (foldl (++) "" lDecls) ++ (show body)

source = namedDecl "source" "source" $ rule . (mkSource <$>)
  where rule x = x <*> equateExpr
        mkSource srcName src = srcName ++ (show src)

-- TODO: bind vs <- syntax for attaching sources
instruction = choice [bindInstr, consumeInstr]

bindInstr = mkBind <$> (keyword "bind" *> identifier) <*> identifier
  where mkBind srcId sinkId = srcId ++ sinkId

consumeInstr = mkConsume <$> (keyword "consume" *> identifier)
  where mkConsume srcId = srcId

role = namedBraceDecl "role" "role" roleBody mkRole
  where mkRole name body = name ++ (foldl (++) "" body)

annotation = namedBraceDecl "annotation" "annotation" (some annotationBody) mkAnnotation
  where mkAnnotation name body = name ++ (foldl (++) "" body)

-- TODO: requires, provides, methods, attributes, etc
annotationBody = identifier

{- Types -}
typeExpr :: K3TypeParser
typeExpr = parseError "type" "expression" $ choice [
    tPrimitive, tOption, tIndirection,
    tTuple, tRecord,
    tCollection, tFunction
  ]

--qualifiedTypeExpr :: (String, K3Type)
qualifiedTypeExpr = 
  (,) <$> choice [tNamed "immut", tNamed "mut"] <*> typeExpr


-- TODO: qualified type subcomponents
mkType x    = x
tNamed x    = keyword x >> return x
tNested f k = f <$> (keyword k *> typeExpr)

tPrimitive = choice $ map tConst ["bool", "int", "real", "string"]
  where tConst x   = keyword x >> return (typeCstr x)
        typeCstr x = case x of
                      "bool"    -> f TBool
                      "int"     -> f TInt
                      "real"    -> f TFloat
                      "string"  -> f TString
        f t = mkType $ Node t []

tOption = tNested mkOptionType "option"
  where mkOptionType t = mkType $ Node TOption [t]

tIndirection = tNested mkIndType "ind"
  where mkIndType t = mkType $ Node TIndirection [t]    

tTuple = mkTuple <$> (parens . commaSep1) typeExpr
  where mkTuple l = mkType $ Node TTuple l

tRecord = mkRecord . unzip <$> (braces . semiSep1) idType
  where mkRecord (ids, types) = mkType $ Node (TRecord ids) types

-- TODO: annotations
tCollection = tNested mkCollectionType "collection"
  where mkCollectionType t = mkType $ Node TCollection [t]

tFunction = mkFunType <$> (typeExpr <* symbol "->") <*> typeExpr
  where mkFunType argType retType = mkType $ Node TFunction [argType, retType]


{- Expressions -}
-- TODO: qualified expressions as subcomponents
expr = parseError "k3" "expression" $ buildExpressionParser opTable eTerm
qualifiedExpr = 
  (,) <$> choice [tNamed "immut", tNamed "mut"] <*> expr

eTerm = choice [
    eLiterals,
    eProject, eLambda, eCondition, 
    eAssign, eLet, eCase, eBind 
  ]

{- Literals -}
eLiterals = choice [eTerminal, eOption, eIndirection, eTuple, eRecord, eCollection]

{- Terminals -}
eTerminal = choice [eConstant, eVariable]
eConstant = choice [eCBool, eCNumber, eCString]

eCBool = (mkConst . CBool) <$>
  choice [keyword "true" >> return True, keyword "false" >> return False]

eCNumber = mkNumber <$> integerOrDouble
  where mkNumber x = case x of
                      Left i  -> mkConst . CInt . fromIntegral $ i
                      Right d -> mkConst . CFloat . double2Float $ d

eCString = mkConst . CString <$> stringLiteral

eVariable = mkVar <$> identifier

{- Complex literals -}
eOption = choice [mkOption <$> (keyword "Some" *> expr),
                  keyword "None" >> return (mkConst CNone)]
  where mkOption e = mkExpr ESome [e]

eIndirection = mkIndirection <$> (keyword "ind" *> expr)
  where mkIndirection e = mkExpr EIndirect [e]

eTuple = mkTuple <$> commaSep expr
  where mkTuple l = mkExpr ETuple l

eRecord = (mkRecord . unzip) <$> idExprList
  where mkRecord (ids, exprs) = mkExpr (ERecord ids) exprs

-- TODO: annotations
eCollection = mkEmpty <$> (keyword "empty" *> tRecord)
  where mkEmpty cType = mkConst CEmpty

{- Operators -}
binary  op cstr assoc parser = Infix   ((pure cstr) <* parser op) assoc
prefix  op cstr parser       = Prefix  ((pure cstr) <* parser op)
postfix op cstr parser       = Postfix ((pure cstr) <* parser op)

mkBinOp  (opName, opTag) = binary opName (mkOp opTag) AssocLeft operator
mkBinOpK (opName, opTag) = binary opName (mkOp opTag) AssocLeft keyword
mkUnOp   (opName, opTag) = prefix opName (mkUOp opTag) operator
mkUnOpK  (opName, opTag) = prefix opName (mkUOp opTag) keyword

opTable = [ [Infix ((pure $ mkOp OApp) <* someSpace) AssocLeft],
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

eProject = mkProject <$> expr <*> (dot *> identifier)
  where mkProject e field = mkExpr (EProject field) [e]

eLambda = mkFun <$> (iArrow "fun") <*> expr
  where mkFun x body = mkExpr (ELambda x) [body]

eCondition = mkCond <$> (ePrefix "if") <*> (ePrefix "then") <*> (ePrefix "else")
  where mkCond eTest eThen eElse = mkExpr EIfThenElse [eTest, eThen, eElse]

eAssign = mkAssign <$> identifier <*> (symbol "=" *> expr)
  where mkAssign lVar rExpr = mkExpr (EAssign lVar) [rExpr]

eLet = mkLet <$> identifier <*> ((symbol "=" *> expr) <* keyword "in")
  where mkLet x body = mkExpr (ELetIn x) [body]

eCase = mkCase <$> ((ePrefix "case") <* keyword "of") <*>
                   (braces eCaseSome) <*> (braces eCaseNone)
  where eCaseSome = (,) <$> (iArrow "Some") <*> expr
        eCaseNone = (keyword "None" >> symbol "->") *> expr
        mkCase eTest (someId, eSome) eNone = mkExpr (ECaseOf someId) [eTest, eSome, eNone]


eBind = mkBind <$> (ePrefix "bind") <*> (keyword "as" *> eBinder) <*> (ePrefix "in")
  where mkBind eBound binding eBody = mkExpr (EBindAs binding) [eBound, eBody]

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
equateExpr = symbol "=" *> expr

{- Main parsing functions -}
parseK3 :: String -> String
parseK3 progStr = case P.parse program "" progStr of
  -- TODO: better parse error handling
  Left err -> "No match " ++ show err
  Right val -> "Found val" ++ show val
