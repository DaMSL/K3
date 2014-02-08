{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- TODO: 
--       convert application and lambdas to use apply's
--       trigger
--       produce default values for nothing in case ... of

-- | K3 Parser.
module Language.K3.Parser.K3Ocaml (
  K3Parser,
  identifier,
  declaration,
  qualifiedTypeExpr,
  typeExpr,
  qualifiedExpr,
  expr,
  parseType,
  parseExpression,
  parseDeclaration,
  {- parseSimpleK3, -}
  parseK3Ocaml,
  runK3Parser,
  ensureUIDs
) where

import qualified Debug.Trace as Trace

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
import qualified Data.Foldable as Fold

import System.FilePath

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP
{-import qualified Text.ParserCombinators.ReadP as RP-}

import Text.Parser.Char
import Text.Parser.Combinators
import Text.Parser.Expression hiding ( buildExpressionParser )
import Text.Parser.Token
import Text.Parser.Token.Style

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
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
    "==", "!=", "<", ">", ">=", "<=", ";", "!",
    "&", "|", "++"
  ]

k3Keywords :: [[Char]]
k3Keywords = [
    {- Types -}
    "unit", "int", "bool", "float", "string", "address",
    "maybe", "ref" , "set", "bag", "list",

    {- Declarations -}
    "declare", "fun", "trigger", "source", "sink",

    {- Expressions -}
    "let", "in", "if", "then", "else", "do",
    "map", "filtermap", "flatten", "fold", "groupby", "sort",
    "peek", "insert", "delete", "update", "send",

    {- Values -}
    "true", "false", "just", "nothing"
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

-- Begin a run of the parser using the K3Parser monad
runK3Parser :: K3Parser a -> String -> Either P.ParseError a
runK3Parser p s = P.runParser p emptyParserState "" s

maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (const Nothing) Just $ runK3Parser p s

parseType :: String -> Maybe (K3 Type)
parseType = maybeParser typeExpr

parseExpression :: String -> Maybe (K3 Expression)
parseExpression = maybeParser expr

parseDeclaration :: String -> Maybe (K3 Declaration)
parseDeclaration s = either (const Nothing) mkRole $ runK3Parser declaration s
  where mkRole l = Just $ DC.role defaultRoleName l

{-
parseSimpleK3 :: String -> Maybe (K3 Declaration)
parseSimpleK3 s = either (const Nothing) Just $ runK3Parser (program False) s
-}

-- This is the main entry point to the module
parseK3Ocaml :: [FilePath] -> String -> IO (Either String (K3 Declaration))
parseK3Ocaml includePaths s = do
  searchPaths   <- if null includePaths then getSearchPath 
                   else return includePaths
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
    parseAtLevel asTopLevel = stringifyError . runK3Parser program
    programError = Left "Invalid program, expected top-level role."


{- K3 grammar parsers -}

program :: DeclParser
program = DSpan <-> (rule >>= selfContainedProgram)
  where rule = mkProgram <$> endBy (roleBody "") eof
        mkProgram l = DC.role defaultRoleName $ concat l
        
        selfContainedProgram d = mkEntryPoints d >>= mkBuiltins
        mkEntryPoints d = withEnvironment $ (uncurry $ processInitsAndRoles d) . fst . safePopFrame
        mkBuiltins = ensureUIDs . declareBuiltins

roleBody :: Identifier -> K3Parser [K3 Declaration]
roleBody n = rule {- >>= postProcessRole n -}
  where rule = some declaration >>= return . concat

{- Declarations -}
declaration :: K3Parser [K3 Declaration]
declaration = (//) attachComment <$> comment <*> 
              singleDecls

  where singleDecls  = (:[]) <$> (DUID # choice [{- dForeign, -}dGlobal, dTrigger]) -- TODO

        attachComment [] _      = []
        attachComment (h:t) cmt = (h @+ DSyntax cmt):t

dGlobal :: DeclParser
dGlobal = (namedDecl "global" "declare" $ rule . (mkGlobal <$>)) <|>
          (namedDecl "foreign" "foreign" $ rule . (mkGlobal <$>))
  where rule x = x <* colon <*> qualifiedTypeExpr <*> (optional equateExpr)
        mkGlobal n qte eOpt = DC.global n qte (propagateQualifier qte eOpt) -- unsure

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


{- Types -}
typeExpr :: TypeParser
typeExpr = typeError "expression" $ TUID # tTermOrFun

qualifiedTypeExpr :: TypeParser
qualifiedTypeExpr = typeExprError "qualified" $ flip (@+) <$> (option TImmutable typeQualifier) <*> typeExpr

qualifiedTypeTerm :: TypeParser
qualifiedTypeTerm = typeExprError "qualified" $ flip (@+) <$> (option TImmutable typeQualifier) <*> tTerm

typeVarDecls :: K3Parser [TypeVarDecl]
typeVarDecls = sepBy typeVarDecl (symbol ",")

typeVarDecl :: K3Parser TypeVarDecl
typeVarDecl = TypeVarDecl <$> identifier <*> pure Nothing <*> pure Nothing
    
-- Look for ref to indicate mutability
typeQualifier :: K3Parser (Annotation Type)
typeQualifier = typeError "qualifier" $ keyword "ref" >> return TMutable 

{- Type terms -}
tTerm :: TypeParser
tTerm = TSpan <-> (//) attachComment <$> comment 
              <*> choice [ tPrimitive, tOption, {-tIndirection,-}
                           tTupleOrNested, tCollection, -- no records
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
                                              , ("unit",    TC.unit)
                                              , ("float",   TC.real)
                                              , ("string",  TC.string)
                                              , ("address", TC.address) ]
  where tConst (x, f) = keyword x >> return f
        tPrimError    = typeExprError "primitive"


tQNested :: (K3 Type -> K3 Type) -> String -> TypeParser
tQNested f k = f <$> (keyword k *> qualifiedTypeTerm)

tOption :: TypeParser
tOption = typeExprError "option" $ tQNested TC.option "maybe"

tIndirection :: TypeParser
tIndirection = typeExprError "indirection" $ tQNested TC.indirection "ind"

-- We'll convert every tuple in k3o to a record in k3, mainly because
-- collections can no longer contain tuples
tTupleOrNested :: TypeParser
tTupleOrNested = choice [try unit, {- try (parens $ nestErr $ clean <$> typeExpr), -} parens $ tupErr tTuple]
  where unit             = typeExprError "unit" $ symbol "(" *> symbol ")" >> return (TC.unit)
        tTuple           = mkTypeTuple <$> qualifiedTypeExpr <* comma <*> commaSep1 qualifiedTypeExpr
        mkTypeTuple t tl = TC.record $ addIds $ t:tl -- covert to record
        
        clean t          = stripAnnot isTSpan $ stripAnnot isTUID t
        stripAnnot f t   = maybe t (t @-) $ t @~ f
        {-nestErr          = typeExprError "nested"-}
        tupErr           = typeExprError "tuple"

-- Records don't exist in k3o. We make them out of tuples
tCollectionRecord :: TypeParser
tCollectionRecord = typeExprError "record" $ ( TUID # ) $
            TC.record <$> addIds <$> commaSep1 qualifiedTypeExpr
            -- We add ids to to types to create valid records for the collections

-- Add ids to types or any other list
addIds :: [a] -> [(Identifier, a)]
addIds ts = snd $ foldr (\t (last, acc) -> 
              (last - 1, (to_str last, t):acc) 
            )
            (length ts, [])
            ts
  where to_str i = "_" ++ show i 

tCollection :: TypeParser
tCollection = typeExprError "collection" $ ( TUID # ) $
                mkCollectionType <$> tCollectionElement
  where mkCollectionType (t, a) = (TC.collection t) @+ a

tCollectionElement :: K3Parser(K3 Type, Annotation Type)
tCollectionElement = typeExprError "collection-element" $ 
                  (try bag <|> try set <|> list)
                  
  where set = braces $ (,) <$> (TUID # tCollectionRecord) <*> (pure $ TAnnotation "Set") -- Set
        bag = pipeBraces $ (,) <$> (TUID # tCollectionRecord) <*> (pure $ TAnnotation "Bag") -- Bag
        list = brackets $ (,) <$> (TUID # tCollectionRecord) <*> (pure $ TAnnotation "List") -- List

-- Like braces, but for bags in k3o
pipeBraces p = between (symbol "{|") (symbol "|}") p

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
expr = parseError "expression" "k3" $ buildExpressionParser nonSeqOpTable eApp

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
  -- Handle slicing, which is left recursive, by adding it optionally
  e  <- EUID # (ESpan <-> rawTerm)
  mi <- optional (spanned eSlice)
  case mi of
    Nothing      -> return e -- Just an expression
    Just (x, sp) -> EUID # (return $ (handleSlice e x) @+ ESpan sp)
  where
    rawTerm = (//) attachComment <$> comment <*> 
      choice [ 
               try eDo,
               try eFilterMap,
               try eFold,
               try eMap,
               try eFlatten,
               try ePeek,
               try eGroupBy,
               try eInsert,
               try eUpdate,
               try eDelete,
               try eSend,
               try eSort,
               try eAssign,
               try eRange,
               try eAddress, -- this uses : and confuses eRange
               eLiterals,
               eLambda,
               eCondition,
               eLet
             ]
    eSlice = brackets (commaSep1 underscoreOrExpr)
    attachComment e cmt = e @+ (ESyntax cmt)

doHere a = seq (Trace.trace "here\n") a

-- Parse do sequences
eDo :: ExpressionParser
eDo = exprError "do" $ keyword "do" *> braces (EC.block <$> (semiSep1 $ expr))

eFilterMap :: ExpressionParser
eFilterMap = exprError "filtermap" $ keyword "filtermap" *> parens
               (make <$> (eLambda <* comma) <*> (eLambda <* comma) <*> expr)
    where
      -- We don't have a filtermap, so we need to do map(filter(col))
      make lam1 lam2 eCol =
        let filterCol = applyMethod eCol "filter" [lam1]
        in applyMethod filterCol "map" [lam2]

eFold :: ExpressionParser
eFold = exprError "fold" $ keyword "fold" *> parens
               (make <$> (eLambda <* comma) <*> (expr <* comma) <*> expr)
    where
      make lam1 acc eCol = applyMethod eCol "fold" [lam1, acc]

eMap :: ExpressionParser
eMap = exprError "map" $ keyword "map" *> parens
               (make <$> (eLambda <* comma) <*> expr)
    where
      make lam1 eCol = applyMethod eCol "map" [lam1]

eFlatten :: ExpressionParser
eFlatten = exprError "flatten" $ keyword "flatten" *> parens (make <$> expr)
    where
      make eCol = applyMethod eCol "flatten" []

ePeek :: ExpressionParser
ePeek = exprError "peek" $ keyword "peek" *> parens
               (make <$> expr)
    where
      make eCol = applyMethod eCol "peek" []

eGroupBy :: ExpressionParser
eGroupBy = exprError "groupBy" $ keyword "groupby" *> parens
               (make <$> (eLambda <* comma) <*> (eLambda <* comma) <*> (expr <* comma) <*> expr)
    where
      make lam1 lam2 acc col = applyMethod col "groupby" [lam1, lam2, acc]

-- insert creates a record
eInsert :: ExpressionParser
eInsert = exprError "insert" $ keyword "insert" *> parens
               (make <$> (expr <* comma) <*> (makeRecord <$> commaSep1 expr))
    where
      makeRecord es = EC.record $ addIds es
      make col record = applyMethod col "insert" [record]

-- update translates tuples to records
eUpdate :: ExpressionParser
eUpdate = exprError "update" $ keyword "update" *> parens
               (make <$> (expr <* comma) <*> (expr <* comma) <*> expr)
    where
      make col oldrec newrec = applyMethod col "update" [oldrec, newrec]

eDelete :: ExpressionParser
eDelete = exprError "delete" $ keyword "delete" *> parens
               (make <$> (expr <* comma) <*> expr)
    where
      make col elem = applyMethod col "delete" [elem]

-- send needs to translate arguments to records
eSend :: ExpressionParser
eSend = exprError "send" $ keyword "send" *> parens
               (make <$> (expr <* comma) <*> (expr <* comma) <*> (makeRecord <$> commaSep1 expr))
    where
      makeRecord es = EC.record $ addIds es
      make target address argRecord = EC.send target address argRecord

eSort :: ExpressionParser
eSort = exprError "sort" $ keyword "sort" *> parens
               (make <$> (expr <* comma) <*> eLambda)
    where
      make col lambda = applyMethod col "sort" [lambda]

eRange :: ExpressionParser
eRange = exprError "range" $ brackets (
        do
           e1 <- expr
           symbol "::"
           e2 <- expr
           symbol "::"
           e3 <- expr
           return $ EC.range e1 e2 e3)

applyMethod :: K3 Expression -> Identifier -> [K3 Expression] -> K3 Expression
applyMethod col method args = EC.applyMany (EC.project method $ col) args


{- Expression literals -}
eLiterals :: ExpressionParser
eLiterals = choice [ 
    try eCollection,
    eTerminal,
    eOption,
    {-eIndirection,-}
    eTuplePrefix,
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

-- We need to handle numbers like 0.
eCNumber :: ExpressionParser
eCNumber = choice [
                   try ((EC.constant . CReal) <$> double),
                   try ((EC.constant . CReal. fromIntegral) <$> integer <* symbol "."),
                   (EC.constant . CInt . fromIntegral) <$> integer
                  ]

eCString :: ExpressionParser
eCString = EC.constant . CString <$> stringLiteral

eVariable :: ExpressionParser
eVariable = exprError "variable" $ EC.variable <$> identifier

{- Complex literals -}

-- NOTE: we hardwired none to be immutable for now
eOption :: ExpressionParser
eOption = exprError "option" $
  choice [EC.some <$> (keyword "just" *> qualifiedExpr),
          {-(keyword "nothing" *> colon *> qualifiedTypeTerm) *> (EC.constant . CNone <$> exprNoneQualifier)]-}
          (keyword "nothing" *> colon *> qualifiedTypeTerm) *> (EC.constant . CNone <$> pure NoneImmut)]

eIndirection :: ExpressionParser
eIndirection = exprError "indirection" $ EC.indirect <$> (keyword "ref" *> qualifiedExpr)

eTuplePrefix :: ExpressionParser
eTuplePrefix = choice [try unit, try eNested, eTupleOrSend]
  where unit          = symbol "(" *> symbol ")" >> return (EC.tuple [])
        eNested       = stripSpan  <$> parens expr
        eTupleOrSend  = do
                          elements <- parens $ commaSep1 qualifiedExpr
                          mkTupOrSend elements

        mkTupOrSend [e] = return $ stripSpan <$> e
        mkTupOrSend l   = return $ EC.record $ addIds l    -- change to record

        stripSpan e               = maybe e (e @-) $ e @~ isESpan

eEmpty :: ExpressionParser
eEmpty = exprError "empty" $ EC.empty <$> col
  where mkEmpty l a = foldl (@+) l a 
        col = do
                  choice [try $ keyword "{}", keyword "{||}", keyword "[]"]
                  colon
                  qualifiedTypeTerm

-- Data type to parse the destructed tuples
data A a = Arg Identifier | Tup [A a] a

-- Add numbers to the mini-ast
add_numbers :: A a -> A Int
add_numbers x = snd $ loop x 1
  where
    loop (Arg a)  i = (i, Arg a)
    loop (Tup xs _) i = 
        let (i_max, xs') = foldr (\x (i, acc) ->
              let (i', res) = loop x (i+1)
              in (i', res:acc))
              (i, []) xs
        in (i_max, Tup xs' i)

 -- We take the mini-ast and a function for binding at the top level (just an id)
make_binds (Arg id) f     = f id
 -- make the arg the first number, and then bind at each level
make_binds a@(Tup _ i) f  = f (toId i) . binder a
  where
    binder :: A Int -> K3 Expression -> K3 Expression
    binder (Arg _)    = error "bad code path"
    binder (Tup xs i) = 
      let ids = map getIds xs
          cur_bind = bind (toId i) ids
      in foldl' (\acc x -> if checkTup x then acc . binder x else acc) cur_bind xs

    checkTup (Tup _ _) = True
    checkTup (Arg _)   = False

    -- Curried binding functions to create our bind expression
    bind parentId ids = EC.bindAs (EC.variable parentId) $ BRecord $ addIds ids

    getIds (Tup _ i) = toId i
    getIds (Arg id)  = id

    toId i = "_"++show i

eLambda :: ExpressionParser
eLambda = exprError "lambda" $ destruct
  where
    -- We need to convert tuple destruction to binds
    -- Use a mini AST to do the parsing with, then convert that
    destruct = do 
                  symbol "\\"
                  as <- deepBinds -- read the destructing into AST
                  let as_num = add_numbers as -- add nums to AST
                      lambdaFn = make_binds as_num EC.lambda -- convert to binds
                  symbol "->"
                  exp <- nonSeqExpr
                  return $ lambdaFn exp

-- Common functions used by lambda and let to parse mini-AST
deepBinds :: K3Parser(A ())
deepBinds = tupleDest <|> labelDest

tupleDest :: K3Parser (A ())
tupleDest = Tup <$> (parens $ commaSep1 $ deepBinds) <*> pure ()
                  
labelDest :: K3Parser(A ())
-- Must only call tTerm here since we can't have functions
labelDest = Arg <$> anyOrLabel
  where 
     anyOrLabel = try(identifier <* colon <* qualifiedTypeTerm) <|> underscore

underscore :: K3Parser(Identifier)
underscore = do 
                id <- identifier
                case id of
                  "_" -> return "_"
                  _   -> fail "not underscore"

-- Application works for us because we just see application as passing a tuple (record) to a function
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
eLet = exprError "let" $ keyword "let" *> (try justBind <|> binding) <*> equateQExpr <*> (keyword "in" *> expr)
  where binding = do
                    as <- deepBinds              -- make mini-ast of binding pattern
                    let as_num = add_numbers as -- add nums to mini-ast
                        letFn = make_binds as_num EC.letIn -- convert to binds
                    return letFn
        -- We also did option pattern matching with let, so we need to translate it to
        -- case ... of
        justBind = do
                     keyword "just"
                     id <- identifier
                     colon
                     t <- qualifiedTypeTerm
                     -- Because of the way we handled maybes in k3ocaml, we need to 
                     -- create a default, unused value for the None -> branch
                     return $ \bindE e -> EC.caseOf bindE id e $ defaultValue t

eAddress :: ExpressionParser
eAddress = exprError "address" $ EC.address <$> ipAddress <* colon <*> port
  where ipAddress = EUID # EC.constant . CString <$> (some $ choice [alphaNum, oneOf "."])
        port = EUID # EC.constant . CInt . fromIntegral <$> natural

-- We unify underscores and expressions (so we can tell them apart)
underscoreOrExpr :: K3Parser (Maybe (K3 Expression))
underscoreOrExpr = (try underscore *> pure Nothing) <|> (Just <$> expr)

-- Handle a slice by creating a filter function
handleSlice :: K3 Expression -> [(Maybe (K3 Expression))] -> K3 Expression
handleSlice e l = mkFilter e l
  where
        -- We need to convert to a filter function
        mkFilter :: K3 Expression -> [Maybe (K3 Expression)] -> K3 Expression
        mkFilter col m_list = let id_terms :: [(Identifier, K3 Expression)] =
                                    map (\(id, m) -> (id, unwrapM m)) $ filter (isJust . snd) $ addIds m_list
                                  -- Create a lambda to filter by the things we care about
                                  lambda   = EC.lambda "rec" $ mkAndChain id_terms
                              in applyMethod col "filter" [lambda]

        -- Create a chain of 'and's, comparing all the elements we need
        mkAndChain id_terms = foldr (\x acc -> EC.binop OAnd (mkCompare x) acc)
                                (mkCompare $ head id_terms) $
                                tail id_terms
        -- Create a comparison with a projection of the "rec" object
        mkCompare (id, x) = EC.binop OEqu (EC.project id $ EC.variable "rec") x

        isJust Nothing = False
        isJust _       = True

        unwrapM (Just x) = x
        unwrapM Nothing  = error "nothing"

            -- TC.record <$> addIds <$> commaSep1 qualifiedTypeExpr
-- TODO: treating collection literals as immutable will fail when initializing mutable collections.
eCollection :: ExpressionParser
eCollection = exprError "collection" $ singleField
  where 
        singleField =     choice [
                            try $ pipeBraces $ colWithType "Bag"
                          , braces $ colWithType "Set"
                          , brackets $ colWithType "List"
                          ]

        colWithType :: String -> K3Parser (K3 Expression)
        colWithType annoStr = 
          do es <- readElems
             let es' = Trace.trace ("elements are " ++ show es ++ "\n") $ es
                 idsTypes = idTypeOfE $ head es'
                 recs = mkRecords idsTypes es'
                 col = mkCollection idsTypes recs $ [EAnnotation annoStr]
             return col

        readElems :: K3Parser ([[K3 Expression]])
        readElems  = semiSep1 $ commaSep1 $ expr
                            
        idTypeOfE :: [a] -> [(Identifier, K3 Type)]
        idTypeOfE e = addIds $ map (const (TC.top @+ TImmutable)) e
                      -- we make all the types Top for simplicity
        
        mkRecords :: [(Identifier, K3 Type)] -> [[K3 Expression]] -> [K3 Expression]
        mkRecords idsTypes es = let ids = fst . unzip $ idsTypes
                                -- Create records out of the ids and elements
                                in map (EC.record . zip ids . (map (@+ EImmutable))) es
        
        -- Make a collection given ids-types, elements, and an annotation
        mkCollection idtyl el anno = 
          EC.letIn cId (emptyC idtyl anno) $ EC.binop OSeq (mkInserts el) cVar

        -- Create empty and add annotations
        emptyC idtyl anno = foldl (@+) ((EC.empty $ TC.record idtyl) @+ EImmutable) anno
        mkInserts el      = foldl (\acc e -> EC.binop OSeq acc $ mkInsert e) (mkInsert $ head el) $ tail el
        mkInsert          = EC.binop OApp $ EC.project "insert" cVar
        cId               = "__collection"
        cVar              = EC.variable cId

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
      map mkBinOp  [("==",  OEqu), ("!=", ONeq)],
      map mkUnOpK  [("!",   ONot)],
      map mkBinOpK [("&",   OAnd)],
      map mkBinOpK [("|",   OOr)]
  ]

{-fullOpTable :: OperatorTable K3Parser (K3 Expression)-}
{-fullOpTable = nonSeqOpTable ++-}
  {-[   map mkBinOp  [(";",   OSeq)]-}
  {-]-}

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

{- Identifiers and their list forms -}

idList :: K3Parser [Identifier]
idList = commaSep identifier

idPairList :: K3Parser [(Identifier, Identifier)]
idPairList = commaSep idPair

idQExprList :: K3Parser [(Identifier, K3 Expression)]
idQExprList = commaSep idQExpr

{- Note: unused
idQTypeList :: K3Parser [(Identifier, K3 Type)]
idQTypeList = commaSep idQType
-}

idPair :: K3Parser (Identifier, Identifier)
idPair = (,) <$> identifier <*> (colon *> identifier)

idQExpr :: K3Parser (Identifier, K3 Expression)
idQExpr = (,) <$> identifier <*> (colon *> qualifiedExpr)

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
{-
postProcessRole :: Identifier -> [K3 Declaration] -> K3Parser [K3 Declaration]
postProcessRole n dl  = 
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
-}
        

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

-- | Default values for some simple types
defaultValue :: K3 Type -> K3 Expression
defaultValue (tag -> TBool)       = boolOf False
defaultValue (tag -> TInt)        = intOf 0
defaultValue (tag -> TReal)       = realOf 0.0
defaultValue (tag -> TString)     = stringOf ""
defaultValue (tag -> TAddress)    = let defaultIp = stringOf $ "127.0.0.1"
                                        defaultPort = stringOf $ "40000"
                                    in addressOf defaultIp defaultPort
defaultValue (tag -> _)           = error "unhandled default value for type"

-- Functions to create constants easily
addressOf ip port = EC.address ip port
stringOf s        = constOf CString s
realOf r          = constOf CReal r
intOf i           = constOf CInt i
boolOf b          = constOf CBool b
constOf typ c     = EC.constant $ typ c

-- | Propagates a mutability qualifier from a type to an expression.
--   This is used in desugaring non-function initializers and annotation members.
propagateQualifier :: K3 Type -> Maybe (K3 Expression) -> Maybe (K3 Expression)
propagateQualifier _ Nothing  = Nothing
propagateQualifier (tag &&& annotations -> (ttag, tas)) (Just e@(annotations -> eas))
  | any isEQualified eas || inApplicable ttag = Just e
  | otherwise = Just $ if any isTImmutable tas then (e @+ EImmutable) else (e @+ EMutable)
  where inApplicable = flip elem [TTrigger, TSink, TSource]


-- | Duplicate implementation of buildExpressionParser adopted from the parsers-0.10 source.
--   This applies a bugfix for detecting ambiguous operators.
buildExpressionParser :: forall m a. (Parsing m, Applicative m)
                      => OperatorTable m a -> m a -> m a
buildExpressionParser operators simpleExpr
    = foldl makeParser simpleExpr operators
    where
      makeParser term ops
        = let (rassoc,lassoc,nassoc,prefix',postfix') = foldr splitOp ([],[],[],[],[]) ops
              rassocOp   = choice rassoc
              lassocOp   = choice lassoc
              nassocOp   = choice nassoc
              prefixOp   = choice prefix'  <?> ""
              postfixOp  = choice postfix' <?> ""

              -- Note: parsers-0.10 does not employ a 'try' parser here. 
              ambiguous assoc op = try $ op *> empty <?> ("ambiguous use of a " ++ assoc ++ "-associative operator")

              ambiguousRight    = ambiguous "right" rassocOp
              ambiguousLeft     = ambiguous "left" lassocOp
              ambiguousNon      = ambiguous "non" nassocOp

              termP      = (prefixP <*> term) <**> postfixP

              postfixP   = postfixOp <|> pure id

              prefixP    = prefixOp <|> pure id

              rassocP, rassocP1, lassocP, lassocP1, nassocP :: m (a -> a)

              rassocP  = (flip <$> rassocOp <*> (termP <**> rassocP1)
                          <|> ambiguousLeft
                          <|> ambiguousNon)

              rassocP1 = rassocP <|> pure id

              lassocP  = ((flip <$> lassocOp <*> termP) <**> ((.) <$> lassocP1)
                          <|> ambiguousRight
                          <|> ambiguousNon)

              lassocP1 = lassocP <|> pure id

              nassocP = (flip <$> nassocOp <*> termP)
                        <**> (ambiguousRight
                              <|> ambiguousLeft
                              <|> ambiguousNon
                              <|> pure id)
           in termP <**> (rassocP <|> lassocP <|> nassocP <|> pure id) <?> "operator"


      splitOp (Infix op assoc) (rassoc,lassoc,nassoc,prefix',postfix')
        = case assoc of
            AssocNone  -> (rassoc,lassoc,op:nassoc,prefix',postfix')
            AssocLeft  -> (rassoc,op:lassoc,nassoc,prefix',postfix')
            AssocRight -> (op:rassoc,lassoc,nassoc,prefix',postfix')

      splitOp (Prefix op) (rassoc,lassoc,nassoc,prefix',postfix')
        = (rassoc,lassoc,nassoc,op:prefix',postfix')

      splitOp (Postfix op) (rassoc,lassoc,nassoc,prefix',postfix')
        = (rassoc,lassoc,nassoc,prefix',op:postfix')
