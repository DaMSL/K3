{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Parser.DataTypes where

import Control.Applicative
import Control.Monad

import Data.Functor.Identity
import Data.List
import Data.String

import qualified Data.HashSet as HashSet
import qualified Data.Map     as Map
import Data.HashSet ( HashSet )
import Data.Map     ( Map )

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP

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
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Core.Metaprogram

{- Type synonyms for parser return types -}

-- | Endpoint name => endpoint spec, bound triggers, qualified name, initializer expression
type EndpointInfo      = (EndpointSpec, Maybe [Identifier], Identifier, Maybe (K3 Expression))
type EndpointsBQG      = [(Identifier, EndpointInfo)]

-- | Role name, default name
type DefaultEntries    = [(Identifier, Identifier)]

{-| Type alias support.
    This is a framed environment, with frames managed based on type variable scopes.
    The outer key is the alias identifier, while the inner key is the frame level.
    The datatype maintains a level counter on every push or pop of the environment frame.
-}
type TAEnv        = Map Identifier (Map Int [K3 Type])
data TypeAliasEnv = TypeAliasEnv Int TAEnv

{-| Parser environment type.
    This includes two scoped frames, one for source metadata, and another as a
    list of K3 program entry points as role-qualified sources that can be consumed.
-}
type EnvFrame     = (EndpointsBQG, DefaultEntries)
type ParserEnv    = [EnvFrame]

{-| Parsing mode, including pattern and splice modes as well as the default K3.
    Rather than maintaining distinct ASTs for patterns and splices, we embed and strip
    them out of the standard type, expression and declaration ASTs.
-}
data ParseMode = Normal | Splice | SourcePattern

{-| Parsing state.
    This includes a parse mode, a UID counter, a type alias environment
    and a parser environment as defined above.
-}
type ParserState  = (ParseMode, Int, TypeAliasEnv, ParserEnv)

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

type AnnotationCtor a = Identifier -> Annotation a
type ApplyAnnCtor   a = Identifier -> SpliceEnv -> Annotation a

{- Note: debugging helper
import Debug.Trace

myTrace :: String -> K3Parser a -> K3Parser a
myTrace s p = PP.getInput >>= (\i -> trace (s++" "++i) p)
-}

{- Language definition constants -}
k3Operators :: [[Char]]
k3Operators = [
    "+", "-", "*", "/", "%",
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
    "annotation", "lifted", "provides", "requires", "given", "type",

    {- Annotation keywords -}
    "self", "structure", "horizon", "content", "forall",

    {- Metaprogramming keyworks -}
    "label", "expr", "decl"
  ]

{- Style definitions for parsers library -}

k3Ops :: IdentifierStyle K3Parser
k3Ops = emptyOps { _styleReserved = set k3Operators }

k3Idents :: IdentifierStyle K3Parser
k3Idents = emptyIdents { _styleReserved = set k3Keywords }

k3PatternIdents :: IdentifierStyle K3Parser
k3PatternIdents = emptyIdents { _styleStart    = letter <|> char '_' <|> char '?'
                              , _styleReserved = set k3Keywords }

keyword :: String -> K3Parser ()
keyword = reserve k3Idents

operator :: String -> K3Parser ()
operator = reserve k3Ops

-- Copied from parsers to push splice concatenation inside tokenization.
nonTokenIdent :: IdentifierStyle K3Parser -> K3Parser String
nonTokenIdent s = do
  name <- highlight (_styleHighlight s)
            ((:) <$> _styleStart s <*> many (_styleLetter s) <?> _styleName s)
  when (HashSet.member name (_styleReserved s)) $ unexpected $ "reserved " ++ _styleName s ++ " " ++ show name
  return $ fromString name

identSplice :: Bool -> K3Parser String
identSplice idOnly = fmap fromString $ ctor <$> string  "#[" <*> try (nonTokenIdent k3Idents) <*> char ']'
  where ctor a b c = if idOnly then b else a ++ b ++ [c]

typeSplice :: Bool -> K3Parser String
typeSplice idOnly = fmap fromString $ ctor <$> string "::[" <*> many (noneOf "]") <*> char ']'
  where ctor a b c = if idOnly then b else a ++ b ++ [c]

exprSplice :: Bool -> K3Parser String
exprSplice idOnly = fmap fromString $ ctor <$> string  "$[" <*> many (noneOf "]") <*> char ']'
  where ctor a b c = if idOnly then b else a ++ b ++ [c]

identifier :: K3Parser String
identifier = fmap fromString $ token $ concat <$> some (choice . map try =<< parserWithPMode parts)
  where parts Normal        = return [i]
        parts SourcePattern = return [patI]
        parts _             = return [i, identSplice False, typeSplice False, exprSplice False]
        i    = nonTokenIdent k3Idents
        patI = nonTokenIdent k3PatternIdents

identParts :: K3Parser [Either String TypedSpliceVar]
identParts = token $ some (choice $ map try parts)
  where parts = [l i] ++ (map (\(p,c) -> r c (p True)) [(identSplice, STLabel), (typeSplice, STType), (exprSplice, STExpr)])
        i = nonTokenIdent k3Idents
        r v x = x >>= return . Right . (v,)
        l x = x >>= return . Left


{- Comments -}

mkComment :: Bool -> Bool -> P.SourcePos -> String -> P.SourcePos -> SyntaxAnnotation
mkComment post multi start contents end = SourceComment post multi (mkSpan start end) contents

multiComment :: Bool -> K3Parser SyntaxAnnotation
multiComment post = (mkComment post True
                     <$> PP.getPosition
                     <*> (symbol "/*" *> manyTill anyChar (try $ symbol "*/") <* spaces)
                     <*> PP.getPosition) <?> "multi-line comment"

singleComment :: Bool -> K3Parser SyntaxAnnotation
singleComment post = (mkComment post False
                      <$> PP.getPosition
                      <*> (symbol "//" *> manyTill anyChar (try newline) <* spaces)
                      <*> PP.getPosition) <?> "single line comment"

comment :: Bool -> K3Parser [SyntaxAnnotation]
comment post = many (choice [try $ multiComment post, try $ singleComment post])

-- | Helper to attach comment annotations
(//) :: (a -> SyntaxAnnotation -> a) -> [SyntaxAnnotation] -> a -> a
(//) attachF l x = foldl attachF x l


{- Parsing state accessors -}
emptyParserEnv :: ParserEnv
emptyParserEnv = []

emptyTypeAliasEnv :: TypeAliasEnv
emptyTypeAliasEnv = TypeAliasEnv 0 Map.empty

defaultParseMode :: ParseMode
defaultParseMode = Normal

emptyParserState :: ParserState
emptyParserState = (defaultParseMode, 0, emptyTypeAliasEnv, emptyParserEnv)

getTypeAliasEnv :: ParserState -> TypeAliasEnv
getTypeAliasEnv (_, _, tae, _) = tae

getParseMode :: ParserState -> ParseMode
getParseMode (pmd, _, _, _) = pmd

getParserEnv :: ParserState -> ParserEnv
getParserEnv (_, _, _, env) = env

modifyParserEnv :: (ParserEnv -> Either String (ParserEnv, a)) -> ParserState -> Either String (ParserState, a)
modifyParserEnv f (pmd,uc,ta,p) = either Left (\(np,r) -> Right ((pmd,uc,ta,np), r)) $ f p

modifyTypeAliasEnv :: (TypeAliasEnv -> Either String (TypeAliasEnv, a)) -> ParserState -> Either String (ParserState, a)
modifyTypeAliasEnv f (pmd,uc,ta,p) = either Left (\(nta,r) -> Right ((pmd,uc,nta,p),r)) $ f ta

modifyParseMode :: (ParseMode -> Either String (ParseMode, a)) -> ParserState -> Either String (ParserState, a)
modifyParseMode f (pmd,uc,ta,p) = either Left (\(npmd,r) -> Right ((npmd,uc,ta,p),r)) $ f pmd

modifyParserState :: (ParserState -> Either String (ParserState, a)) -> K3Parser a
modifyParserState f = PP.getState >>= \st -> either PP.parserFail (\(nst,r) -> PP.putState nst >> return r) $ f st

parserWithUID :: (Int -> K3Parser a) -> K3Parser a
parserWithUID f = PP.getState >>= (\(pmd,uc,ta,p) -> PP.putState (pmd,uc+1, ta, p) >> f uc)

parserWithPMode :: (ParseMode -> K3Parser a) -> K3Parser a
parserWithPMode f = PP.getState >>= f . getParseMode

parserWithTAEnv :: (TypeAliasEnv -> K3Parser a) -> K3Parser a
parserWithTAEnv f = PP.getState >>= f . getTypeAliasEnv

withUID :: (Int -> a) -> K3Parser a
withUID f = parserWithUID $ return . f

withTAEnv :: (TypeAliasEnv -> a) -> K3Parser a
withTAEnv f = parserWithTAEnv $ return . f

withPMode :: (ParseMode -> a) -> K3Parser a
withPMode f = parserWithPMode $ return . f

withEnv :: (ParserEnv -> a) -> K3Parser a
withEnv f = PP.getState >>= return . f . getParserEnv

modifyEnv :: (ParserEnv -> (ParserEnv, a)) -> K3Parser a
modifyEnv f = modifyEnvF $ Right . f

modifyEnv_ :: (ParserEnv -> ParserEnv) -> K3Parser ()
modifyEnv_ f = modifyEnv $ (,()) . f

modifyEnvF :: (ParserEnv -> Either String (ParserEnv, a)) -> K3Parser a
modifyEnvF f = modifyParserState $ modifyParserEnv f

modifyEnvF_ :: (ParserEnv -> Either String ParserEnv) -> K3Parser ()
modifyEnvF_ f = modifyEnvF $ (>>= Right . (,())) . f

modifyTAEnvF :: (TypeAliasEnv -> Either String (TypeAliasEnv, a)) -> K3Parser a
modifyTAEnvF f = modifyParserState $ modifyTypeAliasEnv f

modifyTAEnvF_ :: (TypeAliasEnv -> Either String TypeAliasEnv) -> K3Parser ()
modifyTAEnvF_ f = modifyTAEnvF $ (>>= Right . (,())) . f

modifyPModeF :: (ParseMode -> Either String (ParseMode, a)) -> K3Parser a
modifyPModeF f = modifyParserState $ modifyParseMode f

modifyPModeF_ :: (ParseMode -> Either String ParseMode) -> K3Parser ()
modifyPModeF_ f = modifyPModeF $ (>>= Right . (,())) . f


{- Note: unused
modifyUID :: (Int -> (Int,a)) -> K3Parser a
modifyUID f = PP.getState >>= (\(old, env) -> let (new, r) = f old in PP.putState (new, env) >> return r)

modifyUID_ :: (Int -> Int) -> K3Parser ()
modifyUID_ f = modifyUID $ (,()) . f
-}

nextUID :: K3Parser UID
nextUID = withUID UID

uidOver :: K3Parser (UID -> a) -> K3Parser a
uidOver parser = parserWithUID $ ap (fmap (. UID) parser) . return

parseInMode :: ParseMode -> K3Parser a -> K3Parser a
parseInMode pMode p = parserWithPMode $ \curPMode -> do
  void $ modifyPModeF_ $ const $ Right pMode
  r <- p
  void $ modifyPModeF_ $ const $ Right curPMode
  return r


{- Parser environment accessors  -}

{- Note: unused
sourceState :: EnvFrame -> EndpointsBQG
sourceState = fst

defaultEntries :: EnvFrame -> DefaultEntries
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

addFrame :: ParserEnv -> ParserEnv
addFrame env = ([],[]):env

removeFrame :: ParserEnv -> ParserEnv
removeFrame = tail

currentFrame :: ParserEnv -> EnvFrame
currentFrame = head

safePopFrame :: ParserEnv -> (EnvFrame, ParserEnv)
safePopFrame [] = (([],[]),[])
safePopFrame (h:t) = (h,t)


{- Type alias enviroment helpers -}
lookupTAliasE :: Identifier -> TypeAliasEnv -> Maybe (K3 Type)
lookupTAliasE n (TypeAliasEnv lvl env) = Map.lookup n env >>= Map.lookupLE lvl >>= tryHead . snd
  where tryHead []    = Nothing
        tryHead (h:_) = Just h

appendTAliasEAtLevel :: Identifier -> K3 Type -> Int -> Int -> TypeAliasEnv -> TypeAliasEnv
appendTAliasEAtLevel n t ilvl nlvl (TypeAliasEnv _ env) = TypeAliasEnv nlvl appendAlias
  where slvl                 = Map.singleton ilvl [t]
        appendAlias          = Map.insertWith appendLevel n slvl env
        appendLevel _ lvlEnv = Map.insertWith (++) ilvl [t] lvlEnv

appendTAliasE :: Identifier -> K3 Type -> TypeAliasEnv -> TypeAliasEnv
appendTAliasE n t tae@(TypeAliasEnv lvl _) = appendTAliasEAtLevel n t lvl lvl tae

pushTAliasE :: [(Identifier, K3 Type)] -> TypeAliasEnv -> TypeAliasEnv
pushTAliasE taBindings tae@(TypeAliasEnv lvl _) =
  foldl (\accTAE (n,t) -> appendTAliasEAtLevel n t lvl (lvl+1) accTAE) tae taBindings

popTAliasE :: TypeAliasEnv -> TypeAliasEnv
popTAliasE (TypeAliasEnv lvl env) = TypeAliasEnv (lvl-1) rebuildEnv
  where rebuildEnv = Map.foldlWithKey deleteLevel Map.empty env
        deleteLevel nEnv n lvlEnv = let nlvlEnv = Map.delete lvl lvlEnv
                                    in if Map.null nlvlEnv then nEnv else Map.insert n nlvlEnv nEnv

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

{- Top-level parsing helpers -}
stringifyError :: Either P.ParseError a -> Either String a
stringifyError = either (Left . show) Right

runK3Parser :: Maybe ParserState -> K3Parser a -> String -> Either P.ParseError a
runK3Parser Nothing   p s = P.runParser p emptyParserState "" s
runK3Parser (Just st) p s = P.runParser p st "" s

maybeParser :: K3Parser a -> String -> Maybe a
maybeParser p s = either (const Nothing) Just $ runK3Parser Nothing (head <$> endBy1 p eof) s
