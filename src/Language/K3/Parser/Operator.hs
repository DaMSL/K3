{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoMonoLocalBinds #-}

module Language.K3.Parser.Operator where

import Control.Applicative
import Text.Parser.Combinators
import Text.Parser.Expression hiding ( buildExpressionParser )

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import qualified Language.K3.Core.Constructor.Expression  as EC

import Language.K3.Parser.DataTypes

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

mkUnOp :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkUnOp x = unaryParseOp x operator

mkUnOpK :: (String, K3Operator) -> ParserOperator (K3 Expression)
mkUnOpK x = unaryParseOp x keyword

nonSeqOpTable :: OperatorTable K3Parser (K3 Expression)
nonSeqOpTable =
  [   map mkUnOp   [("-",   ONeg)],
      map mkBinOp  [("*",   OMul), ("/",  ODiv), ("%",  OMod)],
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
              -- ambiguous :: Parsing m => forall c d. String -> m c -> m d
              ambiguous assoc op = try $ op *> empty <?> ("ambiguous use of a " ++ assoc ++ "-associative operator")

              -- ambiguousRight :: forall a. m a
              ambiguousRight    = ambiguous "right" rassocOp
              -- ambiguousLeft :: forall a. m a
              ambiguousLeft     = ambiguous "left" lassocOp
              -- ambiguousNon :: forall a. m a
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
