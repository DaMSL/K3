{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Language.K3.Parser.Test (tests) where

import Control.Monad

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Text.Parsec (runParser)
import Text.Parsec.Error (ParseError, errorPos, errorMessages)

import Language.K3.Core.Annotation
import Language.K3.Core.Expression

import Language.K3.Pretty

import qualified Language.K3.Core.Constructor.Expression as E

import Language.K3.Parser

instance Eq ParseError where
    p == q = errorPos p == errorPos q && errorMessages p == errorMessages q

(<%>) :: K3Parser a -> String -> Either ParseError a
p <%> s = runParser p (0,[]) "" s
  -- TODO: provide a correct ParserEnvironment (instead of just [])

-- | Apply a function until the argument stops changing.
fixMap :: Eq a => (a -> a) -> a -> a
fixMap f x = if f x == x then x else fixMap f (f x)

a @~- f = case a @~ f of
    Just t -> a @- t
    Nothing -> a

-- | A pretty assertEqual for expression trees.
(@!=?) :: K3 Expression -> Either ParseError (K3 Expression) -> Assertion
(@!=?) x (Right y) = unless (x' == y') (assertFailure msg)
  where
    x' = fmap (fixMap (@~- \case ESpan _ -> True ; otherwise -> False)) x
    y' = fmap (@<- []) y
    msg = "\nexpected: \n" ++ pretty x' ++ "\nbut got:\n" ++ pretty y'
(@!=?) x (Left y) = assertFailure ("Got: " ++ show y)

infix 3 @!=?

case_addition :: Assertion
case_addition = (E.binop OAdd (E.constant (CInt 1)) (E.constant (CInt 1))) @!=? (expr <%> "1 + 1")

case_subtraction :: Assertion
case_subtraction = (E.binop OSub (E.constant (CInt 1)) (E.constant (CInt 1))) @!=? (expr <%> "1 - 1")

case_multiplication :: Assertion
case_multiplication = (E.binop OMul (E.constant (CInt 1)) (E.constant (CInt 1))) @!=? (expr <%> "1 * 1")

case_division :: Assertion
case_division = (E.binop ODiv (E.constant (CInt 1)) (E.constant (CInt 1))) @!=? (expr <%> "1 / 1")

case_precedence :: Assertion
case_precedence = E.binop OAdd (E.constant $ CInt 1) (E.binop OMul (E.constant $ CInt 2) (E.constant $ CInt 3))
    @!=? expr <%> "1 + 2 * 3"

case_parenthetical :: Assertion
case_parenthetical =
        E.binop OMul (
            E.binop OAdd (E.constant $ CInt 1) (E.constant $ CInt 2)
        ) (
            E.constant $ CInt 3
        )
   @!=? expr <%> "(1 + 2) * 3"

tests :: [Test]
tests = [
        testGroup "Arithmetic" [
                testCase "Addition" case_addition,
                testCase "Subtraction" case_subtraction,
                testCase "Multiplication" case_multiplication,
                testCase "Division" case_division,
                testCase "Precedence/1" case_precedence,
                testCase "Parenthetical" case_parenthetical
            ]
    ]
