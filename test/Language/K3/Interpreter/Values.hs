module Language.K3.Interpreter.Values (tests) where

import Data.Map ( Map )
import qualified Data.Map as Map

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Language.K3.Core.Common

import Language.K3.Interpreter_new.Data.Types
import Language.K3.Interpreter_new.Data.Accessors
import Language.K3.Interpreter_new.Values

equatePrimitives :: [Test]
equatePrimitives = []

equateStructure :: [Test]
equateStructure = []

equateCollection :: [Test]
equateCollection = []

comparePrimitives :: [Test]
comparePrimitives = []

compareStructure :: [Test]
compareStructure = []

compareCollection :: [Test]
compareCollection = []

showPrimitives :: [Test]
showPrimitives = [
      testCase "Bool"    $ testShow (VBool False)   "VBool False"
    , testCase "Byte"    $ testShow (VByte 0)       "VByte 0"
    , testCase "Int"     $ testShow (VInt 0)        "VInt 0"
    , testCase "Real"    $ testShow (VReal 0.0)     "VReal 0.0"
    , testCase "String"  $ testShow (VString "foo") "VString \"foo\""
    , testCase "Address" $ testShow (VAddress defaultAddress) "VAddress 127.0.0.1:40000"
  ]
  where success = return ()
        failed  = assertFailure "Show value failed"
        testShow v expected = if show v == expected then success else failed 

showStructure :: [Test]
showStructure = [
      testCase "Option"       $ testShow  (VOption . Just $ VInt 0) "VOption (VInt 0)"
    , testCase "Indirection"  $ testShowM (newMVar (VInt 0) >>= return . VIndirection) "VIndirection <.*>"
    , testCase "Tuple"        $ testShow  (VTuple [VInt 0, VString "foo"]) "VTuple [VInt 0, VString \"foo\"]"
    , testCase "Record"       $ testShow  (Map.fromList [("a", VInt 0), ("b", VString "foo")]) "VRecord (fromList [(\"a\", VInt 0), (\"b\", VString \"foo\")])"
    ]
  where success = return ()
        failed  = assertFailure "Show value failed" 
        testShow  v expected = if show v == expected then success else failed
        testShowM m expected = m >>= \v -> if show v == expected then success else failed

showCollection :: [Test]
showCollection = []
  where success = return ()
        failed  = assertFailure "Show value failed" 
        testShow v expected = if show v == expected then success else failed

packPrimitives :: [Test]
packPrimitives = []

packStructure :: [Test]
packStructure = []

packCollection :: [Test]
packCollection = []

tests :: [Test]
tests = [
          testGroup "Equality" [
            testGroup "Primitives"        equatePrimitives,
            testGroup "Structured Values" equateStructure,
            testGroup "Collection"        equateCollection ],

          testGroup "Comparisons" [
            testGroup "Primitives"        comparePrimitives,
            testGroup "Structured Values" compareStructure,
            testGroup "Collection"        compareCollection ],

          testGroup "Show/Read" [
            testGroup "Primitives"        showPrimitives,
            testGroup "Structured Values" showStructure,
            testGroup "Collection"        showCollection ],

          testGroup "Pack/Unpack" [
            testGroup "Primitives"        packPrimitives,
            testGroup "Structured values" packStructure,
            testGroup "Collection"        packCollection ]
        ]
