import Test.Framework
import Test.Framework.Runners.Console

import qualified Language.K3.Parser.Test as Parser

import qualified Language.K3.Runtime.Test as Runtime

tests :: [Test]
tests = [
      testGroup "Parser"  Parser.tests
    , testGroup "Runtime" Runtime.tests
  ]

main :: IO ()
main = defaultMain tests
