import Test.Framework
import Test.Framework.Runners.Console

import qualified Language.K3.Parser.Test as Parser

tests :: [Test]
tests = [
        testGroup "Parser" Parser.tests
    ]

main :: IO ()
main = defaultMain tests
