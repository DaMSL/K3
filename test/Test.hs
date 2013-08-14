import Control.Applicative
import Control.Monad
import Test.Framework
import Test.Framework.Runners.Console

import qualified Language.K3.Parser.Test as Parser
import qualified Language.K3.Runtime.Test as Runtime
import qualified Language.K3.TypeSystem.Test as TypeSystem

tests :: IO [Test]
tests = sequence
  [ return $ testGroup "Parser"  Parser.tests
  , return $ testGroup "Runtime" Runtime.tests
  , testGroup "Type system" <$> TypeSystem.tests
  ]

main :: IO ()
main = defaultMain =<< tests
