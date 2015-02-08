import Control.Applicative
import Control.Monad
import Data.Maybe
import Data.Monoid
import System.Environment
import System.Exit
import Test.Framework
import Test.Framework.Runners.Console

import Language.K3.Test.Options
import Language.K3.Utils.Logger

import qualified Language.K3.Parser.Test as Parser
import qualified Language.K3.Runtime.Test as Runtime
import qualified Language.K3.TypeSystem.Test as TypeSystem
import qualified Language.K3.Runtime.Dataspace.Test as Dataspace

tests :: IO [Test]
tests = sequence
  [ return $ testGroup "Parser"  Parser.tests
  , return $ testGroup "Runtime" Runtime.tests
  , testGroup "Type system" <$> TypeSystem.tests Nothing
  , return $ testGroup "Dataspace" Dataspace.tests
  ]

-- |The main for the K3 unit tests.  We accept more options than the default
--  test runner, so we have to bolt into the side of test-framework and parse
--  its options.  This is accomplished by building our own getOpt options (as
--  a record structure of @Maybe@ values) and tupling it with the option
--  structure from test-framework.
main :: IO ()
main = do
  args <- getArgs
  result <- parseOptions args
  case result of
    Left (msg,exitcode) -> do
      putStrLn msg
      exitWith exitcode
    Right (tfOpts,k3tOpts) -> do
      -- ## First, process K3 tester options
      -- Logger options first
      let loggerSettings = fromJust $ loggerInstructions k3tOpts
      mconcat <$> mapM configureByInstruction loggerSettings
      -- The type system override next
      let typeSystemFilter = fromJust $ typeSystemOnlyByName k3tOpts
      -- ## Then run the test-framework main
      tests' <- case typeSystemFilter of
                  Nothing -> tests
                  Just filename -> TypeSystem.tests $ Just filename
      defaultMainWithOpts tests' tfOpts
