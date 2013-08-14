module Language.K3.TypeSystem.Test
( tests
) where

import Control.Applicative
import Data.List
import qualified Data.Map as Map
import Data.Maybe
import System.Directory
import System.FilePath
import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit
import Text.Parsec.Error
import Text.Parsec.Prim as PPrim

import Language.K3.Core.Annotation
import Language.K3.Core.Constructor.Declaration
import Language.K3.Core.Declaration
import Language.K3.Parser
import Language.K3.TypeSystem

tests :: IO [Test]
tests =
  let files = getDirectoryContents testFilePath in
  let k3files = filter (".k3" `isSuffixOf`) <$> files in
  map (\path -> testCase path $ mkDirectSourceTest path) <$> k3files

testFilePath :: FilePath
testFilePath = "examples" </> "typeSystem"
  
-- |This function, when given the path of an example source file, will generate
--  a test to parse and typecheck it.  The parsed code is submitted directly to
--  the type system; it is not preprocessed in any way.
mkDirectSourceTest :: FilePath -> Assertion
mkDirectSourceTest path = do
  src <- readFile $ testFilePath </> path
  case parseSource path src of
    Left err -> assertFailure $ "Parse failure: " ++ show err
    Right decl ->
      case typecheck Map.empty Map.empty decl of
        Left errs -> assertFailure $ "Typechecking errors: " ++ show errs
        Right _ -> assert True

-- |Parses a top-level source file in K3 *without* processing the AST for
--  program generation and the like.
parseSource :: String -> String -> Either ParseError (K3 Declaration)
parseSource name src =
  let parser = PPrim.many declaration in
  role "__global" <$> catMaybes <$> runParser parser (0,[]) name src
