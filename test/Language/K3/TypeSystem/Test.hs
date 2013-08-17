module Language.K3.TypeSystem.Test
( tests
) where

import Control.Applicative
import Control.Monad
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

import Debug.Trace

tests :: IO [Test]
tests =
  concat <$> sequence
    [ mkTests "Typecheck" True "success"
    , mkTests "Type fail" True "failure"
    ]
  where
    mkTests :: String -> Bool -> FilePath -> IO [Test]
    mkTests name success subdir =
      let prefix = testFilePath </> subdir in
      let files = filter (".k3" `isSuffixOf`) <$> getDirectoryContents prefix in
      sequence    
        [
          testGroup name <$>
            map (\path -> testCase path $ mkDirectSourceTest path success) <$>
            map (prefix </>) <$> files
        ]

testFilePath :: FilePath
testFilePath = "examples" </> "typeSystem"
  
-- |This function, when given the path of an example source file, will generate
--  a test to parse and typecheck it.  The parsed code is submitted directly to
--  the type system; it is not preprocessed in any way.
mkDirectSourceTest :: FilePath -> Bool -> Assertion
mkDirectSourceTest path success = do
  src <- readFile path
  case parseSource path src of
    Left err -> assertFailure $ "Parse failure: " ++ show err
    Right decl ->
      case (typecheck Map.empty Map.empty decl, success) of
        (Left errs, True) ->
          assertFailure $ "Typechecking errors: " ++ show errs
        (Right _, True) -> assert True
        (Left _, False) -> assert True
        (Right _, False) -> assert "Incorrectly typechecked!"

-- |Parses a top-level source file in K3 *without* processing the AST for
--  program generation and the like.
parseSource :: String -> String -> Either ParseError (K3 Declaration)
parseSource name src =
  let parser = join $ mapM ensureUIDs <$>
                  catMaybes <$> PPrim.many declaration in
  let tree = role "__global" <$> runParser parser (0,[]) name src in
  trace (show tree) tree
