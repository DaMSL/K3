{-# LANGUAGE ViewPatterns #-}

module Language.K3.Parser.Preprocessor (
    processIncludes,
    preprocess
  ) where

import Control.Applicative
import Control.Monad

import Data.Char
import Data.Functor.Identity
import qualified Data.HashSet as HashSet
import Data.List

import qualified Text.Parsec          as P
import qualified Text.Parsec.Prim     as PP

import Text.Parser.Combinators
import Text.Parser.Token
import Text.Parser.Token.Style

import System.FilePath
import qualified System.FilePath.Find as SF

type Preprocessor = PP.ParsecT String () Identity

runPreprocessor :: Preprocessor a -> String -> Either P.ParseError a
runPreprocessor p s = P.runParser p () "" s

{- Language definitions -}
k3PPKeywords :: [[Char]]
k3PPKeywords = [
    "include"
  ]

k3PPIdents :: TokenParsing m => IdentifierStyle m
k3PPIdents = emptyIdents { _styleReserved = HashSet.fromList k3PPKeywords }

keyword :: (TokenParsing m, Monad m) => String -> m ()
keyword = reserve k3PPIdents

{- Preprocessor for include statements -}
includeParser :: String -> Either String (Maybe FilePath)
includeParser s = either (Left . show) Right $ runPreprocessor parser s
  where parser  = option Nothing $ try (keyword "include" >> Just . deQuote <$> stringLiteral)
        deQuote = dropWhile isPunctuation . dropWhileEnd isPunctuation

processIncludes :: [FilePath] -> [String] -> [FilePath] -> IO [FilePath]
processIncludes searchPaths prog excludes = do
  let includeFile s = either includeError id $ includeParser s
  let includes      = foldl (\acc s -> maybe acc (\p -> acc++[p]) $ includeFile s) [] $ prog
  newIncludes <- foldM (recurAndCombine excludes) [] $ includes \\ excludes
  return $ nub $ newIncludes ++ excludes

  where includeError = error "Invalid include statement"
        recurAndCombine td acc p = preprocess searchPaths p (acc ++ td) >>= return . (acc ++)

preprocess :: [FilePath] -> FilePath -> [FilePath] -> IO [FilePath]
preprocess searchPaths (normalise -> path) excludes = searchForPath >>= \actualPath ->
  if actualPath `elem` excludes
    then return []
    else do
      contents    <- readFile actualPath
      subIncludes <- processIncludes searchPaths (lines contents) ([actualPath, path] ++ excludes)
      return $ if actualPath == path then subIncludes else filter (path /=) subIncludes

  where matchPath p = SF.find SF.always matchClause p
        matchClause = SF.fileName SF.==? (snd $ splitFileName path) SF.&&? isInDirectory
        isInDirectory = SF.directory `hasSuffix` (dropTrailingPathSeparator . fst $ splitFileName path)
        hasSuffix = SF.liftOp $ flip isSuffixOf
        preprocessError msg = error $ "Preprocessing failed: " ++ msg ++ "\nSearch paths: " ++ unlines searchPaths

        searchForPath = do
          matches <- mapM matchPath searchPaths >>= return . concat  
          case matches of
            [x] -> return x
            []  -> preprocessError $ "no matching file found for " ++ path
            l   -> preprocessError $ "multiple matches found for " ++ path ++ ": " ++ unlines l
