module Language.K3.Driver.Common where

import System.IO

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Parser
import Language.K3.Runtime.Common ( SystemEnvironment )
import Language.K3.Utils.Pretty
import Language.K3.Utils.Pretty.Syntax

{- Defaults -}
defaultLanguage :: String
defaultLanguage = "haskell"

defaultProgramName :: String
defaultProgramName = "A"

defaultOutputFile :: Maybe FilePath
defaultOutputFile = Just "a.out"

defaultBuildDir :: Maybe FilePath
defaultBuildDir = Just "__build"

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

parseK3Input :: [FilePath] -> FilePath -> IO (Either String (K3 Declaration))
parseK3Input searchPaths path = do
    h <- openFileOrStdIn path
    parseK3 searchPaths =<< hGetContents h

prettySysEnv :: SystemEnvironment -> [String]
prettySysEnv env = ["System environment: "] ++ concatMap prettyEnvEntry env
  where
    prettyEnvEntry (addr, bs) = prettyLines addr ++ (indent 2 $ prettyBootstrap bs)
    prettyBootstrap bs        = concatMap (prettyPair $ maxNameLength bs) bs
    prettyPair w (a,b)        = [a ++ replicate (w - length a) ' ' ++ " => " 
                                   ++ (either (const "<syntax error>") id $ literalS b)]
    maxNameLength l           = maximum $ map (length . fst) l

