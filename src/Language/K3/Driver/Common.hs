module Language.K3.Driver.Common where

import Data.Functor

import System.IO

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import Language.K3.Runtime.Engine ( SystemEnvironment )

import Language.K3.Parser
import Language.K3.Pretty
import Language.K3.Pretty.Syntax

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

parseK3Input :: FilePath -> IO (Either String (K3 Declaration))
parseK3Input path = do
    h <- openFileOrStdIn path
    parseK3 <$> hGetContents h

prettySysEnv :: SystemEnvironment -> [String]
prettySysEnv env = ["System environment: "] ++ concatMap prettyEnvEntry env
  where
    prettyEnvEntry (addr, bs) = prettyLines addr ++ (indent 2 $ prettyBootstrap bs)
    prettyBootstrap bs        = concatMap (prettyPair $ maxNameLength bs) bs
    prettyPair w (a,b)        = [a ++ replicate (w - length a) ' ' ++ " => " 
                                   ++ (either (const "<syntax error>") id $ exprS b)]
    maxNameLength l           = maximum $ map (length . fst) l

