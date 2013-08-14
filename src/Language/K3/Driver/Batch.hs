{-# LANGUAGE ViewPatterns #-}
module Language.K3.Driver.Batch where

import Control.Arrow

import Data.Functor

import System.IO

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Interpreter
import Language.K3.Parser
import Language.K3.Pretty
import Language.K3.Runtime.Engine (defaultAddress)

import Language.K3.Driver.Options

-- | Helper for resolving standard input.
openFileOrStdIn :: String -> IO Handle
openFileOrStdIn "-" = return stdin
openFileOrStdIn f = openFile f ReadMode

setDefaultRole :: K3 Declaration -> String -> String -> K3 Declaration
setDefaultRole (tag &&& children -> (DRole roleName, subDecls)) targetName newDefault
    | roleName == targetName = D.role roleName (newDefaultDecl : filter matchRole subDecls)
  where
    newDefaultDecl = D.global "role" T.string (Just $ E.constant $ E.CString newDefault)
    matchRole (tag -> DGlobal "role" _ _) = False
    matchRole _ = True

setDefaultRole d _ _ = d

runBatch :: Options -> IO ()
runBatch op = do
    h <- openFileOrStdIn $ input op

    p <- parseK3 <$> hGetContents h

    case p of
        Left e -> putStrLn e
        Right q -> do
            let q' = setDefaultRole q "__global" (defaultRole $ mode op)
            putStr $ pretty q'
            runProgram [defaultAddress] q'
