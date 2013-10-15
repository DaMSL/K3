{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Batch where

import Control.Arrow

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Runtime.Engine
import Language.K3.Interpreter

import Language.K3.Driver.Common
import Language.K3.Driver.Options

setDefaultRole :: K3 Declaration -> String -> String -> K3 Declaration
setDefaultRole (tag &&& children -> (DRole roleName, subDecls)) targetName newDefault
    | roleName == targetName = D.role roleName (newDefaultDecl : filter matchRole subDecls)
  where
    newDefaultDecl = D.global "role" T.string (Just $ E.constant $ E.CString newDefault)
    matchRole (tag -> DGlobal "role" _ _) = False
    matchRole _ = True

setDefaultRole d _ _ = d


runBatch :: Options -> InterpretOptions -> IO ()
runBatch progOpts interpOpts@(Batch asNetwork _ _) = do
    p <- parseK3Input (includes $ paths progOpts) (input progOpts)
    case p of
        Left e  -> putStrLn e
        Right q -> if not asNetwork then runProgram (sysEnv interpOpts) q >>= either (\(EngineError s) -> putStrLn s) return
                   else runNetwork (sysEnv interpOpts) q >>= mapM_ (\x -> either (\(EngineError s) -> putStrLn s) (\(addr,threadid) -> putStrLn $ "Node " ++ show addr ++ " running on thread " ++ show threadid)  x)
