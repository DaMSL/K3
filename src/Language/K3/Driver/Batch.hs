{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Batch where

import Control.Arrow
import Control.Concurrent.MVar
import Control.Monad

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


runBatch :: Options -> InterpretOptions -> (K3 Declaration -> K3 Declaration) -> IO ()
runBatch progOpts interpOpts@(Batch asNetwork _ _ parallel) addPreloadVals = do
    p <- parseK3Input (includes $ paths progOpts) (input progOpts)
    case p of
        Left e  -> putStrLn e
        Right q -> let q' = addPreloadVals q in
                   if not asNetwork then do
                      pStatus <- runProgram parallel (sysEnv interpOpts) q'
                      void $ printError return pStatus
                   else do
                      nodeStatuses <- runNetwork parallel (sysEnv interpOpts) q'
                      void $ mapM_ (printError printNode) nodeStatuses

  where printNode (addr, engine, threadid) = do
          void $ putStrLn $ "Waiting for node " ++ show addr ++ " running on thread " ++ show threadid
          readMVar (waitV $ control engine)
          void $ putStrLn $ "Node " ++ show addr ++ " finished."

        printError validF pStatus = either (\err -> putStrLn $ message err) validF pStatus

runBatch _ _ _ = error "Invalid batch processing mode"
