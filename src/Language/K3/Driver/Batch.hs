{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Batch where

import Control.Arrow
import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class

import Data.List.Split
import System.Console.Haskeline

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Runtime.Engine
import Language.K3.Interpreter

import Language.K3.Driver.Common
import Language.K3.Driver.Options

type NetworkStatus = [Either EngineError (Address, Engine Value, ThreadId)]

setDefaultRole :: K3 Declaration -> String -> String -> K3 Declaration
setDefaultRole (tag &&& children -> (DRole roleName, subDecls)) targetName newDefault
    | roleName == targetName = D.role roleName (newDefaultDecl : filter matchRole subDecls)
  where
    newDefaultDecl = D.global "role" T.string (Just $ E.constant $ E.CString newDefault)
    matchRole (tag -> DGlobal "role" _ _) = False
    matchRole _ = True

setDefaultRole d _ _ = d


runBatch :: Options -> InterpretOptions -> (K3 Declaration -> K3 Declaration) -> IO ()
runBatch progOpts interpOpts@(Batch asNetwork _ _ parallel printConf) addPreloadVals = do
    p <- parseK3Input (includes $ paths progOpts) (input progOpts)
    case p of
        Left e  -> putStrLn e
        Right q -> let q' = addPreloadVals q in
                   if not asNetwork then do
                      pStatus <- runProgram printConf parallel (sysEnv interpOpts) q'
                      void $ either (\err -> putStrLn $ message err) return pStatus
                   else do
                      nodeStatuses <- runNetwork printConf parallel (sysEnv interpOpts) q'
                      runInputT defaultSettings $ runConsole defaultPrompt nodeStatuses

runBatch _ _ _ = error "Invalid batch processing mode"

runConsole :: String -> NetworkStatus -> InputT IO ()
runConsole prompt networkStatus = do
    userInput <- getInputLine prompt
    case userInput of
      Nothing -> return ()
      Just s  -> 
        let (cmd,args) = case splitOneOf " \t" s of
                          []  -> ("", [])
                          h:t -> (h,t)
        in runCommand cmd args >>= \case
                                      True  -> runConsole prompt networkStatus
                                      False -> return ()

  where runCommand :: String -> [String] -> InputT IO Bool
        runCommand "quit"  _ = return False
        runCommand "nodes" _ = mapM_ (wrapError outputStatus) networkStatus >> return True
        runCommand "wait"  _ = mapM_ (wrapError waitForNodes) networkStatus >> return False
        runCommand _ _       = return False

        wrapError statusF = either (\err -> outputStrLn $ message err) statusF
        
        outputStatus (addr, _, threadid) =
          outputStrLn $ "Node " ++ show addr ++ " running on thread " ++ show threadid

        waitForNodes (addr, engine, _) = do
          void $ outputStrLn $ "Waiting for node " ++ show addr
          void $ liftIO $ readMVar (waitV $ control engine)
          void $ outputStrLn $ "Node " ++ show addr ++ " finished."

