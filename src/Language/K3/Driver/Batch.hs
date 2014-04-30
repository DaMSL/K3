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
import Language.K3.Interpreter.Utils
import Language.K3.Utils.Pretty

import Language.K3.Driver.Common
import Language.K3.Driver.Options

type NetworkStatus = [Either EngineError (Address, Engine Value, ThreadId, VirtualizedMessageProcessor)]

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
        in runCmd cmd args >>= \case
                                      True  -> runConsole prompt networkStatus
                                      False -> return ()

  where runCmd :: String -> [String] -> InputT IO Bool
        runCmd "quit"  _ = mapM_ (wrapError (\st -> finishNode st >> waitForNode st)) networkStatus >> stop
        runCmd "nodes" _ = mapM_ (wrapError outputStatus) networkStatus >> continue
        runCmd "envs"  _ = mapM_ (wrapError outputEnv) networkStatus >> continue
        runCmd "wait"  _ = interruptibleWait $ mapM_ (wrapError waitForNode) networkStatus >> stop
        runCmd _ _       = stop

        continue = return True
        stop     = return False

        wrapError statusF = either (\err -> outputStrLn $ message err) statusF
        
        nodeAction addr str = outputStrLn $ "[" ++ show addr ++ "] " ++ str
        
        nodeEnv egnSt =
          void $ outputStr $ boxToString $ indent 2 $ either ((:[]) . message) id $ egnSt
        
        withEngine addr msg engine m f = do
          nodeAction addr msg
          egnSt <- liftIO $ runEngineM m engine
          f egnSt          

        outputStatus (addr, _, threadid, _) = nodeAction addr $ "running on thread " ++ show threadid

        outputEnv (_, engine, _, msgProc) = do
          mpRes <- liftIO $ readMVar $ snapshot msgProc
          flip mapM_ mpRes $
            \(addr, r) -> withEngine addr "env" engine (prettyIResultM r) nodeEnv

        finishNode (addr, engine, _, _) = do
          withEngine addr "Shutting down" engine (terminateEngine True) $ wrapError return

        waitForNode (addr, engine, _, _) = do
          nodeAction addr "Waiting for graceful completion"
          void $ liftIO $ readMVar (waitV $ control engine)
          nodeAction addr " finished."

        interruptibleWait action = handle (\Interrupt -> continue) $ withInterrupt $ action
