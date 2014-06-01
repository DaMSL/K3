{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Batch where

import Control.Arrow
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Control.Monad
import System.Console.Haskeline

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration

import qualified Language.K3.Core.Constructor.Declaration as D
import qualified Language.K3.Core.Constructor.Expression as E
import qualified Language.K3.Core.Constructor.Type as T

import Language.K3.Runtime.Engine
import Language.K3.Interpreter

import Language.K3.Driver.Common
import Language.K3.Driver.Console
import Language.K3.Driver.Options


setDefaultRole :: K3 Declaration -> String -> String -> K3 Declaration
setDefaultRole (tag &&& children -> (DRole roleName, subDecls)) targetName newDefault
    | roleName == targetName = D.role roleName (newDefaultDecl : filter matchRole subDecls)
  where
    newDefaultDecl = D.global "role" T.string (Just $ E.constant $ E.CString newDefault)
    matchRole (tag -> DGlobal "role" _ _) = False
    matchRole _ = True

setDefaultRole d _ _ = d

runBatch :: Options -> InterpretOptions -> K3 Declaration -> IO ()
runBatch _ iOpts prog =
   if not (network iOpts) then prepareAndRun prepareProgram runProgram (map Right)
                    else prepareAndRun prepareNetwork networkRunF id
  where 
    prepareAndRun prepareF runF statusF = do
      prepared <- prepareF (printConfig iOpts) (isPar iOpts) (sysEnv iOpts) prog
      either engineError (\p -> loopWithConsole $ runOnce runF statusF p) prepared

    networkRunF pc pn = runNetwork pc pn >>= waitF >>= return . Right

    waitF = if (noConsole iOpts) then return . id else mapM (printError printNode)

    loopWithConsole rcrF = runInputT defaultSettings $ rcrF cEngineError (runConsole rcrF)

    runOnce runF statusF prepared errorF continueF = do
      runStatus <- liftIO $ runF (printConfig iOpts) prepared
      either errorF (continueF . statusF) runStatus

    runConsole rcrF ns = if (noConsole iOpts) then console defaultPrompt rcrF ns else return ()
    engineError        = putStrLn . message
    cEngineError       = outputStrLn . message

    printNode (addr, engine, threadid,_) = do
      void $ putStrLn $ "Waiting for node " ++ show addr ++ " running on thread " ++ show threadid
      readMVar (waitV $ control engine)
      void $ putStrLn $ "Node " ++ show addr ++ " finished."

    -- printError returns the provided pStatus
    printError validF pStatus = either (\err -> (putStrLn $ message err) >> return pStatus) (\arg -> validF arg >> return pStatus) pStatus

runBatch _ _ _ = error "Invalid batch processing mode"
