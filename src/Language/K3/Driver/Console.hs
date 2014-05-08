{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Console where

import Control.Concurrent
import Control.Monad
import Control.Monad.IO.Class
import Data.List
import System.Console.Haskeline

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression

import Language.K3.Parser
import Language.K3.Runtime.Engine
import Language.K3.Interpreter
import Language.K3.Interpreter.Utils
import Language.K3.Utils.Pretty

type NetworkStatus = [Either EngineError (Address, Engine Value, ThreadId, VirtualizedMessageProcessor)]

console :: String -> NetworkStatus -> InputT IO ()
console prompt networkStatus = do
    userInput <- getInputLine prompt
    case userInput of
      Nothing      -> return ()
      Just (':':t) -> loop $ uncurry runCmd $ extractCmd t
      Just eStr    -> case parseExpression eStr of
                        Nothing -> outputStrLn $ "Invalid expression"
                        Just e  -> loop $ runExpr e

  where 
    extractCmd (words -> [])    = ("", [])
    extractCmd (words -> (h:t)) = (h, t)
    extractCmd _ = undefined

    anInterpreter :: IO (Maybe (Address, ThreadId, Engine Value, IState))
    anInterpreter = case find validInterpreter networkStatus of
      Just (Right (addr, e, tid, mp)) -> do
       prevRes <- readMVar $ snapshot mp
       return $ case lookup addr prevRes of
         Nothing      -> Nothing
         Just nodeRes -> Just (addr, tid, e, getResultState nodeRes)
      _ -> return Nothing
    
    validInterpreter (Left _)  = False
    validInterpreter (Right _) = True

    loop :: InputT IO Bool -> InputT IO ()
    loop action = action >>= \case 
      True  -> console prompt networkStatus
      False -> return ()

    runExpr :: K3 Expression -> InputT IO Bool
    runExpr e = liftIO anInterpreter >>= \case 
      Nothing -> outputStrLn "No engine found for interpretation" >> continue
      Just (addr, threadId, egn, st) ->
        do
          eStatus <- liftIO $ runInterpretation egn st $ expression e
          wrapError (outputValue threadId egn . (addr,)) eStatus
          continue

    runCmd :: String -> [String] -> InputT IO Bool
    runCmd "quit"  _ = mapM_ (wrapError (\st -> finishNode st >> waitForNode st)) networkStatus >> stop
    runCmd "nodes" _ = mapM_ (wrapError outputStatus) networkStatus >> continue
    runCmd "envs"  _ = mapM_ (wrapError outputEnv) networkStatus >> continue
    runCmd "wait"  _ = interruptibleWait $ mapM_ (wrapError waitForNode) networkStatus >> stop
    runCmd cmd args  = (outputStrLn $ "Invalid command: " ++ (intercalate " " $ cmd:args)) >> continue

    continue = return True
    stop     = return False

    wrapError statusF = either (\err -> outputStrLn $ message err) statusF
    
    nodeAction addr threadId str =
      outputStrLn $ "[" ++ show addr ++ "~" ++ show threadId ++ "] " ++ str
    
    nodeEnv egnSt =
      void $ outputStr $ boxToString $ indent 2 $ either ((:[]) . message) id $ egnSt
    
    withEngine addr threadId msg engine m f = do
      nodeAction addr threadId msg
      egnSt <- liftIO $ runEngineM m engine
      f egnSt          

    outputStatus (addr, _, threadId, _) = nodeAction addr threadId "Running"

    outputNodeResult threadId engine (addr, r) =
      withEngine addr threadId "Environment" engine (prettyIResultM r) nodeEnv

    outputValue threadId _ (addr, r) =
      nodeAction addr threadId (showResultValue $ getResultVal r)

    outputEnv (_, engine, threadId, msgProc) = do
      mpRes <- liftIO $ readMVar $ snapshot msgProc
      flip mapM_ mpRes $ outputNodeResult threadId engine

    finishNode (addr, engine, threadId, _) = do
      withEngine addr threadId "Shutting down" engine (terminateEngine True) $ wrapError return

    waitForNode (addr, engine, threadId, _) = do
      nodeAction addr threadId "Waiting for graceful completion"
      void $ liftIO $ readMVar (waitV $ control engine)
      nodeAction addr threadId "Finished."

    interruptibleWait action = handle (\Interrupt -> continue) $ withInterrupt $ action
