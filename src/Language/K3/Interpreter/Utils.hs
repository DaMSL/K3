{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Interpreter.Utils where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad.State

import Data.Function
import Data.List
import qualified Data.HashTable.IO as HT 

import Language.K3.Core.Common
import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Runtime.Engine
import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["Dispatch", "BindPath"])


{- Pretty printing -}

-- Print a value, obeying the instructions in the printconfig structure
valuePrint :: PrintConfig -> Value -> String
valuePrint pc (VCollection (_, c)) = "VCollection (Collection {" ++ 
    if printNamespace pc then "namespace = " ++ (show $ namespace c) ++ ", " else "" ++
    if printDataspace pc then "dataspace = " ++ (show $ dataspace c) ++ ", " else "" ++
    if printRealizationId pc then "realizationId = " ++ (show $ realizationId $ c) ++ ", " else "" ++
    "})"
valuePrint pc v@(VFunction _) = if printFunctions pc then pretty v else ""
valuePrint _ v = pretty v

prettyIEnvEntry :: PrintConfig -> IEnvEntry Value -> EngineM Value String
prettyIEnvEntry pc (IVal v)  = return $ prettyPC pc v
prettyIEnvEntry pc (MVal mv) = liftIO (readMVar mv) >>= return . prettyPC pc

prettyIEnvM :: PrintConfig -> IEnvironment Value -> EngineM Value [String]
prettyIEnvM pc env = do
    env_list <- liftIO $ HT.toList env
    let nWidth = maximum $ map (length . fst) env_list
        sorted_env = sortBy (compare `on` fst) env_list
    bindings <- mapM (prettyEnvEntries nWidth) sorted_env
    return $ concat bindings 
  where 
    prettyEnvEntries w (n, eel) = do
      sl <- foldM checkAndPretty [] eel
      return . concat $ map (shift (prettyName w n) (prefixPadTo (w+4) " .. ") . wrap 70) sl

    prettyName w n    = (suffixPadTo w n) ++ " => "
    suffixPadTo len n = n ++ replicate (max (len - length n) 0) ' '
    prefixPadTo len n = replicate (max (len - length n) 0) ' ' ++ n
    -- If we have an empty string, drop this value
    checkAndPretty acc e = do
      str <- prettyIEnvEntry pc e
      case str of
        "" -> return acc
        _  -> return $ acc++[str]

prettyIStateM :: IState -> EngineM Value [String]
prettyIStateM st = do
  let pc = getPrintConfig st
  envLines <- prettyIEnvM pc $ getEnv st
  return $ if printEnv pc then ["Environment:"]         ++ (indent 2 $ envLines) else []
        ++ if printAnnotations pc then ["Annotations:"] ++ (indent 2 $ lines $ show $ getAnnotEnv st) else []
        ++ if printStaticEnv pc then ["Static:"]        ++ (indent 2 $ lines $ show $ getStaticEnv st) else []
        ++ if printProxyStack pc then ["Proxy stack:"]  ++ (indent 2 $ lines $ show $ getProxyStack st) else []
        ++ if printTracer pc then ["Tracing: "]         ++ (indent 2 $ lines $ show $ getTracer st) else []

prettyIResultM :: IResult Value -> EngineM Value [String]
prettyIResultM ((res, st), _) =
  return . ([showResultValue res] ++) =<< prettyIStateM st 


{- Additional show methods -}

showResultValue :: Either InterpretationError Value -> String
showResultValue (Left err)  = "Error: " ++ show err
showResultValue (Right val) = "Value: " ++ show val

showTag :: String -> [String] -> [String]
showTag str l = [str ++ " { "] ++ (indent 2 l) ++ ["}"]

showIEnvTagM :: String -> IEnvironment Value -> EngineM Value [String]
showIEnvTagM str env = prettyIEnvM defaultPrintConfig env >>= return . showTag str

showIStateTagM :: String -> IState -> EngineM Value [String]
showIStateTagM str st = prettyIStateM st >>= return . showTag str

showIResultTagM :: String -> IResult Value -> EngineM Value [String]
showIResultTagM str r = prettyIResultM r >>= return . showTag str

showIEnvM :: IEnvironment Value -> EngineM Value String
showIEnvM e = prettyIEnvM defaultPrintConfig e >>= return . boxToString

showIStateM :: IState -> EngineM Value String
showIStateM s = prettyIStateM s >>= return . boxToString

showIResultM :: IResult Value -> EngineM Value String
showIResultM r = prettyIResultM r >>= return . boxToString

showDispatchM :: Address -> Identifier -> Value -> IState -> Maybe (Either InterpretationError Value) -> String -> EngineM Value [String]
showDispatchM addr name args st m_res str = 
  -- If we have a result, show that. Otherwise show state
  case m_res of
    Just v  -> wrap' (showPC (getPrintConfig st) args) <$> (showIResultTagM str ((v,st),[])) >>= return . indent 2
    Nothing -> wrap' (showPC (getPrintConfig st) args) <$> (showIStateTagM  str st) >>= return . indent 2
  where
    wrap' arg res =  ["", "TRIGGER " ++ name ++ " " ++ show addr ++ " { "]
                  ++ ["  Args: " ++ arg] 
                  ++ res ++ ["}"]

{- Debugging helpers -}

debugDecl :: (Show a, Pretty b) => a -> b -> c -> c
debugDecl n t = _debugI . boxToString $
  [concat ["Declaring ", show n, " : "]] ++ (indent 2 $ prettyLines t)

logIStateM :: String -> Maybe Address -> IState -> EngineM Value ()
logIStateM tag' addr st = do
    msg <- showIStateTagM (tag' ++ (maybe "" show $ addr)) st
    void $ _notice_Dispatch $ boxToString msg

logIResultM :: String -> Maybe Address -> IResult Value -> EngineM Value ()
logIResultM tag' addr r = do
    msg <- showIResultTagM (tag' ++ (maybe "" show $ addr)) r 
    void $ _notice_Dispatch $ boxToString msg

logTriggerM :: Address -> Identifier -> Value -> IState -> Maybe (Either InterpretationError Value) -> String -> EngineM Value ()
logTriggerM addr n args st m_res str = do
    msg   <- showDispatchM addr n args st m_res str
    void $ _notice_Dispatch $ boxToString msg

logIStateMI :: Interpretation ()
logIStateMI = get >>= liftEngine . showIStateM >>= liftIO . putStrLn 

logProxyPathI :: Interpretation ()
logProxyPathI = getProxyPath >>= void . _notice_BindPath . ("BIND PATH: "++) . show

