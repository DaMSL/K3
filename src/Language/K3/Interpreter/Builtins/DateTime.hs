{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

module Language.K3.Interpreter.Builtins.DateTime where

import Control.Monad.State

import System.Locale

import Data.Time

import qualified Data.Map as Map
import qualified System.Clock as Clock

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

genDateTimeBuiltin :: Identifier -> K3 Type -> Maybe (Interpretation Value)

-- Helper --

-- Parse a date in SQL format
parseSQLDate :: String -> Maybe Int
parseSQLDate s = toInt $ foldr readChar [""] s
  where readChar _   []   = []
        readChar '-' xs   = "":xs
        readChar n (x:xs) = (n:x):xs
        toInt [y,m,d]     = Just $ (read y)*10000 + (read m)*100 + (read d)
        toInt _           = Nothing

-- Time operation on record maps
timeBinOp :: NamedMembers Value -> NamedMembers Value -> (Int -> Int -> Int) -> Interpretation (NamedMembers Value)
timeBinOp map1 map2 op = do
  sec1 <- secF map1
  sec2 <- secF map2
  nsec1 <- nsecF map1
  nsec2 <- nsecF map2
  let sec = sec1 `op` sec2
      nsec = nsec1 `op` nsec2
      (sec', nsec') = normalize sec nsec
  return $ Map.fromList [("sec", (VInt sec', MemImmut)), ("nsec", (VInt nsec', MemImmut))]
  where
      get' nm m =
        case fst $ Map.findWithDefault (VInt 0, MemImmut) nm m of
          VInt i -> return i
          x      -> throwE $ RunTimeTypeError $ "Expected Int but found "++show x
      secF m  = get' "sec" m
      nsecF m = get' "nsec" m
      normalize s ns =
        let mega = 1000000 in
        if ns > mega || ns < -mega then
          let (q, r) = ns `quotRem` mega
          in  (s + q, r)
        else  (s, ns)

------------------------------------------------------------------------------

-- TIME --

-- getUTCTime :: {h : int, m : int, s : int}
genDateTimeBuiltin "getUTCTime" _ = Just $ vfun $ \_ -> do
    currentTime <- liftIO $ getCurrentTime
    dayTime <- return $ utctDayTime currentTime
    timeOfDay <- return $ timeToTimeOfDay dayTime
    return $ VRecord $ Map.fromList $
      [("h", (VInt $ todHour timeOfDay, MemImmut)),
       ("m", (VInt $ todMin timeOfDay, MemImmut)),
       ("s", (VInt $ truncate $ todSec timeOfDay, MemImmut))]

-- getUTCTimeWithFormat :: string -> string
genDateTimeBuiltin "getUTCTimeWithFormat" _ = Just $ vfun $ \(VString formatString) -> do
    currentTime <- liftIO $ getCurrentTime
    if formatString == ""
        then return $ VString $ formatTime defaultTimeLocale "%H:%M:%S" currentTime
        else return $ VString $ formatTime defaultTimeLocale formatString currentTime

-- getLocalTime :: {h : int, m : int, s : int}
genDateTimeBuiltin "getLocalTime" _ = Just $ vfun $ \_ -> do
    zonedTime <- liftIO $ getZonedTime
    localTime <- return $ zonedTimeToLocalTime zonedTime
    timeOfDay <- return $ localTimeOfDay localTime
    return $ VRecord $ Map.fromList $
      [("h", (VInt $ todHour timeOfDay, MemImmut)),
       ("m", (VInt $ todMin timeOfDay, MemImmut)),
       ("s", (VInt $ truncate $ todSec timeOfDay, MemImmut))]

-- getLocalTimeWithFormat :: string -> string
genDateTimeBuiltin "getLocalTimeWithFormat" _ = Just $ vfun $ \(VString formatString) -> do
    zonedTime <- liftIO $ getZonedTime
    if formatString == ""
        then return $ VString $ formatTime defaultTimeLocale "%H:%M:%S" zonedTime
        else return $ VString $ formatTime defaultTimeLocale formatString zonedTime

-- DATE --

-- getUTCDate :: () -> {y : int, m : int, d : int}
genDateTimeBuiltin "getUTCDate" _ = Just $ vfun $ \_ -> do
    currentTime <- liftIO $ getCurrentTime
    (y,m,d) <- return $ toGregorian $ utctDay currentTime
    return $ VRecord $ Map.fromList $
      [("y", (VInt $ fromInteger y, MemImmut)),
       ("m", (VInt $ m, MemImmut)),
       ("d", (VInt $ d, MemImmut))]

-- getLocalDate :: () -> {y : int, m : int, d : int}
genDateTimeBuiltin "getLocalDate" _ = Just $ vfun $ \_ -> do
    zonedTime <- liftIO $ getZonedTime
    (y, m, d) <- return $ toGregorian $ localDay $ zonedTimeToLocalTime zonedTime
    return $ VRecord $ Map.fromList $
      [("y", (VInt $ fromInteger y, MemImmut)),
       ("m", (VInt $ m, MemImmut)),
       ("d", (VInt $ d, MemImmut))]

-- parseDate :: string -> string -> option {y : int, m : int, d : int}
genDateTimeBuiltin "parseDate" _ = Just $ vfun $ \(VString formatString) -> vfun $ \(VString dateString) -> do
    let day = parseTime defaultTimeLocale formatString dateString :: Maybe Day in
        case day of
            Nothing -> return $ VOption (Nothing, MemImmut)
            Just someDay ->
                let (y, m, d) = toGregorian someDay
                in let record = VRecord $ Map.fromList $
                        [("y", (VInt $ fromInteger y, MemImmut)),
                        ("m", (VInt $ m, MemImmut)),
                        ("d", (VInt $ d, MemImmut))]
                   in return $ VOption (Just record, MemImmut)

-- Parse an SQL date string and convert to integer
genDateTimeBuiltin "parse_sql_date" _ = Just $ vfun $ \(VString s) -> do
  let v = parseSQLDate s
  case v of
    Just i  -> return $ VInt i
    Nothing -> throwE $ RunTimeInterpretationError "Bad date format"

genDateTimeBuiltin "now" _ = Just $ vfun $ \(VTuple []) -> do
  v <- liftIO $ Clock.getTime Clock.Realtime
  return $ VRecord $ Map.fromList $
    [("sec", (VInt $ Clock.sec v, MemImmut)),
     ("nsec", (VInt $ Clock.nsec v, MemImmut))]

genDateTimeBuiltin "add_time" _ = Just $ vfun $ \(VRecord map1) -> vfun $ \(VRecord map2) ->
  timeBinOp map1 map2 (+) >>= return . VRecord

genDateTimeBuiltin "sub_time" _ = Just $ vfun $ \(VRecord map1) -> vfun $ \(VRecord map2) ->
  timeBinOp map1 map2 (-) >>= return . VRecord

genDateTimeBuiltin _ _ = Nothing
