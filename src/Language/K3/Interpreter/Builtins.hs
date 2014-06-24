{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TemplateHaskell #-}

-- | K3 builtin and standard library function interpretation
module Language.K3.Interpreter.Builtins where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar

import Control.Monad.State

import System.Locale

import Data.List
import Data.Time

import qualified Data.Map as Map
import qualified System.Random as Random
import qualified System.Clock as Clock

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values
import Language.K3.Interpreter.Dataspace()
import Language.K3.Interpreter.Collection

import Language.K3.Runtime.Engine
import Language.K3.Runtime.Dataspace

-- For the log function
import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["Function"])

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

-- Compare operation on record maps [for date]
compareDateOp :: NamedMembers Value -> NamedMembers Value -> Interpretation Int
compareDateOp date1 date2 = do
    y1 <- gety date1
    y2 <- gety date2
    m1 <- getm date1
    m2 <- getm date2
    d1 <- getd date1
    d2 <- getd date2
    if y1 /= y2
        then
            if y1 > y2
                then return $ 1
                else return $ -1
        else
            if m1 /= m2
                then
                    if m1 > m2
                        then return $ 1
                        else return $ -1
            else
                if d1 /= d2
                    then
                        if d1 > d2
                            then return $ 1
                            else return $ -1
                        else return $ 0
    where
        get' elm rec =
            case fst $ Map.findWithDefault (VInt 0, MemImmut) elm rec of
                VInt i -> return i
                x      -> throwE $ RunTimeTypeError $ "Expected Int but found " ++ show x
        gety date = get' "y" date
        getm date = get' "m" date
        getd date = get' "d" date

{- Built-in functions -}

builtin :: Identifier -> K3 Type -> Interpretation ()
builtin n t = genBuiltin n t >>= insertE n . IVal

genBuiltin :: Identifier -> K3 Type -> Interpretation Value

-- TODO: error handling on all open/close/read/write methods.
-- TODO: argument for initial endpoint bindings for open method as a list of triggers
-- TODO: correct element type (rather than function type sig) for openFile / openSocket

-- type ChannelId = String

-- openBuiltin :: ChannelId -> String -> ()
genBuiltin "openBuiltin" _ =
  vfun $ \(VString cid) ->
    vfun $ \(VString builtinId) ->
      vfun $ \(VString format) ->
        do
          sEnv <- get >>= return . getStaticEnv
          let wd = wireDesc sEnv format
          void $ liftEngine (openBuiltin cid builtinId wd)
          return vunit

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  vfun $ \(VString cid) ->
    vfun $ \(VString path) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openFile cid path wd (Just t) mode)
            return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  vfun $ \(VString cid) ->
    vfun $ \(VAddress addr) ->
      vfun $ \(VString format) ->
        vfun $ \(VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openSocket cid addr wd (Just t) mode)
            return vunit


-- close :: ChannelId -> ()
genBuiltin "close" _ = vfun $ \(VString cid) -> liftEngine (close cid) >> return vunit

-- TODO: deregister methods
-- register*Trigger :: ChannelId -> TTrigger () -> ()
genBuiltin "registerFileDataTrigger"     _ = registerNotifier "data"
genBuiltin "registerFileCloseTrigger"    _ = registerNotifier "close"

genBuiltin "registerSocketAcceptTrigger" _ = registerNotifier "accept"
genBuiltin "registerSocketDataTrigger"   _ = registerNotifier "data"
genBuiltin "registerSocketCloseTrigger"  _ = registerNotifier "close"

-- <source>HasRead :: () -> Bool
genBuiltin (channelMethod -> ("HasRead", Just n)) _ = vfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasRead n) >>= maybe invalid (return . VBool)
        invalid = throwE $ RunTimeInterpretationError $ "Invalid source \"" ++ n ++ "\""

-- <source>Read :: () -> t
genBuiltin (channelMethod -> ("Read", Just n)) _ = vfun $ \_ -> liftEngine (doRead n) >>= throwOnError
  where throwOnError (Just v) = return v
        throwOnError Nothing =
          throwE $ RunTimeInterpretationError $ "Invalid next value from source \"" ++ n ++ "\""

-- <sink>HasWrite :: () -> Bool
genBuiltin (channelMethod -> ("HasWrite", Just n)) _ = vfun $ \_ -> checkChannel
  where checkChannel = liftEngine (hasWrite n) >>= maybe invalid (return . VBool)
        invalid      = throwE $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) _ =
  vfun $ \arg -> liftEngine (doWrite n arg) >> return vunit

-- random :: int -> int
genBuiltin "random" _ =
  vfun $ \x -> case x of
    VInt upper -> liftIO (Random.randomRIO (0::Int, upper)) >>= return . VInt
    _          -> throwE $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- randomFraction :: () -> real
genBuiltin "randomFraction" _ = vfun $ \_ -> liftIO Random.randomIO >>= return . VReal

-- hash :: forall a . a -> int
genBuiltin "hash" _ = vfun $ \v -> valueHash v

-- substring :: int -> string -> int
genBuiltin "substring" _ =
    vfun $ \(VInt n) ->
        vfun $ \(VString s) ->
            return $ (VString (take n s))

-- range :: int -> collection {i : int} @ { Collection }
genBuiltin "range" _ =
  vfun $ \(VInt upper) ->
    initialAnnotatedCollection "Collection"
      $ map (\i -> VRecord (insertMember "i" (VInt i, MemImmut) $ emptyMembers)) [0..(upper-1)]

-- truncate :: int -> real
genBuiltin "truncate" _ = vfun $ \x -> case x of
  VReal r   -> return $ VInt $ truncate r
  _         -> throwE $ RunTimeInterpretationError $ "Expected real but got " ++ show x

-- real_of_int :: real -> int
genBuiltin "real_of_int" _ = vfun $ \x -> case x of
  VInt i    -> return $ VReal $ fromIntegral i
  _         -> throwE $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- get_max_int :: () -> int
genBuiltin "get_max_int" _ = vfun $ \_  -> return $ VInt maxBound

{- Time Related Functions -}

-- Parse an SQL date string and convert to integer
genBuiltin "parse_sql_date" _ = vfun $ \(VString s) -> do
    let v = parseSQLDate s
    case v of
        Just i  -> return $ VInt i
        Nothing -> throwE $ RunTimeInterpretationError "Bad date format"

genBuiltin "now" _ = vfun $ \(VTuple []) -> do
    v <- liftIO $ Clock.getTime Clock.Realtime
    return $ VRecord $ Map.fromList $
        [("sec", (VInt $ Clock.sec v, MemImmut)),
         ("nsec", (VInt $ Clock.nsec v, MemImmut))]

genBuiltin "add_time" _ = vfun $ \(VRecord map1) -> vfun $ \(VRecord map2) ->
    timeBinOp map1 map2 (+) >>= return . VRecord

genBuiltin "sub_time" _ = vfun $ \(VRecord map1) -> vfun $ \(VRecord map2) ->
    timeBinOp map1 map2 (-) >>= return . VRecord

-- getUTCTime :: {h : int, m : int, s : int}
genBuiltin "getUTCTime" _ = vfun $ \_ -> do
    currentTime <- liftIO $ getCurrentTime
    dayTime <- return $ utctDayTime currentTime
    timeOfDay <- return $ timeToTimeOfDay dayTime
    return $ VRecord $ Map.fromList $
        [("h", (VInt $ todHour timeOfDay, MemImmut)),
         ("m", (VInt $ todMin timeOfDay, MemImmut)),
         ("s", (VInt $ truncate $ todSec timeOfDay, MemImmut))]

-- getUTCTimeWithFormat :: string -> string
genBuiltin "getUTCTimeWithFormat" _ = vfun $ \(VString formatString) -> do
    currentTime <- liftIO $ getCurrentTime
    if formatString == ""
        then return $ VString $ formatTime defaultTimeLocale "%H:%M:%S" currentTime
        else return $ VString $ formatTime defaultTimeLocale formatString currentTime

-- getLocalTime :: {h : int, m : int, s : int}
genBuiltin "getLocalTime" _ = vfun $ \_ -> do
    zonedTime <- liftIO $ getZonedTime
    localTime <- return $ zonedTimeToLocalTime zonedTime
    timeOfDay <- return $ localTimeOfDay localTime
    return $ VRecord $ Map.fromList $
        [("h", (VInt $ todHour timeOfDay, MemImmut)),
         ("m", (VInt $ todMin timeOfDay, MemImmut)),
         ("s", (VInt $ truncate $ todSec timeOfDay, MemImmut))]

-- getLocalTimeWithFormat :: string -> string
genBuiltin "getLocalTimeWithFormat" _ = vfun $ \(VString formatString) -> do
    zonedTime <- liftIO $ getZonedTime
    if formatString == ""
        then return $ VString $ formatTime defaultTimeLocale "%H:%M:%S" zonedTime
        else return $ VString $ formatTime defaultTimeLocale formatString zonedTime

-- {Date}

-- getUTCDate :: () -> {y : int, m : int, d : int}
genBuiltin "getUTCDate" _ = vfun $ \_ -> do
    currentTime <- liftIO $ getCurrentTime
    (y,m,d) <- return $ toGregorian $ utctDay currentTime
    return $ VRecord $ Map.fromList $
        [("y", (VInt $ fromInteger y, MemImmut)),
         ("m", (VInt $ m, MemImmut)),
         ("d", (VInt $ d, MemImmut))]

-- getLocalDate :: () -> {y : int, m : int, d : int}
genBuiltin "getLocalDate" _ = vfun $ \_ -> do
    zonedTime <- liftIO $ getZonedTime
    (y, m, d) <- return $ toGregorian $ localDay $ zonedTimeToLocalTime zonedTime
    return $ VRecord $ Map.fromList $
        [("y", (VInt $ fromInteger y, MemImmut)),
         ("m", (VInt $ m, MemImmut)),
         ("d", (VInt $ d, MemImmut))]

-- parseDate :: string -> string -> option {y : int, m : int, d : int}
genBuiltin "parseDate" _ = vfun $ \(VString formatString) -> vfun $ \(VString dateString) -> do
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

-- compareDate :: record -> record -> int
genBuiltin "compareDate" _ = vfun $ \(VRecord date1) -> vfun $ \(VRecord date2) ->
    compareDateOp date1 date2 >>= return . VInt

genBuiltin "error" _ = vfun $ \_ -> throwE $ RunTimeTypeError "Error encountered in program"

-- Show values
genBuiltin "show" _ = vfun $ \x -> do
    st <- get
    return $ VString $ showPC (getPrintConfig st) x

-- Log to the screen
genBuiltin "print" _ = vfun logString
  where logString (VString s) = do
              -- liftIO $ putStrLn s
              _notice_Function s
              return $ VTuple []
        logString x           = throwE $ RunTimeTypeError ("In 'print': Expected a string but received " ++ show x)

-- Shutdown the engine after processing current message
genBuiltin "haltEngine" _ = vfun $ \_ -> liftEngine (terminateEngine True) >> return vunit

-- Shutdown the engine once it has empty queues
genBuiltin "drainEngine" _ = vfun $ \_ -> liftEngine (terminateEngine False) >> return vunit

-- Sleep
genBuiltin "sleep" _ = vfun $ \(VInt x) -> liftIO (threadDelay x) >> return vunit


genBuiltin n _ = throwE $ RunTimeTypeError $ "Invalid builtin \"" ++ n ++ "\""

channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)

registerNotifier :: Identifier -> Interpretation Value
registerNotifier n =
  vfun $ \cid -> vfun $ \target -> attach cid n target >> return vunit

  where attach (VString cid) _ (targetOfValue -> (addr, tid, v)) =
          liftEngine $ attachNotifier_ cid n (addr, tid, v)
        attach _ _ _ = undefined

        targetOfValue (VTuple [(VTrigger (m, _, _), _), (VAddress addr, _)]) = (addr, m, vunit)
        targetOfValue _ = error "Invalid notifier target"


{- Builtin annotation members -}
providesError :: String -> Identifier -> a
providesError kind n = error $
  "Invalid " ++ kind ++ " definition for " ++ n ++ ": no initializer expression"

-- BREAKING EXCEPTION SAFETY
modifyCollection :: MVar (Collection Value)
                  -> (Collection Value -> Interpretation (Collection Value))
                  -> Interpretation Value
--TODO modifyMVar_ function has to be over IO
modifyCollection cmv f = do
    old_col <- liftIO $ readMVar cmv
    result  <- f old_col
    --liftIO $ putMVar cmv result
    liftIO $ modifyMVar_ cmv (const $ return result)
    return vunit

{-
 - Collection API : head, map, fold, append/concat, delete
 - these can handle in memory vs external
 - other functions here use this api
 -}
builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type
                       -> Interpretation (Maybe (Identifier, Value))
builtinLiftedAttribute annId n _ =
  let wrapF f = f >>= \x -> return $ Just (n, x) in
  if annId `elem` dataspaceAnnotationIds then case n of
    "peek"        -> wrapF peekFn
    "insert"      -> wrapF insertFn
    "erase"       -> wrapF deleteFn
    "update"      -> wrapF updateFn
    "combine"     -> wrapF combineFn
    "split"       -> wrapF splitFn
    "iterate"     -> wrapF iterateFn
    "map"         -> wrapF mapFn
    "filter"      -> wrapF filterFn
    "fold"        -> wrapF foldFn
    "groupBy"     -> wrapF groupByFn
    "ext"         -> wrapF extFn

    -- Sequential collection methods
    "sort"        -> wrapF sortFn

    -- Set collection methods
    "member"      -> wrapF memberFn
    "isSubsetOf"  -> wrapF isSubsetOfFn
    "union"       -> wrapF unionFn
    "intersect"   -> wrapF intersectFn
    "difference"  -> wrapF differenceFn

    -- Sorted collection methods
    "min"         -> wrapF minFn
    "max"         -> wrapF maxFn
    "lowerBound"  -> wrapF lowerBoundFn
    "upperBound"  -> wrapF upperBoundFn
    "slice"       -> wrapF sliceFn

    _             -> providesError "lifted attribute" n

  else providesError "unknown lifted attribute" n
  where
    copy = copyCollection

    -- | Collection accessor implementation
    peekFn = vfun $ \_ -> withSelf $ \(Collection _ ds _) -> do
      inner_val <- peekDS ds
      return $ VOption (inner_val, MemImmut)

    -- | Collection modifier implementation
    insertFn = vfun $ \v -> modifySelf $ \selfMV (Collection ns ds cId) -> do
      new_ds <- insertDS ds v
      return $ (VCollection (selfMV, Collection ns new_ds cId), vunit)

    deleteFn = vfun $ \v -> modifySelf $ \selfMV (Collection ns ds cId) -> do
      new_ds <- deleteDS v ds
      return $ (VCollection (selfMV, Collection ns new_ds cId), vunit)

    updateFn = vfun $ \old -> vfun $ \new -> modifySelf $ \selfMV (Collection ns ds cId) -> do
      new_ds <- updateDS old new ds
      return $ (VCollection (selfMV, Collection ns new_ds cId), vunit)

    -- | Collection effector implementation
    iterateFn = vfun $ \f -> withSelf $ \(Collection _ ds _) ->
      flip (matchFunction $ funArgError "iterate") f $
      \f' -> mapDS_ (withClosure f') ds >> return vunit

    -- | Collection transformer implementation
    binaryCollectionFn binaryDSFn crossDSFn = vfun $ \other -> withSelf $ \(Collection ns ds cId) ->
      flip (matchCollection collectionError) (IVal other) $
        \(Collection _ ds' cId') ->
          if cId == cId'
            then binaryDSFn ns cId ds ds'
            else crossDSFn ns cId ds ds'

    injectFn binaryDSFn ns cId ds ds' = binaryDSFn ds ds' >>= \nds -> copy $ Collection ns nds cId

    -- | Implement a membership test as a complete traversal. This is necessary, since we do not
    --   know that the backing collection is actually a set during cross-collection binary operations.
    memberDSFn ds v = foldDS (\acc v' -> if acc then return acc else return $ v == v') False ds

    -- | Linear time union, and quadratic time intersection and difference.
    --   These are all LHS-domain restrictive implementations.
    unionCrossFn ds ds'      = copyDS ds  >>= \nds -> foldDS (\accDS v -> insertDS accDS v) nds ds'
    intersectCrossFn ds ds'  = emptyDS (Just ds) >>= \nds -> foldDS (\accDS v -> memberDSFn ds' v >>= onSuccess insertDS accDS v) nds ds
    differenceCrossFn ds ds' = emptyDS (Just ds) >>= \nds -> foldDS (\accDS v -> memberDSFn ds' v >>= onFail insertDS accDS v) nds ds

    setSubsetFn _ _ ds ds'   = isSubsetOfDS ds ds' >>= return . VBool
    crossSubsetFn _ _ ds ds' = foldDS (\acc v -> if not acc then return acc else memberDSFn ds' v) True ds >>= return . VBool

    onSuccess f ds v True  = f ds v
    onSuccess _ ds _ False = return ds
    onFail _ ds _ True  = return ds
    onFail f ds v False = f ds v

    combineFn    = binaryCollectionFn (injectFn combineDS)    (injectFn unionCrossFn)
    unionFn      = binaryCollectionFn (injectFn unionDS)      (injectFn unionCrossFn)
    intersectFn  = binaryCollectionFn (injectFn intersectDS)  (injectFn intersectCrossFn)
    differenceFn = binaryCollectionFn (injectFn differenceDS) (injectFn differenceCrossFn)
    isSubsetOfFn = binaryCollectionFn setSubsetFn crossSubsetFn

    splitFn = vfun $ \_ -> withSelf $ \(Collection ns ds cId) -> do
        (l, r) <- splitDS ds
        lc <- copy (Collection ns l cId)
        rc <- copy (Collection ns r cId)
        return $ VTuple [(lc, MemImmut), (rc, MemImmut)]

    -- Pass in the namespace
    mapFn = vfun $ \f -> withSelf $ \(Collection ns ds cId) ->
      flip (matchFunction $ funArgError "map") f $
        \f'  -> mapDS (withClosure f') ds >>=
        \ds' -> copy (Collection ns ds' cId)

    filterFn = vfun $ \f -> withSelf $ \(Collection ns ds cId) ->
      flip (matchFunction $ funArgError "filter") f $
        \f'  -> filterDS (\v -> withClosure f' v >>= matchBool filterValError) ds >>=
        \ds' -> copy (Collection ns ds' cId)

    foldFn = vfun $ \f -> vfun $ \accInit -> withSelf $ \(Collection _ ds _) ->
      flip (matchFunction $ funArgError "fold") f $
        \f' -> foldDS (curryFoldFn f') accInit ds

    curryFoldFn :: (IFunction, Closure Value, EntityTag) -> Value -> Value -> Interpretation Value
    curryFoldFn f' acc v = do
      result <- withClosure f' acc
      (matchFunction curryFnError) (flip withClosure v) result

    -- TODO: replace assoc lists with a hashmap.
    groupByFn = vfun $ \gb -> vfun $ \f -> vfun $ \accInit -> withSelf $ \(Collection ns ds cId) ->
      flip (matchFunction $ funArgError "group-by partition") gb $ \gb' ->
      flip (matchFunction $ funArgError "group-by aggregate") f  $ \f'  ->
        do
          new_space <- emptyDS (Just ds)
          kvRecords <- foldDS (groupByElement gb' f' accInit) new_space ds
          -- TODO typecheck that collection
          copy (Collection ns kvRecords cId)

    groupByElement :: (AssociativeDataspace (Interpretation) ads Value Value)
                   => (IFunction, Closure Value, EntityTag)
                   -> (IFunction, Closure Value, EntityTag)
                   -> Value -> ads -> Value -> Interpretation ads
    groupByElement gb' f' accInit acc v = do
      k <- withClosure gb' v
      look_result <- lookupKV acc k
      case look_result of
        Nothing         -> do
          val    <- curryFoldFn f' accInit v
          tmp_ds <- emptyDS (Just acc) >>= \ds -> insertKV ds k val
          combineDS acc tmp_ds

        Just partialAcc -> do
          new_val <- curryFoldFn f' partialAcc v
          replaceKV acc k new_val

    extFn = vfun $ \f -> vfun $ \emptyColV -> withSelf $ \(Collection _ ds _) ->
      flip (matchFunction $ funArgError "ext") f $ \f' ->
      flip (matchCollection extError) (IVal emptyColV) $ \emptyCol ->
        do
          val_ds <- mapDS (withClosure f') ds
          first_subcol <- peekDS val_ds
          case first_subcol of
            Nothing -> copy emptyCol
            Just sub_val -> do
              val_ds2 <- deleteDS sub_val val_ds
              result  <- foldDS (\acc val -> combine' acc (Just val)) (Just sub_val) val_ds2
              maybe (typeMismatchError "ext combine") return result

    sortFn = vfun $ \f -> withSelf $ \(Collection ns ds cId) ->
      flip (matchFunction $ funArgError "sort") f $
        \f'  -> sortDS (sortResultAsOrdering f') ds >>=
        \ds' -> copy (Collection ns ds' cId)

    sortResultAsOrdering (f, cl, sn) = \v1 v2 -> do
        f2V  <- withClosure (f, cl, sn) v1
        cmpV <- (matchFunction curryFnError) (flip withClosure v2) f2V
        intAsOrdering cmpV
      where intAsOrdering (VInt i) | i < 0     = return LT
                                   | i == 0    = return EQ
                                   | otherwise = return GT
            intAsOrdering _        = throwE $ RunTimeTypeError "Invalid sort comparator result"

    memberFn = vfun $ \el -> withSelf $ \(Collection _ ds _) -> memberDS el ds >>= return . VBool

    minFn = vfun $ \_ -> withSelf $ \(Collection _ ds _) -> minDS ds >>= return . VOption . (, MemImmut)
    maxFn = vfun $ \_ -> withSelf $ \(Collection _ ds _) -> maxDS ds >>= return . VOption . (, MemImmut)

    lowerBoundFn = vfun $ \el -> withSelf $ \(Collection _ ds _) -> lowerBoundDS el ds >>= return . VOption . (, MemImmut)
    upperBoundFn = vfun $ \el -> withSelf $ \(Collection _ ds _) -> upperBoundDS el ds >>= return . VOption . (, MemImmut)

    sliceFn = vfun $ \lowerEl -> vfun $ \upperEl -> withSelf $ \(Collection ns ds cId) ->
        sliceDS lowerEl upperEl ds >>= \nds -> copy $ Collection ns nds cId


    combine' :: Maybe Value -> Maybe Value -> Interpretation (Maybe Value)
    combine' Nothing _ = return Nothing
    combine' _ Nothing = return Nothing

    -- TODO: make more efficient by avoiding intermediate MVar construction.
    combine' (Just acc) (Just cv) =
      flip (matchCollection $ return Nothing) (IVal acc) $ \(Collection ns1 ds1 cId1) ->
      flip (matchCollection $ return Nothing) (IVal cv)  $ \(Collection _ ds2 cId2) ->
      if cId1 /= cId2 then return Nothing
      else do
        new_ds <- combineDS ds1 ds2
        copy (Collection ns1 new_ds cId1) >>= return . Just


    -- | Collection implementation helpers.
    withClosure :: (IFunction, Closure Value, EntityTag) -> Value -> Interpretation Value
    withClosure (f, cl, _) arg = mergeE cl >> f arg >>= \r -> pruneE cl >> return r

    withSelf :: (Collection Value -> Interpretation Value) -> Interpretation Value
    withSelf f = lookupE annotationSelfId >>= matchCollection collectionError f

    -- | Modifies the collection currently referenced by the self keyword with the given
    --   function, and rebinds all collection components with the function's result.
    modifySelf :: (MVar Value -> Collection Value -> Interpretation (Value, Value)) -> Interpretation Value
    modifySelf f = lookupE annotationSelfId >>= matchSelf collectionError modifySMV
      where modifySMV selfMV c = do
              (nSelfV, r) <- f selfMV c
              void $ liftIO (modifyMVar_ selfMV $ const $ return nSelfV)
              void $ rebindSelf nSelfV
              return r

    -- | Refresh components of a collection bound in the environment.
    rebindSelf :: Value -> Interpretation ()
    rebindSelf v@(VCollection (_, c)) = do
      void $ replaceE annotationSelfId (IVal v)
      refreshEnvMembers (collectionNS (namespace c))

    rebindSelf _ = throwE $ RunTimeInterpretationError "Invalid collection when rebinding self"

    matchCollection :: Interpretation a
                    -> (Collection Value -> Interpretation a) -> IEnvEntry Value
                    -> Interpretation a
    matchCollection _ f (IVal (VCollection (_,c))) = f c
    matchCollection err f (MVal mv) = liftIO (readMVar mv) >>= \case
      VCollection (_,c) -> f c
      _ -> err
    matchCollection err _  _ =  err

    matchSelf :: Interpretation a
              -> (MVar Value -> Collection Value -> Interpretation a) -> IEnvEntry Value
              -> Interpretation a
    matchSelf _ f (IVal (VCollection (s,c))) = f s c
    matchSelf err f (MVal mv) = liftIO (readMVar mv) >>= \case
      VCollection (s,c) -> f s c
      _ -> err
    matchSelf err _ _ = err

    matchFunction :: a -> ((IFunction, Closure Value, EntityTag) -> a) -> Value -> a
    matchFunction _ f (VFunction f') = f f'
    matchFunction err _ _ = err

    matchBool :: Interpretation Bool -> Value -> Interpretation Bool
    matchBool _ (VBool b) = return b
    matchBool err _ = err

    collectionError = throwE $ RunTimeTypeError "Invalid collection"
    filterValError  = throwE $ RunTimeTypeError "Invalid filter function result"
    curryFnError    = throwE $ RunTimeTypeError "Invalid curried function"
    extError        = throwE $ RunTimeTypeError "Invalid collection argument for ext"

    funArgError       fnName = throwE $ RunTimeTypeError $ "Invalid function argument in " ++ fnName
    typeMismatchError fnName = throwE $ RunTimeTypeError $ "Mismatched collection types on " ++ fnName

builtinAttribute :: Identifier -> Identifier -> K3 Type
                 -> Interpretation (Maybe (Identifier, Value))
builtinAttribute _ n _ = providesError "attribute" n

