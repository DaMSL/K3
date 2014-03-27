{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | K3 builtin and standard library function interpretation
module Language.K3.Interpreter.Builtins where

import Control.Concurrent.MVar
import Control.Monad.State
import Data.List
import Debug.Trace
import System.Mem.StableName
import System.Random

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


{- Built-in functions -}

builtin :: Identifier -> K3 Type -> Interpretation ()
builtin n t = genBuiltin n t >>= modifyE . (:) . (n,)

genBuiltin :: Identifier -> K3 Type -> Interpretation Value

-- TODO: error handling on all open/close/read/write methods.
-- TODO: argument for initial endpoint bindings for open method as a list of triggers
-- TODO: correct element type (rather than function type sig) for openFile / openSocket

-- type ChannelId = String

-- openBuilting :: ChannelId -> String -> ()
genBuiltin "openBuiltin" _ =
  vfun $ \_ (VString cid) -> 
    vfun $ \_ (VString builtinId) ->
      vfun $ \_ (VString format) -> 
        do   
          sEnv <- get >>= return . getStaticEnv
          let wd = wireDesc sEnv format
          void $ liftEngine (openBuiltin cid builtinId wd)
          return vunit

-- openFile :: ChannelId -> String -> String -> String -> ()
genBuiltin "openFile" t =
  vfun $ \_ (VString cid) ->
    vfun $ \_ (VString path) ->
      vfun $ \_ (VString format) ->
        vfun $ \_ (VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openFile cid path wd (Just t) mode)
            return vunit

-- openSocket :: ChannelId -> Address -> String -> String -> ()
genBuiltin "openSocket" t =
  vfun $ \_ (VString cid) ->
    vfun $ \_ (VAddress addr) ->
      vfun $ \_ (VString format) ->
        vfun $ \_ (VString mode) ->
          do
            sEnv <- get >>= return . getStaticEnv
            let wd = wireDesc sEnv format
            void $ liftEngine (openSocket cid addr wd (Just t) mode)
            return vunit


-- close :: ChannelId -> ()
genBuiltin "close" _ = vfun $ \_ (VString cid) -> liftEngine (close cid) >> return vunit

-- TODO: deregister methods
-- register*Trigger :: ChannelId -> TTrigger () -> ()
genBuiltin "registerFileDataTrigger"     _ = registerNotifier "data"
genBuiltin "registerFileCloseTrigger"    _ = registerNotifier "close"

genBuiltin "registerSocketAcceptTrigger" _ = registerNotifier "accept"
genBuiltin "registerSocketDataTrigger"   _ = registerNotifier "data"
genBuiltin "registerSocketCloseTrigger"  _ = registerNotifier "close"

-- <source>HasRead :: () -> Bool
genBuiltin (channelMethod -> ("HasRead", Just n)) _ = vfun $ \su _ -> checkChannel su
  where checkChannel su = liftEngine (hasRead n) >>= maybe (invalid su) (return . VBool)
        invalid su = throwSE su $ RunTimeInterpretationError $ "Invalid source \"" ++ n ++ "\""

-- <source>Read :: () -> t
genBuiltin (channelMethod -> ("Read", Just n)) _ = vfun $ \su _ -> liftEngine (doRead n) >>= throwOnError su
  where throwOnError _  (Just v) = return v
        throwOnError su Nothing =
          throwSE su $ RunTimeInterpretationError $ "Invalid next value from source \"" ++ n ++ "\""

-- <sink>HasWrite :: () -> Bool
genBuiltin (channelMethod -> ("HasWrite", Just n)) _ = vfun $ \su _ -> checkChannel su
  where checkChannel su = liftEngine (hasWrite n) >>= maybe (invalid su) (return . VBool)
        invalid su      = throwSE su $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) _ =
  vfun $ \_ arg -> liftEngine (doWrite n arg) >> return vunit

-- random :: int -> int
genBuiltin "random" _ =
  vfun $ \su x -> case x of
    VInt upper -> liftIO (randomRIO (0::Int, upper)) >>= return . VInt
    _          -> throwSE su $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- randomFraction :: () -> real
genBuiltin "randomFraction" _ = vfun $ \_ _ -> liftIO randomIO >>= return . VReal

-- hash :: forall a . a -> int
genBuiltin "hash" _ = vfun $ \_ v -> valueHash v

-- range :: int -> collection {i : int} @ { Collection }
genBuiltin "range" _ =
  vfun $ \su x -> case x of
      VInt upper -> initialAnnotatedCollection "Collection" $ map (\i -> VRecord [("i", VInt i)]) [0..(upper-1)]
      _          -> throwSE su $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- int_of_real :: int -> real
genBuiltin "int_of_real" _ = vfun $ \su x -> case x of 
  VReal r   -> return $ VInt $ truncate r
  _         -> throwSE su $ RunTimeInterpretationError $ "Expected real but got " ++ show x

-- real_of_int :: real -> int
genBuiltin "real_of_int" _ = vfun $ \su x -> case x of
  VInt i    -> return $ VReal $ fromIntegral i
  _         -> throwSE su $ RunTimeInterpretationError $ "Expected int but got " ++ show x

-- get_max_int :: () -> int
genBuiltin "get_max_int" _ = vfun $ \_ _  -> return $ VInt maxBound

-- Parse an SQL date string and convert to integer
genBuiltin "parse_sql_date" _ = vfun $ \su (VString s) ->
  return (VInt $ toInt $ parseString s)
  where parseString s = foldl' readChar [0] s
        readChar xs '-'   = 0:xs
        readChar (x:xs) n = (x*10 + read [n]):xs
        toInt [y,m,d]     = y*10000 + m*100 + d

genBuiltin "coerce" _ = 
  vfun $ \su cmv ->
    flip (matchCollectionMV $ coerceError su) cmv $ \cmv' ->
  vfun $ \_  str   ->
    matchString (coerceError su) str >>= \s ->
  modifyCollection cmv' (castCol su s) >> return cmv
  where 
    coerceError su = throwSE su $ RunTimeTypeError "Invalid coerce call"

    castCol :: Maybe (Span, UID) -> Identifier -> Collection (Value) -> Interpretation (Collection Value)
    castCol su s (Collection ns ds ext) = do
      newDS  <- emptyDataspace su [s]
      newDS' <- foldDS (\accDS v -> insertDS newDS v) newDS ds
      return $ Collection ns newDS' s
    castCol su _                      _ = coerceError su
  
genBuiltin "error" _ = vfun $ \su _ -> throwSE su $ RunTimeTypeError "Error encountered in program"

genBuiltin n _ = throwE $ RunTimeTypeError $ "Invalid builtin \"" ++ n ++ "\""

channelMethod :: String -> (String, Maybe String)
channelMethod x =
  case find (flip isSuffixOf x) ["HasRead", "Read", "HasWrite", "Write"] of
    Just y -> (y, stripSuffix y x)
    Nothing -> (x, Nothing)
  where stripSuffix sfx lst = maybe Nothing (Just . reverse) $ stripPrefix (reverse sfx) (reverse lst)

registerNotifier :: Identifier -> Interpretation Value
registerNotifier n =
  vfun $ \_ cid -> vfun $ \_ target -> attach cid n target >> return vunit

  where attach (VString cid) _ (targetOfValue -> (addr, tid, v)) = 
          liftEngine $ attachNotifier_ cid n (addr, tid, v)
        attach _ _ _ = undefined

        targetOfValue (VTuple [VTrigger (m, _), VAddress addr]) = (addr, m, vunit)
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
builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> UID
                          -> Interpretation (Maybe (Identifier, Value))
builtinLiftedAttribute annId n _ _ =
  let wrap f = f >>= \x -> return $ Just (n, x) in
  if annId `elem` dataspaceAnnotationIds then case n of
    "peek"        -> wrap peekFn
    "insert"      -> wrap insertFn
    "delete"      -> wrap deleteFn
    "update"      -> wrap updateFn
    "combine"     -> wrap combineFn
    "split"       -> wrap splitFn
    "iterate"     -> wrap iterateFn
    "map"         -> wrap mapFn
    "filter"      -> wrap filterFn
    "fold"        -> wrap foldFn
    "groupBy"     -> wrap groupByFn
    "ext"         -> wrap extFn

    -- Sequential collection methods
    "sort"        -> wrap sortFn

    -- Set collection methods
    "member"      -> wrap memberFn
    "isSubsetOf"  -> wrap isSubsetOfFn
    "union"       -> wrap unionFn
    "intersect"   -> wrap intersectFn
    "difference"  -> wrap differenceFn

    -- Sorted collection methods
    "min"         -> wrap minFn
    "max"         -> wrap maxFn
    "lowerBound"  -> wrap lowerBoundFn
    "upperBound"  -> wrap upperBoundFn
    "slice"       -> wrap sliceFn

    _             -> providesError "lifted attribute" n

  else providesError "unknown lifted attribute" n
  where
    copy = copyCollection

    -- | Collection accessor implementation
    peekFn = valWithCollection $ \_ _ (Collection _ ds _) -> do 
      inner_val <- peekDS ds
      return $ VOption inner_val

    -- | Collection modifier implementation
    insertFn = valWithCollectionMV $ \_ el cmv -> modifyCollection cmv (insertCollection el)
    deleteFn = valWithCollectionMV $ \_ el cmv -> modifyCollection cmv (deleteCollection el)
    updateFn = valWithCollectionMV $ \_ old cmv -> vfun $ \_ new -> modifyCollection cmv (updateCollection old new)


    -- TODO move wrapping / unwrapping DataSpace into modifyCollection?
    insertCollection :: Value -> Collection (Value) -> Interpretation (Collection Value)
    insertCollection v  (Collection ns ds extId) = do
        new_ds <- insertDS ds v
        return $ Collection ns new_ds extId

    deleteCollection v    (Collection ns ds extId) = do
        new_ds <- deleteDS v ds
        return $ Collection ns new_ds extId

    updateCollection v v' (Collection ns ds extId) = do
        new_ds <- updateDS v v' ds
        return $ Collection ns new_ds extId

    -- | Collection effector implementation
    iterateFn = valWithCollection $ \su f (Collection _ ds _) -> 
      flip (matchFunction $ funArgError su "iterate") f $
      \f' -> mapDS_ (withClosure su f') ds >> return vunit

    -- | Collection transformer implementation
    {-
    combineFn = valWithCollection $ \other (Collection ns ds extId) ->
      flip (matchCollection collectionError) other $ 
        \(Collection _ ds' extId') ->
          if extId /= extId' then typeMismatchError "combine"
          else do 
            new_ds <- combineDS ds ds'
            copy $ Collection ns new_ds extId
    -}

    binaryCollectionFn fnName binaryDSFn = valWithCollection $ \su other (Collection ns ds extId) ->
      flip (matchCollection $ collectionError su) other $ 
        \(Collection _ ds' extId') ->
          if extId /= extId' then typeMismatchError su fnName (Just (extId, extId'))
          else binaryDSFn ns extId ds ds'

    injectFn binaryDSFn ns extId ds ds' = binaryDSFn ds ds' >>= \nds -> copy $ Collection ns nds extId
    
    combineFn    = binaryCollectionFn "combine"    $ injectFn combineDS
    isSubsetOfFn = binaryCollectionFn "isSubsetOf" $ (\_ _ ds ds' -> isSubsetOfDS ds ds' >>= return . VBool)
    unionFn      = binaryCollectionFn "union"      $ injectFn unionDS
    intersectFn  = binaryCollectionFn "intersect"  $ injectFn intersectDS
    differenceFn = binaryCollectionFn "difference" $ injectFn differenceDS

    splitFn = valWithCollection $ \_ _ (Collection ns ds extId) -> do
        (l, r) <- splitDS ds
        lc <- copy (Collection ns l extId)
        rc <- copy (Collection ns r extId)
        return $ VTuple [lc, rc]
    
    -- Pass in the namespace
    mapFn = valWithCollection $ \su f (Collection ns ds ext) ->
      flip (matchFunction $ funArgError su "map") f $ 
        \f'  -> mapDS (withClosure su f') ds >>= 
        \ds' -> copy (Collection ns ds' ext)

    filterFn = valWithCollection $ \su f (Collection ns ds extId) ->
      flip (matchFunction $ funArgError su "filter") f $
        \f'  -> filterDS (\v -> withClosure su f' v >>= matchBool (filterValError su)) ds >>=
        \ds' -> copy (Collection ns ds' extId)

    foldFn = valWithCollection $ \su f (Collection _ ds _) ->
      flip (matchFunction $ funArgError su "fold") f $
        \f' -> vfun $ \su accInit -> foldDS (curryFoldFn su f') accInit ds

    curryFoldFn :: Maybe (Span, UID) -> (IFunction, Closure Value, StableName IFunction) -> Value -> Value -> Interpretation Value
    curryFoldFn su f' acc v = do
      result <- withClosure su f' acc
      (matchFunction (curryFnError su)) (flip (withClosure su) v) result

    -- TODO: replace assoc lists with a hashmap.
    groupByFn = valWithCollection heres_the_answer
      where
        heres_the_answer :: Maybe (Span, UID) -> Value -> Collection Value -> Interpretation Value
        heres_the_answer su gb (Collection ns ds ext) = -- Am I passing the right namespace & stuff to the later collections?
          flip (matchFunction $ (funArgError su) "group-by partition") gb $ \gb' -> vfun $ \_ f -> 
          flip (matchFunction $ (funArgError su) "group-by aggregate") f $ \f' -> vfun $   \_ accInit ->
            do
              new_space <- emptyDS (Just ds)
              kvRecords <- foldDS (groupByElement su gb' f' accInit) new_space ds
              -- TODO typecheck that collection
              copy (Collection ns kvRecords ext)

    groupByElement :: (AssociativeDataspace (Interpretation) ads Value Value)
                   => Maybe (Span, UID)
                   -> (IFunction, Closure Value, StableName IFunction)
                   -> (IFunction, Closure Value, StableName IFunction)
                   -> Value -> ads -> Value -> Interpretation ads
    groupByElement su gb' f' accInit acc v = do
      k <- withClosure su gb' v
      look_result <- lookupKV acc k
      case look_result of
        Nothing         -> do
          val <- curryFoldFn su f' accInit v 
          --tmp_ds <- emptyDS ()
          tmp_ds <- emptyDS (Just acc)
          tmp_ds <- insertKV tmp_ds k val
          combineDS acc tmp_ds
        Just partialAcc -> do 
          new_val <- curryFoldFn su f' partialAcc v
          replaceKV acc k new_val

    extFn = valWithCollection $ \su f (Collection ns ds ext) ->
      vfun $ \_ emptyCol ->
      flip (matchCollection $ extError su) emptyCol $ \(Collection ns1 ds1 extId1) -> 
      flip (matchFunction $ (funArgError su) "ext") f $ 
      \f' -> do
            val_ds <- mapDS (withClosure su f') ds
            first_subcol <- peekDS val_ds
            case first_subcol of
              Nothing -> --extError su -- Maybe not the right error here
                                     -- really, I should create an empty VCollection
                                     -- with the right type (that of f(elem)), but I
                                     -- don't have a value to copy the type out of
                        copy (Collection ns1 ds1 extId1)
              Just sub_val -> do
                val_ds <- deleteDS sub_val val_ds
                result <- foldDS (\acc val -> combine' acc $ Just val) (Just sub_val) val_ds
                maybe (typeMismatchError su "ext combine" Nothing) return result

    sortFn = valWithCollection $ \su f (Collection ns ds extId) ->
      flip (matchFunction $ (funArgError su) "sort") f $ 
        \f'  -> sortDS (sortResultAsOrdering su f') ds >>= 
        \ds' -> copy (Collection ns ds' extId)

    sortResultAsOrdering su (f, cl, sn) = \v1 v2 -> do
        f2V  <- withClosure su (f, cl, sn) v1
        cmpV <- (matchFunction (curryFnError su)) (flip (withClosure su) v2) f2V
        intAsOrdering cmpV
      where intAsOrdering (VInt i) | i < 0     = return LT
                                   | i == 0    = return EQ
                                   | otherwise = return GT
            intAsOrdering _        = throwSE su $ RunTimeTypeError "Invalid sort comparator result"

    memberFn = valWithCollection $ \_ el (Collection _ ds _) -> memberDS el ds >>= return . VBool

    minFn = valWithCollection $ \_ _ (Collection _ ds _) -> minDS ds >>= return . VOption
    maxFn = valWithCollection $ \_ _ (Collection _ ds _) -> maxDS ds >>= return . VOption

    lowerBoundFn = valWithCollection $ \_ el (Collection _ ds _) -> lowerBoundDS el ds >>= return . VOption
    upperBoundFn = valWithCollection $ \_ el (Collection _ ds _) -> upperBoundDS el ds >>= return . VOption

    sliceFn = valWithCollection $ 
      \_ lowerEl (Collection ns ds extId) -> vfun $ \_ upperEl -> 
        sliceDS lowerEl upperEl ds >>= \nds -> copy $ Collection ns nds extId


    combine' :: Maybe Value -> Maybe Value -> Interpretation (Maybe Value)
    combine' Nothing _ = return Nothing
    combine' _ Nothing = return Nothing

    -- TODO: make more efficient by avoiding intermediate MVar construction.
    combine' (Just acc) (Just cv) =
      flip (matchCollection $ return Nothing) acc $ \(Collection ns1 ds1 extId1) -> 
      flip (matchCollection $ return Nothing) cv  $ \(Collection _ ds2 extId2) -> 
      if extId1 /= extId2 then return Nothing
      else do
        new_ds <- combineDS ds1 ds2
        copy (Collection ns1 new_ds extId1) >>= return . Just


    -- | Collection implementation helpers.
    withClosure :: Maybe (Span, UID) -> (IFunction, Closure Value, StableName IFunction) -> Value -> Interpretation Value
    withClosure su (f, cl, _) arg = modifyE (cl ++) >> f su arg >>= flip (foldM $ flip removeE) cl

    valWithCollection :: (Maybe (Span, UID) -> Value -> Collection Value -> Interpretation Value) -> Interpretation Value
    valWithCollection f = vfun $ \su arg -> 
      lookupE Nothing annotationSelfId >>= matchCollection (collectionError su) (f su arg)

    valWithCollectionMV :: (Maybe (Span, UID) -> Value -> MVar (Collection Value) -> Interpretation Value) -> Interpretation Value
    valWithCollectionMV f = vfun $ \su arg -> 
      lookupE Nothing annotationSelfId >>= matchCollectionMV (collectionError su) (f su arg)

    matchCollection :: Interpretation a -> (Collection Value -> Interpretation a) -> Value -> Interpretation a
    matchCollection _ f (VCollection cmv) = liftIO (readMVar cmv) >>= f
    matchCollection err _  _ =  err

    collectionError su = throwSE su $ RunTimeTypeError "Invalid collection"
    filterValError su  = throwSE su $ RunTimeTypeError "Invalid filter function result"
    curryFnError su    = throwSE su $ RunTimeTypeError "Invalid curried function"
    extError su        = throwSE su $ RunTimeTypeError "Invalid function argument for ext"

    funArgError       su fnName = throwSE su $ RunTimeTypeError $ "Invalid function argument in " ++ fnName
    typeMismatchError :: Maybe (Span, UID) -> String -> Maybe (Identifier, Identifier) -> Interpretation b
    typeMismatchError su fnName Nothing = throwSE su $ RunTimeTypeError $ "Mismatched collection types on " ++ fnName
    typeMismatchError su fnName (Just (t1, t2)) =
      throwSE su $ RunTimeTypeError $ "Mismatched collection types on " ++ fnName ++ " type 1: " ++ show t1 ++ " type 2: " ++ show t2

matchCollectionMV :: Interpretation a -> (MVar (Collection Value) -> Interpretation a) -> Value -> Interpretation a
matchCollectionMV _ f (VCollection cmv) = f cmv
matchCollectionMV err _ _ = err

matchFunction :: a -> ((IFunction, Closure Value, StableName IFunction) -> a) -> Value -> a
matchFunction _ f (VFunction f') = f f'
matchFunction err _ _ = err

matchString :: Interpretation String -> Value -> Interpretation String
matchString _ (VString s) = return s
matchString err _ = err

matchBool :: Interpretation Bool -> Value -> Interpretation Bool
matchBool _ (VBool b) = return b
matchBool err _ = err

builtinAttribute :: Identifier -> Identifier -> K3 Type -> UID
                 -> Interpretation (Maybe (Identifier, Value))
builtinAttribute _ n _ _ = providesError "attribute" n

