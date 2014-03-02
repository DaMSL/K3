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
        invalid = throwE $ RunTimeInterpretationError $ "Invalid sink \"" ++ n ++ "\""

-- <sink>Write :: t -> ()
genBuiltin (channelMethod -> ("Write", Just n)) _ =
  vfun $ \arg -> liftEngine (doWrite n arg) >> return vunit

-- random :: int -> int
genBuiltin "random" _ =
  vfun $ \(VInt upper) -> liftIO (randomRIO (0::Int, upper)) >>= return . VInt

-- randomFraction :: () -> real
genBuiltin "randomFraction" _ = vfun $ \_ -> liftIO randomIO >>= return . VReal

-- hash :: forall a . a -> int
genBuiltin "hash" _ = vfun $ \v -> valueHash v

-- range :: int -> collection {i : int} @ { Collection }
genBuiltin "range" _ =
  vfun $ \(VInt upper) ->
    initialAnnotatedCollection "Collection" $ map (\i -> VRecord [("i", VInt i)]) [0..(upper-1)]

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

        targetOfValue (VTuple [VTrigger (m, _), VAddress addr]) = (addr, m, vunit)
        targetOfValue _ = error "Invalid notifier target"


{- Builtin annotation members -}
providesError :: String -> Identifier -> a
providesError kind n = error $
  "Invalid " ++ kind ++ " definition for " ++ n ++ ": no initializer expression"


{-
 - Collection API : head, map, fold, append/concat, delete
 - these can handle in memory vs external
 - other functions here use this api
 -}
builtinLiftedAttribute :: Identifier -> Identifier -> K3 Type -> UID
                          -> Interpretation (Maybe (Identifier, Value))
builtinLiftedAttribute annId n _ _ 
  | annId == collectionAnnotationId && n == "peek"    = return . Just . (n,) =<< peekFn
  | annId == collectionAnnotationId && n == "insert"  = return . Just . (n,) =<< insertFn
  | annId == collectionAnnotationId && n == "delete"  = return . Just . (n,) =<< deleteFn
  | annId == collectionAnnotationId && n == "update"  = return . Just . (n,) =<< updateFn
  | annId == collectionAnnotationId && n == "combine" = return . Just . (n,) =<< combineFn
  | annId == collectionAnnotationId && n == "split"   = return . Just . (n,) =<< splitFn
  | annId == collectionAnnotationId && n == "iterate" = return . Just . (n,) =<< iterateFn
  | annId == collectionAnnotationId && n == "map"     = return . Just . (n,) =<< mapFn
  | annId == collectionAnnotationId && n == "filter"  = return . Just . (n,) =<< filterFn
  | annId == collectionAnnotationId && n == "fold"    = return . Just . (n,) =<< foldFn
  | annId == collectionAnnotationId && n == "groupBy" = return . Just . (n,) =<< groupByFn
  | annId == collectionAnnotationId && n == "ext"     = return . Just . (n,) =<< extFn
  | annId == collectionAnnotationId && n == "sort"    = return . Just . (n,) =<< sortFn

  | annId == externalAnnotationId && n == "peek"    = return . Just . (n,) =<< peekFn
  | annId == externalAnnotationId && n == "insert"  = return . Just . (n,) =<< insertFn
  | annId == externalAnnotationId && n == "delete"  = return . Just . (n,) =<< deleteFn
  | annId == externalAnnotationId && n == "update"  = return . Just . (n,) =<< updateFn
  | annId == externalAnnotationId && n == "combine" = return . Just . (n,) =<< combineFn
  | annId == externalAnnotationId && n == "split"   = return . Just . (n,) =<< splitFn
  | annId == externalAnnotationId && n == "iterate" = return . Just . (n,) =<< iterateFn
  | annId == externalAnnotationId && n == "map"     = return . Just . (n,) =<< mapFn
  | annId == externalAnnotationId && n == "filter"  = return . Just . (n,) =<< filterFn
  | annId == externalAnnotationId && n == "fold"    = return . Just . (n,) =<< foldFn
  | annId == externalAnnotationId && n == "groupBy" = return . Just . (n,) =<< groupByFn
  | annId == externalAnnotationId && n == "ext"     = return . Just . (n,) =<< extFn
  | annId == externalAnnotationId && n == "sort"    = return . Just . (n,) =<< sortFn
  
  | otherwise = providesError "lifted attribute" n

  where
        copy = copyCollection

        -- | Collection accessor implementation
        peekFn = valWithCollection $ \_ (Collection _ ds _) -> do 
          inner_val <- peekDS ds
          return $ VOption inner_val

        -- | Collection modifier implementation
        insertFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (insertCollection el)
        deleteFn = valWithCollectionMV $ \el cmv -> modifyCollection cmv (deleteCollection el)
        updateFn = valWithCollectionMV $ \old cmv -> vfun $ \new -> modifyCollection cmv (updateCollection old new)

        -- BREAKING EXCEPTION SAFETY
        modifyCollection :: MVar (Collection Value) 
                         -> (Collection Value -> Interpretation (Collection Value))
                         -> Interpretation Value
        --TODO modifyMVar_ function has to be over IO
        modifyCollection cmv f = do
            old_col <- liftIO $ readMVar cmv
            result <- f old_col
            --liftIO $ putMVar cmv result
            liftIO $ modifyMVar_ cmv (const $ return result)
            return vunit

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
        iterateFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction iterateFnError) f $
          \f' -> mapDS_ (withClosure f') ds >> return vunit

        -- | Collection transformer implementation
        combineFn = valWithCollection $ \other (Collection ns ds extId) ->
          flip (matchCollection collectionError) other $ 
            \(Collection _ ds' extId') ->
              if extId /= extId' then combineError
              else do 
                new_ds <- combineDS ds ds'
                copy $ Collection ns new_ds extId

        splitFn = valWithCollection $ \_ (Collection ns ds extId) -> do
            (l, r) <- splitDS ds
            lc <- copy (Collection ns l extId)
            rc <- copy (Collection ns r extId)
            return $ VTuple [lc, rc]
        
        -- Pass in the namespace
        mapFn = valWithCollection $ \f (Collection ns ds ext) ->
          flip (matchFunction mapFnError) f $ 
            \f'  -> mapDS (withClosure f') ds >>= 
            \ds' -> copy (Collection ns ds' ext)

        filterFn = valWithCollection $ \f (Collection ns ds extId) ->
          flip (matchFunction filterFnError) f $
            \f'  -> filterDS (\v -> withClosure f' v >>= matchBool filterValError) ds >>=
            \ds' -> copy (Collection ns ds' extId)

        foldFn = valWithCollection $ \f (Collection _ ds _) ->
          flip (matchFunction foldFnError) f $
            \f' -> vfun $ \accInit -> foldDS (curryFoldFn f') accInit ds

        curryFoldFn :: (IFunction, Closure Value, StableName IFunction) -> Value -> Value -> Interpretation Value
        curryFoldFn f' acc v = do
          result <- withClosure f' acc
          (matchFunction curryFnError) (flip withClosure v) result

        -- TODO: replace assoc lists with a hashmap.
        groupByFn = valWithCollection heres_the_answer
          where
            heres_the_answer :: Value -> Collection Value -> Interpretation Value
            heres_the_answer gb (Collection ns ds ext) = -- Am I passing the right namespace & stuff to the later collections?
              flip (matchFunction partitionFnError) gb $ \gb' -> vfun $ \f -> 
              flip (matchFunction foldFnError) f $ \f' -> vfun $ \accInit ->
                do
                  new_space <- emptyDS (Just ds)
                  kvRecords <- foldDS (groupByElement gb' f' accInit) new_space ds
                  -- TODO typecheck that collection
                  copy (Collection ns kvRecords ext)

        groupByElement :: (AssociativeDataspace (Interpretation) ads Value Value)
                       => (IFunction, Closure Value, StableName IFunction)
                       -> (IFunction, Closure Value, StableName IFunction)
                       -> Value -> ads -> Value -> Interpretation ads
        groupByElement gb' f' accInit acc v = do
          k <- withClosure gb' v
          look_result <- lookupKV acc k
          case look_result of
            Nothing         -> do
              val <- curryFoldFn f' accInit v 
              --tmp_ds <- emptyDS ()
              tmp_ds <- emptyDS (Just acc)
              tmp_ds <- insertKV tmp_ds k val
              combineDS acc tmp_ds
            Just partialAcc -> do 
              new_val <- curryFoldFn f' partialAcc v
              replaceKV acc k new_val

        extFn = valWithCollection $ \f (Collection _ ds _) -> 
          flip (matchFunction extError) f $ 
          \f' -> do
                val_ds <- mapDS (withClosure f') ds
                first_subcol <- peekDS val_ds
                case first_subcol of
                  Nothing -> extError -- Maybe not the right error here
                                      -- really, I should create an empty VCollection
                                      -- with the right type (that of f(elem)), but I
                                      -- don't have a value to copy the type out of
                  Just sub_val -> do
                    val_ds <- deleteDS sub_val val_ds
                    result <- foldDS (\acc val -> combine' acc (Just val)) (Just sub_val) val_ds
                    maybe combineError return result

        sortFn = valWithCollection $ \f (Collection ns ds ext) ->
          flip (matchFunction sortFnError) f $ 
            \f'  -> sortDS (sortResultAsOrdering f') ds >>= 
            \ds' -> copy (Collection ns ds' ext)

        sortResultAsOrdering (f, cl, sn) = \v1 v2 -> do
            f2V  <- withClosure (f, cl, sn) v1
            cmpV <- (matchFunction curryFnError) (flip withClosure v2) f2V
            intAsOrdering cmpV
          where intAsOrdering (VInt i) | i < 0     = return LT
                                       | i == 0    = return EQ
                                       | otherwise = return GT
                intAsOrdering _        = throwE $ RunTimeTypeError "Invalid sort comparator result"

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
        withClosure :: (IFunction, Closure Value, StableName IFunction) -> Value -> Interpretation Value
        withClosure (f, cl, _) arg = modifyE (cl ++) >> f arg >>= flip (foldM $ flip removeE) cl

        valWithCollection :: (Value -> Collection Value -> Interpretation Value) -> Interpretation Value
        valWithCollection f = vfun $ \arg -> 
          lookupE annotationSelfId >>= matchCollection collectionError (f arg)

        valWithCollectionMV :: (Value -> MVar (Collection Value) -> Interpretation Value) -> Interpretation Value
        valWithCollectionMV f = vfun $ \arg -> 
          lookupE annotationSelfId >>= matchCollectionMV collectionError (f arg)

        matchCollection :: Interpretation a -> (Collection Value -> Interpretation a) -> Value -> Interpretation a
        matchCollection _ f (VCollection cmv) = liftIO (readMVar cmv) >>= f
        matchCollection err _  _ =  err

        matchCollectionMV :: Interpretation a -> (MVar (Collection Value) -> Interpretation a) -> Value -> Interpretation a
        matchCollectionMV _ f (VCollection cmv) = f cmv
        matchCollectionMV err _ _ = err

        matchFunction :: a -> ((IFunction, Closure Value, StableName IFunction) -> a) -> Value -> a
        matchFunction _ f (VFunction f') = f f'
        matchFunction err _ _ = err

        matchBool :: Interpretation Bool -> Value -> Interpretation Bool
        matchBool _ (VBool b) = return b
        matchBool err _ = err

        collectionError  = throwE $ RunTimeTypeError "Invalid collection"
        combineError     = throwE $ RunTimeTypeError "Mismatched collection types for combine"
        iterateFnError   = throwE $ RunTimeTypeError "Invalid iterate function"
        mapFnError       = throwE $ RunTimeTypeError "Invalid map function"
        filterFnError    = throwE $ RunTimeTypeError "Invalid filter function"
        filterValError   = throwE $ RunTimeTypeError "Invalid filter function result"
        foldFnError      = throwE $ RunTimeTypeError "Invalid fold function"
        partitionFnError = throwE $ RunTimeTypeError "Invalid grouping function"
        curryFnError     = throwE $ RunTimeTypeError "Invalid curried function"
        extError         = throwE $ RunTimeTypeError "Invalid function argument for ext"
        sortFnError      = throwE $ RunTimeTypeError "Invalid sort function"
        
builtinAttribute :: Identifier -> Identifier -> K3 Type -> UID
                 -> Interpretation (Maybe (Identifier, Value))
builtinAttribute _ n _ _ = providesError "attribute" n

