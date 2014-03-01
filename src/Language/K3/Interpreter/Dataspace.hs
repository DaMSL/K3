{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Language.K3.Interpreter.Dataspace where

import Control.Monad
import Data.List

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values

import Language.K3.Runtime.Dataspace
import Language.K3.Runtime.FileDataspace

{- Dataspaces -}

{- generalize on value, move to Runtime/Dataspace.hs -}
instance (Monad m) => Dataspace m [Value] Value where
  emptyDS _        = return []
  initialDS vals _ = return vals
  copyDS ls        = return ls
  peekDS ls        = case ls of
    []    -> return Nothing
    (h:_) -> return $ Just h
  insertDS ls v    = return $ ls ++ [v]
  deleteDS v ls    = return $ delete v ls
  updateDS v v' ls = return $ (delete v ls) ++ [v']
  foldDS           = foldM
  mapDS            = mapM
  mapDS_           = mapM_
  filterDS         = filterM
  combineDS l r    = return $ l ++ r
  splitDS l        = return $ if length l <= threshold then (l, []) else splitAt (length l `div` 2) l
    where
      threshold = 10

{- TODO have the engine delete all the collection files in it's cleanup
 - generalize Value -> b (in EngineM b), and monad -> engine
 -}
instance Dataspace Interpretation (FileDataspace Value) Value where
  emptyDS _        = liftEngine $ emptyFile ()
  initialDS vals _ = initialFile liftEngine vals
  copyDS old_id    = liftEngine $ copyFile old_id
  peekDS ext_id    = peekFile liftEngine ext_id
  insertDS         = insertFile liftEngine
  deleteDS         = deleteFile liftEngine
  updateDS         = updateFile liftEngine
  foldDS           = foldFile liftEngine
  -- Takes the file id of an external collection, then runs the second argument on
  -- each argument, then returns the identifier of the new collection
  mapDS            = mapFile liftEngine
  mapDS_           = mapFile_ liftEngine
  filterDS         = filterFile liftEngine
  combineDS        = combineFile liftEngine
  splitDS          = splitFile liftEngine

instance Dataspace Interpretation (CollectionDataspace Value) Value where
  emptyDS maybeHint =
    case maybeHint of
      Nothing ->
        (emptyDS Nothing) >>= return . InMemoryDS
      Just (InMemoryDS ls) ->
        (emptyDS (Just ls)) >>= return . InMemoryDS
      Just (ExternalDS ext) ->
        (emptyDS (Just ext)) >>= return . ExternalDS
  initialDS vals maybeHint =
    case maybeHint of
      Nothing ->
        (initialDS vals Nothing) >>= return . InMemoryDS
      Just (InMemoryDS ls) ->
        (initialDS vals (Just ls)) >>= return . InMemoryDS
      Just (ExternalDS ext) ->
        (initialDS vals (Just ext)) >>= return . ExternalDS
  copyDS ds        = case ds of
    InMemoryDS lst -> copyDS lst >>= return . InMemoryDS
    ExternalDS f   -> copyDS f >>= return . ExternalDS
  peekDS ds        = case ds of
    InMemoryDS lst -> peekDS lst
    ExternalDS f   -> peekDS f
  insertDS ds val  = case ds of
    InMemoryDS lst -> insertDS lst val >>= return . InMemoryDS
    ExternalDS f   -> insertDS f   val >>= return . ExternalDS
  deleteDS val ds  = case ds of
    InMemoryDS lst -> deleteDS val lst >>= return . InMemoryDS
    ExternalDS f   -> deleteDS val f   >>= return . ExternalDS
  updateDS v v' ds  = case ds of
    InMemoryDS lst -> updateDS v v' lst >>= return . InMemoryDS
    ExternalDS f   -> updateDS v v' f   >>= return . ExternalDS
  foldDS acc acc_init ds = case ds of
    InMemoryDS lst -> foldDS acc acc_init lst
    ExternalDS f   -> foldDS acc acc_init f
  mapDS func ds     = case ds of
    InMemoryDS lst -> mapDS func lst >>= return . InMemoryDS
    ExternalDS f   -> mapDS func f   >>= return . ExternalDS
  mapDS_ func ds    = case ds of
    InMemoryDS lst -> mapDS_ func lst
    ExternalDS f   -> mapDS_ func f
  filterDS func ds     = case ds of
    InMemoryDS lst -> filterDS func lst >>= return . InMemoryDS
    ExternalDS f   -> filterDS func f   >>= return . ExternalDS
  combineDS l r     = case (l,r) of
    (InMemoryDS l_lst, InMemoryDS r_lst) ->
      combineDS l_lst r_lst >>= return . InMemoryDS
    (ExternalDS l_f, ExternalDS r_f) ->
      combineDS l_f r_f >>= return . ExternalDS
    _ -> throwE $ RunTimeInterpretationError "Mismatched collection types in combine"
        -- TODO Could combine an in memory store and an external store into an external store
  splitDS ds       = case ds of
    InMemoryDS lst -> splitDS lst >>= \(l, r) -> return (InMemoryDS l, InMemoryDS r)
    ExternalDS f   -> splitDS f   >>= \(l, r) -> return (ExternalDS l, ExternalDS r)

{- moves to Runtime/Dataspace.hs -}
matchPair :: Value -> Interpretation (Value, Value)
matchPair v =
  case v of
    VTuple (h:t) -> 
      case t of
        (p:[])    -> return (h, p)
        _         -> throwE $ RunTimeTypeError "Wrong number of elements in tuple; expected a pair"
    _ -> throwE $ RunTimeTypeError "Non-tuple"

-- TODO kill dependence on Interpretation for error handling
instance EmbeddedKV Interpretation Value Value where
  extractKey value = do
    (key, _) <- matchPair value
    return key
  embedKey key value = return $ VTuple [key, value]
  
instance (Dataspace Interpretation dst Value) => AssociativeDataspace Interpretation dst Value Value where
  lookupKV ds key = 
    foldDS (\result cur_val ->  do
      (cur_key, cur_val) <- matchPair cur_val
      return $ case result of
        Nothing -> if cur_key == key then Just cur_val else Nothing
        Just val -> Just val
     ) Nothing ds
  removeKV ds key _ =
    filterDS (inner) ds
     where
        inner :: Value -> Interpretation Bool
        inner val = do
          cur_val <- extractKey val
          return $ if cur_val == key then False else True
  insertKV ds key value = embedKey key value >>= insertDS ds
  replaceKV ds k v = do
    _ <- removeKV ds k vunit
    insertKV ds k v