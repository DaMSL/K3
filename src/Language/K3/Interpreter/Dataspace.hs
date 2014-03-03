{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module Language.K3.Interpreter.Dataspace where

import Control.Monad
import Data.List

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors
import Language.K3.Interpreter.Values()

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
  splitDS l        = return $ if length l <= threshold then (l, []) else splitAt (length l `quot` 2) l
    where threshold = 10

  -- | Sort a list dataspace.
  --   Since the comparator can have side effects, we must write our own sorting algorithm.
  --   TODO: improve performance.
  sortDS _ []     = return []
  sortDS _ [x]    = return [x]
  sortDS cmpF ls  = do
    let (a, b) = splitAt (length ls `quot` 2) ls
    a' <- sortDS cmpF a
    b' <- sortDS cmpF b
    merge a' b'
    where merge [] bs = return bs
          merge as [] = return as
          merge (a:as) (b:bs) = cmpF a b >>= \case
              LT -> return . (a:) =<< merge as (b:bs)
              _  -> return . (b:) =<< merge (a:as) bs

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
  sortDS           = sortFile liftEngine

-- | Splice in chained dataspace instance for the CollectionDataspace
$(dsChainInstanceGenerator
    [t|Interpretation|] [t|CollectionDataspace Value|] [t|Value|]
    [| (throwE . RunTimeInterpretationError) |]
    [("InMemoryDS", "lst"), ("ExternalDS", "f")] "InMemoryDS")

{-
instance Dataspace Interpretation (CollectionDataspace Value) Value where
  emptyDS Nothing                 = (emptyDS Nothing)    >>= return . InMemoryDS
  emptyDS (Just (InMemoryDS lst)) = (emptyDS (Just lst)) >>= return . InMemoryDS
  emptyDS (Just (ExternalDS f))   = (emptyDS (Just f))   >>= return . ExternalDS

  initialDS vals Nothing                 = (initialDS vals Nothing)    >>= return . InMemoryDS
  initialDS vals (Just (InMemoryDS lst)) = (initialDS vals (Just lst)) >>= return . InMemoryDS
  initialDS vals (Just (ExternalDS f))   = (initialDS vals (Just f))   >>= return . ExternalDS

  copyDS (InMemoryDS lst) = copyDS lst >>= return . InMemoryDS 
  copyDS (ExternalDS f)   = copyDS f   >>= return . ExternalDS

  peekDS (InMemoryDS lst) = peekDS lst
  peekDS (ExternalDS f)   = peekDS f

  insertDS (InMemoryDS lst) val = insertDS lst val >>= return . InMemoryDS 
  insertDS (ExternalDS f)   val = insertDS f   val >>= return . ExternalDS

  deleteDS val (InMemoryDS lst) = deleteDS val lst >>= return . InMemoryDS 
  deleteDS val (ExternalDS f)   = deleteDS val f   >>= return . ExternalDS

  updateDS v v' (InMemoryDS lst) = updateDS v v' lst >>= return . InMemoryDS 
  updateDS v v' (ExternalDS f)   = updateDS v v' f   >>= return . ExternalDS

  foldDS acc acc_init (InMemoryDS lst) = foldDS acc acc_init lst
  foldDS acc acc_init (ExternalDS f)   = foldDS acc acc_init f

  mapDS func (InMemoryDS lst) = mapDS func lst >>= return . InMemoryDS 
  mapDS func (ExternalDS f)   = mapDS func f   >>= return . ExternalDS

  mapDS_ func (InMemoryDS lst) = mapDS_ func lst
  mapDS_ func (ExternalDS f)   = mapDS_ func f

  filterDS func (InMemoryDS lst) = filterDS func lst >>= return . InMemoryDS 
  filterDS func (ExternalDS f)   = filterDS func f   >>= return . ExternalDS

  combineDS (InMemoryDS l_lst) (InMemoryDS r_lst) = combineDS l_lst r_lst >>= return . InMemoryDS 
  combineDS (ExternalDS l_f)   (ExternalDS r_f)   = combineDS l_f   r_f   >>= return . ExternalDS
  combineDS _ _ = throwE $ RunTimeInterpretationError "Mismatched collection types in combine"
    -- TODO Could combine an in memory store and an external store into an external store

  splitDS (InMemoryDS lst) = splitDS lst >>= \(l,r) -> return (InMemoryDS l, InMemoryDS r)
  splitDS (ExternalDS f)   = splitDS f   >>= \(l,r) -> return (ExternalDS l, ExternalDS r)

  sortDS sortF (InMemoryDS lst) = sortDS sortF lst >>= return . InMemoryDS
  sortDS sortF (ExternalDS f)   = sortDS sortF f   >>= return . ExternalDS
-}


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
