{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}

module Language.K3.Interpreter.Dataspace where

import Control.Monad

import Data.Function
import Data.List
import qualified Data.List.Ordered as OrdList ( unionBy )
import Data.List.Ordered

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
  splitDS l        = return $ if length l <= threshold then (l, []) else splitAt (length l `quot` 2) l
    where threshold = 10

instance (Monad m) => SequentialDataspace m [Value] Value where
  -- | Sort a list dataspace.
  --   Since the comparator can have side effects, we must write our own sorting algorithm.
  --   TODO: improve performance.
  sortDS _ []     = return []
  sortDS _ [x]    = return [x]
  sortDS cmpF ls  = do
    let (a, b) = splitAt (length ls `quot` 2) ls
    a' <- sortDS cmpF a
    b' <- sortDS cmpF b
    mergeLM a' b'
    where mergeLM [] bs = return bs
          mergeLM as [] = return as
          mergeLM (a:as) (b:bs) = cmpF a b >>= \case
              LT -> return . (a:) =<< mergeLM as (b:bs)
              _  -> return . (b:) =<< mergeLM (a:as) bs


{- In-memory dataspace instances -}

instance (Monad m) => Dataspace m (ListMDS Value) Value where
  emptyDS _                  = return $ ListMDS []
  initialDS vals _           = return $ ListMDS vals
  copyDS (ListMDS ls)        = return $ ListMDS ls

  peekDS (ListMDS [])        = return Nothing
  peekDS (ListMDS (h:_))     = return $ Just h

  insertDS (ListMDS ls) v    = return $ ListMDS $ ls ++ [v]
  deleteDS v (ListMDS ls)    = return $ ListMDS $ delete v ls
  updateDS v v' (ListMDS ls) = return $ ListMDS $ (delete v ls) ++ [v']

  foldDS acc acc_init (ListMDS ls) = foldM acc acc_init ls
  mapDS f (ListMDS ls)             = return . ListMDS =<< mapM f ls
  mapDS_  f (ListMDS ls)           = mapM_ f ls
  filterDS  f (ListMDS ls)         = return . ListMDS =<< filterM f ls

  combineDS (ListMDS l) (ListMDS r) = return $ ListMDS $ l ++ r

  splitDS (ListMDS l) = return $ uncurry ((,) `on` ListMDS) $
      if length l <= threshold then (l, [])
                               else splitAt (length l `quot` 2) l
    where threshold = 10

instance (Monad m) => SequentialDataspace m (ListMDS Value) Value where
  -- | Sort a list dataspace.
  --   Since the comparator can have side effects, we must write our own sorting algorithm.
  --   TODO: improve performance.
  sortDS _ (ListMDS [])     = return $ ListMDS []
  sortDS _ (ListMDS [x])    = return $ ListMDS [x]
  sortDS cmpF (ListMDS ls)  = do
    let (a, b) = splitAt (length ls `quot` 2) ls
    (ListMDS a') <- sortDS cmpF $ ListMDS a
    (ListMDS b') <- sortDS cmpF $ ListMDS b
    return . ListMDS =<< mergeLM a' b'
    where mergeLM [] bs = return bs
          mergeLM as [] = return as
          mergeLM (a:as) (b:bs) = cmpF a b >>= \case
              LT -> return . (a:) =<< mergeLM as (b:bs)
              _  -> return . (b:) =<< mergeLM (a:as) bs


-- | Set dataspace
instance OrdM Interpretation Value where
  compareV v1 v2 = valueCompare v1 v2 >>= \(VInt sgn) -> return $
    if sgn < 0 then LT else if sgn == 0 then EQ else GT

instance (Monad m) => Dataspace m (SetAsOrdListMDS Value) Value where
  emptyDS _                      = return $ SetAsOrdListMDS []
  initialDS vals _               = return $ SetAsOrdListMDS vals
  copyDS (SetAsOrdListMDS ls)    = return $ SetAsOrdListMDS ls

  peekDS (SetAsOrdListMDS [])    = return Nothing
  peekDS (SetAsOrdListMDS (h:_)) = return $ Just h

  -- TODO: implement compare for VCollections or use compareV
  insertDS (SetAsOrdListMDS ls) v    = return $ SetAsOrdListMDS $ insertSetBy compare v ls
  deleteDS v (SetAsOrdListMDS ls)    = return $ SetAsOrdListMDS $ deleteBy (\v1 v2 -> compare v1 v2 == EQ) v ls
  updateDS v v' (SetAsOrdListMDS ls) = return $ SetAsOrdListMDS $ insertSetBy compare v' (deleteBy (\v1 v2 -> compare v1 v2 == EQ) v ls)

  foldDS acc acc_init (SetAsOrdListMDS ls) = foldM acc acc_init ls
  mapDS f (SetAsOrdListMDS ls)             = return . SetAsOrdListMDS =<< mapM f ls
  mapDS_  f (SetAsOrdListMDS ls)           = mapM_ f ls
  filterDS  f (SetAsOrdListMDS ls)         = return . SetAsOrdListMDS =<< filterM f ls

  combineDS (SetAsOrdListMDS l) (SetAsOrdListMDS r) = return $ SetAsOrdListMDS $ OrdList.unionBy compare l r

  splitDS (SetAsOrdListMDS l) = return $ uncurry ((,) `on` SetAsOrdListMDS) $
      if length l <= threshold then (l, [])
                               else splitAt (length l `quot` 2) l
    where threshold = 10


instance SetDataspace Interpretation (SetAsOrdListMDS Value) Value where

  memberDS v (SetAsOrdListMDS ls) = return $ memberBy compare v ls
  isSubsetOfDS (SetAsOrdListMDS ls) (SetAsOrdListMDS rs) = return $ subsetBy compare ls rs

  unionDS = combineDS

  intersectDS (SetAsOrdListMDS ls) (SetAsOrdListMDS rs) = return . SetAsOrdListMDS $ isectBy compare ls rs
  differenceDS (SetAsOrdListMDS ls) (SetAsOrdListMDS rs)   = return . SetAsOrdListMDS $ minusBy compare ls rs


instance SortedDataspace Interpretation (SetAsOrdListMDS Value) Value where

  minDS (SetAsOrdListMDS []) = return Nothing
  minDS (SetAsOrdListMDS ls) = return . Just $ head ls

  maxDS (SetAsOrdListMDS []) = return Nothing
  maxDS (SetAsOrdListMDS ls) = return . Just $ last ls

  -- TODO: implement Ord.compare for VCollections or use compareV
  lowerBoundDS v (SetAsOrdListMDS ls) =
    case partition (\v2 -> compare v2 v == LT) ls of
      ([],_) -> return Nothing
      (x,_)  -> return . Just $ last x

  upperBoundDS v (SetAsOrdListMDS ls) =
    case partition (\v2 -> compare v2 v == GT) ls of
      ([],_) -> return Nothing
      (x,_)  -> return . Just $ last x

  sliceDS lowerV upperV (SetAsOrdListMDS ls) =
    return . SetAsOrdListMDS . fst $
      partition (\v -> all (`elem` [LT,EQ]) $ [compare lowerV v, compare v upperV]) ls


-- | Bag dataspace
instance (Monad m) => Dataspace m (BagAsOrdListMDS Value) Value where
  emptyDS _                      = return $ BagAsOrdListMDS []
  initialDS vals _               = return $ BagAsOrdListMDS vals
  copyDS (BagAsOrdListMDS ls)    = return $ BagAsOrdListMDS ls

  peekDS (BagAsOrdListMDS [])    = return Nothing
  peekDS (BagAsOrdListMDS (h:_)) = return $ Just h

  -- TODO: implement Ord.compare for VCollections or use compareV
  insertDS (BagAsOrdListMDS ls) v    = return $ BagAsOrdListMDS $ insertBagBy compare v ls
  deleteDS v (BagAsOrdListMDS ls)    = return $ BagAsOrdListMDS $ deleteBy (\v1 v2 -> compare v1 v2 == EQ) v ls
  updateDS v v' (BagAsOrdListMDS ls) = return $ BagAsOrdListMDS $ insertBagBy compare v' (deleteBy (\v1 v2 -> compare v1 v2 == EQ) v ls)

  foldDS acc acc_init (BagAsOrdListMDS ls) = foldM acc acc_init ls
  mapDS f (BagAsOrdListMDS ls)             = return . BagAsOrdListMDS =<< mapM f ls
  mapDS_  f (BagAsOrdListMDS ls)           = mapM_ f ls
  filterDS  f (BagAsOrdListMDS ls)         = return . BagAsOrdListMDS =<< filterM f ls

  combineDS (BagAsOrdListMDS l) (BagAsOrdListMDS r) = return $ BagAsOrdListMDS $ mergeBy compare l r

  splitDS (BagAsOrdListMDS l) = return $ uncurry ((,) `on` BagAsOrdListMDS) $
      if length l <= threshold then (l, [])
                               else splitAt (length l `quot` 2) l
    where threshold = 10

-- | Set-like interface with bag semantics
instance SetDataspace Interpretation (BagAsOrdListMDS Value) Value where

  memberDS v (BagAsOrdListMDS ls) = return $ memberBy compare v ls
  isSubsetOfDS (BagAsOrdListMDS ls) (BagAsOrdListMDS rs) = return $ subsetBy compare ls rs

  unionDS = combineDS

  intersectDS (BagAsOrdListMDS ls) (BagAsOrdListMDS rs) = return . BagAsOrdListMDS $ isectBy compare ls rs
  differenceDS (BagAsOrdListMDS ls) (BagAsOrdListMDS rs)   = return . BagAsOrdListMDS $ minusBy compare ls rs


instance SortedDataspace Interpretation (BagAsOrdListMDS Value) Value where

  minDS (BagAsOrdListMDS []) = return Nothing
  minDS (BagAsOrdListMDS ls) = return . Just $ head ls

  maxDS (BagAsOrdListMDS []) = return Nothing
  maxDS (BagAsOrdListMDS ls) = return . Just $ last ls

  -- TODO: implement compare for VCollections or use compareV
  lowerBoundDS v (BagAsOrdListMDS ls) =
    case partition (\v2 -> compare v2 v == LT) ls of
      ([],_) -> return Nothing
      (x,_)  -> return . Just $ last x

  upperBoundDS v (BagAsOrdListMDS ls) =
    case partition (\v2 -> compare v2 v == GT) ls of
      ([],_) -> return Nothing
      (x,_)  -> return . Just $ last x

  sliceDS lowerV upperV (BagAsOrdListMDS ls) =
    return . BagAsOrdListMDS . fst $
      partition (\v -> all (`elem` [LT,EQ]) $ [compare lowerV v, compare v upperV]) ls


-- | Splice in chained dataspace and sequential dataspace instances for the PrimitiveMDS
$(dsChainInstanceGenerator
  [| (throwE . RunTimeInterpretationError) |]
  [ ([t|Dataspace Interpretation (PrimitiveMDS Value) Value|]
    , "Dataspace"
    , [("MemDS", "lst"), ("SeqDS", "lst"), ("SetDS", "lst"), ("SortedDS", "lst")]
    , "MemDS", False)

  , ([t|SequentialDataspace Interpretation (PrimitiveMDS Value) Value|]
    , "SequentialDataspace"
    , [("SeqDS", "lst")]
    , "SeqDS", True)

  , ([t|SetDataspace Interpretation (PrimitiveMDS Value) Value|]
    , "SetDataspace"
    , [("SetDS", "lst"), ("SortedDS", "lst")]
    , "SetDS", True)

  , ([t|SortedDataspace Interpretation (PrimitiveMDS Value) Value|]
    , "SortedDataspace"
    , [("SetDS", "lst"), ("SortedDS", "lst")]
    , "SortedDS", True)
  ])


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

instance SequentialDataspace Interpretation (FileDataspace Value) Value where
  sortDS = sortFile liftEngine

-- | Splice in chained dataspace and sequential dataspace instances for the PrimitiveMDS
$(dsChainInstanceGenerator
  [| (throwE . RunTimeInterpretationError) |]
  [ ([t|Dataspace Interpretation (CollectionDataspace Value) Value|]
    , "Dataspace"
    , [("InMemoryDS", "lst"), ("ExternalDS", "f"), ("InMemDS", "mds")]
    , "InMemoryDS", False)

  , ([t|SequentialDataspace Interpretation (CollectionDataspace Value) Value|]
    , "SequentialDataspace"
    , [("InMemoryDS", "lst"), ("ExternalDS", "f"), ("InMemDS", "mds")]
    , "InMemoryDS", False)

  , ([t|SetDataspace Interpretation (CollectionDataspace Value) Value|]
    , "SetDataspace"
    , [("InMemDS", "mds")]
    , "InMemDS", True)

  , ([t|SortedDataspace Interpretation (CollectionDataspace Value) Value|]
    , "SortedDataspace"
    , [("InMemDS", "mds")]
    , "InMemDS", True)
  ])


{- moves to Runtime/Dataspace.hs -}
matchPair :: Value -> Interpretation (Value, Value)
matchPair v@(VRecord nb) = case membersToList nb of
    [(_, (v1, _)), (_, (v2, _))] -> return (v1, v2)
    _ -> throwE $ RunTimeTypeError $ "Expected a key/value record, but got " ++ show v

matchPair x = throwE $ RunTimeTypeError $ "Expected a key/value record, but got " ++ show x

-- TODO kill dependence on Interpretation for error handling
instance EmbeddedKV Interpretation Value Value where
  extractKey value = matchPair value >>= return . fst
  embedKey key value = return . VRecord $ membersFromList [("key", (key, MemImmut)), ("value", (value, MemImmut))]

instance (Dataspace Interpretation dst Value) => AssociativeDataspace Interpretation dst Value Value where
  lookupKV ds key =
    foldDS (\result cur_val ->  do
      (match_key, match_val) <- matchPair cur_val
      return $ case result of
        Nothing -> if match_key == key then Just match_val else Nothing
        Just val -> Just val
     ) Nothing ds

  removeKV ds key _ = filterDS inner ds
     where
        inner :: Value -> Interpretation Bool
        inner val = do
          cur_val <- extractKey val
          return $ if cur_val == key then False else True

  insertKV ds key value = embedKey key value >>= insertDS ds

  replaceKV ds k v = do
    ds' <- removeKV ds k vunit
    insertKV ds' k v
