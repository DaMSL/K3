{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Language.K3.Runtime.Dataspace (
  Dataspace,
  emptyDS,
  initialDS,
  copyDS,
  peekDS,
  insertDS,
  deleteDS,
  updateDS,
  foldDS,
  mapDS,
  mapDS_,
  filterDS,
  combineDS,
  splitDS,
  sortDS,
  
  EmbeddedKV,
  extractKey,
  embedKey,

  AssociativeDataspace,
  lookupKV,
  removeKV,
  insertKV,
  replaceKV,
) where

-- (move the instances to Interpreter/IDataspace.hs)
class (Monad m) => Dataspace m ds v | ds -> v where
  -- The Maybe ds arguemnt to constructors is a hint about which kind of
  -- dataspace to construct
  emptyDS       :: Maybe ds -> m ds
  initialDS     :: [v] -> Maybe ds -> m ds
  copyDS        :: ds -> m ds
  peekDS        :: ds -> m (Maybe v)
  insertDS      :: ds -> v -> m ds
  deleteDS      :: v -> ds -> m ds
  updateDS      :: v -> v -> ds -> m ds
  foldDS        :: ( a -> v -> m a ) -> a -> ds -> m a
  mapDS         :: ( v -> m v ) -> ds -> m ds
  mapDS_        :: ( v -> m v ) -> ds -> m ()
  filterDS      :: ( v -> m Bool ) -> ds -> m ds
  combineDS     :: ds -> ds -> m ds
  splitDS       :: ds -> m (ds, ds)
  sortDS        :: ( v -> v -> m Ordering ) -> ds -> m ds
{- casting? -}

class (Monad m) => EmbeddedKV m v k where
  extractKey :: v -> m k
  embedKey   :: k -> v -> m v

class (Monad m, Dataspace m ds v) => AssociativeDataspace m ds k v where
  lookupKV       :: ds -> k -> m (Maybe v)
  removeKV       :: ds -> k -> v -> m ds
  insertKV       :: ds -> k -> v -> m ds
  replaceKV      :: ds -> k -> v -> m ds

