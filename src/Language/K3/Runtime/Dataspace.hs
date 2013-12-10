{-# LANGUAGE MultiParamTypeClasses #-}

module Language.K3.Runtime.Dataspace (
  Dataspace,
  newDS,
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
  
  EmbeddedKV,
  extractKey,
  embedKey,

  AssociativeDataspace,
  lookupKV,
  removeKV,
  insertKV,
  replaceKV,
) where

{- TODO take out dependence on Interpretation (-> m)
 - move the typeclasses to Runtime/Dataspace.hs
 - (move the instances to Interpreter/IDataspace.hs)
 -}
class (Monad m) => Dataspace m ds v where
  newDS         :: ds -> v -> m ds -- this is bad?
  -- rename to emptyDS
  initialDS     :: [v] -> m ds
  copyDS        :: ds -> v -> m ds
  -- ideal:
  -- newDS      :: () -> m ds
  -- copyDS     :: ds -> m ds
  -- initialDS  :: [v] -> m ds
  peekDS        :: ds -> m (Maybe v)
  insertDS      :: ds -> v -> m ds
  deleteDS      :: v -> ds -> m ds
  updateDS      :: v -> v -> ds -> m ds
  foldDS        :: ( a -> v -> m a ) -> a -> ds -> m a
  mapDS         :: ( v -> m v ) -> ds -> m ds
  mapDS_        :: ( v -> m v ) -> ds -> m ()
  filterDS      :: ( v -> m Bool ) -> ds -> m ds
  combineDS     :: ds -> ds -> v -> m ds
  splitDS       :: ds -> v -> m (ds, ds)
{- casting? -}

-- TODO monads
class (Monad m) => EmbeddedKV m v k where
  extractKey :: v -> m k
  embedKey   :: k -> v -> m v

{- move embed / extract into its own typeclass -}
class (Monad m, Dataspace m ds v) => AssociativeDataspace m ds k v where
  lookupKV       :: ds -> k -> m (Maybe v)
  removeKV       :: ds -> k -> v -> m ds
  insertKV       :: ds -> k -> v -> m ds
  replaceKV      :: ds -> k -> v -> m ds

