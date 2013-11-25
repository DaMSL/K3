{-# LANGUAGE MultiParamTypeClasses #-}

module Language.K3.Runtime.Dataspace (
  Dataspace,

  newDS,
  copyDataspace,
  peekDS,
  insertDS,
  deleteDS,
  updateDS,
  foldDS,
  mapDS,
  mapDS_,
  filterDS,
  combineDS,
  splitDS
) where

{- TODO take out dependence on Interpretation (-> m)
 - move the typeclasses to Runtime/Dataspace.hs
 - (move the instances to Interpreter/IDataspace.hs)
 -}
class (Monad m) => Dataspace m ds v where
  newDS         :: ds -> v -> m ds -- this is bad?
  copyDataspace :: ds -> v -> m ds
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
