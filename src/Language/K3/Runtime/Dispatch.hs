-- | Generic mechanism for dispatch stateful actions against multiple indexed peers.
module Language.K3.Runtime.Dispatch where

import Data.Functor

import Control.Monad.State
import Control.Monad.Trans.Maybe

-- | A dispatch action is a stateful sub-action dispatched against an indexed entity. The
-- modifications to indexed state are saved, for the next sub-action against that index.
type DispatchT i s m = MaybeT (StateT [(i, s)] m)

-- | Run a dispatch action.
runDispatchT :: DispatchT i s m a -> [(i, s)] -> m (Maybe a, [(i, s)])
runDispatchT d s = flip runStateT s $ runMaybeT d

-- | Dispatch a stateful action against a speficied index.
dispatch :: (Eq i, Functor m, Monad m) => i -> (s -> m (a, s)) -> DispatchT i s m a
dispatch i f = do
    s <- lookup i <$> get
    flip (maybe mzero) s $ \q -> do
        (a', s') <- lift . lift $ f q
        modify $ replace (i, s')
        return a'

-- | Dispatch a stateful action against all available indices.
dispatchAll :: (Eq i, Functor m, Monad m) => (s -> m (a, s)) -> DispatchT i s m [a]
dispatchAll f = do
    targets <- fst . unzip <$> get
    mapM (flip dispatch f) targets

-- | Replace an entry in an associative list.
replace :: Eq a => (a, b) -> [(a, b)] -> [(a, b)]
replace (x, y) xys = w ++ [(x, y)] ++ z where (w, z) = fmap (drop 1) $ break ((== x) . fst) xys
