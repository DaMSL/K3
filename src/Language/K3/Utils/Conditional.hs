module Language.K3.Utils.Conditional
( unlessM
, whenJust
) where

import Control.Applicative
import Control.Monad

{-# INLINE unlessM #-}
unlessM :: (Applicative m, Functor m, Monad m) => m Bool -> m () -> m ()
unlessM c a = join $ unless <$> c <*> return a

{-# INLINE whenJust #-}
whenJust :: (Monad m) => Maybe a -> (a -> m ()) -> m ()
whenJust Nothing _ = return ()
whenJust (Just x) f = f x
