module Language.K3.Utils.Conditional
( unlessM
, whenJust
, whenLeft
, whenLeftM
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

{-# INLINE whenLeft #-}
whenLeft :: (Monad m) => Either a () -> (a -> m ()) -> m ()
whenLeft (Left x) f = f x
whenLeft (Right _) _ = return ()

{-# INLINE whenLeftM #-}
whenLeftM :: (Monad m) => m (Either a ()) -> (a -> m ()) -> m ()
whenLeftM me f = join $ whenLeft `liftM` me `ap` return f
