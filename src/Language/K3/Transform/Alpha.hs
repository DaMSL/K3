{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE NamedFieldPuns #-}

-- | A transformation pass to perform α-renaming over a K3 program.
module Language.K3.Transform.Alpha (Alpha(..), runAlpha) where

import Data.Maybe
import Data.Tree

import Control.Applicative
import Control.Monad.Reader

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression

-- | The renaming environment consists of a supply of unique identifiers (integers), and a mapping
-- between old and new names.
type Store = (Integer, [(Identifier, Identifier)])

-- | Defines the class of tags which permit α-renaming over their corresponding annotated trees.
class Alpha a where
    alpha :: K3 a -> Reader Store (K3 a)

-- | Run α-renaming over an annotated K3 tree.
runAlpha :: Alpha a => K3 a -> K3 a
runAlpha = flip runReader (0, []) . alpha

-- | Lookup an identifier and produce the bound name. Is partial, use only when the name is already
-- bound (within a @local (genSym i)@.
nameOf :: Identifier -> Reader Store Identifier
nameOf i = (fromJust . lookup i) <$> asks snd

-- | Generate a new name for an identifier, and put it in the store.
genSym :: Identifier -> Store -> Store
genSym i (c, ms) = (c + 1, (i, show c):ms)

-- | Expressions can be classified into three types for the purposes of α-renaming: bind-points,
-- use-points, and everything else.
--
-- Bind-points perform the following steps:
--
--      1. Rename any pre-bind expressions.
--
--      2. Introduce the current binding into the environment.
--
--      3. Rename any post-bind expressions, with the new bindings.
--
--      4. Construct the new expression, with the new bindings.
--
--  Use-points perform the following steps:
--
--      1. Replace usage with new binding.
--
--      2. Store original name as an annotation for posterity.
--
--  Every other type of expression doesn't care; they just delegate renaming to their children.
instance Alpha Expression where

    -- | Bind-Point.
    alpha (Node (ELambda i :@: as) [b]) = local (genSym i) $ do
        i' <- nameOf i
        b' <- alpha b
        return $ Node (ELambda i' :@: as) [b']

    -- | Bind-Point.
    alpha (Node (ELetIn i :@: as) [e, b]) = do
        e' <- alpha e
        (i', b') <- local (genSym i) $ do
            i'' <- nameOf i
            b'' <- alpha b
            return (i'', b'')
        return $ Node (ELetIn i' :@: as) [e', b']

    -- | Bind-Point.
    alpha (Node (ECaseOf i :@: as) [e, s, n]) = do
        e' <- alpha e
        (i', s', n') <- local (genSym i) $ do
            i'' <- nameOf i
            s'' <- alpha s
            n'' <- alpha n
            return (i'', s'', n'')
        return $ Node (ECaseOf i' :@: as) [e', s', n']

    -- | Bind-Point. @EBindAs@ may potentially introduce multiple bindings, so we need to handle all
    -- the different binder forms.
    alpha (Node (EBindAs b :@: as) [e, d]) = do
        e' <- alpha e
        (b', d') <- local (genSymB b) $ do
            b'' <- nameOfB b
            d'' <- alpha d
            return (b'', d'')
        return $ Node (EBindAs b' :@: as) [e', d']
      where
        genSymB :: Binder -> Store -> Store
        genSymB (BIndirection i) s = genSym i s
        genSymB (BTuple is) s = foldr genSym s is
        genSymB (BRecord iis) s = foldr genSym s (snd $ unzip iis)

        nameOfB :: Binder -> Reader Store Binder
        nameOfB (BIndirection i) = return . BIndirection =<< nameOf i
        nameOfB (BTuple is) = return . BTuple . sequence =<< mapM nameOf is
        nameOfB (BRecord (unzip -> (fs, is))) = return . BRecord . zip fs . sequence =<< mapM nameOf is

    -- | Usage-Point.
    alpha e@(tag -> EVariable i) =
        nameOf i >>= \i' -> return $ e { rootLabel = (EVariable i' :@: annotations e) } @+ (ESyntax $ LexicalName i)

    -- | Every other expression.
    alpha e = do
        children <- mapM alpha (subForest e)
        return $ e { subForest = children }
