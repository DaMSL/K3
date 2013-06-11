{-# LANGUAGE FlexibleContexts #-}

-- | Pretty Printing for K3 Trees.
module Language.K3.Pretty (pretty) where

import Data.Tree

import Language.K3.Core.Annotation

-- | Pretty-Print a K3 syntax tree.
pretty :: (Show a, Show (Annotation a)) => K3 a -> String
pretty = drawTree . fmap show
