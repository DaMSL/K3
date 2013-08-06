{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Pretty Printing for K3 Trees.
module Language.K3.Pretty (
    pretty,

    Pretty(..),

    shift
) where

import Data.Tree

import Language.K3.Core.Annotation

-- | Pretty-Print a K3 syntax tree.

class Pretty a where
    prettyLines :: Bool -> a -> [String]

instance Pretty a => Pretty (K3 a) where
    prettyLines terminal (Node (t :@: _) cs)
        = (shift "" "" payload)
        ++ drawSubTrees cs
      where
        payload = prettyLines (null cs) t
        drawSubTrees [] = []
        drawSubTrees [x] = "|" : shift "`- " "   " (prettyLines False x)
        drawSubTrees (x:xs) = "|" : shift "+- " "|  " (prettyLines True x) ++ drawSubTrees xs

shift :: String -> String -> [String] -> [String]
shift first other = zipWith (++) (first : repeat other)

pretty :: Pretty a => a -> String
pretty = unlines . prettyLines False
