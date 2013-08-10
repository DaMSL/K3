{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Pretty Printing for K3 Trees.
module Language.K3.Pretty (
    pretty,

    Pretty(..),

    drawSubTrees,
    drawAnnotations,

    shift,
    terminalShift,
    nonTerminalShift
) where

import Data.Tree

import Language.K3.Core.Annotation

class Pretty a where
    prettyLines :: a -> [String]

drawSubTrees [] = []
drawSubTrees [x] = "|" : terminalShift x
drawSubTrees (x:xs) = "|" : nonTerminalShift x ++ drawSubTrees xs

drawAnnotations :: Show a => [a] -> String
drawAnnotations as = if null as then "" else " :@: " ++ show as

shift :: String -> String -> [String] -> [String]
shift first other = zipWith (++) (first : repeat other)

terminalShift :: Pretty a => a -> [String]
terminalShift = shift "`- " "   " . prettyLines

nonTerminalShift :: Pretty a => a -> [String]
nonTerminalShift = shift "+- " "|  " . prettyLines

pretty :: Pretty a => a -> String
pretty = unlines . prettyLines
