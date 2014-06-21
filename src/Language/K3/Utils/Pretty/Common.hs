{-|
  Contains common routines for pretty-printing.
-}

module Language.K3.Utils.Pretty.Common
( explicitPrettyMap
, prettyMap
, explicitNormalPrettyMap
, normalPrettyMap

, normalPrettySet

, normalPrettyPair
, explicitNormalPrettyPair
) where

import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Utils.Pretty

-- |Creates a box containing a pretty-printing of a map whose elements may not
--  be pretty.
explicitPrettyMap :: [String] -- ^The pretty map's prefix.
                  -> [String] -- ^The pretty map's postfix.
                  -> String -- ^The separator between elements.
                  -> String -- ^The separator between keys and values.
                  -> (k -> [String]) -- ^A pretty function for keys.
                  -> (v -> [String]) -- ^A pretty function for values.
                  -> Map k v
                  -> [String]
explicitPrettyMap pre post elsep innersep pk pv m =
  pre %+ sequenceBoxes maxWidth elsep (map prettyEl $ Map.toList m) +% post
  where
    prettyEl (x,y) = pk x %+ [innersep] %+ pv y

-- |Creates a box containing a pretty-printing of a map.
prettyMap :: (Pretty k, Pretty v)
          => [String] -- ^The pretty map's prefix.
          -> [String] -- ^The pretty map's postfix.
          -> String -- ^The separator between elements.
          -> String -- ^The separator between keys and values.
          -> Map k v
          -> [String]
prettyMap pre post elsep innersep =
  explicitPrettyMap pre post elsep innersep prettyLines prettyLines

-- |Creates a box containing a pretty-printing of a map whose elements may not
--  be pretty.
explicitNormalPrettyMap :: (k -> [String]) -- ^A pretty function for keys.
                        -> (v -> [String]) -- ^A pretty function for values.
                        -> Map k v
                        -> [String]
explicitNormalPrettyMap = explicitPrettyMap ["{"] ["}"] ", " "=>"

-- |Creates a generic beautification of a map.
normalPrettyMap :: (Pretty k, Pretty v) => Map k v -> [String]
normalPrettyMap = prettyMap ["{"] ["}"] ", " "=>"


-- |Creates a generic beautification of a set.
normalPrettySet :: (Pretty a) => Set a -> [String]
normalPrettySet s =
  ["{"] %+ intersperseBoxes [", "] (map prettyLines $ Set.toList s) +% ["}"]


-- |Creates a generic beautification of a 2-tuple.
normalPrettyPair :: (Pretty a, Pretty b) => (a, b) -> [String]
normalPrettyPair = explicitNormalPrettyPair prettyLines prettyLines

-- |Creates an explicit beautification of a 2-tuple.
explicitNormalPrettyPair :: (a -> [String])
                         -> (b -> [String])
                         -> (a,b)
                         -> [String]
explicitNormalPrettyPair pa pb (a,b) =
  ["("] %+ pa a %+ [", "] %+ pb b %+ [")"]
