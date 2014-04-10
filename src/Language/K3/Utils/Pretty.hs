{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Pretty Printing for K3 Trees.
module Language.K3.Utils.Pretty (
    pretty,
    prettyPC,
    boxToString,
    removeTrailingWhitespace,

    PrintConfig(..),
    defaultPrintConfig,
    tersePrintConfig,
    Pretty(..),
    PrettyPC(..),

    drawSubTrees,
    drawAnnotations,

    shift,
    terminalShift,
    nonTerminalShift,
    
    wrap,
    indent,
    hconcatTop,
    hconcatBottom,
    vconcat,
    vconcats,
    boxWidth,
    boxHeight,
    maxWidth,
    hconcatSoft,
    intersperseBoxes,
    sequenceBoxes,
    
    (%+),
    (+%),
    (%$),
    (%/),
) where

import Data.Char

-- TODO: Maybe we want a type alias TextBox for [String]?  Or even a newtype?

pretty :: Pretty a => a -> String
pretty = boxToString . prettyLines

-- Print according to a printConfig
prettyPC :: PrettyPC a => PrintConfig -> a -> String
prettyPC pc a = boxToString $ prettyLinesPC pc a

boxToString :: [String] -> String
boxToString = unlines . map removeTrailingWhitespace

removeTrailingWhitespace :: String -> String
removeTrailingWhitespace s = case s of
  [] -> []
  c:s' -> let s'' = removeTrailingWhitespace s' in
          if isSpace c && null s''
            then []
            else c:s''

-- Configuration for the kind of printing we want to do
data PrintConfig = PrintConfig { 
                     printVerboseTypes  :: Bool
                   , printEnv           :: Bool
                   , printNamespace     :: Bool
                   , printDataspace     :: Bool
                   , printRealizationId :: Bool
                   , printFunctions     :: Bool
                   , printAnnotations   :: Bool
                   , printStaticEnv     :: Bool
                   , printProxyStack    :: Bool
                   , printTracer        :: Bool
                   , printQualifiers    :: Bool
                   } deriving (Eq, Read, Show)

defaultPrintConfig :: PrintConfig
defaultPrintConfig  = PrintConfig True True True True True True True True True True True

tersePrintConfig :: PrintConfig
tersePrintConfig = defaultPrintConfig {printNamespace=False, 
                                       printFunctions=False,
                                       printQualifiers=False,
                                       printVerboseTypes=False}

class Pretty a where
    prettyLines :: a -> [String]

-- Pretty printing as directed by a PrintConfig data structure
class Pretty a => PrettyPC a where
    prettyLinesPC :: PrintConfig -> a -> [String]

drawSubTrees :: Pretty a => [a] -> [String]
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

wrap :: Int -> String -> [String]
wrap n str
   | length str <= n = [str]
   | otherwise       = [take n str] ++ wrap n (drop n str)

indent :: Int -> [String] -> [String]
indent n = let s = replicate n ' ' in shift s s

hconcatTop :: [String] -> [String] -> [String]
hconcatTop = hconcat (++[""])
 
hconcatBottom :: [String] -> [String] -> [String]
hconcatBottom = hconcat ([""]++)
  
hconcat :: ([String] -> [String]) -> [String] -> [String] -> [String]
hconcat f x y =
  let (x',y') = mkSameLength x y f in
  let size = maximum (map length x') in
  let x'' = map (\s -> s ++ replicate (size - length s) ' ') x' in
  zipWith (++) x'' y'
  
mkSameLength :: [a] -> [a] -> ([a] -> [a]) -> ([a],[a])
mkSameLength x y f =
  case (length x, length y) of
    (nx,ny) | nx < ny -> mkSameLength (f x) y f
    (nx,ny) | nx > ny -> let (y',x') = mkSameLength y x f in (x',y')
    _ -> (x,y)

vconcat :: [String] -> [String] -> [String]
vconcat x y = x ++ y

vconcats :: [[String]] -> [String]
vconcats = concat

boxWidth :: [String] -> Int
boxWidth = maximum . map length

boxHeight :: [String] -> Int
boxHeight =
  -- Assuming that individual lines do not contain '\n'
  length

maxWidth :: Int
maxWidth = 80

-- |Horizontally concatenates two boxes if the result does not exceed maximum
--  width; vertically concatenates them otherwise.
hconcatSoft :: [String] -> [String] -> [String]
hconcatSoft x y =
  if boxWidth x + boxWidth y + 1 <= maxWidth
    then hconcatTop x y
    else vconcat x y

intersperseBoxes :: [String] -> [[String]] -> [String]
intersperseBoxes sep boxes = case boxes of
  [] -> []
  box:[] -> box
  box:boxes' -> foldl (%+) box $ map (sep %+) boxes'

-- |Creates a wrapped textual list from a list of boxes.  Elements in the list
--  are interspersed with a separator string.  When a line fills up, a newline
--  is automatically inserted.
sequenceBoxes :: Int -> String -> [[String]] -> [String]
sequenceBoxes n sep boxesIn =
  let (box, boxes) = pullNextBox True n boxesIn in
  if null boxes then box else box %$ sequenceBoxes n sep boxes
  where
    pullNextBox :: Bool -> Int -> [[String]] -> ([String],[[String]])
    pullNextBox force spaceLeft boxes =
      case boxes of
        [] -> ([], boxes)
        box:boxes' ->
          let size = boxWidth box + length sep in
          if size <= spaceLeft || force
            then let (rbox, boxes'') =
                        pullNextBox False (spaceLeft - size) boxes' in
                 ( if null rbox && null boxes''
                      then box
                      else box %+ [sep] %+ rbox
                 , boxes'')
            else ([],boxes)

(%+) :: [String] -> [String] -> [String]
(%+) = hconcatTop
infixl 8 %+

(+%) :: [String] -> [String] -> [String]
(+%) = hconcatBottom
infixl 8 +%

(%$) :: [String] -> [String] -> [String]
(%$) = vconcat
infixl 7 %$

(%/) :: [String] -> [String] -> [String]
(%/) = hconcatSoft
infixl 7 %/

