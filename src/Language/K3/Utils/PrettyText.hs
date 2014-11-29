{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Pretty Printing for K3 Trees.
--   This module is a Data.Text-oriented replacement for L.K3.U.Pretty
module Language.K3.Utils.PrettyText (
    pretty,
    prettyPC,
    boxToString,
    removeTrailingWhitespace,

    PrintConfig(..),
    defaultPrintConfig,
    tersePrintConfig,
    simplePrintConfig,
    Pretty(..),
    PrettyPC(..),
    ShowPC(..),

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

import Data.Text ( Text )
import qualified Data.Text as T

-- | Data types that can be prettily printed.
class Pretty a where
    prettyLines :: a -> [Text]

-- | Pretty printing as directed by a PrintConfig data structure
class Pretty a => PrettyPC a where
    prettyLinesPC :: PrintConfig -> a -> [Text]

-- | Class to handle Show with a PrintConfig
class Show a => ShowPC a where
  showPC :: PrintConfig -> a -> Text


pretty :: Pretty a => a -> Text
pretty = boxToString . prettyLines

-- Print according to a printConfig
prettyPC :: PrettyPC a => PrintConfig -> a -> Text
prettyPC pc a = boxToString $ prettyLinesPC pc a

boxToString :: [Text] -> Text
boxToString = T.unlines . map removeTrailingWhitespace

removeTrailingWhitespace :: Text -> Text
removeTrailingWhitespace s = T.stripEnd s

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
                   , printComplex       :: Bool -- Don't print with simple symbols
                                                -- sets {}, seq [], collections {||}
                   , convertToTuples    :: Bool -- Convert i, _r1..._rn, key, val records to tuples
                   } deriving (Eq, Read, Show)

defaultPrintConfig :: PrintConfig
defaultPrintConfig  = PrintConfig True True True True True True True True True True True True True

tersePrintConfig :: PrintConfig
tersePrintConfig = defaultPrintConfig {printNamespace=False,
                                       printFunctions=False,
                                       printQualifiers=False,
                                       printVerboseTypes=False}

simplePrintConfig :: PrintConfig
simplePrintConfig = tersePrintConfig {printComplex = False}


drawSubTrees :: Pretty a => [a] -> [Text]
drawSubTrees []     = []
drawSubTrees [x]    = T.pack "|" : terminalShift x
drawSubTrees (x:xs) = T.pack "|" : nonTerminalShift x ++ drawSubTrees xs

drawAnnotations :: Show a => [a] -> Text
drawAnnotations as = if null as then T.empty else T.pack $ " :@: " ++ show as

shift :: Text -> Text -> [Text] -> [Text]
shift first other = zipWith T.append (first : repeat other)

terminalShift :: Pretty a => a -> [Text]
terminalShift = shift (T.pack "`- ") (T.pack "   ") . prettyLines

nonTerminalShift :: Pretty a => a -> [Text]
nonTerminalShift = shift (T.pack "+- ") (T.pack "|  ") . prettyLines

wrap :: Int -> Text -> [Text]
wrap n str
   | T.length str <= n = [str]
   | otherwise         = [T.take n str] ++ wrap n (T.drop n str)

indent :: Int -> [Text] -> [Text]
indent n = let s = T.replicate n (T.pack " ") in shift s s

hconcatTop :: [Text] -> [Text] -> [Text]
hconcatTop = hconcat (++[T.empty])

hconcatBottom :: [Text] -> [Text] -> [Text]
hconcatBottom = hconcat ([T.empty]++)

hconcat :: ([Text] -> [Text]) -> [Text] -> [Text] -> [Text]
hconcat f x y =
  let (x',y') = mkSameLength x y f in
  let size = maximum (map T.length x') in
  let x'' = map (\s -> T.append s $ T.replicate (size - T.length s) (T.pack " ")) x' in
  zipWith T.append x'' y'

mkSameLength :: [a] -> [a] -> ([a] -> [a]) -> ([a],[a])
mkSameLength x y f =
  case (length x, length y) of
    (nx,ny) | nx < ny -> mkSameLength (f x) y f
    (nx,ny) | nx > ny -> let (y',x') = mkSameLength y x f in (x',y')
    _ -> (x,y)

vconcat :: [Text] -> [Text] -> [Text]
vconcat x y = x ++ y

vconcats :: [[Text]] -> [Text]
vconcats = concat

boxWidth :: [Text] -> Int
boxWidth = maximum . map T.length

boxHeight :: [Text] -> Int
boxHeight =
  -- Assuming that individual lines do not contain '\n'
  length

maxWidth :: Int
maxWidth = 80

-- |Horizontally concatenates two boxes if the result does not exceed maximum
--  width; vertically concatenates them otherwise.
hconcatSoft :: [Text] -> [Text] -> [Text]
hconcatSoft x y =
  if boxWidth x + boxWidth y + 1 <= maxWidth
    then hconcatTop x y
    else vconcat x y

intersperseBoxes :: [Text] -> [[Text]] -> [Text]
intersperseBoxes sep boxes = case boxes of
  [] -> []
  box:[] -> box
  box:boxes' -> foldl (%+) box $ map (sep %+) boxes'

-- |Creates a wrapped textual list from a list of boxes.  Elements in the list
--  are interspersed with a separator string.  When a line fills up, a newline
--  is automatically inserted.
sequenceBoxes :: Int -> Text -> [[Text]] -> [Text]
sequenceBoxes n sep boxesIn =
  let (box, boxes) = pullNextBox True n boxesIn in
  if null boxes then box else box %$ sequenceBoxes n sep boxes
  where
    pullNextBox :: Bool -> Int -> [[Text]] -> ([Text],[[Text]])
    pullNextBox force spaceLeft boxes =
      case boxes of
        [] -> ([], boxes)
        box:boxes' ->
          let size = boxWidth box + T.length sep in
          if size <= spaceLeft || force
            then let (rbox, boxes'') =
                        pullNextBox False (spaceLeft - size) boxes' in
                 ( if null rbox && null boxes''
                      then box
                      else box %+ [sep] %+ rbox
                 , boxes'')
            else ([],boxes)

(%+) :: [Text] -> [Text] -> [Text]
(%+) = hconcatTop
infixl 8 %+

(+%) :: [Text] -> [Text] -> [Text]
(+%) = hconcatBottom
infixl 8 +%

(%$) :: [Text] -> [Text] -> [Text]
(%$) = vconcat
infixl 7 %$

(%/) :: [Text] -> [Text] -> [Text]
(%/) = hconcatSoft
infixl 7 %/

