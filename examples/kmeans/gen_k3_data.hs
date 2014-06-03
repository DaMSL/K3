import Control.Applicative
import Control.Monad
import Data.List
import System.IO


filename = "seeds_dataset.txt"

parse_floats :: String -> [[Float]]
parse_floats = (map (map read)) . (map words) . lines

join_c :: Char -> [String] ->  String
join_c c = concat . (intersperse [c])

gen_DS :: (Show a) => [a] -> [String]
gen_DS vals = let
  in map gen $ zip [1..] vals 
  where
    gen (i, v) = "{key = (" ++ (show i) ++ ", MemImmut), value= (" ++ (show v) ++ ", MemImmut)}"

gen_k3_vector :: (Show a) => [a] -> String
gen_k3_vector vals = let
  ds_str = join_c ',' (gen_DS vals)
  in "{CNS={}, ANS={}, InMemoryDS=[" ++ ds_str ++  "], Collection}"

main :: IO ()
main = do
  floats      <- pure parse_floats <*> readFile filename
  collections <- return $ map gen_k3_vector floats
  mapM_ putStrLn collections 
  
