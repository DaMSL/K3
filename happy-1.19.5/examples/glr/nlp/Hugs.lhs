> module Main where
> import System(getArgs)
> import Data.Maybe(fromJust)
> import FiniteMap(fmToList,lookupFM)
> import English


> main 
>  = do
>	[s] <- getArgs
>	test s

> test s 
>  = do
>	case doParse $ lexer s of 
>	  ParseOK r f -> do 
>			    putStrLn $ "Ok " ++ show r ++ "\n" 
>					++ unlines (map show $ fmToList f)
>	  ParseEOF f  -> do 
>			    putStrLn $ "Premature end of input:\n" 
>					++ unlines (map show $ fmToList f)
>	  ParseError ts f -> do 
>			    putStrLn $ "Error: " ++ show ts

> forest_lookup f i 
>  = fromJust $ lookupFM f i
