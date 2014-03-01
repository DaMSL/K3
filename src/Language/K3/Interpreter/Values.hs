{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Interpreter.Values where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad

import Data.IORef

import Text.Read hiding (get, lift)
import qualified Text.Read          as TR (lift)
import Text.ParserCombinators.ReadP as P (skipSpaces)

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

import Language.K3.Runtime.Common ( defaultSystem )
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace
import Language.K3.Utils.Pretty

{- Value equality -}
-- | TODO: Equality based on stable names

-- | Haskell Eq type class implementation.
--   This does not support comparison over indirections or collections.
instance Eq Value where
  VBool v        == VBool v'        = v == v'
  VByte b        == VByte b'        = b == b'
  VInt  v        == VInt  v'        = v == v'
  VReal v        == VReal v'        = v == v'
  VString v      == VString v'      = v == v'
  VOption v      == VOption v'      = v == v'
  VTuple v       == VTuple v'       = all (uncurry (==)) $ zip v v'
  VRecord v      == VRecord v'      = all (uncurry (==)) $ zip v v'
  VAddress v     == VAddress v'     = v == v'  
  VCollection  v == VCollection v'  = v == v'
  VIndirection v == VIndirection v' = v == v'  
  _              == _               = False


{- Value show and display -}
showsPrecTag :: Show a => String -> Int -> a -> ShowS
showsPrecTag s d v = showsPrecTagF s d $ showsPrec (appPrec+1) v
  where appPrec = 10

showsPrecTagF :: String -> Int -> ShowS -> ShowS
showsPrecTagF s d showF =
  showParen (d > appPrec) $ showString (s++" ") . showF
  where appPrec = 10

-- | Verbose stringification of values through show instance.
--   This produces <tag> placeholders for unshowable values (IORefs and functions)
instance Show Value where
  showsPrec d (VBool v)        = showsPrecTag "VBool" d v
  showsPrec d (VByte v)        = showsPrecTag "VByte" d v
  showsPrec d (VInt v)         = showsPrecTag "VInt" d v
  showsPrec d (VReal v)        = showsPrecTag "VReal" d v
  showsPrec d (VString v)      = showsPrecTag "VString" d v
  showsPrec d (VOption v)      = showsPrecTag "VOption" d v
  showsPrec d (VTuple v)       = showsPrecTag "VTuple" d v
  showsPrec d (VRecord v)      = showsPrecTag "VRecord" d v
  showsPrec d (VAddress v)     = showsPrecTag "VAddress" d v
  
  showsPrec d (VCollection _)  = showsPrecTagF "VCollection"  d $ showString "<opaque>"
  showsPrec d (VIndirection _) = showsPrecTagF "VIndirection" d $ showString "<opaque>"
  showsPrec d (VFunction _)    = showsPrecTagF "VFunction"    d $ showString "<function>"
  showsPrec d (VTrigger (_, Nothing)) = showsPrecTagF "VTrigger" d $ showString "<uninitialized>"
  showsPrec d (VTrigger (_, Just _))  = showsPrecTagF "VTrigger" d $ showString "<function>"


-- | Verbose stringification of values through read instance.
--   This errors on attempting to read unshowable values (IORefs and functions)
instance Read Value where
  readPrec = parens $ 
        (prec app_prec $ do
          Ident "VBool" <- lexP
          v <- step readPrec
          return (VBool v))

    +++ (prec app_prec $ do
          Ident "VByte" <- lexP
          v <- step readPrec
          return (VByte v))

    +++ (prec app_prec $ do
          Ident "VInt" <- lexP
          v <- step readPrec
          return (VInt v))

    +++ (prec app_prec $ do
          Ident "VReal" <- lexP
          v <- step readPrec
          return (VReal v))

    +++ (prec app_prec $ do
          Ident "VString" <- lexP
          v <- step readPrec
          return (VString v))

    +++ (prec app_prec $ do
          Ident "VOption" <- lexP
          v <- step readPrec
          return (VOption v))

    +++ (prec app_prec $ do
          Ident "VTuple" <- lexP
          v <- step readPrec
          return (VTuple v))

    +++ (prec app_prec $ do
          Ident "VRecord" <- lexP
          v <- step readPrec
          return (VRecord v))

    +++ (prec app_prec $ do
          Ident "VCollection" <- lexP
          Ident "<opaque>" <- step readPrec
          error "Cannot read collections")

    +++ (prec app_prec $ do
          Ident "VIndirection" <- lexP
          Ident "<opaque>" <- step readPrec
          error "Cannot read indirections")

    +++ (prec app_prec $ do
          Ident "VFunction" <- lexP
          Ident "<function>" <- step readPrec
          error "Cannot read functions")

    +++ (prec app_prec $ do
          Ident "VAddress" <- lexP
          v <- step readPrec
          return (VAddress v))

    +++ (prec app_prec $ do
          Ident "VTrigger" <- lexP
          Ident _ <- lexP
          Ident "<uninitialized>" <- step readPrec
          error "Cannot read triggers")

    +++ (prec app_prec $ do
          Ident "VTrigger" <- lexP
          Ident _ <- lexP
          Ident "<function>" <- step readPrec
          error "Cannot read triggers")

    where app_prec = 10

  readListPrec = readListPrecDefault

-- TODO
instance Pretty Value where
  prettyLines v = [show v]


{- Wire description support for values. -}

valueWD :: WireDesc Value
valueWD = WireDesc (return . show) (return . Just . read) $ Delimiter "\n"

syntaxValueWD :: SEnvironment Value -> WireDesc Value
syntaxValueWD sEnv =
  WireDesc (packValueSyntax True)
           (\s -> unpackValueSyntax sEnv s >>= return . Just) $ Delimiter "\n"

wireDesc :: SEnvironment Value -> String -> WireDesc Value
wireDesc sEnv "k3" = syntaxValueWD sEnv
wireDesc _ fmt  = error $ "Invalid format " ++ fmt


-- | Syntax-oriented stringification of values.
packValueSyntax :: Bool -> Value -> IO String
packValueSyntax forTransport v = packValue 0 v >>= return . ($ "")
  where
    rt = return
    packValue :: Int -> Value -> IO ShowS
    packValue d = \case
      VBool v'     -> rt $ showsPrec d v'
      VByte v'     -> rt $ showChar 'B' . showParen True (showsPrec appPrec1 v')
      VInt v'      -> rt $ showsPrec d v'
      VReal v'     -> rt $ showsPrec d v'
      VString v'   -> rt $ showsPrec d v'
      VOption vOpt -> packOpt d vOpt
      VTuple v'    -> parens' (packValue $ d+1) v'
      VRecord v'   -> packNamedValues (d+1) v'
      
      VCollection c  -> packCollectionPrec (d+1) c      
      VIndirection r -> readIORef r >>= (\v' -> (.) <$> rt (showChar 'I') <*> packValue (d+1) v')
      VAddress v'    -> rt $ showsPrec d v'

      VFunction _    -> (forTransport ? error $ (rt . showString)) funSym
      VTrigger (n,_) -> (forTransport ? error $ (rt . showString)) $ trigSym n

    funSym    = "<function>"
    trigSym n = "<trigger " ++ n ++ " >"

    parens'  = packCustomList "(" ")" ","
    braces   = packCustomList "{" "}" ","
    brackets = packCustomList "[" "]" ","

    packOpt d vOpt =
      maybe (rt ("Nothing "++)) 
            (\v' -> packValue appPrec1 v' >>= \showS -> rt (showParen (d > appPrec) ("Just " ++) . showS))
            vOpt

    packCollectionPrec d cmv = do
      Collection (CollectionNamespace cns ans) v' extId <- readMVar cmv 
      wrap' "{" "}" ((\a b c d' -> a . b . c . d')
        <$> packCollectionNamespace (d+1) (cns, ans)
        <*> rt (showChar ',')
        <*> packCollectionDataspace (d+1) v'
        <*> rt (showChar ',' . showString extId))

    packCollectionNamespace d (cns, ans) =
      (\a b c d' e -> a . b . c . d' . e)
        <$> rt (showString "CNS=") <*> packNamedValues d (namespaceNonFunctions cns) 
        <*> rt (showChar ',')
        <*> rt (showString "ANS=") 
        <*> braces (packDoublyNamedValues d) (map (\(x,y) -> (x, namespaceNonFunctions y)) ans)
    
    -- TODO Need to pattern match on kind of dataspace
    packCollectionDataspace d (InMemoryDS ds) =
      (\a b -> a . b) <$> (rt $ showString "InMemoryDS=") <*> brackets (packValue d) ds
    
    packCollectionDataspace _ (ExternalDS filename) =
      (\a b -> a . b) <$> (rt $ showString "ExternalDS=") <*> (rt $ showString $ getFile filename)
    -- for now, external ds are shared file path
    -- Need to preserve copy semantics for external dataspaces
    
    packDoublyNamedValues d (n, nv)  = (.) <$> rt (showString $ n ++ "=") <*> packNamedValues d nv

    packNamedValues d nv        = braces (packNamedValuePrec d) nv
    packNamedValuePrec d (n,v') = (.) <$> rt (showString n . showChar '=') <*> packValue d v'

    packCustomList :: String -> String -> String -> (a -> IO ShowS) -> [a] -> IO ShowS
    packCustomList lWrap rWrap _ _ []             = rt $ \s -> lWrap ++ rWrap ++ s
    packCustomList lWrap rWrap sep packF (x:xs)   = (\a b -> (lWrap ++) . a . b) <$> packF x <*> packl xs
      where packl []     = rt $ \s -> rWrap ++ s
            packl (y:ys) = (\a b -> (sep++) . a . b) <$> packF y <*> packl ys

    wrap' lWrap rWrap packx =
      (\a b c -> a . b . c) <$> rt (showString lWrap) <*> packx <*> rt (showString rWrap)

    appPrec  = 10
    appPrec1 = 11  

    namespaceNonFunctions = filter (nonFunction . snd)
    nonFunction (VFunction _) = False
    nonFunction _             = True

    True  ? x = const x  
    False ? _ = id


unpackValueSyntax :: SEnvironment Value -> String -> IO Value
unpackValueSyntax sEnv = readSingleParse unpackValue
  where
    rt :: a -> IO a
    rt = return

    unpackValue :: ReadPrec (IO Value)
    unpackValue = parens $ reset $
           (readPrec  >>= return . rt . VInt)
      <++ ((readPrec >>= return . rt . VBool)
      +++  (readPrec  >>= return . rt . VReal)
      +++  (readPrec  >>= return . rt . VString)
      +++  (readPrec  >>= return . rt . VAddress)

      +++ (do
            Ident "B" <- lexP
            v <- readPrec
            return . rt $ VByte v)

      +++ (do
            Ident "Just" <- lexP
            v <- unpackValue
            return (v >>= rt . VOption . Just))

      +++ (do
            Ident "Nothing" <- lexP
            return . rt $ VOption Nothing)

      +++ (prec appPrec1 $ do
            v <- readParens unpackValue
            return (sequence v >>= rt . VTuple))

      +++ (do
            nv <- readNamedValues unpackValue
            return (nv >>= rt . VRecord))

      +++ (do
            Ident "I" <- lexP
            v <- unpackValue
            return (v >>= newIORef >>= rt . VIndirection))
      
      +++ readCollectionPrec)

    readCollectionPrec = parens $
      (prec appPrec1 $ do
        Punc "{" <- lexP
        ns       <- readCollectionNamespace
        Punc "," <- lexP
        v        <- readCollectionDataspace
        Punc "," <- lexP
        Ident n  <- lexP
        Punc "}" <- lexP
        return $ (do
          x   <- ns
          y   <- v
          cmv <- rebuildCollection n $ Collection x y n
          rt $ VCollection cmv))

    readCollectionNamespace :: ReadPrec (IO (CollectionNamespace Value))
    readCollectionNamespace = parens $
      (prec appPrec1 $ do
        void      $ readExpectedName "CNS"
        cns      <- readNamedValues unpackValue
        Punc "," <- lexP
        void      $ readExpectedName "ANS"
        ans      <- readDoublyNamedValues
        return $ (do
          cns' <- cns
          ans' <- ans
          rt $ CollectionNamespace cns' ans'))

    readCollectionDataspace :: ReadPrec (IO (CollectionDataspace Value))
    readCollectionDataspace =
      (parens $ do
        void $ readExpectedName "InMemoryDS"
        v <- readBrackets unpackValue
        return $ InMemoryDS <$> sequence v
      )
      +++
      (parens $ do
        void $ readExpectedName "ExternalDS"
        String filename <- lexP
        return $ rt $ ExternalDS $ FileDataspace filename
      )
      

    readDoublyNamedValues :: ReadPrec (IO [(String, [(String, Value)])])
    readDoublyNamedValues = parens $ do
        v <- readBraces $ readNamedPrec $ readNamedValues unpackValue
        return $ sequence v

    readNamedValues :: ReadPrec (IO a) -> ReadPrec (IO [(String, a)])
    readNamedValues readF = parens $ do
        v <- readBraces $ readNamedPrec readF
        return $ sequence v

    readNamedPrec :: ReadPrec (IO a) -> ReadPrec (IO (String, a))
    readNamedPrec readF = parens $ do
        n <- readName 
        v <- step readF
        return $ v >>= rt . (n,)

    readExpectedName :: String -> ReadPrec ()
    readExpectedName expected = do
      Ident n  <- lexP
      Punc "=" <- lexP
      if n == expected then return () else pfail

    readName :: ReadPrec String
    readName = do
      Ident n  <- lexP
      Punc "=" <- lexP
      return n

    readParens   = readCustomList "(" ")" ","
    readBraces   = readCustomList "{" "}" ","
    readBrackets = readCustomList "[" "]" ","
    
    readSingleParse :: Show a => ReadPrec (IO a) -> String -> IO a
    readSingleParse readP s =
      case [ x | (x,"") <- readPrec_to_S read' minPrec s ] of
        [x] -> x
        []  -> error ("Interpreter.unpackValueSyntax: no parse for " ++ s)
        l   -> error $ "Interpreter.unpackValueSyntax: ambiguous parse (" ++ (show $ length l) ++ " variants)"
      where read' = do
              x <- readP
              TR.lift P.skipSpaces
              return x

    readCustomList :: String -> String -> String -> ReadPrec (IO a) -> ReadPrec [IO a]
    readCustomList lWrap rWrap sep readx =
      parens
      ( do Punc c <- lexP
           if c == lWrap
           then (listRest False +++ listNext) else pfail
      )
     where
      listRest started =
        do Punc c <- lexP
           if c == rWrap then return []
           else if c == sep && started then listNext
           else pfail
      
      listNext =
        do x  <- reset readx
           xs <- listRest True
           return (x:xs)

    rebuildCollection comboId c = 
      case lookup comboId $ realizations $ snd sEnv of
        Nothing -> newMVar c
        Just r  -> do
          let copyCstr = copyCtor r
          cCopyResult <- simpleEngine >>= \e -> runInterpretation e emptyState (copyCstr c)
          either (const $ newMVar c) (either (const $ newMVar c) return . getResultVal) cCopyResult
    
    appPrec1 = 11  

{- Misc. helpers -}

--TODO is it okay to have empty trigger list here? QueueConfig
simpleEngine :: IO (Engine Value)
simpleEngine = simulationEngine [] False defaultSystem (syntaxValueWD emptyStaticEnv)

