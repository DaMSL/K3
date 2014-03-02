{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Interpreter.Values where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.IO.Class

import Data.Function
import Data.Hashable
import Data.IORef
import Data.List

import System.Mem.StableName

import Text.Read hiding (get, lift)
import qualified Text.Read          as TR (lift)
import Text.ParserCombinators.ReadP as P (skipSpaces)

import Language.K3.Core.Common

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

import Language.K3.Runtime.Common ( defaultSystem )
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace
import Language.K3.Utils.Pretty

{- Value equality -}
-- | TODO: Equality based on stable names

-- | Haskell Eq type class implementation.
--   This does not support comparison for triggers. Also, collection comparison
--   here is based on MVar comparison, and not K3 record comparison semantics.
instance Eq Value where
  VBool v           == VBool v'           = v == v'
  VByte v           == VByte v'           = v == v'
  VInt  v           == VInt  v'           = v == v'
  VReal v           == VReal v'           = v == v'
  VString v         == VString v'         = v == v'
  VAddress v        == VAddress v'        = v == v'  
  VOption v         == VOption v'         = v == v'
  VIndirection v    == VIndirection v'    = v == v'  
  VTuple v          == VTuple v'          = v == v'
  VRecord v         == VRecord v'         = (sortBy (compare `on` fst) v) == (sortBy (compare `on` fst) v')
  VCollection  v    == VCollection v'     = v == v'
  VFunction (_,_,n) == VFunction (_,_,n') = n == n'
  _                 == _                  = False

-- | Haskell Ord type class implementation.
--   This does not support comparisons for indirections, collections, functions or triggers.
instance Ord Value where
  compare (VBool a)    (VBool b)    = compare a b
  compare (VByte a)    (VByte b)    = compare a b
  compare (VInt a)     (VInt b)     = compare a b
  compare (VReal a)    (VReal b)    = compare a b
  compare (VString a)  (VString b)  = compare a b
  compare (VAddress a) (VAddress b) = compare a b
  compare (VOption a)  (VOption b)  = compare a b
  compare (VTuple a)   (VTuple b)   = compare a b
  compare (VRecord a)  (VRecord b)  = compare (sortBy (compare `on` fst) a) (sortBy (compare `on` fst) b)
  compare _ _                       = error "Invalid value comparison"


-- | Haskell Hashable type class implementation.
--   This does not support hashing indirections, collections, or triggers.
--   Functions can be hashed, based on the hash value of their stable names.
instance Hashable Value where
  hashWithSalt salt (VBool a)           = hashWithSalt salt a
  hashWithSalt salt (VByte a)           = hashWithSalt salt a
  hashWithSalt salt (VInt a)            = hashWithSalt salt a
  hashWithSalt salt (VReal a)           = hashWithSalt salt a
  hashWithSalt salt (VString a)         = hashWithSalt salt a
  hashWithSalt salt (VAddress a)        = hashWithSalt salt a
  hashWithSalt salt (VOption a)         = hashWithSalt salt a
  hashWithSalt salt (VTuple a)          = hashWithSalt salt a
  hashWithSalt salt (VRecord a)         = hashWithSalt salt a
  hashWithSalt salt (VFunction (_,_,n)) = salt `hashWithSalt` (hashStableName n)
  hashWithSalt _ _ = error "Invalid value hash operation"

-- | Interpreter value equality operation
valueEq :: Value -> Value -> Interpretation Value
valueEq  (VOption (Just v)) (VOption (Just v')) = valueEq v v'
valueEq  (VOption v)        (VOption v')        = return . VBool $ v == v'
valueEq  (VTuple v)         (VTuple v')         = listEq v v' >>= return . VBool

valueEq  (VCollection  v)   (VCollection v') =
  uncurry collectionEq =<< ((,) <$> liftIO (readMVar v) <*> liftIO (readMVar v'))

valueEq  (VRecord v) (VRecord v') = 
  let (ids,  vals)  = unzip $ sortBy (compare `on` fst) v
      (ids', vals') = unzip $ sortBy (compare `on` fst) v'
  in
  if ids == ids' then listEq vals vals' >>= return . VBool
                 else return $ VBool False

valueEq  x y = return . VBool $ x == y

valueNeq :: Value -> Value -> Interpretation Value
valueNeq x y = (\(VBool z) -> VBool $ not z) <$> valueEq x y

listEq :: [Value] -> [Value] -> Interpretation Bool
listEq a b = listCompare a b >>= \sgn -> return $ sgn == 0

collectionEq :: Collection Value -> Collection Value -> Interpretation Value
collectionEq c1 c2 = collectionCompare c1 c2 >>= \(VInt sgn) -> return . VBool $ sgn == 0

dataspaceEq :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceEq ds1 ds2 = dataspaceCompare ds1 ds2 >>= \(VInt sgn) -> return . VBool $ sgn == 0


-- | Interpreter value ordering operation
valueCompare :: Value -> Value -> Interpretation Value
valueCompare (VOption (Just v)) (VOption (Just v')) = valueCompare v v'
valueCompare (VOption v)        (VOption v')        = return . VInt . orderingAsInt $ compare v v'
valueCompare (VTuple v)         (VTuple v')         = listCompare v v' >>= return . VInt
valueCompare (VCollection  v)   (VCollection v')    = uncurry collectionCompare =<< ((,) <$> liftIO (readMVar v) <*> liftIO (readMVar v'))

valueCompare (VRecord v) (VRecord v') = 
  listCompare (map snd $ sortBy (compare `on` fst) v) (map snd $ sortBy (compare `on` fst) v') >>= return . VInt  

valueCompare (VIndirection _) _ = throwE $ RunTimeTypeError "Invalid indirection comparison"
valueCompare _ (VIndirection _) = throwE $ RunTimeTypeError "Invalid indirection comparison"

valueCompare (VFunction _) _ = throwE $ RunTimeTypeError "Invalid function comparison"
valueCompare _ (VFunction _) = throwE $ RunTimeTypeError "Invalid function comparison"

valueCompare (VTrigger _) _ = throwE $ RunTimeTypeError "Invalid trigger comparison"
valueCompare _ (VTrigger _) = throwE $ RunTimeTypeError "Invalid trigger comparison"

valueCompare x y = return . VInt $ orderingAsInt $ compare x y

orderingAsInt :: Ordering -> Int
orderingAsInt LT = -1
orderingAsInt EQ = 0
orderingAsInt GT = 1

listCompare :: [Value] -> [Value] -> Interpretation Int
listCompare [] [] = return 0
listCompare [] _  = return $ -1
listCompare _ []  = return 1
listCompare (a:as) (b:bs) = valueCompare a b >>= \(VInt sgn) -> if sgn == 0 then listCompare as bs else return sgn

collectionCompare :: Collection Value -> Collection Value -> Interpretation Value
collectionCompare (Collection _ ds cId) (Collection _ ds' cId') = 
  let sgn = orderingAsInt $ compare cId cId' in
  if sgn == 0 then dataspaceCompare ds ds'
              else return $ VInt sgn

dataspaceCompare :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceCompare (InMemoryDS l) (InMemoryDS l') = listCompare l l' >>= return . VInt
dataspaceCompare (ExternalDS _) (ExternalDS _)  = throwE $ RunTimeInterpretationError "External DS comparison not implemented"
dataspaceCompare _ _ = throwE $ RunTimeInterpretationError "Cross-representation dataspace comparison not implemented"

valueSign :: (Int -> Bool) -> Value -> Value -> Interpretation Value
valueSign sgnOp a b = (\(VInt sgn) -> VBool $ sgnOp sgn) <$> valueCompare a b

valueLt :: Value -> Value -> Interpretation Value
valueLt = valueSign $ \sgn -> sgn < 0

valueLte :: Value -> Value -> Interpretation Value
valueLte = valueSign $ \sgn -> sgn <= 0

valueGt :: Value -> Value -> Interpretation Value
valueGt = valueSign $ \sgn -> sgn > 0

valueGte :: Value -> Value -> Interpretation Value
valueGte = valueSign $ \sgn -> sgn >= 0

-- | Interpreter hash function
valueHash :: Value -> Interpretation Value
valueHash (VOption Nothing)  = return . VInt $ hash (Nothing :: Maybe Value)
valueHash (VOption (Just v)) = composeHash (VInt 0) v
valueHash (VTuple v)         = foldM composeHash (VInt 1) v
valueHash (VRecord v)        = foldM composeRecordHash (VInt 2) v
valueHash (VIndirection v)   = liftIO (readIORef v) >>= composeHash (VInt 3)
valueHash (VCollection v)    = liftIO (readMVar v) >>= collectionHash
valueHash (VTrigger _)       = throwE $ RunTimeTypeError "Invalid hash operation on trigger value"
valueHash x = return . VInt $ hash x

composeHash :: Value -> Value -> Interpretation Value
composeHash (VInt s) v = valueHash v >>= \(VInt vh) -> return $ VInt $ hashWithSalt s vh
composeHash _ _ = throwE $ RunTimeInterpretationError "Invalid salt value in hash operation"

composeRecordHash :: Value -> (Identifier, Value) -> Interpretation Value
composeRecordHash (VInt s) (n,v) = valueHash v >>= \(VInt vh) -> return $ VInt (s `hashWithSalt` vh `hashWithSalt` n)
composeRecordHash _ _ = throwE $ RunTimeInterpretationError "Invalid salt value in hash operation"

collectionHash :: Collection Value -> Interpretation Value
collectionHash (Collection _ ds cId) = dataspaceHash ds >>= flip composeHash (VString cId)

dataspaceHash :: CollectionDataspace Value -> Interpretation Value
dataspaceHash (InMemoryDS l) = foldM composeHash (VInt 4) l
dataspaceHash _ = throwE $ RunTimeInterpretationError "External DS hash not implemented"


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
  showsPrec d (VFunction (_,_,n))     = showsPrecTagF "VFunction" d $ showString $ "<function " ++ (show $ hashStableName n) ++ ">"
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

      VFunction (_,_,n) -> (forTransport ? error $ (rt . showString)) $ funSym n
      VTrigger (n,_)    -> (forTransport ? error $ (rt . showString)) $ trigSym n

    funSym n  = "<function " ++ (show $ hashStableName n) ++ ">"
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

