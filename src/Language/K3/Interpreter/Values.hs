{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Interpreter.Values where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad

import Data.Hashable
import qualified Data.Map             as Map
import qualified Data.Hashable.Extras as HE ( salt )

import System.Mem.StableName

import Text.Read                    hiding ( get, lift )
import qualified Text.Read          as TR ( lift )
import Text.ParserCombinators.ReadP as P  ( skipSpaces )

import Language.K3.Core.Common

import Language.K3.Interpreter.Data.Types
import Language.K3.Interpreter.Data.Accessors

import Language.K3.Runtime.Common ( defaultSystem )
import Language.K3.Runtime.Engine
import Language.K3.Runtime.FileDataspace


orderingAsInt :: Ordering -> Int
orderingAsInt LT = -1
orderingAsInt EQ = 0
orderingAsInt GT = 1

intOfValue :: Value -> Interpretation Int
intOfValue (VInt i) = return i
intOfValue _        = throwE $ RunTimeInterpretationError "Invalid value, expected an integer"

composeHash :: Int -> Value -> Interpretation Int
composeHash s1 (VInt s2) = return $ hashWithSalt s1 s2
composeHash _ _          = throwE $ RunTimeInterpretationError "Invalid salt value in hash composition"

-- | Interpreter implementations of equality and comparison functions.
--   Since these operate in the interpretation monad, these can perform side-effects,
--   for example, to compare external dataspaces.
valueEq :: Value -> Value -> Interpretation Value
valueEq a b = valueCompare a b >>= asBool
  where asBool (VInt sgn) = return . VBool $ sgn == 0
        asBool _          = throwE $ RunTimeInterpretationError "Invalid comparison value in equality test"

valueCompare :: Value -> Value -> Interpretation Value
valueCompare (VOption (Just v, q))  (VOption (Just v', q')) =
    if q /= q' then return (VInt $ orderingAsInt $ compare q q') else valueCompare v v'

valueCompare (VTuple v)             (VTuple v')             = qvalueListCompare v v' >>= return . VInt
valueCompare (VRecord v)            (VRecord v')            = namedMembersCompare v v'
valueCompare (VCollection (_,v))    (VCollection (_,v'))    = collectionCompare v v'
valueCompare a b = return . VInt . orderingAsInt $ compare a b

valueSign :: (Int -> Bool) -> Value -> Value -> Interpretation Value
valueSign sgnOp a b = (\(VInt sgn) -> VBool $ sgnOp sgn) <$> valueCompare a b

valueNeq :: Value -> Value -> Interpretation Value
valueNeq x y = (\(VBool z) -> VBool $ not z) <$> valueEq x y

valueLt :: Value -> Value -> Interpretation Value
valueLt = valueSign $ \sgn -> sgn < 0

valueLte :: Value -> Value -> Interpretation Value
valueLte = valueSign $ \sgn -> sgn <= 0

valueGt :: Value -> Value -> Interpretation Value
valueGt = valueSign $ \sgn -> sgn > 0

valueGte :: Value -> Value -> Interpretation Value
valueGte = valueSign $ \sgn -> sgn >= 0

valueHashWithSalt :: Int -> Value -> Interpretation Value
valueHashWithSalt salt (VOption (Just v, q)) =
  valueHashWithSalt salt v >>= composeHash (salt `hashWithSalt` (0 :: Int) `hashWithSalt` q)
                           >>= return . VInt

valueHashWithSalt salt (VTuple v)          = qvalueListHashWithSalt (salt `hashWithSalt` (1 :: Int)) v
valueHashWithSalt salt (VRecord v)         = namedMembersHashWithSalt (salt `hashWithSalt` (2 :: Int)) v
valueHashWithSalt salt (VCollection (_,v)) = collectionHashWithSalt (salt `hashWithSalt` (3 :: Int)) v
valueHashWithSalt salt x = return . VInt $ hashWithSalt salt x

valueHash :: Value -> Interpretation Value
valueHash = valueHashWithSalt HE.salt


-- | Value list comparison helpers.
valueListEq :: [Value] -> [Value] -> Interpretation Bool
valueListEq a b = valueListCompare a b >>= \sgn -> return $ sgn == 0

valueListCompare :: [Value] -> [Value] -> Interpretation Int
valueListCompare [] [] = return 0
valueListCompare [] _  = return $ -1
valueListCompare _ []  = return 1
valueListCompare (a:as) (b:bs) =
  valueCompare a b >>= \(VInt sgn) ->
    if sgn == 0 then valueListCompare as bs else return sgn

qvalueListCompare :: [(Value, VQualifier)] -> [(Value, VQualifier)] -> Interpretation Int
qvalueListCompare [] [] = return 0
qvalueListCompare [] _  = return $ -1
qvalueListCompare _ []  = return 1
qvalueListCompare ((a,aq):as) ((b,bq):bs) =
  if aq /= bq
    then return . orderingAsInt $ compare aq bq 
    else valueCompare a b >>= \(VInt sgn) ->
            if sgn == 0 then qvalueListCompare as bs else return sgn

valueListHashWithSalt :: Int -> [Value] -> Interpretation Value
valueListHashWithSalt salt l = foldM hashNext salt l >>= return . VInt
  where hashNext accSalt v = valueHashWithSalt accSalt v >>= intOfValue

qvalueListHashWithSalt :: Int -> [(Value, VQualifier)] -> Interpretation Value
qvalueListHashWithSalt salt l = foldM hashNext salt l >>= return . VInt
  where hashNext accSalt (v, q) = valueHashWithSalt (accSalt `hashWithSalt` q) v >>= intOfValue

valueListHash :: [Value] -> Interpretation Int
valueListHash vl = valueListHashWithSalt HE.salt vl >>= intOfValue

-- | Collection comparison, including the namespace and dataspace components.
collectionEq :: Collection Value -> Collection Value -> Interpretation Value
collectionEq c1 c2 = collectionCompare c1 c2 >>= \(VInt sgn) -> return . VBool $ sgn == 0

collectionCompare :: Collection Value -> Collection Value -> Interpretation Value
collectionCompare (Collection ns ds cId) (Collection ns' ds' cId') = 
  let idOrd = compare cId cId' in
  if idOrd /= EQ then return . VInt $ orderingAsInt idOrd
                 else do
                        (VInt nsOrd) <- namespaceCompare ns ns'
                        if nsOrd /= 0 then return $ VInt nsOrd else dataspaceCompare ds ds'

collectionHashWithSalt :: Int -> Collection Value -> Interpretation Value
collectionHashWithSalt salt (Collection ns ds cId) = do
  let idSalt = salt `hashWithSalt` cId
  (VInt nsSalt) <- namespaceHashWithSalt idSalt ns
  dataspaceHashWithSalt nsSalt ds

collectionHash :: Collection Value -> Interpretation Value
collectionHash = collectionHashWithSalt HE.salt

-- | Collection namespace comparisons.
namespaceEq :: CollectionNamespace Value -> CollectionNamespace Value -> Interpretation Value
namespaceEq a b = namespaceCompare a b >>= \(VInt sgn) -> return . VBool $ sgn == 0

namespaceCompare :: CollectionNamespace Value -> CollectionNamespace Value -> Interpretation Value
namespaceCompare (CollectionNamespace cns ans) (CollectionNamespace cns' ans') =
  do 
    (VInt cSgn) <- namedMembersCompare cns cns'
    if cSgn /= 0 
      then return $ VInt cSgn
      else foldM chainCompare (VInt 0) $ zip ans ans' 
  
  where chainCompare (VInt 0) ((na,a), (nb,b)) = if na /= nb then return . VInt . orderingAsInt $ compare na nb 
                                                      else namedMembersCompare a b
        chainCompare (VInt sgn) _ = return $ VInt sgn
        chainCompare _ _          = throwE $ RunTimeInterpretationError "Invalid comparison result"

namespaceHashWithSalt :: Int -> CollectionNamespace Value -> Interpretation Value
namespaceHashWithSalt salt (CollectionNamespace cns ans) =
  do 
    (VInt cSalt) <- namedMembersHashWithSalt salt cns
    foldM hashAnnNs cSalt ans >>= return . VInt
  
  where hashAnnNs accSalt (n, mems) =
          namedMembersHashWithSalt (accSalt `hashWithSalt` n) mems >>= intOfValue

namespaceHash :: CollectionNamespace Value -> Interpretation Value
namespaceHash = namespaceHashWithSalt HE.salt


-- | Dataspace comparisons
dataspaceEq :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceEq a b = dataspaceCompare a b >>= \(VInt sgn) -> return . VBool $ sgn == 0

dataspaceCompare :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceCompare (InMemoryDS l) (InMemoryDS l') = valueListCompare l l' >>= return . VInt
dataspaceCompare (InMemDS l)    (InMemDS l')    = memDSCompare l l'

dataspaceCompare (ExternalDS _) (ExternalDS _)  =
  throwE $ RunTimeInterpretationError "External DS comparison not implemented"

dataspaceCompare _ _ =
  throwE $ RunTimeInterpretationError "Cross-representation dataspace comparison not implemented"

memDSCompare :: PrimitiveMDS Value -> PrimitiveMDS Value -> Interpretation Value
memDSCompare l r = case (listOfMemDS l, listOfMemDS r) of
    (Just l', Just r') -> valueListCompare l' r' >>= return . VInt
    (_, _) -> throwE $ RunTimeInterpretationError "Invalid in-memory dataspace comparison"

dataspaceHashWithSalt :: Int -> CollectionDataspace Value -> Interpretation Value
dataspaceHashWithSalt salt (InMemoryDS l)                           = valueListHashWithSalt (salt `hashWithSalt` (0 :: Int)) l
dataspaceHashWithSalt salt (InMemDS (MemDS    (ListMDS l)))         = valueListHashWithSalt (salt `hashWithSalt` (1 :: Int)) l
dataspaceHashWithSalt salt (InMemDS (SeqDS    (ListMDS l)))         = valueListHashWithSalt (salt `hashWithSalt` (2 :: Int)) l
dataspaceHashWithSalt salt (InMemDS (SetDS    (SetAsOrdListMDS l))) = valueListHashWithSalt (salt `hashWithSalt` (3 :: Int)) l
dataspaceHashWithSalt salt (InMemDS (SortedDS (BagAsOrdListMDS l))) = valueListHashWithSalt (salt `hashWithSalt` (4 :: Int)) l
dataspaceHashWithSalt _ _ = throwE $ RunTimeInterpretationError "Unsupported dataspace hashing"

dataspaceHash :: CollectionDataspace Value -> Interpretation Value
dataspaceHash = dataspaceHashWithSalt HE.salt


-- | Named bindings (i.e., record value contents) comparisons
namedBindingsCompareBy :: (Interpretation b -> a -> a -> Interpretation b)
                       -> (NamedBindings a -> NamedBindings a -> Interpretation b)
                       -> Interpretation b
                       -> NamedBindings a -> NamedBindings a
                       -> Interpretation b
namedBindingsCompareBy f failF initR a b =
  let pairs = Map.intersectionWith (,) a b in
  if Map.size pairs /= max (Map.size a) (Map.size b)
    then failF a b
    else Map.foldl (\macc (x,y) -> f macc x y) initR pairs

namedBindingsEq :: NamedBindings Value -> NamedBindings Value -> Interpretation Value
namedBindingsEq a b = namedBindingsCompareBy chainValueEq (\_ _ -> return $ VBool False) (return $ VBool True) a b
  where 
    chainValueEq macc x y     = macc >>= chainEq valueEq x y
    chainEq f x y (VBool True)  = f x y
    chainEq _ _ _ (VBool False) = return $ VBool False
    chainEq _ _ _ _             = throwE $ RunTimeInterpretationError "Invalid equality test result"

namedBindingsCompare :: NamedBindings Value -> NamedBindings Value -> Interpretation Value
namedBindingsCompare a b = namedBindingsCompareBy chainValueCompare compareAsList (return $ VInt 0) a b
  where 
    chainValueCompare macc x y = macc >>= chainCompare valueCompare x y
    chainCompare f x y (VInt 0)       = f x y
    chainCompare _ _ _ (VInt i)       = return $ VInt i
    chainCompare _ _ _ _              = throwE $ RunTimeInterpretationError "Invalid comparison result"

    compareAsList x y                         = foldM chainEntryPairCompare (VInt 0) $ zip (Map.toAscList x) (Map.toAscList y)
    chainEntryPairCompare compareAcc (e1, e2) = chainCompare namedEntryCompare e1 e2 compareAcc
    namedEntryCompare (n1,x) (n2,y)           = if n1 /= n2 then return . VInt . orderingAsInt $ compare n1 n2 else valueCompare x y

namedBindingsHashWithSalt :: Int -> NamedBindings Value -> Interpretation Value
namedBindingsHashWithSalt salt a = Map.foldl (\macc v -> macc >>= chainHash v) (return $ VInt salt) a
  where chainHash v (VInt slt) = valueHashWithSalt slt v
        chainHash _ _          = throwE $ RunTimeInterpretationError "Invalid salt value for hashing a binding"

namedBindingsHash :: NamedBindings Value -> Interpretation Value
namedBindingsHash = namedBindingsHashWithSalt HE.salt


-- | Name members (i.e., collection namespaces) comparison
namedMembersEq :: NamedBindings (Value, VQualifier)
               -> NamedBindings (Value, VQualifier)
               -> Interpretation Value
namedMembersEq a b = namedBindingsCompareBy chainValueQualEq (\_ _ -> return $ VBool False) (return $ VBool True) a b
  where
    chainValueQualEq macc (v,q) (v',q') =
      macc >>= \eq -> if q == q' then chainEq valueEq v v' eq else return $ VBool False
    chainEq f x y (VBool True)  = f x y
    chainEq _ _ _ (VBool False) = return $ VBool False
    chainEq _ _ _ _             = throwE $ RunTimeInterpretationError "Invalid collection member equality test result"

namedMembersCompare :: NamedBindings (Value, VQualifier)
                    -> NamedBindings (Value, VQualifier)
                    -> Interpretation Value
namedMembersCompare a b = namedBindingsCompareBy chainValueQualCompare compareAsList (return $ VInt 0) a b
  where 
    chainValueQualCompare macc (v,q) (v',q') =
      macc >>= \sgn -> if q == q' then chainCompare valueCompare v v' sgn else return . VInt . orderingAsInt $ compare q q'
    chainCompare f x y (VInt 0) = f x y
    chainCompare _ _ _ (VInt i) = return $ VInt i  
    chainCompare _ _ _ _        = throwE $ RunTimeInterpretationError "Invalid collection member comparison result"

    compareAsList x y                         = foldM chainEntryPairCompare (VInt 0) $ zip (Map.toAscList x) (Map.toAscList y)
    chainEntryPairCompare compareAcc (e1, e2) = chainCompare namedEntryCompare e1 e2 compareAcc
    namedEntryCompare (n1,(x,xq)) (n2,(y,yq)) =
      if n1 /= n2 then return . VInt . orderingAsInt $ compare n1 n2
                  else valueCompare x y >>= intOfValue
                                        >>= \i -> return . VInt $ if i == 0 then orderingAsInt $ compare xq yq else i

namedMembersHashWithSalt :: Int -> NamedBindings (Value, VQualifier) -> Interpretation Value
namedMembersHashWithSalt salt a = Map.foldl (\macc vq -> macc >>= chainHash vq) (return $ VInt salt) a
  where chainHash (v,q) (VInt slt) = valueHashWithSalt (slt `hashWithSalt` q) v
        chainHash _ _              = throwE $ RunTimeInterpretationError "Invalid salt value for hashing a collection member"

namedMembersHash :: NamedBindings (Value, VQualifier) -> Interpretation Value
namedMembersHash = namedMembersHashWithSalt HE.salt


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
      VBool v'          -> rt $ showsPrec d v'
      VByte v'          -> rt $ showChar 'B' . showParen True (showsPrec appPrec1 v')
      VInt v'           -> rt $ showsPrec d v'
      VReal v'          -> rt $ showsPrec d v'
      VString v'        -> rt $ showsPrec d v'
      VOption vOptQ     -> packOptQ d vOptQ
      VTuple v'         -> parens' (packValueQ $ d+1) v'
      VRecord v'        -> packNamedMembers (d+1) v'
      VAddress v'       -> rt $ showsPrec d v'

      VCollection (_,c)     -> packCollection (d+1) c      
      VIndirection (i,q,_)  -> readMVar i >>= (\v' -> (.) <$> rt (showChar 'I') <*> packValueQ (d+1) (v', q))

      VFunction (_,_,tg) -> (forTransport ? error $ (rt . showString)) $ funSym tg
      VTrigger (n,_,tg)  -> (forTransport ? error $ (rt . showString)) $ trigSym n tg

    funSym    tg = "<function " ++ (show tg) ++ ">"
    trigSym n tg = "<trigger " ++ n ++ " " ++ (show tg) ++ " >"

    parens'  = packCustomList "(" ")" ","
    braces   = packCustomList "{" "}" ","
    brackets = packCustomList "[" "]" ","

    packValueQ d (v',q) = (\x y -> x . showChar ' ' . y) <$> packQualifier d q <*> packValue d v'

    packQualifier d q = rt $ showsPrec d q

    packOptQ d (vOpt,q) =
      maybe (rt (("Nothing " ++ show q ++ " ") ++)) 
            (\v' -> packValue appPrec1 v' >>= \showS -> rt (showParen (d > appPrec) (("Just " ++ show q ++ " ") ++) . showS))
            vOpt

    packCollection d (Collection ns ds cId) = do
      wrap' "{" "}" ((\a b c d' -> a . b . c . d')
        <$> packCollectionNamespace (d+1) ns
        <*> rt (showChar ',')
        <*> packCollectionDataspace (d+1) ds
        <*> rt (showChar ',' . showString cId))

    packCollectionNamespace d (CollectionNamespace cns ans) =
      (\a b c d' e -> a . b . c . d' . e)
        <$> rt (showString "CNS=") <*> packNamedMembers d (nonMemberFunctions cns) 
        <*> rt (showChar ',')
        <*> rt (showString "ANS=") 
        <*> braces (packAnnotationNamespace d) (map (\(x,y) -> (x, nonMemberFunctions y)) ans)

    packAnnotationNamespace d (n, namedMems) =
      (.) <$> rt (showString $ n ++ "=") <*> packNamedMembers d namedMems

    packCollectionDataspace d (InMemoryDS ds) =
      (\a b -> a . b) <$> (rt $ showString "InMemoryDS=") <*> brackets (packValue d) ds

    packCollectionDataspace d (InMemDS mds) =
      case listOfMemDS mds of
        Just l -> (\a b -> a . b) <$> (rt $ showString $ "InMemDS=" ++ typeTagOfMemDS mds)
                                  <*> brackets (packValue d) l
        _      -> error "Non-list in-memory dataspace packing not supported"

    packCollectionDataspace _ (ExternalDS filename) =
      (\a b -> a . b) <$> (rt $ showString "ExternalDS=") <*> (rt $ showString $ getFile filename)
    -- for now, external ds are shared file path
    -- Need to preserve copy semantics for external dataspaces

    {- UNUSED
    packNamedBindings d nb = braces (packNamedValue d) $ Map.toList nb
    packNamedValue d (n,v') = (.) <$> t (showString n . showChar '=') <*> packValue d v'
    -}
    packNamedMembers d nm = braces (packNamedValueQual d) $ Map.toList nm
    packNamedValueQual d (n,(v',q)) =
      (\x y z -> x . showChar '(' . y . showChar ',' . z . showChar ')') 
        <$> rt (showString n . showChar '=') <*> packValue d v' <*> packQual d q 

    packQual d q = rt $ showsPrec d q

    packCustomList :: String -> String -> String -> (a -> IO ShowS) -> [a] -> IO ShowS
    packCustomList lWrap rWrap _ _ []             = rt $ \s -> lWrap ++ rWrap ++ s
    packCustomList lWrap rWrap sep packF (x:xs)   = (\a b -> (lWrap ++) . a . b) <$> packF x <*> packl xs
      where packl []     = rt $ \s -> rWrap ++ s
            packl (y:ys) = (\a b -> (sep++) . a . b) <$> packF y <*> packl ys

    wrap' lWrap rWrap packx =
      (\a b c -> a . b . c) <$> rt (showString lWrap) <*> packx <*> rt (showString rWrap)

    appPrec  = 10
    appPrec1 = 11  

    nonMemberFunctions = Map.filter (nonFunction . fst)
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
            q <- readPrec
            v <- unpackValue
            return (v >>= rt . VOption . (,q) . Just))

      +++ (do
            Ident "Nothing" <- lexP
            q <- readPrec
            return . rt $ VOption (Nothing, q))

      +++ (prec appPrec1 $ do
            v <- readParens unpackValueQ
            return (sequence v >>= rt . VTuple))

      +++ (do
            nv <- readNamedMembers
            return (nv >>= rt . VRecord))

      +++ (do
            Ident "I" <- lexP
            vq <- unpackValueQ
            return (vq >>= \(nv,q) ->
              (\x y -> VIndirection (x, q, MemEntTag y)) <$> newMVar nv <*> makeStableName nv))
      
      +++ readCollectionPrec)

    unpackValueQ =
      parens $ reset $ do
        q <- readPrec
        v <- unpackValue
        return (v >>= rt . (,q))

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
          x <- ns
          y <- v
          c <- rebuildCollection n $ Collection x y n
          s <- newEmptyMVar
          r <- return $ VCollection (s, c)
          void $ putMVar s r
          rt $ r))

    readCollectionNamespace :: ReadPrec (IO (CollectionNamespace Value))
    readCollectionNamespace = parens $
      (prec appPrec1 $ do
        void      $ readExpectedName "CNS"
        cns      <- readNamedMembers
        Punc "," <- lexP
        void      $ readExpectedName "ANS"
        ans      <- readAnnotationNamespace
        return $ (do
          cns' <- cns
          ans' <- ans
          rt $ CollectionNamespace cns' ans'))

    readCollectionDataspace :: ReadPrec (IO (CollectionDataspace Value))
    readCollectionDataspace =
      (parens $ do
        void $ readExpectedName "InMemoryDS"
        v <- readBrackets unpackValue
        return $ InMemoryDS <$> sequence v)

      +++ (parens $ do
            void $ readExpectedName "InMemDS"
            n <- readMemDSTag
            v <- readBrackets unpackValue
            case n of 
              "MemDS"    -> return $ InMemDS . MemDS . ListMDS            <$> sequence v
              "SeqDS"    -> return $ InMemDS . SeqDS . ListMDS            <$> sequence v
              "SetDS"    -> return $ InMemDS . SetDS . SetAsOrdListMDS    <$> sequence v
              "SortedDS" -> return $ InMemDS . SortedDS . BagAsOrdListMDS <$> sequence v
              _ -> pfail)
      
      +++ (parens $ do
            void $ readExpectedName "ExternalDS"
            String filename <- lexP
            return $ rt $ ExternalDS $ FileDataspace filename)
    
    readAnnotationNamespace :: ReadPrec (IO ([(Identifier, NamedMembers Value)]))
    readAnnotationNamespace = 
      (parens $ do
        v <- readBraces $ readNamedF readNamedMembers
        return $ sequence v)

    {- UNUSED
    readNamedBindings :: ReadPrec (IO (NamedBindings Value))
    readNamedBindings = parens $ do
        v <- readBraces $ readNamedF unpackValue
        return $ Map.fromList <$> sequence v 
    -}

    readNamedMembers :: ReadPrec (IO (NamedMembers Value))
    readNamedMembers = parens $ do
        v <- readBraces $ readNamedF readValueQual
        return $ Map.fromList <$> sequence v

    readValueQual :: ReadPrec (IO (Value, VQualifier))
    readValueQual = parens $ do
        Punc "(" <- lexP
        v        <- step unpackValue
        Punc "," <- lexP
        memQ     <- readPrec
        Punc ")" <- lexP
        return $ v >>= rt . (, memQ)

    readNamedF :: ReadPrec (IO a) -> ReadPrec (IO (String, a))
    readNamedF readF = parens $ do
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

    readMemDSTag :: ReadPrec String
    readMemDSTag = do
        Ident n  <- lexP
        if n `elem` ["MemDS","SeqDS","SetDS","SortedDS"]
        then return n else pfail

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
        Nothing -> return c
        Just r  -> do
          let copyCstr = copyCtor r
          cCopyResult <- simpleEngine >>= \e -> emptyStateIO >>= \st -> runInterpretation e st (copyCstr c)
          either (const $ return c) (either (const $ return c) (\(VCollection (_,c')) -> return c') . getResultVal) cCopyResult
    
    appPrec1 = 11  

{- Misc. helpers -}

--TODO is it okay to have empty trigger list here? QueueConfig
simpleEngine :: IO (Engine Value)
simpleEngine = emptyStaticEnvIO >>= \sEnv -> 
  simulationEngine [] False defaultSystem (syntaxValueWD sEnv)

