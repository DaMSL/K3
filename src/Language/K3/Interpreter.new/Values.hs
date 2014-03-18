{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}

module Language.K3.Interpreter_new.Values where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.IO.Class

import Data.Function
import Data.Hashable
import Data.IORef
import Data.List
import Data.Map ( Map )
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
import Language.K3.Utils.Pretty

{- Value equality -}

-- | Haskell Eq type class implementation.
--   This uses entity-tag equality for indirections, functions and triggers.
--   All other values use structural equality.
instance Eq Value where
  VBool v             == VBool v'             = v == v'
  VByte v             == VByte v'             = v == v'
  VInt  v             == VInt  v'             = v == v'
  VReal v             == VReal v'             = v == v'
  VString v           == VString v'           = v == v'
  VAddress v          == VAddress v'          = v == v'  
  VOption v           == VOption v'           = v == v'
  VTuple v            == VTuple v'            = v == v'
  VRecord v           == VRecord v'           = v == v'   -- Equality based on Data.Map.Eq
  VIndirection (_,tg) == VIndirection (_,tg') = tg == tg'  
  VFunction (_,_,tg)  == VFunction (_,_,tg')  = tg == tg'
  VTrigger (n,_,tg)   == VTrigger (n',_,tg')  = n == n' && tg == tg'
  
  -- Collections must compare their namespaces due to mutable members.
  VCollection (_,v)   == VCollection (_,v') =
    (realizationId v, namespace v, dataspace v)
      == (realizationId v', namespace v', dataspace v')

  _                   == _                    = False

instance (Eq a) => Eq (CollectionNamespace a) where
  (CollectionNamespace cns ans) == (CollectionNamespace cns' ans') =
    cns == cns' && ans == ans'

-- | Eq typeclass instance for dataspaces.
--   This does not support equality for external dataspaces, due to purity constraints.
--   The interpreter must provide its own method for equality guarding against external dataspaces.
instance (Eq a) => Eq (CollectionDataspace a) where
  (InMemoryDS l)    == (InMemoryDS l')   = l == l'
  (InMemDS primDS)  == (InMemDS primDS') = primDS == primDS'
  _                 == _                 = error "Incomparable dataspaces"


-- | Haskell Ord type class implementation.
--   Comparisons for indirections, functions and triggers use hash ordering.
instance Ord Value where
  compare (VBool a)    (VBool b)    = compare a b
  compare (VByte a)    (VByte b)    = compare a b
  compare (VInt a)     (VInt b)     = compare a b
  compare (VReal a)    (VReal b)    = compare a b
  compare (VString a)  (VString b)  = compare a b
  compare (VAddress a) (VAddress b) = compare a b
  compare (VOption a)  (VOption b)  = compare a b
  compare (VTuple a)   (VTuple b)   = compare a b
  compare (VRecord a)  (VRecord b)  = compare a b    -- Based on Data.Map.Ord

  compare (VIndirection (_,tga)) (VIndirection (_,tgb)) = compare tga tgb
  compare (VFunction (_,_,tga))  (VFunction (_,_,tgb))  = compare tga tgb
  compare (VTrigger (n,_,tga))   (VTrigger (n',_,tgb))  = compare (n,tga) (n',tgb)

  compare (VCollection (_,v)) (VCollection (_,v')) =
    compare (realizationId v, namespace v, dataspace v)
            (realizationId v', namespace v', dataspace v')

  compare _ _ = error "Invalid value comparison"

instance (Ord a) => Ord (CollectionNamespace a) where
  (CollectionNamespace cns ans) == (CollectionNamespace cns' ans') =
    compare (cns, ans) (cns', ans')

-- | Ord typeclass instance for dataspaces.
--   Much like Eq, this does not support comparison of external dataspaces.
--   The interpreter must provide its own method guarding against external dataspaces.
instance (Ord a) => Ord (CollectionDataspace a) where
  compare (InMemoryDS l)   (InMemoryDS l')   = compare l l'
  compare (InMemDS primDS) (InMemDS primDS') = compare primDS primDS'
  compare _                _                 = error "Incomparable dataspaces"


-- | Haskell Hashable type class implementation.
--   Indirections, functions and triggers return the hash code of their entity tag.
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

  hashWithSalt salt (VIndirection (_,tg)) = hashWithSalt salt tg
  hashWithSalt salt (VFunction (_,_,tg))  = hashWithSalt salt tg
  hashWithSalt salt (VTrigger (_,_,tg))   = hashWithSalt salt tg

  hashWithSalt salt (VCollection (_,a)) =
    salt `hashWithSalt` (namespace a) `hashWithSalt` (dataspace a) 

  hashWithSalt _ _ = error "Invalid value hash operation"

instance Hashable MemberQualifier

instance (Hashable a) => Hashable (CollectionNamespace a) where
  hashWithSalt salt (CollectionNamespace cns ans) =
    salt `hashWithSalt` cns `hashWithSalt` ans

-- | Hashable instance for dataspaces. External dataspaces are not supported.
instance (Hashable a) => Hashable (CollectionDataspace a) where
  hashWithSalt salt (CollectionDataspace (InMemoryDS l))   = salt `hashWithSalt` 0 `hashWithSalt` l
  hashWithSalt salt (CollectionDataspace (InMemDS primDS)) = salt `hashWithSalt` 1 `hashWithSalt` primDS
  hashWithSalt salt (CollectionDataspace _)                = error "Unhashable dataspace"


orderingAsInt :: Ordering -> Int
orderingAsInt LT = -1
orderingAsInt EQ = 0
orderingAsInt GT = 1

composeHash :: Int -> Value -> Int
composeHash s1 (VInt s2) = s1 `hashWithSalt` s2
composeHash _ _          = throwE $ RunTimeInterpretationError "Invalid salt value in hash composition"


-- | Interpreter implementations of equality and comparison functions.
--   Since these operate in the interpretation monad, these can perform side-effects,
--   for example, to compare external dataspaces.
valueEq :: Value -> Value -> Interpretation Value
valueEq a b = valueCompare a b >>= guard
  where guard (VInt sgn) = return . VBool $ sgn == 0
        guard _          = throwE $ RunTimeInterpretationError "Invalid comparison value in equality test"

valueCompare :: Value -> Value -> Interpretation Value
valueCompare (VOption (Just v))  (VOption (Just v'))  = valueCompare v v'
valueCompare (VTuple v)          (VTuple v')          = listCompare v v' >>= return . VInt
valueCompare (VRecord v)         (VRecord v')         = namedBindingsCompare v v'
valueCompare (VCollection (_,v)) (VCollection (_,v')) = collectionCompare v v'
valueCompare a b = return . VInt . orderingAsInt $ compare a b

valueHashWithSalt :: Int -> Value -> Interpretation Value
valueHashWithSalt salt (VOption (Just v))  = valueHashWithSalt v >>= return . VInt . composeHash (salt `hashWithSalt` 0)
valueHashWithSalt salt (VTuple v)          = listHashWithSalt 1 v
valueHashWithSalt salt (VRecord v)         = namedBindingsHashWithSalt 2 v
valueHashWithSalt salt (VCollection (_,v)) = collectionHashWithSalt 3 v
valueHashWithSalt salt x = return . VInt $ salt `hashWithSalt` x

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
    if sgn == 0 then listCompare as bs else return sgn

valueListHashWithSalt :: Int -> [Value] -> Interpretation Value
valueListHashWithSalt salt l = foldM hashNext salt l >>= return . VInt
  where hashNext accSalt v = valueHashWithSalt v >>= return . composeHash accSalt

valueListHash :: [Value] -> Interpretation Int
valueListHash = valueListHashWithSalt HE.salt 

-- | Collection comparison, including the namespace and dataspace components.
collectionEq :: Collection Value -> Collection Value -> Interpretation Value
collectionEq c1 c2 = collectionCompare c1 c2 >>= \(VInt sgn) -> return . VBool $ sgn == 0

collectionCompare :: Collection Value -> Collection Value -> Interpretation Value
collectionCompare (Collection ns ds cId) (Collection ns' ds' cId') = 
  let idOrd = compare cId cId' in
  if idOrd /= EQ then return . VInt $ orderingAsInt idOrd
                 else do
                        (VInt nsSgn) <- namespaceCompare ns ns'
                        if nsOrd /= 0 then return $ VInt nsSgn else dataspaceCompare ds ds'

collectionHashWithSalt :: Int -> Collection Value -> Interpretation Value
collectionHashWithSalt salt (Collection ns ds cId) = do
  let idSalt = salt `hashWithSalt` cId
  (VInt nsSalt) <- namespaceHashWithSalt idSalt ns
  return dataspaceHashWithSalt nsSalt ds

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
      else foldM guard (VInt 0) $ zip ans ans' 
  
  where guard (VInt 0) ((na,a), (nb,b)) = if na /= nb then return . VInt $ compare na nb 
                                                      else namedMembersCompare a b
        guard (VInt sgn) _ = return $ VInt sgn
        guard _            = throwE $ RunTimeInterpretationError "Invalid comparison result"

namespaceHashWithSalt :: Int -> CollectionNamespace Value -> Interpretation Value
namespaceHashWithSalt salt (CollectionNamespace cns ans) =
  do 
    (VInt cSalt) <- namedMembersHashWithSalt salt cns
    return foldM hashAnnNs cSalt ans >>= return . VInt
  
  where hashAnnNs accSalt (n, mems) =
          namedMembersHashWithSalt mems >>= composeHash (accSalt `hashWithSalt` n)

namespaceHash :: CollectionNamespace Value -> Interpretation Value
namespaceHash = namespaceHashWithSalt HE.salt


-- | Dataspace comparisons
dataspaceEq :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceEq a b = dataspaceCompare a b >>= \(VInt sgn) -> return . VBool $ sgn == 0

dataspaceCompare :: CollectionDataspace Value -> CollectionDataspace Value -> Interpretation Value
dataspaceCompare (InMemoryDS l) (InMemoryDS l') = listCompare l l' >>= return . VInt
dataspaceCompare (InMemDS l)    (InMemDS l')    = memDSCompare l l'
dataspaceCompare (ExternalDS _) (ExternalDS _)  =
  throwE $ RunTimeInterpretationError "External DS comparison not implemented"
dataspaceCompare _ _ =
  throwE $ RunTimeInterpretationError "Cross-representation dataspace comparison not implemented"

memDSCompare :: PrimitiveMDS Value -> PrimitiveMDS Value -> Interpretation Value
memDSCompare l r = case (listOfMemDS l, listOfMemDS r) of
    (Just l', Just r') -> listCompare l' r' >>= return . VInt
    (_, _) -> throwE $ RunTimeInterpretationError "Invalid in-memory dataspace comparison"

dataspaceHashWithSalt :: CollectionDataspace Value -> Interpretation Value
dataspaceHashWithSalt (InMemoryDS l)      = listHashWithSalt 0 l
dataspaceHashWithSalt (InMemDS (MemDS l)) = listHashWithSalt 1 l
dataspaceHashWithSalt (InMemDS (SeqDS l)) = listHashWithSalt 2 l
dataspaceHashWithSalt (InMemDS (SetDS l)) = listHashWithSalt 3 l
dataspaceHashWithSalt (InMemDS (BagDS l)) = listHashWithSalt 4 l
dataspaceHashWithSalt _ = throwE $ RunTimeInterpretationError "Unsupported dataspace hashing"

dataspaceHash :: CollectionDataspace Value -> Interpretation Value
dataspaceHash = dataspaceHashWithSalt HE.salt


-- | Named bindings (i.e., record value contents) comparisons
namedBindingsCompareBy :: (Interpretation a -> a -> a -> Interpretation a)
                       -> Interpretation a
                       -> NamedBindings a -> NamedBindings a
                       -> Interpretation Value
namedBindingsCompareBy f init a b =
  let pairs = Map.intersectionWith (,) a b
  if Map.size pairs /= max (Map.size a) $ Map.size b
    then return $ VBool False
    else Map.foldl (\macc (x,y) -> f macc x y) init pairs

namedBindingsEq :: NamedBindings Value -> NamedBindings Value -> Interpretation Value
namedBindingsEq a b = namedBindingsCompareBy chainValueEq (return $ VBool True) a b
  where 
    chainValueEq macc x y     = macc >>= guard valueEq x y
    guard f x y (VBool True)  = f x y
    guard f x y (VBool False) = return $ VBool False
    guard _ _ _ _             = throwE $ RunTimeInterpretationError "Invalid equality test result"

namedBindingsCompare :: NamedBindings Value -> NamedBindings Value -> Interpretation Value
namedBindingsCompare a b = namedBindingsCompareBy chainValueCompare (return $ VInt 0) a b
  where 
    chainValueCompare macc x y = macc >>= guard valueEq x y
    guard f x y (VInt 0)       = f x y
    guard f x y (VInt i)       = return $ VInt i
    guard _ _ _ _              = throwE $ RunTimeInterpretationError "Invalid comparison result"

namedBindingsHashWithSalt :: Int -> NamedBindings Value -> Interpretation Value
namedMembersHash salt a = Map.foldl (\macc v -> macc >>= guard v) (return $ VInt salt) a
  where guard v (VInt salt) = valueHashWithSalt salt v
        guard _ _           = throwE $ RunTimeInterpretationError "Invalid salt value for hashing a binding"

namedBindingsHash :: NamedBindings Value -> Interpretation Value
namedBindingsHash = namedBindingsHashWithSalt HE.salt


-- | Name members (i.e., collection namespaces) comparison
namedMembersEq :: NamedBindings (Value, MemberQualifier)
               -> NamedBindings (Value, MemberQualifier)
               -> Interpretation Value
namedMembersEq a b = namedBindingsCompareBy chainValueQualEq (return $ VBool True) a b
  where
    chainValueQualEq macc (v,q) (v',q') =
      macc >>= \eq -> if q == q' then guard valueEq v v' eq else return $ VBool False
    guard f x y (VBool True)  = f x y
    guard f x y (VBool False) = return $ VBool False
    guard _ _ _ _             = throwE $ RunTimeInterpretationError "Invalid collection member equality test result"

namedMembersCompare :: NamedBindings (Value, MemberQualifier)
                    -> NamedBindings (Value, MemberQualifier)
                    -> Interpretation Value
namedMembersCompare a b = namedBindingsCompareBy chainValueQualCompare (return $ VInt 0) a b
  where 
    chainValueQualCompare macc (v,q) (v,q') =
      macc >>= \sgn -> if q == q' then guard valueCompare v v' sgn else return . VInt $ compare q q'
    guard f x y (VInt 0) = f x y
    guard f x y (VInt i) = return $ VInt i  
    guard _ _ _ _        = throwE $ RunTimeInterpretationError "Invalid collection member comparison result"

namedMembersHashWithSalt :: Int -> NamedBindings (Value, MemberQualifier) -> Interpretation Value
namedMembersHashWithSalt salt a = Map.foldl (\macc v -> guard macc v) (return $ VInt salt) a
  where guard (v,q) (VInt salt) = valueHashWithSalt (salt `hashWithSalt` q) v
        guard _ _               = throwE $ RunTimeInterpretationError "Invalid salt value for hashing a collection member"

namedMembersHash :: NamedBindings (Value, MemberQualifier) -> Interpretation Value
namedMembersHash = namedMembersHashWithSalt HE.salt


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
  showsPrec d (VBool v)            = showsPrecTag "VBool" d v
  showsPrec d (VByte v)            = showsPrecTag "VByte" d v
  showsPrec d (VInt v)             = showsPrecTag "VInt" d v
  showsPrec d (VReal v)            = showsPrecTag "VReal" d v
  showsPrec d (VString v)          = showsPrecTag "VString" d v
  showsPrec d (VOption v)          = showsPrecTag "VOption" d v
  showsPrec d (VTuple v)           = showsPrecTag "VTuple" d v
  showsPrec d (VRecord v)          = showsPrecTag "VRecord" d v
  showsPrec d (VAddress v)         = showsPrecTag "VAddress" d v  
  showsPrec d (VCollection (_,c))  = showsPrecTag "VCollection"  d c

  showsPrec d (VIndirection (_,tg))       = showsPrecTagF "VIndirection" d $ showString $ "<" ++ (show tg) ++ ">"
  showsPrec d (VFunction (_,_,tg))        = showsPrecTagF "VFunction"    d $ showString $ "<" ++ (show tg) ++ ">"
  showsPrec d (VTrigger (_, Nothing, tg)) = showsPrecTagF "VTrigger"     d $ showString $ "<uninitialized:" ++ (show tg) ++">"
  showsPrec d (VTrigger (_, Just _, tg))  = showsPrecTagF "VTrigger"     d $ showString $ "<function:" ++ (show tg) ++ ">"

instance Show EntityTag where
  showsPrec d (MemEntTag s) = showsPrecTag "MemEntTag" d (hashStableName s)
  showsPrec d (ExtEntTag i) = showsPrecTag "ExtEntTag" d i

instance Show (Collection Value) where
  showsPrec d (Collection ns ds cId) =
    showsPrecTagF "Collection" d $ showString $ (show ns) ++ " " ++ (show ds) " " ++ cId 

instance Show (CollectionNamespace Value) where
  showsPrec d (CollectionNamespace cns ans) =
    showsPrecTagF "CollectionNamespace" d $ showString $ (show cns) ++ " " ++ (show ans)

instance Show (CollectionDataspace Value) where
  showsPrec d (InMemoryDS l)    = showsPrecTag "InMemoryDS" d l
  showsPrec d (InMemDS primMDS) = showsPrecTag "InMemDS" d primMDS 
  showsPrec d (ExternalDS (FileDataspace fId)) = showsPrecTag "ExternalDS" d fId

instance Show (PrimitiveMDS Value) where
  showsPrec d (MemDS l) = showsPrecTag "MemDS" d l
  showsPrec d (SeqDS l) = showsPrecTag "SeqDS" d l
  showsPrec d (SetDS l) = showsPrecTag "SetDS" d l
  showsPrec d (BagDS l) = showsPrecTag "BagDS" d l


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

    -- We cannot initialize the self pointer in pure fashion for collections
    +++ (prec app_prec $ do
          Ident "VCollection" <- lexP
          c <- step (readPrec :: ReadPrec (Collection Value))
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

instance Read EntityTag where
  readPrec = parens $ 
        (prec app_prec $ do
          Ident "MemEntTag" <- lexP
          v <- step (readPrec :: ReadPrec Int)
          error "Cannot read in-memory entity tags")
          -- Above, we cannot obtain the original stable name corresponding to its hashed value.

    +++ (prec app_prec $ do
          Ident "ExtEntTag" <- lexP
          n <- step readPrec
          return $ ExtEntTag n)

    where app_prec = 10

instance Read (Collection Value) where
  readPrec = parens $ prec app_prec $ do
      Ident "Collection" <- lexP
      ns  <- step readPrec
      ds  <- step readPrec
      cId <- step readPrec
      return $ Collection ns ds cId

    where app_prec = 10

instance Read (CollectionNamespace Value) where
  readPrec = parens $ prec app_prec $ do
      Ident "CollectionNamespace" <- lexP
      cns <- step readPrec
      ans <- step readPrec
      return $ CollectionNamespace cns ans

    where app_prec = 10

instance Read (CollectionDataspace Value) where
  readPrec = parens $
          (prec app_prec $ do
            Ident "InMemoryDS" <- lexP
            l <- step readPrec
            return $ InMemoryDS l)
      
      +++ (prec app_prec $ do
            Ident "InMemDS" <- lexP
            primMDS <- step readPrec
            return $ InMemDS primMDS)

      +++ (prec app_prec $ do
            Ident "ExternalDS" <- lexP
            fId <- step readPrec
            return $ ExternalDS (FileDataspace fId)) 

    where app_prec = 10

instance Read (PrimitiveMDS Value) where
  readPrec = parens $
          (prec app_prec $ do
            Ident "MemDS" <- lexP
            l <- step readPrec
            return $ MemDS l)
      
      +++ (prec app_prec $ do
            Ident "SeqDS" <- lexP
            l <- step readPrec
            return $ SeqDS l)

      +++ (prec app_prec $ do
            Ident "SetDS" <- lexP
            l <- step readPrec
            return $ SetDS l)

      +++ (prec app_prec $ do
            Ident "BagDS" <- lexP
            l <- step readPrec
            return $ BagDS l)

    where app_prec = 10

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
      VRecord v'   -> packNamedBindings (d+1) v'
      
      VCollection (_,c) -> packCollection (d+1) c      
      VIndirection r    -> readMVar r >>= (\v' -> (.) <$> rt (showChar 'I') <*> packValue (d+1) v')
      VAddress v'       -> rt $ showsPrec d v'

      VFunction (_,_,tg) -> (forTransport ? error $ (rt . showString)) $ funSym tg
      VTrigger (n,_,tg)  -> (forTransport ? error $ (rt . showString)) $ trigSym n tg

    funSym    tg = "<function " ++ (show tg) ++ ">"
    trigSym n tg = "<trigger " ++ n ++ " " ++ (show tg) ++ " >"

    parens'  = packCustomList "(" ")" ","
    braces   = packCustomList "{" "}" ","
    brackets = packCustomList "[" "]" ","

    packOpt d vOpt =
      maybe (rt ("Nothing "++)) 
            (\v' -> packValue appPrec1 v' >>= \showS -> rt (showParen (d > appPrec) ("Just " ++) . showS))
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

    packAnnotationNamespace d (n, namedMems) = (.) <$> rt (showString $ n ++ "=") <*> packNamedMembers d nv

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

    packNamedBindings d nv = braces (packNamedValue d) $ Map.toList nv
    packNamedMembers  d nm = braces (packNamedValueQual d) $ Map.toList nv     

    packNamedValue d (n,v') = (.) <$> rt (showString n . showChar '=') <*> packValue d v'
    
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
            v <- unpackValue
            return (v >>= rt . VOption . Just))

      +++ (do
            Ident "Nothing" <- lexP
            return . rt $ VOption Nothing)

      +++ (prec appPrec1 $ do
            v <- readParens unpackValue
            return (sequence v >>= rt . VTuple))

      +++ (do
            nv <- readNamedBindings unpackValue
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
          x <- ns
          y <- v
          c <- rebuildCollection n $ Collection x y n
          s <- newMVar c
          rt $ VCollection (s, cmv)))

    readCollectionNamespace :: ReadPrec (IO (CollectionNamespace Value))
    readCollectionNamespace = parens $
      (prec appPrec1 $ do
        void      $ readExpectedName "CNS"
        cns      <- readNamedMembers unpackValue
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

    readNamedBindings :: ReadPrec (IO (NamedBindings Value))
    readNamedBindings = parens $ do
        v <- readBraces $ readNamedF unpackValue
        return $ Map.fromList <$> sequence v 

    readNamedMembers :: ReadPrec (IO (NamedMembers Value))
    readNamedMembers = parens $ do
        v <- readBraces $ readNamedF readValueQual
        return $ Map.fromList <$> sequence v

    readValueQual :: ReadPrec (IO (Value, MemberQualifier))
    readValueQual = parens $ do
        Punc "(" <- lexP
        v        <- step unpackValue
        Punc "," <- lexP
        memQ     <- readPrec
        Punc ")" <- lexP
        return $ rt (v, memQ)

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

