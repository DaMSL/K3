{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Primitive Definitions for Compiler-Wide Terms.
module Language.K3.Core.Common (
    Identifier,
    UID(..),
    NoneMutability(..),

    ParGenSymS(..),
    zerosymS,
    resetsymS,
    contigsymS,
    contigsymAtS,
    lowerboundsymS,
    advancesymS,
    rewindsymS,
    forksymS,
    gensym,

    Address(..),
    defaultAddress,

    Span(..),
    coverSpans,
    prefixSpan,

    EndpointSpec(..),

    IShow(..),
    IRead(..),
    ireadEither,
    iread,

    addAssoc,
    insertAssoc,
    removeAssoc,
    replaceAssoc,
    modifyAssoc,

    HasUID(..),
    HasSpan(..)
) where

import Control.Concurrent.MVar
import Control.DeepSeq

import Data.Binary ( Binary )
import Data.Serialize ( Serialize )
import qualified Data.Binary as B
import qualified Data.Serialize as S

import Data.Char
import Data.Hashable ( Hashable(..) )
import Data.IORef
import Data.Typeable

import Data.HashMap.Lazy ( HashMap )
import qualified Data.HashMap.Lazy as HashMap ( toList, fromList )

import Criterion.Types

import GHC.Generics (Generic)

import Text.ParserCombinators.ReadP    as TP
import Text.ParserCombinators.ReadPrec as TRP
import Text.Read                       as TR

import Language.K3.Utils.Pretty


-- | Identifiers are used everywhere.
type Identifier = String


-- | Address implementation
data Address = Address (String, Int) deriving (Eq, Ord, Typeable, Generic)

defaultAddress :: Address
defaultAddress = Address ("127.0.0.1", 40000)


-- | Spans are either locations in the program source, or generated code.
data Span
    = Span String Int Int Int Int
        -- ^ Source name, start line and column, end line and column.

    | GeneratedSpan String
        -- ^ Generator-specific metadata.
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Unique identifiers for AST nodes.
data UID = UID Int deriving (Eq, Ord, Read, Show, Typeable, Generic)


{- Symbol generation -}
data ParGenSymS = ParGenSymS { stride :: Int, offset :: Int, current :: Int }
                  deriving (Eq, Ord, Read, Show, Generic)

zerosymS :: Int -> Int -> ParGenSymS
zerosymS str off = ParGenSymS str off off

resetsymS :: ParGenSymS -> ParGenSymS
resetsymS (ParGenSymS str off _) = ParGenSymS str off off

contigsymS :: ParGenSymS
contigsymS = ParGenSymS 1 0 0

contigsymAtS :: Int -> ParGenSymS
contigsymAtS cur = ParGenSymS 1 0 cur

lowerboundsymS :: Int -> ParGenSymS -> ParGenSymS
lowerboundsymS lb s@(ParGenSymS str off cur)
  | cur <= lb = ParGenSymS str off ((lb `divceil` str) * str + off)
  | otherwise = s
  where divceil x y = let (a,b) = x `divMod` y in (a + (if b == 0 then 0 else 1))

advancesymS :: Int -> ParGenSymS -> Maybe ParGenSymS
advancesymS deltaOff (ParGenSymS str off i)
  | deltaOff + off >= str = Nothing
  | otherwise = Just $ ParGenSymS str (off + deltaOff) i

rewindsymS :: ParGenSymS -> ParGenSymS -> ParGenSymS
rewindsymS (ParGenSymS sa oa ca) (ParGenSymS sb ob cb) = ParGenSymS (max sa sb) (min oa ob) (max ca cb)

forksymS :: Int -> ParGenSymS -> ParGenSymS
forksymS i (ParGenSymS str off cur)
  | i > 0 = ParGenSymS (i*str) off cur
  | otherwise = error "Invalid symbol generator fork factor."

gensym :: ParGenSymS -> (ParGenSymS, Int)
gensym (ParGenSymS str off cur) = (ParGenSymS str off (cur + str), cur + off)

instance Binary    ParGenSymS
instance Serialize ParGenSymS

-- |Mutability modes for @CNone@.  These are kept distinct from the expression
--  annotations because e.g. @mut (mut None mut, mut None mut)@ must have a
--  place to put each @mut@ without overlapping.
data NoneMutability
    = NoneMut
    | NoneImmut
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Endpoint types.
data EndpointSpec
  = ValueEP
  | BuiltinEP String String
    -- ^ Builtin endpoint type (stdin/stdout/stderr), format

  | FileEP    String Bool String
    -- ^ File path (as expression or literal), text/binary, format

  | FileSeqEP String Bool String
    -- ^ File sequence path collection (as expression), text/binary, format

  | NetworkEP String Bool String
    -- ^ Address, format
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Union two spans.
coverSpans :: Span -> Span -> Span
coverSpans (Span n l1 c1 _ _) (Span _ _ _ l2 c2) = Span n l1 c1 l2 c2
coverSpans s@(Span _ _ _ _ _) (GeneratedSpan _)  = s
coverSpans (GeneratedSpan _) s@(Span _ _ _ _ _)  = s
coverSpans (GeneratedSpan s1) (GeneratedSpan s2) = GeneratedSpan $ s1++", "++s2

-- | Left extension of a span.
prefixSpan :: Int -> Span -> Span
prefixSpan i (Span n l1 c1 l2 c2) = Span n l1 (c1-i) l2 c2
prefixSpan _ s = s

-- | Associative lists

-- | Adds an association at the head of the list, allowing for duplicates.
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

-- | Adds an association only if it does not already exist in the list.
insertAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
insertAssoc l a b = maybe (addAssoc l a b) (const l) $ lookup a l

-- | Removes all associations matching the given key from the list.
removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

-- | Replaces all associations matching the given key, with a single new association.
replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

-- | Applies a modifier to the first occurrence of the key, replacing all associations
--   with the result of the modifier.
modifyAssoc :: Eq a => [(a,b)] -> a -> (Maybe b -> (c, Maybe b)) -> (c, [(a,b)])
modifyAssoc l k f = case f $ lookup k l of
  (r, Nothing) -> (r, l)
  (r, Just nv) -> (r, replaceAssoc l k nv)


{- Instance implementations -}
instance NFData Address
instance NFData Span
instance NFData UID
instance NFData NoneMutability
instance NFData EndpointSpec

instance Binary Address
instance Binary Span
instance Binary UID
instance Binary NoneMutability
instance Binary EndpointSpec

instance Serialize Address
instance Serialize Span
instance Serialize UID
instance Serialize NoneMutability
instance Serialize EndpointSpec

instance Show Address where
  show (Address (host, port)) = host ++ ":" ++ show port

instance Read Address where
  readPrec = parens $ do
          host       <- lift $ munch1 $ \c -> isAlpha c || isDigit c || c == '.'
          Symbol ":" <- lexP
          port       <- readPrec
          return (Address (host, port))

instance Hashable Address where
  hashWithSalt salt (Address (host,port)) = hashWithSalt salt (host, port)

instance Pretty Address where
  prettyLines addr = [show addr]

instance Pretty UID where
  prettyLines (UID n) = [show n]


-- | Show and read of impure values
class IShow a where
  ishow :: a -> IO String

instance (Show a) => IShow a where
  ishow = return . show

class IRead a where
  ireadPrec :: ReadPrec (IO a)

instance (Read a) => IRead a where
  ireadPrec = readPrec >>= return . return

ireadEither :: (IRead a) => String -> IO (Either String a)
ireadEither s =
  case [ x | (x,"") <- readPrec_to_S read' minPrec s ] of
    [x] -> x >>= return . Right
    []  -> return $ Left "iread: no parse"
    _   -> return $ Left "iread: ambiguous parse"
 where
  read' = do
    x <- ireadPrec
    TRP.lift TP.skipSpaces
    return x

iread :: (IRead a) => String -> IO a
iread s = ireadEither s >>= return . either error id


-- | IShow, and IRead instance for mutable values.
instance (IShow a) => IShow (IORef a) where
  ishow r = readIORef r >>= ishow >>= return . ("IORef " ++)

instance (IShow a) => IShow (MVar a) where
  ishow mv = readMVar mv >>= ishow >>= return . ("MVar " ++)

instance (IRead a) => IRead (IORef a) where
  ireadPrec = parens ( do
      TR.Ident s <- TR.lexP
      case s of
        "IORef" -> ireadPrec >>= return . (>>= newIORef)
        _ -> TRP.pfail
    )

instance (IRead a) => IRead (MVar a) where
  ireadPrec = parens ( do
      TR.Ident s <- TR.lexP
      case s of
        "MVar" -> ireadPrec >>= return . (>>= newMVar)
        _ -> TRP.pfail
    )

class HasUID a where
  getUID :: a -> Maybe UID

class HasSpan a where
  getSpan :: a -> Maybe Span


{- Additional instances -}
instance (Binary k, Binary v, Eq k, Hashable k) => Binary (HashMap k v) where
    put = B.put . HashMap.toList
    get = fmap HashMap.fromList B.get

instance (Serialize k, Serialize v, Eq k, Hashable k) => Serialize (HashMap k v) where
    put = S.put . HashMap.toList
    get = fmap HashMap.fromList S.get

instance Serialize Measured where
    put Measured{..} = do
      S.put measTime; S.put measCpuTime; S.put measCycles; S.put measIters
      S.put measAllocated; S.put measNumGcs; S.put measBytesCopied
      S.put measMutatorWallSeconds; S.put measMutatorCpuSeconds
      S.put measGcWallSeconds; S.put measGcCpuSeconds
    get = Measured <$> S.get <*> S.get <*> S.get <*> S.get
                   <*> S.get <*> S.get <*> S.get <*> S.get <*> S.get <*> S.get <*> S.get
