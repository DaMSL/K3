{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Primitive Definitions for Compiler-Wide Terms.
module Language.K3.Core.Common (
    Identifier,
    UID(..),

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
    removeAssoc,
    replaceAssoc,
    modifyAssoc,

    HasUID(..),
    HasSpan(..)
) where

import Control.Concurrent.MVar

import Data.Char
import Data.Hashable ( Hashable(..) )
import Data.IORef

import Text.ParserCombinators.ReadP    as TP
import Text.ParserCombinators.ReadPrec as TRP
import Text.Read                       as TR

import Language.K3.Utils.Pretty

-- | Identifiers are used everywhere.
type Identifier = String


-- | Address implementation
data Address = Address (String, Int) deriving (Eq, Ord)

defaultAddress :: Address
defaultAddress = Address ("127.0.0.1", 40000)


-- | Spans are either locations in the program source, or generated code.
data Span
    = Span String Int Int Int Int
        -- ^ Source name, start line and column, end line and column.
    
    | GeneratedSpan String 
        -- ^ Generator-specific metadata.
  deriving (Eq, Ord, Read, Show)

-- | Unique identifiers for AST nodes.
data UID = UID Int deriving (Eq, Ord, Read, Show)

-- | Endpoint types.
data EndpointSpec
  = ValueEP
  | BuiltinEP String String    -- ^ Builtin endpoint type (stdin/stdout/stderr), format  
  | FileEP    String String    -- ^ File path, format
  | NetworkEP String String    -- ^ Address, format
  deriving (Eq, Read, Show)

-- | Union two spans.
coverSpans :: Span -> Span -> Span
coverSpans (Span n l1 c1 _ _) (Span _ _ _ l2 c2) = Span n l1 c1 l2 c2
coverSpans s@(Span _ _ _ _ _) (GeneratedSpan _)  = s
coverSpans (GeneratedSpan _) s@(Span _ _ _ _ _)  = s
coverSpans (GeneratedSpan s1) (GeneratedSpan s2) = GeneratedSpan (s1++", "++s2)

-- | Left extension of a span.
prefixSpan :: Int -> Span -> Span
prefixSpan i (Span n l1 c1 l2 c2) = Span n l1 (c1-i) l2 c2
prefixSpan _ s = s

-- | Associative lists
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

modifyAssoc :: Eq a => [(a,b)] -> a -> (Maybe b -> (c, Maybe b)) -> (c, [(a,b)])
modifyAssoc l k f = case f $ lookup k l of
  (r, Nothing) -> (r, l)
  (r, Just nv) -> (r, replaceAssoc l k nv)


{- Instance implementations -}
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

