{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Primitive Definitions for Compiler-Wide Terms.
module Language.K3.Core.Common (
    Identifier,
    Span(..),
    UID(..),

    IShow(..),
    IRead(..),
    ireadEither,
    iread,

    addAssoc,
    removeAssoc,
    replaceAssoc,
    modifyAssoc
) where

import Control.Concurrent.MVar

import Data.IORef

import Text.ParserCombinators.ReadP    as TP
import Text.ParserCombinators.ReadPrec as TRP
import Text.Read                       as TR

import Language.K3.Pretty

-- | Identifiers are used everywhere.
type Identifier = String

-- | Spans are locations in the program source.
data Span = Span String Int Int Int Int deriving (Eq, Ord, Read, Show)

-- | Unique identifiers for AST nodes.
data UID = UID Int deriving (Eq, Ord, Read, Show)

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

-- | Associative lists
addAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
addAssoc l a b = (a,b):l

removeAssoc :: Eq a => [(a,b)] -> a -> [(a,b)]
removeAssoc l a = filter ((a /=) . fst) l

replaceAssoc :: Eq a => [(a,b)] -> a -> b -> [(a,b)]
replaceAssoc l a b = addAssoc (removeAssoc l a) a b

modifyAssoc :: Eq a => [(a,b)] -> a -> (Maybe b -> (c,b)) -> (c, [(a,b)])
modifyAssoc l k f = (r, replaceAssoc l k nv)
  where (r, nv) = f $ lookup k l
