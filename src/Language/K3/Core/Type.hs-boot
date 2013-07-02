{-# LANGUAGE FlexibleInstances, TypeFamilies #-}

-- An .hs-boot module to resolve the cyclic import between
-- the Expression and Type modules.
module Language.K3.Core.Type where

import Language.K3.Core.Annotation

type Identifier = String

data Span = Span String Int Int Int Int
instance Eq Span
instance Ord Span
instance Read Span
instance Show Span

data Type
instance Eq Type
instance Read Type
instance Show Type

instance Eq (Annotation Type)
instance Read (Annotation Type)
instance Show (Annotation Type)

