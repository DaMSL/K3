{-# LANGUAGE OverloadedStrings #-}

module Language.K3.Codegen.CPP.Representation where

import Data.String

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

class Stringifiable a where
    stringify :: a -> Doc

data Name
    = Name Identifier
    | Qualified Identifier Name
    | Specialized [Type] Name
  deriving (Eq, Read, Show)

