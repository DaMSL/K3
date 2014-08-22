module Language.K3.Codegen.CPP.Core.Type where

import Text.PrettyPrint.ANSI.Leijen hiding ((<$>))

import Language.K3.Core.Common
import Language.K3.Codegen.CPP.Core.Stringify

data Type
    = Named Identifier
    | Inferred
    | Parameter Identifier
    | Primitive PrimitiveType
    | Templated Type [Type]
    | Qualified Identifier Type
  deriving (Eq, Read, Show)

instance Stringifiable Type where
    stringify (Named i) = text i
    stringify (Inferred) = text "auto"
    stringify (Parameter i) = text i
    stringify (Primitive pt) = stringify pt
    stringify (Templated t ts) = stringify t <> angles (fillSep $ punctuate comma $ map stringify ts)
    stringify (Qualified i t) = text i <> colon <> colon <> stringify t

data PrimitiveType
    = PBool
    | PInt
    | PDouble
    | PString
  deriving (Eq, Read, Show)

instance Stringifiable PrimitiveType where
    stringify PBool = text "bool"
    stringify PInt = text "int"
    stringify PDouble = text "double"
    stringify PString = text "string"
