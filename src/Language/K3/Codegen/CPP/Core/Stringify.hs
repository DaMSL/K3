module Language.K3.Codegen.CPP.Core.Stringify (Stringifiable(..)) where

import Text.PrettyPrint.ANSI.Leijen (Doc)

class Stringifiable a where
    stringify :: a -> Doc
