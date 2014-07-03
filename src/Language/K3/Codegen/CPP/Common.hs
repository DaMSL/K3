module Language.K3.Codegen.CPP.Common where

import Text.PrettyPrint.ANSI.Leijen
import Language.K3.Core.Common(Identifier)
import Data.Char(toUpper)

hangBrace :: Doc -> Doc
hangBrace d = text "{" <$$> indent 4 d <$$> text "}"

genDispatchClassName :: Identifier -> Identifier
genDispatchClassName i = "Dispatcher" ++ [toUpper $ head i] ++ tail i

