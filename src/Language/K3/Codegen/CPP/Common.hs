module Language.K3.Codegen.CPP.Common where

import Text.PrettyPrint.ANSI.Leijen

hangBrace :: Doc -> Doc
hangBrace d = text "{" <$$> indent 4 d <$$> text "}"
