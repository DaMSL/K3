{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Common where

import Control.Arrow ((&&&))
import Data.Char(toUpper)
import Text.PrettyPrint.ANSI.Leijen

import Language.K3.Core.Common(Identifier)
import Language.K3.Core.Annotation
import Language.K3.Core.Type

hangBrace :: Doc -> Doc
hangBrace d = text "{" <$$> indent 4 d <$$> text "}"

genDispatchClassName :: Identifier -> Identifier
genDispatchClassName i = "Dispatcher" ++ [toUpper $ head i] ++ tail i

-- Whether the type is a primitive in C++
primitiveType :: K3 Type -> Bool
primitiveType (tag &&& children -> (TTuple, [])) = True
primitiveType (tag -> TInt) = True
primitiveType (tag -> TBool) = True
primitiveType (tag -> TByte) = True
primitiveType (tag -> TReal) = True
primitiveType _ = False

