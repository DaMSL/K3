{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ViewPatterns #-}

-- | Expressions in K3.
module Language.K3.Core.Expression where

import Control.Monad.Identity
import Data.List
import Data.Tree
import Data.Word (Word8)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Annotation.Codegen
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Literal

import Language.K3.Analysis.HMTypes.DataTypes
import Language.K3.Utils.Pretty

-- | Expression tags. Every expression can be qualified with a mutability annotation.
data Expression
    = EConstant   Constant
    | EVariable   Identifier
    | ESome
    | EIndirect
    | ETuple
    | ERecord     [Identifier]
    | ELambda     Identifier
    | EOperate    Operator
    | EProject    Identifier
    | ELetIn      Identifier
    | EAssign     Identifier
    | ECaseOf     Identifier
    | EBindAs     Binder
    | EIfThenElse
    | EAddress
    | ESelf
    | EImperative ImperativeExpression
  deriving (Eq, Read, Show)

data ImperativeExpression
    = EWhile
  deriving (Eq, Read, Show)

-- | Constant expression values.
data Constant
    = CBool    Bool
    | CInt     Int
    | CByte    Word8
    | CReal    Double
    | CString  String
    | CNone    NoneMutability
    | CEmpty   (K3 Type)
  deriving (Eq, Read, Show)

-- | Operators (unary and binary).
data Operator
    = OAdd
    | OSub
    | OMul
    | ODiv
    | OMod
    | ONeg
    | OEqu
    | ONeq
    | OLth
    | OLeq
    | OGth
    | OGeq
    | OAnd
    | OOr
    | ONot
    | OConcat
    | OSeq
    | OApp
    | OSnd
  deriving (Eq, Read, Show)

-- | Binding Forms.
data Binder
    = BIndirection Identifier
    | BTuple       [Identifier]
    | BRecord      [(Identifier, Identifier)]
  deriving (Eq, Read, Show)

-- | Annotations on expressions.
data instance Annotation Expression
    = ESpan Span
    | EUID UID
    | EMutable
    | EImmutable
    | EAnnotation Identifier
    | EProperty   Identifier (Maybe (K3 Literal))
    | ESyntax     SyntaxAnnotation
    | EAnalysis   AnalysisAnnotation

    -- TODO: the remainder of these should be pushed into
    -- an annotation category (e.g., EType, EAnalysis, etc)
    | EType   (K3 Type)
    | EQType  (K3 QType)
    | ETypeLB (K3 Type)
    | ETypeUB (K3 Type)
    | EImplementationType Identifier
    | EEmbedding EmbeddingAnnotation
    | ERead Identifier UID
    | EWrite Identifier UID
    | EConflict Conflict
  deriving (Eq, Read, Show)

instance HasUID (Annotation Expression) where
  getUID (EUID u) = Just u
  getUID _        = Nothing

instance HasSpan (Annotation Expression) where
  getSpan (ESpan s) = Just s
  getSpan _         = Nothing

-- | Data Conflicts
--   TODO: move to Language.K3.Core.Annotation.Analysis
data Conflict
    = RW [(Annotation Expression)] (Annotation Expression)
    | WR (Annotation Expression) [(Annotation Expression)]
    | WW (Annotation Expression) (Annotation Expression)
  deriving (Eq, Read, Show)

instance Pretty (K3 Expression) where
    prettyLines (Node (ETuple :@: as) []) =
      let (annStr, tAnnStrs) = drawExprAnnotations as
      in ["EUnit" ++ annStr] ++ (shift "`- " "   " tAnnStrs)

    prettyLines (Node (EConstant (CEmpty t) :@: as) []) =
      let (annStr, tAnnStrs) = drawExprAnnotations as
      in ["EConstant CEmpty" ++ annStr] ++ (shift "+- " "|  " tAnnStrs) ++ ["|"] ++ prettyLines t

    prettyLines (Node (t :@: as) es) =
      let (annStr, tAnnStrs) = drawExprAnnotations as
          shiftedTAnns       = if null es then (shift "`- " "   " tAnnStrs)
                                          else (shift "+- " "|  " tAnnStrs)
      in
      [show t ++ annStr] ++ shiftedTAnns ++ drawSubTrees es

drawExprAnnotations :: [Annotation Expression] -> (String, [String])
drawExprAnnotations as =
  let (typeAnns, anns) = partition (\a -> isETypeOrBound a || isEQType a) as
      prettyTypeAnns   = case typeAnns of
                          []         -> []
                          [EType t]  -> drawETypeAnnotation $ EType t
                          [EQType t] -> drawETypeAnnotation $ EQType t
                          [t, l, u]  -> drawETypeAnnotation t
                                         %+ indent 2 (drawETypeAnnotation l
                                         %+ indent 2 (drawETypeAnnotation u))
                          _     -> error "Invalid type bound annotations"
  in (drawAnnotations anns, prettyTypeAnns)

  where drawETypeAnnotation (ETypeLB t) = ["ETypeLB "] %+ prettyLines t
        drawETypeAnnotation (ETypeUB t) = ["ETypeUB "] %+ prettyLines t
        drawETypeAnnotation (EType   t) = ["EType   "] %+ prettyLines t
        drawETypeAnnotation (EQType  t) = ["EQType  "] %+ prettyLines t
        drawETypeAnnotation _ = error "Invalid argument to drawETypeAnnotation"


{- Expression annotation predicates -}

isESpan :: Annotation Expression -> Bool
isESpan (ESpan _) = True
isESpan _         = False

isEQualified :: Annotation Expression -> Bool
isEQualified EImmutable = True
isEQualified EMutable   = True
isEQualified _          = False

isEUID :: Annotation Expression -> Bool
isEUID (EUID _) = True
isEUID _        = False

isEAnnotation :: Annotation Expression -> Bool
isEAnnotation (EAnnotation _) = True
isEAnnotation _               = False

isEProperty :: Annotation Expression -> Bool
isEProperty (EProperty _ _) = True
isEProperty _               = False

isEType :: Annotation Expression -> Bool
isEType (EType   _) = True
isEType _           = False

isETypeOrBound :: Annotation Expression -> Bool
isETypeOrBound (EType   _) = True
isETypeOrBound (ETypeLB _) = True
isETypeOrBound (ETypeUB _) = True
isETypeOrBound _           = False

isEQType :: Annotation Expression -> Bool
isEQType (EQType _) = True
isEQType _          = False

namedEAnnotations :: [Annotation Expression] -> [Identifier]
namedEAnnotations anns = map extractId $ filter isEAnnotation anns
  where extractId (EAnnotation n) = n
        extractId _ = error "Invalid named annotation"


{- Expression utilities -}

-- | Retrieves all free variables in an expression.
freeVariables :: K3 Expression -> [Identifier]
freeVariables expr = either (const []) id $ foldMapTree extractVariable [] expr
  where
    extractVariable chAcc (tag -> EVariable n) = return $ concat chAcc ++ [n]
    extractVariable chAcc (tag -> ELambda n)   = return $ filter (/= n) $ concat chAcc
    extractVariable chAcc (tag -> EBindAs b)   = return $ filter (`notElem` bindingVariables b) $ concat chAcc
    extractVariable chAcc (tag -> ELetIn i)    = return $ filter (/= i) $ concat chAcc
    extractVariable chAcc (tag -> ECaseOf i)   = return $ let [e, s, n] = chAcc in e ++ filter (/= i) s ++ n
    extractVariable chAcc _                    = return $ concat chAcc

-- | Retrieves all variables introduced by a binder
bindingVariables :: Binder -> [Identifier]
bindingVariables (BIndirection i) = [i]
bindingVariables (BTuple is)      = is
bindingVariables (BRecord ivs)    = snd (unzip ivs)

-- | Strips all annotations from an expression.
stripAnnotations :: K3 Expression -> K3 Expression
stripAnnotations = runIdentity . mapTree strip
  where strip ch n = return $ Node (tag n :@: []) ch

-- | Compares two expressions for identical AST structures while ignoring annotations
--   (such as UIDs, spans, etc.)
compareWithoutAnnotations :: K3 Expression -> K3 Expression -> Bool
compareWithoutAnnotations e1 e2 = stripAnnotations e1 == stripAnnotations e2
