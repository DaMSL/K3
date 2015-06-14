{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

-- | Expressions in K3.
module Language.K3.Core.Expression (
  Expression(..),
  ImperativeExpression(..),
  Constant(..),
  Operator(..),
  Binder(..),
  Annotation(..),
  PropertyE

  , onEProperty
  , ePropertyName
  , ePropertyValue
  , ePropertyV

  , isEQualified
  , isESpan
  , isEUID
  , isEUIDSpan
  , isEAnnotation
  , isEProperty
  , isEInferredProperty
  , isEUserProperty
  , isESyntax
  , isEApplyGen
  , isEType
  , isETypeOrBound
  , isEQType
  , isEPType
  , isEAnyType
  , isEProvenance
  , isESEffect
  , isEFStructure
  , isEMaterialization
  , isAnyETypeAnn
  , isAnyEEffectAnn
  , isAnyETypeOrEffectAnn
  , namedEAnnotations
) where

import Control.DeepSeq

import Data.Binary
import Data.Serialize

import Data.List
import Data.Tree
import Data.Typeable
import Data.Word (Word8)

import qualified Data.Map as M

import GHC.Generics (Generic)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Analysis
import Language.K3.Core.Annotation.Codegen
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.Core.Literal

import Language.K3.Analysis.HMTypes.DataTypes
import Language.K3.Analysis.Provenance.Core
import qualified Language.K3.Analysis.SEffects.Core as S

import Language.K3.Transform.Hints
import qualified Language.K3.Codegen.CPP.Materialization.Hints as Z

import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT


-- | Cycle-breaking import for metaprogramming
import {-# SOURCE #-} Language.K3.Core.Metaprogram ( SpliceEnv )

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
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

data ImperativeExpression
    = EWhile
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Constant expression values.
data Constant
    = CBool    Bool
    | CByte    Word8
    | CInt     Int
    | CReal    Double
    | CString  String
    | CNone    NoneMutability
    | CEmpty   (K3 Type)
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

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
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Binding Forms.
data Binder
    = BIndirection Identifier
    | BTuple       [Identifier]
    | BRecord      [(Identifier, Identifier)]
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Annotations on expressions.
data instance Annotation Expression
    = ESpan Span
    | EUID UID
    | EMutable
    | EImmutable

    | EAnnotation Identifier
    | EProperty   PropertyE
        -- ^ Properties are either types, with a left variant indicating a
        -- user-defined property, while a right variant represents an inferred property.

    | EApplyGen   Bool Identifier SpliceEnv
        -- ^ Apply a K3 generator, with a bool indicating a control annotation generator (vs a data annotation),
        --   a generator name, and a splice environment.

    | ESyntax     SyntaxAnnotation
    | EAnalysis   AnalysisAnnotation

    -- TODO: the remainder of these should be pushed into
    -- an annotation category (e.g., EType, EAnalysis, etc)
    | EProvenance (K3 Provenance)
    | ESEffect    (K3 S.Effect)
    | EFStructure (K3 S.Effect)
    | EOpt        OptHint
    | EMaterialization (M.Map Identifier Z.Decision)
    | EType       (K3 Type)
    | EQType      (K3 QType)
    | ETypeLB     (K3 Type)
    | ETypeUB     (K3 Type)
    | EPType      (K3 Type)  -- Annotation embedding for pattern types
    | EEmbedding EmbeddingAnnotation
  deriving (Eq, Ord, Read, Show, Generic)

-- | Data Conflicts
--   TODO: move to Language.K3.Core.Annotation.Analysis
data Conflict
    = RW [(Annotation Expression)] (Annotation Expression)
    | WR (Annotation Expression) [(Annotation Expression)]
    | WW (Annotation Expression) (Annotation Expression)
  deriving (Eq, Ord, Read, Show, Generic)

{- NFData instances for expressions. -}
instance NFData Expression
instance NFData ImperativeExpression
instance NFData Constant
instance NFData Operator
instance NFData Binder
instance NFData (Annotation Expression)
instance NFData Conflict

instance Binary Expression
instance Binary ImperativeExpression
instance Binary Constant
instance Binary Operator
instance Binary Binder
instance Binary (Annotation Expression)
instance Binary Conflict

instance Serialize Expression
instance Serialize ImperativeExpression
instance Serialize Constant
instance Serialize Operator
instance Serialize Binder
instance Serialize (Annotation Expression)
instance Serialize Conflict

{- HasUID instances. -}
instance HasUID (Annotation Expression) where
  getUID (EUID u) = Just u
  getUID _        = Nothing

instance HasSpan (Annotation Expression) where
  getSpan (ESpan s) = Just s
  getSpan _         = Nothing

-- | Property helpers
type PropertyV = (Identifier, Maybe (K3 Literal))
type PropertyE = Either PropertyV PropertyV

onEProperty :: (PropertyV -> a) -> PropertyE -> a
onEProperty f (Left  (n, lopt)) = f (n, lopt)
onEProperty f (Right (n, lopt)) = f (n, lopt)

ePropertyName :: PropertyE -> String
ePropertyName (Left  (n,_)) = n
ePropertyName (Right (n,_)) = n

ePropertyValue :: PropertyE -> Maybe (K3 Literal)
ePropertyValue (Left  (_,v)) = v
ePropertyValue (Right (_,v)) = v

ePropertyV :: PropertyE -> PropertyV
ePropertyV (Left  pv) = pv
ePropertyV (Right pv) = pv

{- Expression annotation predicates -}

isEQualified :: Annotation Expression -> Bool
isEQualified EImmutable = True
isEQualified EMutable   = True
isEQualified _          = False

isESpan :: Annotation Expression -> Bool
isESpan (ESpan _) = True
isESpan _         = False

isEUID :: Annotation Expression -> Bool
isEUID (EUID _) = True
isEUID _        = False

isEUIDSpan :: Annotation Expression -> Bool
isEUIDSpan a = isESpan a || isEUID a

isEAnnotation :: Annotation Expression -> Bool
isEAnnotation (EAnnotation _) = True
isEAnnotation _               = False

isEProperty :: Annotation Expression -> Bool
isEProperty (EProperty _) = True
isEProperty _             = False

isEInferredProperty :: Annotation Expression -> Bool
isEInferredProperty (EProperty (Right _)) = True
isEInferredProperty _                     = False

isEUserProperty :: Annotation Expression -> Bool
isEUserProperty (EProperty (Left _)) = True
isEUserProperty _                    = False

isESyntax :: Annotation Expression -> Bool
isESyntax (ESyntax _) = True
isESyntax _ = False

isEApplyGen :: Annotation Expression -> Bool
isEApplyGen (EApplyGen _ _ _) = True
isEApplyGen _ = False

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

isEPType :: Annotation Expression -> Bool
isEPType (EPType _) = True
isEPType _          = False

isEAnyType :: Annotation Expression -> Bool
isEAnyType (EType   _) = True
isEAnyType (ETypeLB _) = True
isEAnyType (ETypeUB _) = True
isEAnyType (EQType  _) = True
isEAnyType (EPType  _) = True
isEAnyType _           = False

isEProvenance :: Annotation Expression -> Bool
isEProvenance (EProvenance _) = True
isEProvenance _               = False

isESEffect :: Annotation Expression -> Bool
isESEffect (ESEffect _) = True
isESEffect _            = False

isEFStructure :: Annotation Expression -> Bool
isEFStructure (EFStructure _) = True
isEFStructure _               = False

isAnyETypeAnn :: Annotation Expression -> Bool
isAnyETypeAnn a = isETypeOrBound a || isEQType a

isAnyEEffectAnn :: Annotation Expression -> Bool
isAnyEEffectAnn a = isEProvenance a || isESEffect a || isEFStructure a

isAnyETypeOrEffectAnn :: Annotation Expression -> Bool
isAnyETypeOrEffectAnn a = isAnyETypeAnn a || isAnyEEffectAnn a

namedEAnnotations :: [Annotation Expression] -> [Identifier]
namedEAnnotations anns = map extractId $ filter isEAnnotation anns
  where extractId (EAnnotation n) = n
        extractId _ = error "Invalid named annotation"

isEMaterialization :: Annotation Expression -> Bool
isEMaterialization (EMaterialization _) = True
isEMaterialization _ = False

{- Pretty instances -}

instance Pretty (K3 Expression) where
    prettyLines (Node (ETuple :@: as) []) =
      let (annStr, pAnnStrs) = drawExprAnnotations as
      in ["EUnit" ++ annStr] ++ (shift "`- " "   " pAnnStrs)

    prettyLines (Node (EConstant (CEmpty t) :@: as) []) =
      let (annStr, pAnnStrs) = drawExprAnnotations as
      in ["EConstant CEmpty" ++ annStr] ++ (shift "+- " "|  " pAnnStrs) ++ ["|"] ++ terminalShift t

    prettyLines (Node (t :@: as) es) =
      let (annStr, pAnnStrs) = drawExprAnnotations as
          shiftedTAnns       = if null es then (shift "`- " "   " pAnnStrs)
                                          else (shift "+- " "|  " pAnnStrs)
      in
      [show t ++ annStr] ++ shiftedTAnns ++ drawSubTrees es

drawExprAnnotations :: [Annotation Expression] -> (String, [String])
drawExprAnnotations as =
  let (typeAnns, anns)    = partition isTypeAnn as
      (effectAnns, anns') = partition isAnyEEffectAnn anns
      (restPAnns, anns'') = partition isEApplyGen anns'
      prettyTypeAnns = case typeAnns of
                         []         -> []
                         [EType t]  -> drawETypeAnnotation $ EType t
                         [EQType t] -> drawETypeAnnotation $ EQType t
                         [EPType t] -> drawETypeAnnotation $ EPType t
                         [t, l, u]  -> drawETypeAnnotation t
                                        %+ indent 2 (drawETypeAnnotation l
                                        %+ indent 2 (drawETypeAnnotation u))
                         _     -> error "Invalid type bound annotations"

      prettyAnns = drawGroup $ [prettyTypeAnns] ++ map drawEEffectAnnotations effectAnns
                                                ++ map drawPrettyAnns restPAnns

  in (drawAnnotations anns'', prettyAnns)

  where drawETypeAnnotation (ETypeLB t) = ["ETypeLB "] %+ prettyLines t
        drawETypeAnnotation (ETypeUB t) = ["ETypeUB "] %+ prettyLines t
        drawETypeAnnotation (EType   t) = ["EType   "] %+ prettyLines t
        drawETypeAnnotation (EQType  t) = ["EQType  "] %+ prettyLines t
        drawETypeAnnotation (EPType  t) = ["EPType  "] %+ prettyLines t
        drawETypeAnnotation _ = error "Invalid argument to drawETypeAnnotation"

        drawEEffectAnnotations (EProvenance p) = ["EProvenance "] %+ prettyLines p
        drawEEffectAnnotations (ESEffect e)    = ["ESEffect "]    %+ prettyLines e
        drawEEffectAnnotations (EFStructure e) = ["EFStructure "] %+ prettyLines e
        drawEEffectAnnotations _ = error "Invalid effect annotation"

        drawPrettyAnns (EApplyGen _ genId sEnv) = ["EApplyGen " ++ genId ++ " "] %+ prettyLines sEnv
        drawPrettyAnns _ = []

        isTypeAnn a = isETypeOrBound a || isEQType a || isEPType a

{- PrettyText instance -}
tPipe :: Text
tPipe = T.pack "|"

aPipe :: [Text] -> [Text]
aPipe t = t ++ [tPipe]

ntShift :: [Text] -> [Text]
ntShift = PT.shift (T.pack "+- ") (T.pack "|  ")

tShift :: [Text] -> [Text]
tShift = PT.shift (T.pack "`- ") (T.pack "   ")

tTA :: Bool -> String -> [Annotation Expression] -> [Text]
tTA asTerm s as =
  let (annTxt, pAnnTxt) = drawExprAnnotationsT as in
  let suffix = if null pAnnTxt then id else aPipe in
  (suffix [T.append (T.pack s) annTxt])
  ++ (if null pAnnTxt then []
      else if asTerm then tShift pAnnTxt else (aPipe $ tShift pAnnTxt))

instance PT.Pretty (K3 Expression) where
    prettyLines (Node (ETuple :@: as) []) = tTA True "EUnit" as

    prettyLines (Node (EConstant (CEmpty t) :@: as) []) =
      tTA False ("EConstant CEmpty") as ++ PT.terminalShift t

    prettyLines (Node (t :@: as) es) =
      let (annTxt, pAnnTxt) = drawExprAnnotationsT as
          shiftedTAnns      = if null es then tShift pAnnTxt else ntShift pAnnTxt
      in
      [T.append (T.pack $ show t) annTxt] ++ shiftedTAnns ++ PT.drawSubTrees es

drawExprAnnotationsT :: [Annotation Expression] -> (Text, [Text])
drawExprAnnotationsT as =
  let (typeAnns, anns)    = partition (\a -> isETypeOrBound a || isEQType a || isEPType a) as
      (effectAnns, anns') = partition isAnyEEffectAnn anns
      prettyTypeAnns = case typeAnns of
                         []         -> []
                         [EType t]  -> drawETypeAnnotationT $ EType t
                         [EQType t] -> drawETypeAnnotationT $ EQType t
                         [EPType t] -> drawETypeAnnotationT $ EPType t
                         [t, l, u]  -> drawETypeAnnotationT t
                                        PT.%+ PT.indent 2 (drawETypeAnnotationT l
                                        PT.%+ PT.indent 2 (drawETypeAnnotationT u))
                         _     -> error "Invalid type bound annotations"

      prettyAnns = PT.drawGroup $ [prettyTypeAnns] ++ map drawEEffectAnnotationsT effectAnns

  in (PT.drawAnnotations anns', prettyAnns)

  -- TODO: PT.Pretty instances for K3 Type, K3 Effect, K3 Symbol
  where drawETypeAnnotationT (ETypeLB t) = [T.pack "ETypeLB "] PT.%+ PT.prettyLines t
        drawETypeAnnotationT (ETypeUB t) = [T.pack "ETypeUB "] PT.%+ PT.prettyLines t
        drawETypeAnnotationT (EType   t) = [T.pack "EType   "] PT.%+ PT.prettyLines t
        drawETypeAnnotationT (EQType  t) = [T.pack "EQType  "] PT.%+ (map T.pack $ prettyLines t)
        drawETypeAnnotationT (EPType  t) = [T.pack "EPType  "] PT.%+ PT.prettyLines t
        drawETypeAnnotationT _ = error "Invalid argument to drawETypeAnnotation"

        drawEEffectAnnotationsT (EProvenance p) = [T.pack "EProvenance "] PT.%+ PT.prettyLines p
        drawEEffectAnnotationsT (ESEffect e)    = [T.pack "ESEffect "]    PT.%+ PT.prettyLines e
        drawEEffectAnnotationsT (EFStructure e) = [T.pack "EFStructure "] PT.%+ PT.prettyLines e
        drawEEffectAnnotationsT _ = error "Invalid effect annotation"
