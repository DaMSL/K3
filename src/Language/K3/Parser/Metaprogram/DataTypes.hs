{-# LANGUAGE TypeFamilies #-}

module Language.K3.Parser.Metaprogram.DataTypes where

import Data.Map ( Map )
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

{-| Metaprograms, as a parsing-time AST.
    Metaprograms enable splicing of label, type, expression, and declaration
    bindings but have a restricted set of binding constructors, specifically
    data and control annotations.
    For now, metaprogramming expressions and types are fully embedded
    into standard expression and type ASTs through identifiers.
-}
data MPDeclaration = Staged   MetaDeclaration
                   | Unstaged Declaration
                   deriving (Eq, Show, Read)

data MetaDeclaration = MDataAnnotation Identifier [TypedSpliceVar] [TypeVarDecl] [AnnMemDecl]
                     | MCtrlAnnotation Identifier [PatternRewriteRule] [K3 Declaration]
                     deriving (Eq, Show, Read)

data instance Annotation MPDeclaration
  = MPSpan Span
  deriving (Eq, Read, Show)


type SpliceEnv     = Map Identifier SpliceReprEnv
type SpliceReprEnv = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

data SpliceValue = SLabel Identifier
                 | SType (K3 Type)
                 | SExpr (K3 Expression)
                 | SDecl (K3 Declaration)
                 deriving (Eq, Read, Show)

data SpliceResult m = SRType (m (K3 Type))
                    | SRExpr (m (K3 Expression))
                    | SRDecl (m (K3 Declaration))

mpDataAnnotation :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> K3 MPDeclaration
mpDataAnnotation n sVars tVars mems = Node ((Staged $ MDataAnnotation n sVars tVars mems) :@: []) []

mpCtrlAnnotation :: Identifier -> [PatternRewriteRule] -> [K3 Declaration] -> K3 MPDeclaration
mpCtrlAnnotation n rules extensions = Node ((Staged $ MCtrlAnnotation n rules extensions) :@: []) []
