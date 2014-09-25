{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

-- | Top-Level Declarations in K3.
module Language.K3.Core.Declaration (
    Declaration(..),
    Annotation(..),

    -- * User defined Annotations
    Polarity(..),
    AnnMemDecl(..),
    PatternRewriteRule,
    SpliceType(..),
    TypedSpliceVar,
    UnorderedConflict(..),

    getTriggerIds,

    isDSpan,
    isDUID,
    isDSyntax
) where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Effects.Core

import Language.K3.Utils.Pretty

-- | Top-Level Declarations
data Declaration
    = DGlobal         Identifier (K3 Type) (Maybe (K3 Expression))

    | DTrigger        Identifier (K3 Type) (K3 Expression)
        -- ^ Trigger declaration.  Type is argument type of trigger.  Expression
        --   must be a function taking that argument type and returning unit.

    | DDataAnnotation Identifier [TypeVarDecl] [AnnMemDecl]
        -- ^ Name, annotation type parameters, and members

    | DCtrlAnnotation Identifier [TypedSpliceVar] [PatternRewriteRule] [K3 Declaration]
        -- ^ Name, splice parameters, a list of pattern-based rewrite rules, and a set of common
        --   declarations for any match.
        --   Control annotations are currently only kept in the AST for lineage,
        --   and should be fully synthesized at all use cases by metaprogram evaluation.

    | DRole           Identifier
        -- ^ Roles, as lightweight modules. These are deprecated.

    | DTypeDef        Identifier (K3 Type)
        -- ^ Type synonym declaration.

  deriving (Eq, Read, Show)

-- | Annotation declaration members
data AnnMemDecl
    = Lifted      Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | Attribute   Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | MAnnotation Polarity Identifier [Annotation Declaration]
  deriving (Eq, Read, Show)

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Read, Show)

-- | Metaprogramming parameters
data SpliceType = STLabel | STType | STExpr | STDecl | STLabelType deriving (Eq, Read, Show)
type TypedSpliceVar = (SpliceType, Identifier)

-- | A pattern-based rewrite rule, as used in control annotations.
--   This includes a pattern matching expression, a rewritten expression,
--   and any declarations used in the rewrite.
type PatternRewriteRule = (K3 Expression, K3 Expression, [K3 Declaration])


-- | Annotations on Declarations.
data instance Annotation Declaration
    = DSpan     Span
    | DUID      UID
    | DProperty Identifier (Maybe (K3 Literal))
    | DSyntax   SyntaxAnnotation
    | DConflict UnorderedConflict  -- TODO: organize into categories.
    | DEffect   (K3 Effect, Maybe (K3 Symbol))
  deriving (Eq, Read, Show)

-- | Unordered Data Conflicts (between triggers)
data UnorderedConflict
    = URW [(Annotation Expression)] (Annotation Expression)
    | UWW (Annotation Expression) (Annotation Expression)
  deriving (Eq, Read, Show)

instance HasUID (Annotation Declaration) where
  getUID (DUID u) = Just u
  getUID _        = Nothing

instance HasSpan (Annotation Declaration) where
  getSpan (DSpan s) = Just s
  getSpan _         = Nothing

instance Pretty (K3 Declaration) where
  prettyLines (Node (DGlobal i t me :@: as) ds) =
    ["DGlobal " ++ i ++ drawAnnotations as, "|"]
    ++ case (me, ds) of
        (Nothing, []) -> terminalShift t
        (Just e, [])  -> nonTerminalShift t ++ ["|"] ++ terminalShift e
        (Nothing, _)  -> nonTerminalShift t ++ ["|"] ++ drawSubTrees ds
        (Just e, _)   -> nonTerminalShift t ++ ["|"] ++ nonTerminalShift e ++ ["|"] ++ drawSubTrees ds

  prettyLines (Node (DTrigger i t e :@: as) ds) =
    ["DTrigger " ++ i ++ drawAnnotations as, "|"]
    ++ nonTerminalShift t ++ ["|"]
    ++ case ds of
        [] -> terminalShift e
        _  -> nonTerminalShift e ++ ["|"] ++ drawSubTrees ds

  prettyLines (Node (DRole i :@: as) ds) =
    ["DRole " ++ i ++ " :@: " ++ show as, "|"] ++ drawSubTrees ds

  prettyLines (Node (DDataAnnotation i tvars members :@: as) ds) =
      ["DDataAnnotation " ++ i
          ++ if null tvars
              then ""
              else ("[" ++ (removeTrailingWhitespace . boxToString $
                      foldl1 (\a b -> a %+ [", "] %+ b) $ map prettyLines tvars)
                  ++ "]"
              ) ++ drawAnnotations as, "|"]
      ++ drawAnnotationMembers members
      ++ drawSubTrees ds
    where
      drawAnnotationMembers []  = []
      drawAnnotationMembers [x] = terminalShift x
      drawAnnotationMembers x   = concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
                                    ++ terminalShift (last x)

  prettyLines (Node (DCtrlAnnotation i svars rewriteRules commonDecls :@: as) ds) =
      ["DCtrlAnnotation " ++ i
        ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
        ++ drawAnnotations as, "|"]
      ++ concatMap drawRule rewriteRules
      ++ drawDecls "common" commonDecls
      ++ drawSubTrees ds
    where
      drawRule (p,r,decls) =
        nonTerminalShift p ++ ["|"] ++
          (if null decls
             then (shift "=> " "   " $ prettyLines r)
             else (shift "=> " "|  " $ prettyLines r) ++ drawDecls "" decls)

      drawDecls tagStr decls =
        if null decls then []
        else ["| " ++ tagStr ++ " +>"] ++ concatMap (\d -> nonTerminalShift d ++ ["|"]) (init decls)
                     ++ terminalShift (last decls)

  prettyLines (Node (DTypeDef i t :@: _) _) = ["DTypeDef " ++ i ++ " "] `hconcatTop` prettyLines t

instance Pretty AnnMemDecl where
  prettyLines (Lifted      pol n t eOpt anns) =
    ["Lifted " ++ unwords [show pol, n, show anns], "|"]
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e

  prettyLines (Attribute   pol n t eOpt anns) =
    ["Attribute " ++ unwords [show pol, n, show anns], "|"]
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e

  prettyLines (MAnnotation pol n anns) =
    ["MAnnotation " ++ unwords [show pol, n, show anns]]


{- Declaration annotation predicates -}

isDSpan :: Annotation Declaration -> Bool
isDSpan (DSpan _) = True
isDSpan _         = False

isDUID :: Annotation Declaration -> Bool
isDUID (DUID _) = True
isDUID _        = False

isDSyntax :: Annotation Declaration -> Bool
isDSyntax (DSyntax _) = True
isDSyntax _           = False

isDEffect :: Annotation Declaration -> Bool
isDEffect (DEffect _) = True
isDEffect _           = False

{- Utils -}
-- Given top level role declaration, return list of all trigger ids in the AST
getTriggerIds :: K3 Declaration -> [Identifier]
getTriggerIds (Node (DRole _ :@: _) cs) = map getTriggerName $ filter isTrigger cs
getTriggerIds _ = error "getTriggerIds expects role declaration"

isTrigger :: K3 Declaration -> Bool
isTrigger (Node (DTrigger _ _ _:@: _) _) = True
isTrigger _ = False

getTriggerName :: K3 Declaration -> Identifier
getTriggerName (Node (DTrigger n _ _ :@: _) _) = n
getTriggerName _ = error "getTriggerName expects trigger declaration"
