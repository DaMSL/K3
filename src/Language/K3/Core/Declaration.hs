{-# LANGUAGE DeriveDataTypeable #-}
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
    UnorderedConflict(..),

    getTriggerIds,

    isDUID,
    isDSpan,
    isDProperty,
    isDSymbol,
    isDSyntax
) where

import Data.List
import Data.Tree
import Data.Typeable

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type
import Language.K3.Analysis.Effects.Core

import Language.K3.Utils.Pretty

import Data.Text ( Text )
import qualified Data.Text as T
import qualified Language.K3.Utils.PrettyText as PT

-- | Cycle-breaking import for metaprogramming
import {-# SOURCE #-} Language.K3.Core.Metaprogram

-- | Top-Level Declarations
data Declaration
    = DGlobal         Identifier (K3 Type) (Maybe (K3 Expression))

    | DTrigger        Identifier (K3 Type) (K3 Expression)
        -- ^ Trigger declaration.  Type is argument type of trigger.  Expression
        --   must be a function taking that argument type and returning unit.

    | DDataAnnotation Identifier [TypeVarDecl] [AnnMemDecl]
        -- ^ Name, annotation type parameters, and members

    | DGenerator      MPDeclaration
        -- ^ Metaprogramming declarations, maintained in the tree for lineage.

    | DRole           Identifier
        -- ^ Roles, as lightweight modules. These are deprecated.

    | DTypeDef        Identifier (K3 Type)
        -- ^ Type synonym declaration.

  deriving (Eq, Ord, Read, Show, Typeable)

-- | Annotation declaration members
data AnnMemDecl
    = Lifted      Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | Attribute   Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | MAnnotation Polarity Identifier [Annotation Declaration]
  deriving (Eq, Ord, Read, Show, Typeable)

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Ord, Read, Show, Typeable)

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
    | DSymbol   (K3 Symbol)
  deriving (Eq, Ord, Read, Show)

-- | Unordered Data Conflicts (between triggers)
data UnorderedConflict
    = URW [(Annotation Expression)] (Annotation Expression)
    | UWW (Annotation Expression) (Annotation Expression)
  deriving (Eq, Ord, Read, Show)


{- Declaration annotation predicates -}

isDSpan :: Annotation Declaration -> Bool
isDSpan (DSpan _) = True
isDSpan _         = False

isDUID :: Annotation Declaration -> Bool
isDUID (DUID _) = True
isDUID _        = False

isDProperty :: Annotation Declaration -> Bool
isDProperty (DProperty _ _) = True
isDProperty _               = False

isDSyntax :: Annotation Declaration -> Bool
isDSyntax (DSyntax _) = True
isDSyntax _           = False

isDSymbol :: Annotation Declaration -> Bool
isDSymbol (DSymbol _) = True
isDSymbol _           = False

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


{- Declaration instances -}
instance HasUID (Annotation Declaration) where
  getUID (DUID u) = Just u
  getUID _        = Nothing

instance HasSpan (Annotation Declaration) where
  getSpan (DSpan s) = Just s
  getSpan _         = Nothing

instance Pretty (K3 Declaration) where
  prettyLines (Node (DGlobal i t me :@: as) ds) =
    let (annStr, pAnnStrs) = drawDeclAnnotations as in
    ["DGlobal " ++ i ++ annStr] ++ ["|"]
    ++ (if null pAnnStrs then [] else (shift "+- " "|  " pAnnStrs) ++ ["|"])
    ++ case (me, ds) of
        (Nothing, []) -> terminalShift t
        (Just e, [])  -> nonTerminalShift t ++ ["|"] ++ terminalShift e
        (Nothing, _)  -> nonTerminalShift t ++ ["|"] ++ drawSubTrees ds
        (Just e, _)   -> nonTerminalShift t ++ ["|"] ++ nonTerminalShift e ++ ["|"] ++ drawSubTrees ds

  prettyLines (Node (DTrigger i t e :@: as) ds) =
    let (annStr, pAnnStrs) = drawDeclAnnotations as in
    ["DTrigger " ++ i ++ annStr] ++ ["|"]
    ++ (if null pAnnStrs then [] else (shift "+- " "|  " pAnnStrs) ++ ["|"])
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

  prettyLines (Node (DGenerator mp :@: as) ds) =
      ["DGenerator" ++ drawAnnotations as, "|"] ++ terminalShift mp ++ drawSubTrees ds

  prettyLines (Node (DTypeDef i t :@: _) _) = ["DTypeDef " ++ i ++ " "] `hconcatTop` prettyLines t

instance Pretty AnnMemDecl where
  prettyLines (Lifted pol n t eOpt anns) =
    let (annStr, pAnnStrs) = drawDeclAnnotations anns in
    ["Lifted " ++ unwords [show pol, n, annStr]] ++ ["|"]
    ++ (if null pAnnStrs then [] else (shift "+- " "|  " pAnnStrs) ++ ["|"])
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e

  prettyLines (Attribute pol n t eOpt anns) =
    let (annStr, pAnnStrs) = drawDeclAnnotations anns in
    ["Attribute " ++ unwords [show pol, n, annStr]] ++ ["|"]
    ++ (if null pAnnStrs then [] else (shift "+- " "|  " pAnnStrs) ++ ["|"])
    ++ case eOpt of
        Nothing -> terminalShift t
        Just e  -> nonTerminalShift t ++ ["|"] ++ terminalShift e

  prettyLines (MAnnotation pol n anns) =
    ["MAnnotation " ++ unwords [show pol, n, show anns]]


drawDeclAnnotations :: [Annotation Declaration] -> (String, [String])
drawDeclAnnotations as =
  let (symAnns, anns) = partition isDSymbol as
      prettyDeclAnns  = concatMap drawDSymbolAnnotation symAnns
  in (drawAnnotations anns, prettyDeclAnns)

  where drawDSymbolAnnotation (DSymbol s) = ["DSymbol "] %+ prettyLines s
        drawDSymbolAnnotation _ = error "Invalid symbol annotation"

{- PrettyText instance -}
tPipe :: Text
tPipe = T.pack "|"

aPipe :: [Text] -> [Text]
aPipe t = t ++ [tPipe]

ntShift :: [Text] -> [Text]
ntShift = PT.shift (T.pack "+- ") (T.pack "|  ")

tTA :: String -> [Annotation Declaration] -> [Text]
tTA s as =
  let (annTxt, pAnnTxt) = drawDeclAnnotationsT as in
  aPipe [T.append (T.pack s) annTxt]
  ++ (if null pAnnTxt then [] else aPipe $ ntShift pAnnTxt)

tNullTerm :: (PT.Pretty a, PT.Pretty b) => a -> [b] -> [Text]
tNullTerm a bl = 
  if null bl then PT.terminalShift a
  else ((aPipe (PT.nonTerminalShift a) ++) . PT.drawSubTrees) bl

tMaybeTerm :: (PT.Pretty a, PT.Pretty b) => a -> Maybe b -> [Text]
tMaybeTerm a bOpt = maybe (PT.terminalShift a) ((aPipe (PT.nonTerminalShift a) ++) . PT.terminalShift) bOpt

instance PT.Pretty (K3 Declaration) where
  prettyLines (Node (DGlobal i t me :@: as) ds) =
    tTA ("DGlobal " ++ i) as
    ++ case (me, ds) of
        (Nothing, []) -> PT.terminalShift t
        (Just e, [])  -> aPipe (PT.nonTerminalShift t) ++ PT.terminalShift e
        (Nothing, _)  -> aPipe (PT.nonTerminalShift t) ++ PT.drawSubTrees ds
        (Just e, _)   -> aPipe (PT.nonTerminalShift t) ++ aPipe (PT.nonTerminalShift e)
                                                       ++ PT.drawSubTrees ds

  prettyLines (Node (DTrigger i t e :@: as) ds) =
    tTA ("DTrigger " ++ i) as ++ aPipe (PT.nonTerminalShift t) ++ tNullTerm e ds

  prettyLines (Node (DRole i :@: as) ds) =
    tTA ("DRole " ++ i) as ++ PT.drawSubTrees ds

  prettyLines (Node (DDataAnnotation i tvars members :@: as) ds) =
      [foldl1 T.append
        [T.pack $ "DDataAnnotation " ++ i
         , if null tvars then T.empty else (wrapT "[" (prettyTvars tvars) "]")
         , PT.drawAnnotations as, T.pack "|"]]
      ++ drawAnnotationMembersT members
      ++ PT.drawSubTrees ds
    where
      wrapT l body r = foldl1 T.append [T.pack l, body, T.pack r]
      prettyTvars tvars' = PT.removeTrailingWhitespace . PT.boxToString $
                             foldl1 (\a b -> a PT.%+ [T.pack ", "] PT.%+ b) $ map PT.prettyLines tvars'

      drawAnnotationMembersT []  = []
      drawAnnotationMembersT [x] = PT.terminalShift x
      drawAnnotationMembersT x   = concatMap (aPipe . PT.nonTerminalShift) (init x)
                                     ++ PT.terminalShift (last x)

  prettyLines (Node (DGenerator mp :@: as) ds) =
    tTA "DGenerator" as ++ map T.pack (terminalShift mp) ++ PT.drawSubTrees ds

  prettyLines (Node (DTypeDef i t :@: _) _) =
    [T.pack $ "DTypeDef " ++ i ++ " "] `PT.hconcatTop` PT.prettyLines t

instance PT.Pretty AnnMemDecl where
  prettyLines (Lifted pol n t eOpt anns) =
    tTA (unwords ["Lifted", show pol, n]) anns ++ tMaybeTerm t eOpt

  prettyLines (Attribute pol n t eOpt anns) =
    tTA (unwords ["Attribute", show pol, n]) anns ++ tMaybeTerm t eOpt

  prettyLines (MAnnotation pol n anns) =
    [T.append (T.pack "MAnnotation ") $ T.unwords $ map T.pack [show pol, n, show anns]]


drawDeclAnnotationsT :: [Annotation Declaration] -> (Text, [Text])
drawDeclAnnotationsT as =
  let (symAnns, anns) = partition isDSymbol as
      prettyDeclAnns  = concatMap drawDSymbolAnnotationT symAnns
  in (PT.drawAnnotations anns, prettyDeclAnns)

  where drawDSymbolAnnotationT (DSymbol s) = map T.pack $ ["DSymbol "] %+ prettyLines s
        drawDSymbolAnnotationT _ = error "Invalid symbol annotation"