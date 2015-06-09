{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
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
    PropertyD

    , declName
    , getTriggerIds

    , onDProperty
    , dPropertyName
    , dPropertyValue
    , dPropertyV

    , isDUID
    , isDSpan
    , isDUIDSpan
    , isDProperty
    , isDInferredProperty
    , isDUserProperty
    , isDSyntax
    , isDProvenance
    , isDEffect
    , isAnyDEffectAnn
    , isDInferredProvenance
    , isDInferredEffect
    , isAnyDInferredEffectAnn
) where

import Control.DeepSeq

import Data.Binary
import Data.List
import Data.Tree
import Data.Typeable

import GHC.Generics (Generic)

import Language.K3.Core.Annotation
import Language.K3.Core.Annotation.Syntax
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Analysis.Provenance.Core
import qualified Language.K3.Analysis.SEffects.Core as S

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

  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Annotation declaration members
data AnnMemDecl
    = Lifted      Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | Attribute   Polarity Identifier
                  (K3 Type) (Maybe (K3 Expression))
                  [Annotation Declaration]

    | MAnnotation Polarity Identifier [Annotation Declaration]
  deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | Annotation member polarities
data Polarity = Provides | Requires deriving (Eq, Ord, Read, Show, Typeable, Generic)

-- | A pattern-based rewrite rule, as used in control annotations.
--   This includes a pattern matching expression, a rewritten expression,
--   and any declarations used in the rewrite.
type PatternRewriteRule = (K3 Expression, K3 Expression, [K3 Declaration])


-- | Annotations on Declarations.
data instance Annotation Declaration
    = DSpan       Span
    | DUID        UID
    | DProperty   PropertyD
    | DSyntax     SyntaxAnnotation
    | DConflict   UnorderedConflict  -- TODO: organize into categories.

    -- Provenance and effects may be user-defined (lefts) or inferred (rights)
    | DProvenance (Either (K3 Provenance) (K3 Provenance))
    | DEffect     (Either (K3 S.Effect) (K3 S.Effect))
  deriving (Eq, Ord, Read, Show, Generic)

-- | Unordered Data Conflicts (between triggers)
data UnorderedConflict
    = URW [(Annotation Expression)] (Annotation Expression)
    | UWW (Annotation Expression) (Annotation Expression)
  deriving (Eq, Ord, Read, Show, Generic)

{- NFData instances for declarations -}
instance NFData Declaration
instance NFData AnnMemDecl
instance NFData Polarity
instance NFData (Annotation Declaration)
instance NFData UnorderedConflict

instance Binary Declaration
instance Binary AnnMemDecl
instance Binary Polarity
instance Binary (Annotation Declaration)
instance Binary UnorderedConflict

{- HasUID instances -}
instance HasUID (Annotation Declaration) where
  getUID (DUID u) = Just u
  getUID _        = Nothing

instance HasSpan (Annotation Declaration) where
  getSpan (DSpan s) = Just s
  getSpan _         = Nothing


declName :: K3 Declaration -> Maybe Identifier
declName d = case tag d of
               DGlobal i _ _ -> Just i
               DTrigger i _ _ -> Just i
               DDataAnnotation i _ _ -> Just i
               DRole i -> Just i
               DTypeDef i _ -> Just i
               DGenerator _ -> Nothing

-- | Property helpers
type PropertyV = (Identifier, Maybe (K3 Literal))
type PropertyD = Either PropertyV PropertyV

onDProperty :: (PropertyV -> a) -> PropertyD -> a
onDProperty f (Left  (n, lopt)) = f (n, lopt)
onDProperty f (Right (n, lopt)) = f (n, lopt)

dPropertyName :: PropertyD -> String
dPropertyName (Left  (n,_)) = n
dPropertyName (Right (n,_)) = n

dPropertyValue :: PropertyD -> Maybe (K3 Literal)
dPropertyValue (Left  (_,v)) = v
dPropertyValue (Right (_,v)) = v

dPropertyV :: PropertyD -> PropertyV
dPropertyV (Left  pv) = pv
dPropertyV (Right pv) = pv


{- Declaration annotation predicates -}

isDSpan :: Annotation Declaration -> Bool
isDSpan (DSpan _) = True
isDSpan _         = False

isDUID :: Annotation Declaration -> Bool
isDUID (DUID _) = True
isDUID _        = False

isDUIDSpan :: Annotation Declaration -> Bool
isDUIDSpan a = isDSpan a || isDUID a

isDProperty :: Annotation Declaration -> Bool
isDProperty (DProperty _) = True
isDProperty _             = False

isDInferredProperty :: Annotation Declaration -> Bool
isDInferredProperty (DProperty (Right _)) = True
isDInferredProperty _                     = False

isDUserProperty :: Annotation Declaration -> Bool
isDUserProperty (DProperty (Left _)) = True
isDUserProperty _                    = False

isDSyntax :: Annotation Declaration -> Bool
isDSyntax (DSyntax _) = True
isDSyntax _           = False

isDProvenance :: Annotation Declaration -> Bool
isDProvenance (DProvenance _) = True
isDProvenance _               = False

isDEffect :: Annotation Declaration -> Bool
isDEffect (DEffect _) = True
isDEffect _           = False

isAnyDEffectAnn :: Annotation Declaration -> Bool
isAnyDEffectAnn a = isDProvenance a || isDEffect a

isDInferredProvenance :: Annotation Declaration -> Bool
isDInferredProvenance (DProvenance (Right _)) = True
isDInferredProvenance _                       = False

isDInferredEffect :: Annotation Declaration -> Bool
isDInferredEffect (DEffect (Right _)) = True
isDInferredEffect _                   = False

isAnyDInferredEffectAnn :: Annotation Declaration -> Bool
isAnyDInferredEffectAnn a = isDInferredProvenance a || isDInferredEffect a

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
  let (prettyAnns, anns) = partition (\a -> isDProvenance a || isDEffect a) as
      prettyDeclAnns     = drawGroup $ map drawDAnnotation prettyAnns

  in (drawAnnotations anns, prettyDeclAnns)

  where drawDAnnotation (DProvenance p) = ["DProvenance "] %+ either prettyLines prettyLines p
        drawDAnnotation (DEffect e)     = ["DEffect "]     %+ either prettyLines prettyLines e
        drawDAnnotation _ = error "Invalid symbol annotation"


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
  let (prettyAnns, anns) = partition (\a -> isDProvenance a || isDEffect a) as
      prettyDeclAnns     = PT.drawGroup $ map drawDAnnotationT prettyAnns
  in (PT.drawAnnotations anns, prettyDeclAnns)

  where drawDAnnotationT (DProvenance p) = [T.pack "DProvenance "] PT.%+ either PT.prettyLines PT.prettyLines p
        drawDAnnotationT (DEffect e)     = [T.pack "DEffect "]     PT.%+ either PT.prettyLines PT.prettyLines e
        drawDAnnotationT _               = error "Invalid symbol annotation"
