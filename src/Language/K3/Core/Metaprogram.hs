{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Core.Metaprogram where

import Data.Either
import Data.List
import qualified Data.Map as Map
import Data.Map ( Map )
import Data.Typeable

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Literal

import Language.K3.Utils.Pretty

{-| Metaprograms, as a parsing-time AST.
    Metaprograms enable splicing of label, type, expression, and declaration
    bindings but have a restricted set of binding constructors, specifically
    data and control annotations.
    For now, metaprogramming expressions and types are fully embedded
    into standard expression and type ASTs through identifiers.
-}

data SpliceValue = SVar     Identifier
                 | SLabel   Identifier
                 | SType    (K3 Type)
                 | SExpr    (K3 Expression)
                 | SDecl    (K3 Declaration)
                 | SLiteral (K3 Literal)
                 | SRecord  NamedSpliceValues
                 | SList    [SpliceValue]
                 deriving (Eq, Ord, Read, Show, Typeable)

data SpliceType = STLabel
                | STType
                | STExpr
                | STDecl
                | STLiteral
                | STRecord NamedSpliceTypes
                | STList   SpliceType
                deriving (Eq, Ord, Read, Show, Typeable)

type NamedSpliceValues = Map Identifier SpliceValue
type NamedSpliceTypes  = Map Identifier SpliceType
type TypedSpliceVar    = (SpliceType, Identifier)

data SpliceResult m = SRType    (m (K3 Type))
                    | SRExpr    (m (K3 Expression))
                    | SRDecl    (m (K3 Declaration))
                    | SRLiteral (m (K3 Literal))
                    | SRRewrite (m (K3 Expression, [K3 Declaration]))

data MPDeclaration = MPDataAnnotation Identifier [TypedSpliceVar] [TypeVarDecl] [Either MPAnnMemDecl AnnMemDecl]
                   | MPCtrlAnnotation Identifier [TypedSpliceVar] [PatternRewriteRule] [K3 Declaration]
                   deriving (Eq, Ord, Read, Show, Typeable)

data MPAnnMemDecl = MPAnnMemDecl Identifier SpliceValue [AnnMemDecl]
                  deriving (Eq, Ord, Read, Show, Typeable)

type SpliceEnv     = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

instance Pretty MPDeclaration where
  prettyLines (MPDataAnnotation i svars tvars (partitionEithers -> (mpAnnMems, annMems))) =
      ["MPDataAnnotation " ++ i
          ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
          ++ if null tvars then "" else ("[" ++ multiLineSep tvars ++ "]")
          , "|"]
      ++ drawMPAnnotationMembers mpAnnMems
      ++ drawAnnotationMembers annMems
    where
      multiLineSep l = removeTrailingWhitespace . boxToString
                         $ foldl1 (\a b -> a %+ [", "] %+ b) $ map prettyLines l

  prettyLines (MPCtrlAnnotation i svars rewriteRules extensions) =
      ["MPCtrlAnnotation " ++ i
        ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
        , "|"]
      ++ concatMap drawRule rewriteRules
      ++ drawDecls "shared" extensions
    where
      drawRule (p,r,decls) =
        nonTerminalShift p ++ ["|"] ++
          (if null decls
             then (shift "=> " "   " $ prettyLines r)
             else (shift "=> " "|  " $ prettyLines r) ++ drawDecls "" decls)

      drawDecls tagStr decls =
        if null decls then []
        else ["|"] ++ concatMap (\d -> nonTermExt tagStr d ++ ["|"]) (init decls)
                   ++ termExt tagStr (last decls)

      extensionPrefix tagStr = "+> " ++ (if null tagStr then "" else "(" ++ tagStr ++ ")")

      nonTermExt tagStr d = shift (extensionPrefix tagStr) "|  " $ prettyLines d
      termExt    tagStr d = shift (extensionPrefix tagStr) "   " $ prettyLines d

instance Pretty MPAnnMemDecl where
  prettyLines (MPAnnMemDecl i c mems) =
    ["MPAnnMemDecl " ++ i] ++ nonTerminalShift c ++ ["|"] ++ drawAnnotationMembers mems

instance Pretty SpliceValue where
  prettyLines (SType    t)   = ["SType "]    %+ prettyLines t
  prettyLines (SExpr    e)   = ["SExpr "]    %+ prettyLines e
  prettyLines (SDecl    d)   = ["SDecl "]    %+ prettyLines d
  prettyLines (SLiteral l)   = ["SLiteral "] %+ prettyLines l
  prettyLines (SRecord  nsv) = ["SRecord"]   %$ (indent 3 $ concatMap (\(i,sv) -> [i] %+ prettyLines sv) $ recordElemsAsList nsv)
  prettyLines (SList    l)   = ["SList"]     %$ (indent 2 $ concatMap prettyLines l)
  prettyLines sv = [show sv]


drawMPAnnotationMembers :: [MPAnnMemDecl] -> [String]
drawMPAnnotationMembers []  = []
drawMPAnnotationMembers [x] = terminalShift x
drawMPAnnotationMembers x   = concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
                              ++ terminalShift (last x)

drawAnnotationMembers :: [AnnMemDecl] -> [String]
drawAnnotationMembers []  = []
drawAnnotationMembers [x] = terminalShift x
drawAnnotationMembers x   = concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
                              ++ terminalShift (last x)

{- Splice value and type constructors -}
spliceRecord :: [(Identifier, SpliceValue)] -> SpliceValue
spliceRecord l = SRecord $ Map.fromList l

spliceList :: [SpliceValue] -> SpliceValue
spliceList l = SList l

spliceRecordT :: [(Identifier, SpliceType)] -> SpliceType
spliceRecordT l = STRecord $ Map.fromList l

spliceListT :: SpliceType -> SpliceType
spliceListT st = STList st

spliceRecordField :: SpliceValue -> Identifier -> Maybe SpliceValue
spliceRecordField (SRecord r) n = Map.lookup n r
spliceRecordField _ _ = Nothing

ltLabel :: SpliceValue -> Maybe SpliceValue
ltLabel v = spliceRecordField v spliceVIdSym

ltType :: SpliceValue -> Maybe SpliceValue
ltType v = spliceRecordField v spliceVTSym

recordElemsAsList :: NamedSpliceValues -> [(Identifier, SpliceValue)]
recordElemsAsList = Map.toList

mpDataAnnotation :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [Either MPAnnMemDecl AnnMemDecl] -> MPDeclaration
mpDataAnnotation n svars tvars mems = MPDataAnnotation n svars tvars mems

mpCtrlAnnotation :: Identifier -> [TypedSpliceVar] -> [PatternRewriteRule] -> [K3 Declaration] -> MPDeclaration
mpCtrlAnnotation n svars rules extensions = MPCtrlAnnotation n svars rules extensions

mpAnnMemDecl :: Identifier -> SpliceValue -> [AnnMemDecl] -> MPAnnMemDecl
mpAnnMemDecl i c mems = MPAnnMemDecl i c mems

{- Splice context accessors -}
lookupSCtxt :: Identifier -> SpliceContext -> Maybe SpliceValue
lookupSCtxt n ctxt = find (Map.member n) ctxt >>= Map.lookup n

addSCtxt :: Identifier -> SpliceValue -> SpliceContext -> SpliceContext
addSCtxt n val [] = [Map.insert n val Map.empty]
addSCtxt n val ctxt = (Map.insert n val $ head ctxt):(tail ctxt)

removeSCtxt :: Identifier -> SpliceContext -> SpliceContext
removeSCtxt _ [] = []
removeSCtxt n ctxt = (Map.delete n $ head ctxt):(tail ctxt)

removeSCtxtFirst :: Identifier -> SpliceContext -> SpliceContext
removeSCtxtFirst n ctxt = snd $ foldl removeOnFirst (False, []) ctxt
  where removeOnFirst (done, acc) senv = if done then (done, acc++[senv]) else removeIfPresent acc senv
        removeIfPresent acc senv = if Map.member n senv then (True, acc++[Map.delete n senv]) else (False, acc++[senv])

pushSCtxt :: SpliceEnv -> SpliceContext -> SpliceContext
pushSCtxt senv ctxt = senv:ctxt

popSCtxt :: SpliceContext -> SpliceContext
popSCtxt = tail

concatCtxt :: SpliceContext -> SpliceContext -> SpliceContext
concatCtxt a b = a ++ b

{- Splice environment helpers -}
spliceVIdSym :: Identifier
spliceVIdSym = "identifier"

spliceVTSym :: Identifier
spliceVTSym  = "type"

spliceVESym :: Identifier
spliceVESym  = "expr"

lookupSpliceE :: Identifier -> SpliceEnv -> Maybe SpliceValue
lookupSpliceE n senv = Map.lookup n senv

addSpliceE :: Identifier -> SpliceValue -> SpliceEnv -> SpliceEnv
addSpliceE n v senv = Map.insert n v senv

emptySpliceEnv :: SpliceEnv
emptySpliceEnv = Map.empty

mkSpliceEnv :: [(Identifier, SpliceValue)] -> SpliceEnv
mkSpliceEnv = Map.fromList

mkRecordSpliceEnv :: [Identifier] -> [K3 Type] -> SpliceEnv
mkRecordSpliceEnv ids tl = mkSpliceEnv $ map mkSpliceEnvEntry $ zip ids tl
  where mkSpliceEnvEntry (i,t) = (i, spliceRecord [(spliceVIdSym, SLabel i), (spliceVTSym, SType t)])

-- | Splice environment merge, favoring elements in the RHS operand.
mergeSpliceEnv :: SpliceEnv -> SpliceEnv -> SpliceEnv
mergeSpliceEnv = Map.unionWith (\_ v -> v)
