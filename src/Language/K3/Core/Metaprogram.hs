{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

module Language.K3.Core.Metaprogram where

import Data.List
import qualified Data.Map as Map
import Data.Map ( Map )
import qualified Data.Set as Set
import Data.Set ( Set )

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Utils.Pretty

{-| Metaprograms, as a parsing-time AST.
    Metaprograms enable splicing of label, type, expression, and declaration
    bindings but have a restricted set of binding constructors, specifically
    data and control annotations.
    For now, metaprogramming expressions and types are fully embedded
    into standard expression and type ASTs through identifiers.
-}

data SpliceValue = SLabel  Identifier
                 | SType   (K3 Type)
                 | SExpr   (K3 Expression)
                 | SDecl   (K3 Declaration)
                 | SVar    Identifier
                 | SRecord NamedSpliceValues
                 | SSet    (Set SpliceValue)
                 deriving (Eq, Ord, Read, Show)

data SpliceType = STLabel
                | STType
                | STExpr
                | STDecl
                | STRecord NamedSpliceTypes
                | STSet    SpliceType
                deriving (Eq, Ord, Read, Show)

type NamedSpliceValues = Map Identifier SpliceValue
type NamedSpliceTypes  = Map Identifier SpliceType
type TypedSpliceVar    = (SpliceType, Identifier)

data SpliceResult m = SRType    (m (K3 Type))
                    | SRExpr    (m (K3 Expression))
                    | SRDecl    (m (K3 Declaration))
                    | SRRewrite (m (K3 Expression, [K3 Declaration]))

data MPDeclaration = MPDataAnnotation Identifier [TypedSpliceVar] [TypeVarDecl] [AnnMemDecl]
                   | MPCtrlAnnotation Identifier [TypedSpliceVar] [PatternRewriteRule] [K3 Declaration]
                   deriving (Eq, Ord, Show, Read)

type SpliceEnv     = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

instance Pretty MPDeclaration where
  prettyLines (MPDataAnnotation i svars tvars members) =
      ["MPDataAnnotation " ++ i
          ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
          ++ if null tvars then "" else ("[" ++ multiLineSep tvars ++ "]")
          , "|"]
      ++ drawAnnotationMembers members
    where
      multiLineSep l = removeTrailingWhitespace . boxToString
                         $ foldl1 (\a b -> a %+ [", "] %+ b) $ map prettyLines l

      drawAnnotationMembers []  = []
      drawAnnotationMembers [x] = terminalShift x
      drawAnnotationMembers x   = concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
                                    ++ terminalShift (last x)

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


{- Splice value and type constructors -}
spliceRecord :: [(Identifier, SpliceValue)] -> SpliceValue
spliceRecord l = SRecord $ Map.fromList l

spliceSet :: [SpliceValue] -> SpliceValue
spliceSet l = SSet $ Set.fromList l

spliceRecordT :: [(Identifier, SpliceType)] -> SpliceType
spliceRecordT l = STRecord $ Map.fromList l

spliceSetT :: SpliceType -> SpliceType
spliceSetT st = STSet st

spliceRecordField :: SpliceValue -> Identifier -> Maybe SpliceValue
spliceRecordField (SRecord r) n = Map.lookup n r
spliceRecordField _ _ = Nothing

mpDataAnnotation :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> MPDeclaration
mpDataAnnotation n svars tvars mems = MPDataAnnotation n svars tvars mems

mpCtrlAnnotation :: Identifier -> [TypedSpliceVar] -> [PatternRewriteRule] -> [K3 Declaration] -> MPDeclaration
mpCtrlAnnotation n svars rules extensions = MPCtrlAnnotation n svars rules extensions

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
