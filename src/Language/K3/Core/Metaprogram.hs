{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleInstances #-}

module Language.K3.Core.Metaprogram where

import Data.List
import qualified Data.Map as Map
import Data.Map ( Map )
import Data.Tree

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
data MetaDeclaration = Staged   MPDeclaration
                     | Unstaged Declaration
                     deriving (Eq, Ord, Show, Read)

data MPDeclaration = MPDataAnnotation Identifier [TypedSpliceVar] [TypeVarDecl] [AnnMemDecl]
                   | MPCtrlAnnotation Identifier [TypedSpliceVar] [PatternRewriteRule] [K3 Declaration]
                   deriving (Eq, Ord, Show, Read)

data instance Annotation MetaDeclaration
  = MPSpan Span
  deriving (Eq, Ord, Read, Show)


type SpliceEnv     = Map Identifier SpliceReprEnv
type SpliceReprEnv = Map Identifier SpliceValue
type SpliceContext = [SpliceEnv]

data SpliceValue = SLabel Identifier
                 | SType (K3 Type)
                 | SExpr (K3 Expression)
                 | SDecl (K3 Declaration)
                 deriving (Eq, Ord, Read, Show)

data SpliceResult m = SRType    (m (K3 Type))
                    | SRExpr    (m (K3 Expression))
                    | SRDecl    (m (K3 Declaration))
                    | SRRewrite (m (K3 Expression, [K3 Declaration]))

instance Pretty (K3 MetaDeclaration) where
  prettyLines (Node (Staged (MPDataAnnotation i svars tvars members) :@: as) ds) =
      ["Staged MPDataAnnotation " ++ i
          ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
          ++ if null tvars then "" else ("[" ++ multiLineSep tvars ++ "]")
          ++ drawAnnotations as, "|"]
      ++ drawAnnotationMembers members
      ++ drawSubTrees ds
    where
      multiLineSep l = removeTrailingWhitespace . boxToString
                         $ foldl1 (\a b -> a %+ [", "] %+ b) $ map prettyLines l

      drawAnnotationMembers []  = []
      drawAnnotationMembers [x] = terminalShift x
      drawAnnotationMembers x   = concatMap (\y -> nonTerminalShift y ++ ["|"]) (init x)
                                    ++ terminalShift (last x)

  prettyLines (Node (Staged (MPCtrlAnnotation i svars rewriteRules extensions) :@: as) ds) =
      ["Staged MPCtrlAnnotation " ++ i
        ++ if null svars then "" else ("[" ++ intercalate ", " (map show svars) ++ "]")
        ++ drawAnnotations as, "|"]
      ++ concatMap drawRule rewriteRules
      ++ drawDecls "common" extensions
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

  prettyLines (Node (Unstaged decl :@: as) ds) =
      ["Unstaged " ++ drawAnnotations as, "|"] ++ terminalShift (asK3 decl) ++ drawSubTrees ds
    where asK3 :: Declaration -> K3 Declaration
          asK3 d = Node (d :@: []) []

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
      ++ drawDecls "common" extensions
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


mpDataAnnotation :: Identifier -> [TypedSpliceVar] -> [TypeVarDecl] -> [AnnMemDecl] -> K3 MetaDeclaration
mpDataAnnotation n svars tvars mems = Node ((Staged $ MPDataAnnotation n svars tvars mems) :@: []) []

mpCtrlAnnotation :: Identifier -> [TypedSpliceVar] -> [PatternRewriteRule] -> [K3 Declaration] -> K3 MetaDeclaration
mpCtrlAnnotation n svars rules extensions = Node ((Staged $ MPCtrlAnnotation n svars rules extensions) :@: []) []

{- Splice context accessors -}
lookupSCtxt :: Identifier -> Identifier -> SpliceContext -> Maybe SpliceValue
lookupSCtxt n k ctxt = find (Map.member n) ctxt >>= Map.lookup n >>= Map.lookup k

addSCtxt :: Identifier -> SpliceReprEnv -> SpliceContext -> SpliceContext
addSCtxt n vals [] = [Map.insert n vals Map.empty]
addSCtxt n vals ctxt = (Map.insert n vals $ head ctxt):(tail ctxt)

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

lookupSpliceE :: Identifier -> Identifier -> SpliceEnv -> Maybe SpliceValue
lookupSpliceE n k senv = Map.lookup n senv >>= Map.lookup k

addSpliceE :: Identifier -> SpliceReprEnv -> SpliceEnv -> SpliceEnv
addSpliceE n renv senv = Map.insert n renv senv

emptySpliceReprEnv :: SpliceReprEnv
emptySpliceReprEnv = Map.empty

mkSpliceReprEnv :: [(Identifier, SpliceValue)] -> SpliceReprEnv
mkSpliceReprEnv = Map.fromList

emptySpliceEnv :: SpliceEnv
emptySpliceEnv = Map.empty

mkSpliceEnv :: [(Identifier, SpliceReprEnv)] -> SpliceEnv
mkSpliceEnv = Map.fromList

mkRecordSpliceEnv :: [Identifier] -> [K3 Type] -> SpliceEnv
mkRecordSpliceEnv ids tl = mkSpliceEnv $ map mkSpliceEnvEntry $ zip ids tl
  where mkSpliceEnvEntry (i,t) = (i, mkSpliceReprEnv [(spliceVIdSym, SLabel i), (spliceVTSym, SType t)])

-- | Splice environment merge, favoring elements in the RHS operand.
mergeSpliceEnv :: SpliceEnv -> SpliceEnv -> SpliceEnv
mergeSpliceEnv = Map.unionWith (\_ v -> v)
