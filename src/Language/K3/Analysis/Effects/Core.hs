{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}

-- | Data Representations for K3 Effects
module Language.K3.Analysis.Effects.Core where

import Data.List
import Data.Map hiding (null, partition)
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Utils.Pretty

data Provenance
    = PRecord Identifier
    | PTuple Integer
    | PIndirection
    | PProject Identifier -- Created by projections
    | PLet
    | PCase
    | PClosure
    | PLambda (K3 Effect)
    | PApply       -- A symbol application only extracts the child symbols
    | PSet         -- Non-deterministic (if-then-else or case)
    | PChoice      -- One of the cases must be chosen ie. they're exclusive
    | PDerived     -- A symbol derived from its children e.g. x + y ==> [x;y]
    -- The following can be roots
    | PVar
    | PTemporary   -- A local leading to no lineage of interest
    | PGlobal
  deriving (Eq, Ord, Read, Show)

data Symbol = Symbol { symIdent :: Identifier
                     , symProv :: Provenance
                     , symHasCopy :: Bool
                     , symHasWb :: Bool
                     , symLambdaChoice :: Int  -- Which lambda version to apply, from a PChoice (default=0)
                     }
            | SymId Int
            deriving (Eq, Ord, Read, Show)

data instance Annotation Symbol = SID Int
                                | SDeclared (K3 Symbol)
                                deriving (Eq, Ord, Read, Show)

isSID :: Annotation Symbol -> Bool
isSID (SID _) = True
isSID _ = False

isSDeclared :: Annotation Symbol -> Bool
isSDeclared (SDeclared _) = True
isSDeclared _ = False

type ClosureInfo = ([K3 Symbol], [K3 Symbol], [K3 Symbol])

data Effect
    = FRead (K3 Symbol)
    | FWrite (K3 Symbol)
    | FScope [K3 Symbol]
    -- An effect application only generates effects
    | FApply (K3 Symbol) (K3 Symbol)
    | FIO
    | FSeq
    | FSet   -- Set of effects, all of which are possible
    | FLoop  -- a flattened loop. can only happen in a foreign function
    | FEffId Int
  deriving (Eq, Ord, Read, Show)

data instance Annotation Effect = FID Int deriving (Eq, Ord, Read, Show)

isFID :: Annotation Effect -> Bool
isFID _ = True


instance Pretty (K3 Symbol) where
  prettyLines (Node (Symbol {symIdent=i, symProv=PLambda eff, symHasCopy=cp} :@: as) ch) =
    ["Symbol " ++ i ++ " PLambda " ++ show cp ++ " " ++ drawAnnotations as] ++
      (if null ch then terminalShift eff
         else nonTerminalShift eff ++ drawSubTrees ch)

  prettyLines (Node (tg :@: as) ch) = (show tg ++ drawAnnotations as) : drawSubTrees ch

instance Pretty (K3 Effect) where
  prettyLines (Node (FRead  sym :@: as) ch) =
    ["FRead " ++ drawAnnotations as] ++
      (if null ch
        then terminalShift sym
        else nonTerminalShift sym ++ drawSubTrees ch)

  prettyLines (Node (FWrite sym :@: as) ch) =
    ["FWrite " ++ drawAnnotations as] ++
      (if null ch
        then terminalShift sym
        else nonTerminalShift sym ++ drawSubTrees ch)

  prettyLines (Node (FScope syms :@: as) ch) =
    ["FScope " ++ drawAnnotations as]
      ++ (concatMap (shift "+- " "|  " . prettyLines) syms)
      ++ drawSubTrees ch

  prettyLines (Node (FApply fSym argSym :@: as) ch) =
    ["FApply " ++ drawAnnotations as] ++
      (if null ch
        then nonTerminalShift fSym ++ terminalShift argSym
        else nonTerminalShift fSym ++ nonTerminalShift argSym ++ drawSubTrees ch)

  prettyLines (Node (tg :@: as) ch) = (show tg ++ drawAnnotations as) : drawSubTrees ch

drawSymAnnotations :: [Annotation Symbol] -> (String, [String])
drawSymAnnotations as =
  let (sdeclAnns, anns) = partition isSDeclared as
      prettySymAnns  = concatMap drawSDeclAnnotation sdeclAnns
  in (drawAnnotations anns, prettySymAnns)

  where drawSDeclAnnotation (SDeclared s) = ["SDeclared "] %+ prettyLines s
        drawSDeclAnnotation _ = error "Invalid symbol annotation"
