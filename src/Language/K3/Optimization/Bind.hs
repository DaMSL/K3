{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Language.K3.Optimization.Bind where

import qualified GHC.Exts (IsList(..))

import Prelude hiding (any)
import Control.Arrow ((&&&))

import Data.Maybe
import Data.Foldable
import Data.Functor
import Data.Tree

import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core

import Language.K3.Optimization.Core

-- Orphan for IsList (S.Set a)
instance (Ord a) => GHC.Exts.IsList (S.Set a) where
    type Item (S.Set a) = a
    fromList = S.fromList
    toList = S.toList

pattern TAC t as cs = Node (t :@: as) cs

bindOpt :: K3 Declaration -> K3 Declaration
bindOpt (TAC (DGlobal i t me) as cs) = TAC (DGlobal i t (bindOptE <$> me)) as cs
bindOpt (TAC (DTrigger i t e) as cs) = TAC (DTrigger i t (bindOptE e)) as cs
bindOpt (TAC (DRole n) as cs) = TAC (DRole n) as (map bindOpt cs)

bindOptE :: K3 Expression -> K3 Expression
bindOptE e@(TAC t@(EBindAs bs) as [s, b])
    = TAC t ((EOpt $ BindHint (symIDs refBound, [], symIDs writeBound)) : as) (map bindOptE [s, b])
  where
    findScope f@(tag -> FScope _ _) = f
    findScope (children -> fs) = findScope (last fs)

    (TAC (FScope ss _) _ [c]) = findScope $ let (EEffect k) = fromJust $ e @~ isEEffect in k
    (writeBound, refBound) = S.partition (conflicts c) $ S.fromList ss
    symIDs = S.map (\(tag -> Symbol i _) -> i)

    -- | Determine whether or not a given symbol has a read/write/superstructure conflict in the given
    -- effect tree. To conflict, the symbol's superstructure must be read after the symbol was written
    -- to, or the superstructure was written anywhere.
    conflicts :: K3 Effect -> K3 Symbol -> Bool
    conflicts e s = conflictsWR e || any (flip hasWrite e) ss
      where
        ss = S.delete s $ anySuperStructure s

        conflictsWR (tag &&& children -> (FSeq, [first, second]))
            = conflictsWR first || conflictsWR second || (hasWrite s first && any (flip hasRead second) ss)
        conflictsWR (children -> cs) = any conflictsWR cs

bindOptE (TAC t as cs)  = TAC t as (map bindOptE cs)
