{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Language.K3.Optimization.Bind where

import qualified GHC.Exts (IsList(..))

import Data.Maybe
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
bindOpt d = d @<- []

bindOptE :: K3 Expression -> K3 Expression
bindOptE e@(TAC t@(EBindAs bs) as [s, b])
    = TAC t ((EOpt $ BindHint (refBound, [], writeBound)) : as) (map bindOptE [s, b])
  where
    bindSet = S.fromList $ bindingVariables bs
    EEffect bodyEffect = fromJust (e @~ \case { EEffect _ -> True; _ -> False })
    writeBound = S.map (\(tag -> Symbol i _) -> i) $ writeSet bodyEffect
    refBound = bindSet S.\\ writeBound
bindOptE (TAC t as cs)  = TAC t as (map bindOptE cs)
