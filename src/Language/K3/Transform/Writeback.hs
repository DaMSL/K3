{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -fno-warn-orphans #-}

module Language.K3.Transform.Writeback where

import qualified GHC.Exts (IsList(..))

import Prelude hiding (any)
import Control.Arrow ((&&&))

import Data.Maybe
import Data.Foldable
import Data.Functor
import Data.Tree

import qualified Data.Set as S

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Declaration

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv, eE, eS, expandEffDeep, expandSymDeep)
import Language.K3.Analysis.Effects.Queries

import Language.K3.Transform.Hints

-- Orphan for IsList (S.Set a)
instance (Ord a) => GHC.Exts.IsList (S.Set a) where
    type Item (S.Set a) = a
    fromList = S.fromList
    toList = S.toList

pattern TAC t as cs = Node (t :@: as) cs

findScope :: EffectEnv -> K3 Effect -> K3 Effect
findScope env f@(tag . eE env -> FScope _) = f
findScope env (children . eE env -> fs) = findScope env (last fs)

symIDs :: S.Set (K3 Symbol) -> QueryM (S.Set Identifier)
symIDs ss = getEnv >>= \env -> return $ S.map (symIdent . tag . eS env) ss

-- | Determine whether or not a given symbol has a read/write/superstructure conflict in the given
-- effect tree. To conflict, the symbol's superstructure must be read after the symbol was written
-- to, or the superstructure was written anywhere.
conflicts :: EffectEnv -> K3 Effect -> K3 Symbol -> Bool
conflicts env (expandEffDeep env -> f) (expandSymDeep env -> k)
    = conflictsWR f || any (\q -> evalQueryM (doesWriteOn f q) env) kk
  where
    kk = S.delete k $ anySuperStructure k

    conflictsWR (eE env -> tnc -> (FSeq, [first, second]))
        = conflictsWR first || conflictsWR second ||
          (evalQueryM (doesWriteOn first k) env && any (\s -> evalQueryM (doesReadOn second s) env) kk)
    conflictsWR (children . eE env -> cs) = any conflictsWR cs

writebackOptPT :: (K3 Declaration, Maybe EffectEnv) -> Either String (K3 Declaration, Maybe EffectEnv)
writebackOptPT (d, me) = maybe (Left "No effect argument given to writebackPT")
                      (\e -> let (d', e') = runQueryM (writebackOptD d) e in Right (d', Just e')) me

writebackOptD :: K3 Declaration -> QueryM (K3 Declaration)

writebackOptD (TAC (DGlobal i t me) as cs) = do
  me' <- case me of
           Nothing -> return Nothing
           Just e -> Just <$> writebackOptE e
  return $ TAC (DGlobal  i t me') as cs

writebackOptD (TAC (DTrigger i t e) as cs)
    = writebackOptE e >>= \e' -> return $ TAC (DTrigger i t e') as cs

writebackOptD (TAC (DRole n) as cs) = TAC (DRole n) as <$> mapM writebackOptD cs
writebackOptD t = return t

writebackOptE :: K3 Expression -> QueryM (K3 Expression)
writebackOptE g@(TAC t@(EBindAs _) as cs) = do
  cs' <- mapM writebackOptE cs
  bindHint <- constructBindHint g
  return $ TAC t (bindHint:as) cs'

writebackOptE g@(TAC t@(ECaseOf _) as cs) = do
  cs' <- mapM writebackOptE cs
  bindHint <- constructBindHint g
  return $ TAC t (bindHint:as) cs'

writebackOptE (TAC t as cs) = TAC t as <$> mapM writebackOptE cs

constructBindHint :: K3 Expression -> QueryM (Annotation Expression)
constructBindHint g = do
  env <- getEnv
  let (EEffect k) = fromJust $ g @~ isEEffect
  let (eE env -> tnc -> (FScope bs, es)) = findScope env k
  let (writeBound, refBound) = if null es
                               then ([], S.fromList bs)
                               else S.partition (conflicts env $ head es) (S.fromList bs)

  forM_ refBound (\s -> toggleCopy s >> toggleWb s)
  rb <- symIDs refBound
  wb <- symIDs writeBound
  return $ EOpt $ BindHint (rb, [], wb)
