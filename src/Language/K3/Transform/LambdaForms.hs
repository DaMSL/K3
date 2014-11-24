{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.LambdaForms where

import Prelude hiding (any, elem, notElem, concatMap, sequence, mapM, mapM_)

import Control.Applicative
import Control.Arrow
import Control.Monad.Identity hiding (sequence, mapM, mapM_)
import Control.Monad.State hiding (sequence, mapM, mapM_)

import Data.Foldable
import Data.Traversable
import Data.Maybe
import Data.Tree

import Data.List (nub)

import qualified Data.Map as M
import qualified Data.Set as S

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects
import Language.K3.Analysis.Effects.Queries

import Language.K3.Transform.Common
import Language.K3.Transform.Hints

symIDs :: EffectEnv -> S.Set (K3 Symbol) -> S.Set Identifier
symIDs env = S.map (symIdent . tag . eS env)

isDerivedFromGlobal :: EffectEnv -> K3 Symbol -> Bool
isDerivedFromGlobal _ (tag -> symProv -> PGlobal) = True
isDerivedFromGlobal env (tag &&& children -> (symProv -> (PRecord _), cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> (PTuple _), cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> PIndirection, cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> (PProject _), cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> PCase, cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> PApply, cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> PSet, cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env (tag &&& children -> (symProv -> PVar, cs)) = any (isDerivedFromGlobal env) cs
isDerivedFromGlobal env s@(tag -> SymId i) = isDerivedFromGlobal env (eS env s)
isDerivedFromGlobal _ _ = False

type LFS = (EffectEnv, [K3 Expression], TransformConfig)
type LFM = StateT LFS Identity

runLFM :: LFM a -> LFS -> (a, LFS)
runLFM = runState

evalLFM :: LFM a -> LFS -> a
evalLFM = evalState

instance EffectMonad LFM where
  getEnv = get >>= \(e, _, _) -> return e
  modifyEnv f = modify (\(e, ds, tc) -> (f e, ds, tc))

downstreams :: LFM [K3 Expression]
downstreams = get >>= \(_, ds, _) -> return ds

withExtraDownstreams :: [K3 Expression] -> LFM a -> LFM a
withExtraDownstreams ds m = do
  (env, ds', tc) <- get
  put (env, ds ++ ds', tc)
  a <- m
  (env', _, tc') <- get
  put (env', ds', tc')
  return a

transformConfig :: LFM TransformConfig
transformConfig = get >>= \(_, _, tc) -> return tc

lambdaFormOpt :: TransformConfig -> EffectEnv -> K3 Declaration -> K3 Declaration
lambdaFormOpt tc env d = evalLFM (lambdaFormOptD d) (env, [], tc)

lambdaFormOptD :: K3 Declaration -> LFM (K3 Declaration)
lambdaFormOptD (Node (DGlobal i t me :@: as) cs) = do
  me' <- sequence $ lambdaFormOptE <$> me
  return $ Node (DGlobal  i t me' :@: as) cs

lambdaFormOptD (Node (DTrigger i t e :@: as) cs) = do
  e' <- lambdaFormOptE e
  return $ Node (DTrigger i t e' :@: as) cs

lambdaFormOptD (Node (DRole n :@: as) cs) = Node (DRole n :@: as) <$> mapM lambdaFormOptD cs
lambdaFormOptD t = return t

lambdaFormOptE :: K3 Expression -> LFM (K3 Expression)
lambdaFormOptE e@(Node (ELambda x :@: as) [b]) = do
  b' <- lambdaFormOptE b
  env <- getEnv
  ds <- downstreams
  conf <- transformConfig
  let fs = mapMaybe getEffects ds
  let (ESymbol (eS env -> tnc -> (symProv -> PLambda (eE env -> fc@(Node (FScope bindings@(binding:closure) :@: _) bes)), returnSymbols))) = fromJust $ e @~ isESymbol
  let  moveable (expandSymDeep env -> g) = not (isDerivedFromGlobal env g) &&
                                           not (any (\f -> let SymbolCategories r w _ _ _
                                                                 = effectSCategories f [g] env
                                                           in g `elemSymbol` r || g `elemSymbol` w) fs)

  let SymbolCategories cRead cWritten cApplied _ _ = exprSCategories False e env

  let parent = head . children

  let funcHint
          | binding `elemSymbol` cWritten = False
          | binding `elemSymbol` cApplied = True
          | binding `elemSymbol` cRead = True
          | otherwise = False

  let captHint' (cref, move, copy) s
          | moveable (parent s) && s `elemSymbol` cApplied && optMoves conf
              = toggleCopy s >> toggleMove s >> return (cref, S.insert (parent s) move, copy)
          | s `notElemSymbol` cWritten && optRefs conf
              = toggleCopy s >> return (S.insert (parent s) cref, move, copy)
          | moveable (parent s) && optMoves conf
              = toggleCopy s >> toggleMove s >> return (cref, S.insert (parent s) move, copy)
          | otherwise = return (cref, move, S.insert (parent s) copy)

  captHint <- foldM captHint' (S.empty, S.empty, S.empty) $
                 mapMaybe (\(expandSymDeep env -> symbol)
                                -> case symbol of
                                     (tag -> symProv -> PClosure) -> Just symbol
                                     _ -> Nothing) bindings

  let a = EOpt $ FuncHint $ funcHint && optRefs conf
  let c = EOpt $ CaptHint $ let (cref, move, copy) = captHint
                            in (symIDs env cref, symIDs env move, symIDs env copy)

  return $ Node (ELambda x :@: (a:c:as)) [b']
 where
  getEffects e' = (\(EEffect f) -> f) <$> e' @~ isEEffect

lambdaFormOptE (Node (EOperate OSeq :@: as) [a, b]) = do
  a' <- withExtraDownstreams [b] $ lambdaFormOptE a
  b' <- lambdaFormOptE b
  return $ Node (EOperate OSeq :@: as) [a', b']

lambdaFormOptE (Node (EOperate OApp :@: as) [f, x]) = do
  f' <- withExtraDownstreams [x] $ lambdaFormOptE f
  x' <- lambdaFormOptE x

  env <- getEnv

  let getEffects e = (\(EEffect f) -> f) <$> e @~ (\case { EEffect _ -> True; _ -> False })
  let getSymbol e = (\(ESymbol f) -> f) <$> e @~ (\case { ESymbol _ -> True; _ -> False })

  ds <- downstreams

  let fs = mapMaybe getEffects ds
  let argument = getSymbol x
  let moveable (expandSymDeep env -> g) = not (isDerivedFromGlobal env g) &&
                                           not (any (\f -> let SymbolCategories r w _ _ _
                                                                 = effectSCategories f [g] env
                                                           in g `elemSymbol` r || g `elemSymbol` w) fs)
  let passHint = maybe True (not . moveable) argument
  let a = EOpt $ PassHint passHint

  return $ Node (EOperate OApp :@: as) [f', x' @+ a]

lambdaFormOptE (Node (EIfThenElse :@: as) [i, t, e]) = do
  i' <- withExtraDownstreams [t, e] $ lambdaFormOptE i
  t' <- lambdaFormOptE t
  e' <- lambdaFormOptE e
  return $ Node (EIfThenElse :@: as) [i', t', e']

lambdaFormOptE (Node (ELetIn i :@: as) [e, b]) = do
  e' <- withExtraDownstreams [b] $ lambdaFormOptE e
  b' <- lambdaFormOptE b
  return $ Node (ELetIn i :@: as) [e', b']

lambdaFormOptE (Node (ECaseOf x :@: as) [e, s, n]) = do
  e' <- withExtraDownstreams [s, n] $ lambdaFormOptE e
  s' <- lambdaFormOptE s
  n' <- lambdaFormOptE n
  return $ Node (ECaseOf x :@: as) [e', s', n']

lambdaFormOptE (Node (EBindAs b :@: as) [i, e]) = do
  i' <- withExtraDownstreams [e] $ lambdaFormOptE i
  e' <- lambdaFormOptE e
  return $ Node (EBindAs b :@: as) [i', e']
lambdaFormOptE (Node (t :@: as) cs) = Node (t :@: as) <$> mapM lambdaFormOptE cs
