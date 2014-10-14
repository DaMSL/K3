{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedLists #-}

module Language.K3.Analysis.Effects.Common where

import Prelude hiding (any)

import Control.Arrow ((&&&))

import Data.Foldable
import Data.Monoid
import Data.Maybe

import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Analysis.Effects.Core

readSet :: K3 Effect -> S.Set (K3 Symbol)
readSet (tag -> FRead s) = S.singleton s
readSet (children -> cs) = mconcat $ map readSet cs

writeSet :: K3 Effect -> S.Set (K3 Symbol)
writeSet (tag -> FWrite s) = S.singleton s
writeSet (children -> cs) = mconcat $ map writeSet cs

anySuperStructure :: K3 Symbol -> S.Set (K3 Symbol)
anySuperStructure s = S.insert s (S.unions $ map anySuperStructure (children s))

hasRead :: K3 Symbol -> K3 Effect -> Bool
hasRead s (tag -> FRead k) = fromJust (k @~ isSID) == fromJust (s @~ isSID)
hasRead _ (children -> []) = False
hasRead s (children -> cs) = any (hasRead s) cs

hasWrite :: K3 Symbol -> K3 Effect -> Bool
hasWrite s (tag -> FWrite k) = fromJust (k @~ isSID) == fromJust (s @~ isSID)
hasWrite _ (children -> []) = False
hasWrite s (children -> cs) = any (hasWrite s) cs

hasWriteInFunction :: K3 Symbol -> K3 Symbol -> Bool
hasWriteInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), [])) = hasWrite s f
hasWriteInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), cs))
    = hasWrite s f || any (hasWriteInFunction s) cs
hasWriteInFunction _ _ = False
