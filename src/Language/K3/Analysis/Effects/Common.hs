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

(===) :: K3 Symbol -> K3 Symbol -> Bool
s === k = (fromJust (k @~ isSID) == fromJust (s @~ isSID))

equalAlias :: K3 Symbol -> K3 Symbol -> Bool
equalAlias s (tag &&& children -> (Symbol _ PVar, [k])) | s === k = True
equalAlias _ _ = False

hasIO :: K3 Effect -> Bool
hasIO (tag -> FIO) = True
hasIO (children -> []) = False
hasIO e@(children -> cs) = any hasIO cs

readSet :: K3 Effect -> S.Set (K3 Symbol)
readSet (tag -> FRead s) = S.singleton s
readSet (children -> cs) = mconcat $ map readSet cs

writeSet :: K3 Effect -> S.Set (K3 Symbol)
writeSet (tag -> FWrite s) = S.singleton s
writeSet (children -> cs) = mconcat $ map writeSet cs

anySuperStructure :: K3 Symbol -> S.Set (K3 Symbol)
anySuperStructure s = S.insert s (S.unions $ map anySuperStructure (children s))

hasRead :: K3 Symbol -> K3 Effect -> Bool
hasRead s (tag -> FRead k) = s === k || equalAlias s k
hasRead s (tag -> FApply f _) = hasReadInFunction s f
hasRead _ (children -> []) = False
hasRead s (children -> cs) = any (hasRead s) cs

hasWrite :: K3 Symbol -> K3 Effect -> Bool
hasWrite s (tag -> FWrite k) = s === k || equalAlias s k
hasWrite s (tag -> FApply f _) = hasWriteInFunction s f
hasWrite _ (children -> []) = False
hasWrite s (children -> cs) = any (hasWrite s) cs

hasReadInFunction :: K3 Symbol -> K3 Symbol -> Bool
hasReadInFunction s (tag &&& children -> (Symbol _ PSet, cs)) = any (hasReadInFunction s) cs
hasReadInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), [])) = hasRead s f
hasReadInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), cs))
    = hasRead s f || any (hasReadInFunction s) cs
hasReadInFunction s (tag &&& children -> ((Symbol _ PApply), [f, k]))
    = hasReadInFunction s f || (s === k && hasReadInFunction k f)
hasReadInFunction _ _ = False

hasWriteInFunction :: K3 Symbol -> K3 Symbol -> Bool
hasWriteInFunction s (tag &&& children -> (Symbol _ PSet, cs)) = any (hasWriteInFunction s) cs
hasWriteInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), [])) = hasWrite s f
hasWriteInFunction s (tag &&& children -> (Symbol _ (PLambda _ f), cs))
    = hasWrite s f || any (hasWriteInFunction s) cs
hasWriteInFunction s (tag &&& children -> ((Symbol _ PApply), [f, k]))
    = hasWriteInFunction s f || (s === k && hasWriteInFunction k f)
hasWriteInFunction _ _ = False
