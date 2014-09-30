{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE OverloadedLists #-}

module Language.K3.Analysis.Effects.Common where

import Data.Monoid

import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Analysis.Effects.Core

readSet :: K3 Effect -> S.Set (K3 Symbol)
readSet (tag -> FRead s) = S.singleton s
readSet (children -> cs) = mconcat $ map readSet cs

writeSet :: K3 Effect -> S.Set (K3 Symbol)
writeSet (tag -> FWrite s) = S.singleton s
writeSet (children -> cs) = mconcat $ map writeSet cs
