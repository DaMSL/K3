{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.LambdaForms where

import Control.Arrow

import Data.Foldable
import Data.Functor
import Data.Maybe
import Data.Tree

import qualified Data.Set as S

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core

import Language.K3.Transform.Hints

symIDs :: S.Set (K3 Symbol) -> S.Set Identifier
symIDs = S.map (\(tag -> Symbol i _) -> i)

lambdaFormOptD :: K3 Declaration -> K3 Declaration
lambdaFormOptD (Node (DGlobal i t me :@: as) cs) = Node (DGlobal i t (lambdaFormOptE <$> me) :@: as) cs
lambdaFormOptD (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t (lambdaFormOptE $ e) :@: as) cs
lambdaFormOptD (Node (DRole n :@: as) cs) = Node (DRole n :@: as) (map lambdaFormOptD cs)

lambdaFormOptE :: K3 Expression -> K3 Expression
lambdaFormOptE e@(Node (ELambda x :@: as) cs) = Node (ELambda x :@: (a:c:as)) cs
  where
    EEffect (Node (FScope [binding] closure :@: _) effects) = fromJust $ e @~ isEEffect
    (cRead, cWritten, cApplied) = closure
    a = EOpt $ FuncHint (if null effects then False else hasRead binding $ head effects)
    c = EOpt $ CaptHint (if null effects then (symIDs $ S.fromList cRead, S.empty, symIDs $ S.fromList cWritten)
                         else (S.empty, S.empty, S.empty))
lambdaFormOptE (Node (t :@: as) cs) = Node (t :@: as) (map lambdaFormOptE cs)
