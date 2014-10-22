{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Language.K3.Transform.LambdaForms where

import Prelude hiding (any)

import Control.Applicative

import Data.Foldable
import Data.List ((\\), partition)
import Data.Maybe
import Data.Tree

import qualified Data.Set as S

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv, substGlobalsE)

import Language.K3.Transform.Hints

symIDs :: S.Set (K3 Symbol) -> S.Set Identifier
symIDs = S.map (\(tag -> Symbol i _) -> i)

lambdaFormOptD :: EffectEnv -> K3 Declaration -> K3 Declaration
lambdaFormOptD env (Node (DGlobal i t me :@: as) cs) = Node (DGlobal  i t (lambdaFormOptE env [] <$> me) :@: as) cs
lambdaFormOptD env (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t (lambdaFormOptE env [] e)      :@: as) cs
lambdaFormOptD env (Node (DRole n :@: as) cs)        = Node (DRole n :@: as) $ map (lambdaFormOptD env) cs
lambdaFormOptD _ t = t

lambdaFormOptE :: EffectEnv -> [K3 Expression] -> K3 Expression -> K3 Expression
lambdaFormOptE env ds e@(Node (ELambda x :@: as) [b]) = Node (ELambda x :@: (a:c:as)) [lambdaFormOptE env ds b]
  where
    ESymbol symbol@(tag -> (Symbol _ (PLambda _ (Node (FScope [binding] closure :@: _) effects))))
        = fromJust $ substGlobalsE env e @~ isESymbol
    (cRead, cWritten, cApplied) = closure

    getEffects :: K3 Expression -> Maybe (K3 Effect)
    getEffects g = fmap (\(EEffect f) -> f) $ g @~ (\case { EEffect _ -> True; _ -> False })

    moveable x' = not $ any ((||) <$> hasWrite x' <*> hasRead x') $ mapMaybe getEffects ds

    (cMove, cCopy) = partition moveable cWritten

    a = EOpt $ FuncHint (not $ hasWriteInFunction binding symbol)
    c = EOpt $ CaptHint (if null effects then ( symIDs $ S.fromList cRead
                                              , S.empty
                                              , symIDs $ S.fromList (cWritten ++ cApplied))
                         else ( symIDs $ S.fromList $ cRead \\ cWritten
                              , symIDs $ S.fromList cMove
                              , symIDs $ S.fromList $ cCopy ++ cApplied))
lambdaFormOptE env ds (Node (t :@: as) cs) = Node (t :@: as) (map (lambdaFormOptE env ds) cs)
