{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Language.K3.Transform.LambdaForms where

import Prelude hiding (any, elem, notElem)

import Control.Applicative

import Data.Foldable
import Data.Maybe
import Data.Tree

import qualified Data.Map as M
import qualified Data.Set as S

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Effects.Common
import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv(..), substGlobalsE, symRWAQuery)

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
    ESymbol (tag -> (Symbol _ (PLambda _ (tag -> FScope [binding] (cRead, cWritten, cApplied)))))
        = fromJust $ substGlobalsE env e @~ isESymbol

    getEffects e = (\(EEffect f) -> f) <$> e @~ (\case { EEffect _ -> True; _ -> False })

    fs = mapMaybe getEffects ds
    moveable = not $ any (\f -> let (r, w, a) = symRWAQuery f [binding] env
                                in binding `elem` r || binding `elem` w) fs

    funcHint
        | binding `elem` cWritten = False
        | binding `elem` cRead && binding `notElem` cWritten && binding `notElem` cApplied = True
        | binding `elem` cApplied = True
        | otherwise = False

    captHint = foldl' captHint' (S.empty, S.empty, S.empty) $ cRead ++ cWritten ++ cApplied

    captHint' (cref, move, copy) s
        | s === binding = (cref, move, copy)
        | moveable && (s `elem` cWritten || s `elem` cApplied) = (cref, S.insert s move, copy)
        | moveable = (S.insert s cref, move, copy)
        | s `elem` cWritten = (cref, move, S.insert s copy)
        | otherwise = (S.insert s cref, move, copy)

    a = EOpt $ FuncHint funcHint
    c = EOpt $ CaptHint $ let (cref, move, copy) = captHint in (symIDs cref, symIDs move, symIDs copy)

lambdaFormOptE env ds (Node (EOperate OSeq :@: as) [a, b])
    = Node (EOperate OSeq :@: as) [lambdaFormOptE env (b:ds) a, lambdaFormOptE env ds b]
lambdaFormOptE env ds (Node (EOperate OApp :@: as) [f, x])
    = Node (EOperate OApp :@: as) [lambdaFormOptE env (x:ds) f, (lambdaFormOptE env ds x) @+ a]
  where
    moveable = null ds
    passHint = not moveable
    a = EOpt $ PassHint passHint
lambdaFormOptE env ds (Node (EIfThenElse :@: as) [i, t, e])
    = Node (EIfThenElse :@: as) [lambdaFormOptE env (t:e:ds) i, lambdaFormOptE env ds t, lambdaFormOptE env ds e]
lambdaFormOptE env ds (Node (ELetIn i :@: as) [e, b])
    = Node (ELetIn i :@: as) [lambdaFormOptE env (b:ds) e, lambdaFormOptE env ds b]
lambdaFormOptE env ds (Node (ECaseOf x :@: as) [e, s, n])
    = Node (ECaseOf x :@: as) [lambdaFormOptE env (s:n:ds) e, lambdaFormOptE env ds s, lambdaFormOptE env ds n]
lambdaFormOptE env ds (Node (EBindAs b :@: as) [i, e])
    = Node (EBindAs b :@: as) [lambdaFormOptE env (e:ds) i, lambdaFormOptE env ds e]
lambdaFormOptE env ds (Node (t :@: as) cs) = Node (t :@: as) (map (lambdaFormOptE env ds) cs)
