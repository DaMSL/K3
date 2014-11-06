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
import Language.K3.Analysis.Effects.InsertEffects(EffectEnv(..), symRWAQuery, eE, eS)

import Language.K3.Transform.Common
import Language.K3.Transform.Hints

symIDs :: EffectEnv -> S.Set (K3 Symbol) -> S.Set Identifier
symIDs env = S.map (\(tag . eS env -> Symbol i _) -> i)

lambdaFormOptD :: TransformConfig -> EffectEnv -> K3 Declaration -> K3 Declaration
lambdaFormOptD c env (Node (DGlobal i t me :@: as) cs) = Node (DGlobal  i t (lambdaFormOptE c env [] <$> me) :@: as) cs
lambdaFormOptD c env (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t (lambdaFormOptE c env [] e)      :@: as) cs
lambdaFormOptD c env (Node (DRole n :@: as) cs)        = Node (DRole n :@: as) $ map (lambdaFormOptD c env) cs
lambdaFormOptD _ _ t = t

lambdaFormOptE :: TransformConfig -> EffectEnv -> [K3 Expression] -> K3 Expression -> K3 Expression
lambdaFormOptE conf env ds e@(Node (ELambda x :@: as) [b]) = Node (ELambda x :@: (a:c:as)) [lambdaFormOptE conf env ds b]
  where
    ESymbol (tag . eS env -> (Symbol _ (PLambda _ (tag . eE env -> FScope [binding] (Right(cRead, cWritten, cApplied))))))
        = fromJust $ e @~ isESymbol

    getEffects e' = (\(EEffect f) -> f) <$> e' @~ (\case { EEffect _ -> True; _ -> False })

    fs = mapMaybe getEffects ds
    moveable g = not $ any (\f -> let (r, w, _) = symRWAQuery f [g] env in g `elem` r || g `elem` w) fs

    funcHint
        | binding `elem` cWritten = False
        | binding `elem` cApplied = True
        | binding `elem` cRead = True
        | otherwise = False

    captHint = foldl' captHint' (S.empty, S.empty, S.empty) $ cRead ++ cWritten ++ cApplied

    captHint' (cref, move, copy) s
        | s === binding                                    = (cref, move, copy)
        | moveable s && s `elem` cApplied && optMoves conf = (cref, S.insert s move, copy)
        | s `notElem` cWritten && optRefs conf             = (S.insert s cref, move, copy)
        | moveable s && optMoves conf                      = (cref, S.insert s move, copy)
        | otherwise                                        = (cref, move, S.insert s copy)

    a = EOpt $ FuncHint $ funcHint && optRefs conf
    c = EOpt $ CaptHint $ let (cref, move, copy) = captHint
                          in (symIDs env cref, symIDs env move, symIDs env copy)

lambdaFormOptE c env ds (Node (EOperate OSeq :@: as) [a, b])
    = Node (EOperate OSeq :@: as) [lambdaFormOptE c env (b:ds) a, lambdaFormOptE c env ds b]
lambdaFormOptE c env ds (Node (EOperate OApp :@: as) [f', x])
    = Node (EOperate OApp :@: as) [lambdaFormOptE c env (x:ds) f', (lambdaFormOptE c env ds x) @+ a]
  where
    getEffects e = (\(EEffect f) -> f) <$> e @~ (\case { EEffect _ -> True; _ -> False })
    getSymbol e = (\(ESymbol f) -> f) <$> e @~ (\case { ESymbol _ -> True; _ -> False })

    isGlobal (tag -> EVariable i) = M.member i (globalEnv env)
    isGlobal _ = False

    fs = mapMaybe getEffects ds
    argument = getSymbol x
    moveable g = not $ any (\f -> let (r, w, _) = symRWAQuery f [g] env
                                in g `elem` r || g `elem` w) fs
    passHint = isGlobal x || argument == Nothing || not (moveable $ fromJust argument)
    a = EOpt $ PassHint passHint
lambdaFormOptE c env ds (Node (EIfThenElse :@: as) [i, t, e])
    = Node (EIfThenElse :@: as) [lambdaFormOptE c env (t:e:ds) i, lambdaFormOptE c env ds t, lambdaFormOptE c env ds e]
lambdaFormOptE c env ds (Node (ELetIn i :@: as) [e, b])
    = Node (ELetIn i :@: as) [lambdaFormOptE c env (b:ds) e, lambdaFormOptE c env ds b]
lambdaFormOptE c env ds (Node (ECaseOf x :@: as) [e, s, n])
    = Node (ECaseOf x :@: as) [lambdaFormOptE c env (s:n:ds) e, lambdaFormOptE c env ds s, lambdaFormOptE c env ds n]
lambdaFormOptE c env ds (Node (EBindAs b :@: as) [i, e])
    = Node (EBindAs b :@: as) [lambdaFormOptE c env (e:ds) i, lambdaFormOptE c env ds e]
lambdaFormOptE c env ds (Node (t :@: as) cs) = Node (t :@: as) (map (lambdaFormOptE c env ds) cs)
