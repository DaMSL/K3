{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}

-- | Purity Analysis.
module Language.K3.Analysis.Effects.Purity where

import Prelude hiding (any, all, concat)

import Control.Arrow

import Data.Foldable
import Data.Functor
import Data.Maybe
import Data.Monoid
import Data.Tree

import qualified Data.Set as S

import Language.K3.Analysis.Effects.Core
import Language.K3.Analysis.Effects.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

pureE :: Annotation Expression
pureE = EProperty "Pure" Nothing

pureD :: Annotation Declaration
pureD = DProperty "Pure" Nothing

isPure :: K3 Expression -> Bool
isPure e = isJust $ e @~ (\case { EProperty "Pure" _ -> True; _ -> False })

guardAddPure :: K3 Expression -> [Annotation Expression] -> [Annotation Expression]
guardAddPure = guardAddPureAll . (:[])

guardAddPureAll :: [K3 Expression] -> [Annotation Expression] -> [Annotation Expression]
guardAddPureAll es as = [pureE | all isPure es && not (null es)] ++ as

runPurity :: K3 Declaration -> K3 Declaration
runPurity = runPurityD

runPurityD :: K3 Declaration -> K3 Declaration
runPurityD (Node (DGlobal i t me :@: as) cs) = Node (DGlobal i t (me') :@: as') cs
  where
    me' = runPurityE <$> me
    as' = if maybe False isPure me' then (pureD:as) else as
runPurityD (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t e' :@: as') cs
  where
    e' = runPurityE e
    as' = if isPure e' then (pureD:as) else as
runPurityD (Node (DRole n :@: as) cs) = Node (DRole n :@: as) (map runPurityD cs)
runPurityD d = d

runPurityE :: K3 Expression -> K3 Expression
runPurityE e@(tag -> EConstant _) = e @+ pureE
runPurityE e@(tag -> EVariable _) = e @+ pureE
runPurityE (Node (ESome :@: as) [c]) = Node (ESome :@: (guardAddPure c' as)) [c'] where c' = runPurityE c
runPurityE (Node (EIndirect :@: as) [c]) = Node (EIndirect :@: (guardAddPure c' as)) [c'] where c' = runPurityE c
runPurityE (Node (ETuple :@: as) cs) = Node (ETuple :@: (guardAddPureAll cs' as)) cs' where cs' = map runPurityE cs
runPurityE (Node (ERecord ids :@: as) cs)
    = Node (ERecord ids :@: (guardAddPureAll cs' as)) cs' where cs' = map runPurityE cs
runPurityE e@(Node (ELambda x :@: as) cs)
    = (Node (ELambda x :@: ([EProperty "Pure" Nothing | isPure] ++ as)) (map runPurityE cs))
  where
    ESymbol (tag -> (Symbol _ (PLambda _ (Node (FScope bindings closure :@: _) effects))))
        = fromJust $ e @~ isESymbol

    isPure = noGlobalReads && noGlobalWrites && noIndirections && readOnlyNonLocalScalars

    nonLocals = let (cRead, cWritten, cApplied)
                        = closure in S.fromList $ bindings ++ concat [cRead, cWritten, cApplied]

    noGlobalReads = not $ any isGlobal $ S.unions $ map readSet effects
    noGlobalWrites = not $ any isGlobal $ S.unions $ map writeSet effects

    noIndirections = not $ any isIndirection nonLocals
    readOnlyNonLocalScalars = all isScalar $ S.intersection nonLocals (S.unions $ map writeSet effects)

    isGlobal :: K3 Symbol -> Bool
    isGlobal (tag -> Symbol _ PGlobal) = True
    isGlobal (tag &&& children -> (Symbol _ (PProject _), ps)) = any isGlobal ps
    isGlobal _ = False

    findSymbolType = fmap getKType . getFirst . flip findSymbolExpr e

    getKType :: K3 Expression -> K3 Type
    getKType e = case e @~ \case { EType _ -> True; _ -> False } of
        Just (EType t) -> t
        _ -> error $ "Absent type at " ++ show e

    findSymbolExpr :: K3 Symbol -> K3 Expression -> First (K3 Expression)
    findSymbolExpr s c = case e @~ (\case { ESymbol s -> True; _ -> False }) of
                         Nothing -> mconcat $ map (findSymbolExpr s) (children e)
                         Just (ESymbol s) -> First $ Just e

    isIndirection :: K3 Symbol -> Bool
    isIndirection (findSymbolType -> Just (tag -> TIndirection)) = True
    isIndirection _ = False

    isScalar :: K3 Symbol -> Bool
    isScalar (findSymbolType -> Just (tag -> TCollection)) = False
    isScalar _ = True

runPurityE (Node (EOperate OApp :@: as) cs)
    = Node (EOperate OApp :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EOperate OSnd :@: as) cs) = Node (EOperate OSnd :@: as) (map runPurityE cs)
runPurityE (Node (EOperate op :@: as) cs) = Node (EOperate op :@: (pureE:as)) (map runPurityE cs)
runPurityE (Node (EProject i :@: as) [c]) = Node (EProject i :@: (guardAddPure c' as)) [c'] where c' = runPurityE c
runPurityE (Node (ELetIn i :@: as) cs)
    = Node (ELetIn i :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EAssign i :@: as) cs) = Node (EAssign i :@: as) (map runPurityE cs)
runPurityE (Node (ECaseOf i :@: as) cs)
    = Node (ECaseOf i :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EBindAs b :@: as) cs)
    = Node (EBindAs b :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EIfThenElse :@: as) cs)
    = Node (EIfThenElse :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EAddress :@: as) cs)
    = Node (EAddress :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (ESelf :@: as) cs)
    = Node (ESelf :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
runPurityE (Node (EImperative e :@: as) cs)
    = Node (EImperative e :@: guardAddPureAll cs' as) cs' where cs' = map runPurityE cs
