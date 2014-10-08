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

runPurity :: K3 Declaration -> K3 Declaration
runPurity = runPurityD

runPurityD :: K3 Declaration -> K3 Declaration
runPurityD (Node (DGlobal i t me :@: as) cs) = Node (DGlobal i t (runPurityE <$> me) :@: as) cs
runPurityD (Node (DTrigger i t e :@: as) cs) = Node (DTrigger i t (runPurityE e) :@: as) cs
runPurityD (Node (DRole n :@: as) cs) = Node (DRole n :@: as) (map runPurityD cs)
runPurityD d = d

runPurityE :: K3 Expression -> K3 Expression
runPurityE e@(Node (ELambda x :@: as) cs) = (if isPure then e @+ (EProperty "Pure" Nothing) else e)
  where
    ESymbol (tag -> (Symbol _ (PLambda _ (Node (FScope [binding] closure :@: _) [effects])))) = fromJust $ e @~ isESymbol

    isPure = noGlobalReads && noGlobalWrites && noIndirections && readOnlyNonLocalScalars

    nonLocals = let (cRead, cWritten, cApplied) = closure in S.fromList $ binding: concat [cRead, cWritten, cApplied]

    noGlobalReads = not $ any isGlobal $ readSet effects
    noGlobalWrites = not $ any isGlobal $ writeSet effects

    noIndirections = not $ any isIndirection nonLocals
    readOnlyNonLocalScalars = all isScalar $ S.intersection nonLocals (writeSet effects)

    isGlobal :: K3 Symbol -> Bool
    isGlobal (tag -> Symbol _ PGlobal) = True
    isGlobal (tag &&& children -> (Symbol _ (PRecord _), ps)) = any isGlobal ps -- TODO: Work out projection vs. bind.
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

runPurityE (Node (t :@: as) cs) = Node (t :@: as) (map runPurityE cs)
