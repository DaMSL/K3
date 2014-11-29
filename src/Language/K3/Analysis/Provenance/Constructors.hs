{-# LANGUAGE ViewPatterns #-}

-- | Provenance data type constructors
module Language.K3.Analysis.Provenance.Constructors where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Analysis.Provenance.Core

leaf :: Provenance -> K3 Provenance
leaf p = Node (p :@: []) []

sing :: K3 Provenance -> Provenance -> K3 Provenance
sing ch p = Node (p :@: []) [ch]

pfvar :: Identifier -> K3 Provenance
pfvar n = leaf $ PFVar n

pbvar :: VarLoc -> PPtr -> K3 Provenance
pbvar vl i = leaf $ PBVar vl i

ptemp :: K3 Provenance
ptemp = leaf $ PTemporary

pglobal :: Identifier -> K3 Provenance -> K3 Provenance
pglobal i p = sing p $ PGlobal i

pset :: [K3 Provenance] -> K3 Provenance
pset ch = Node (PSet :@: []) $ nub $ concatMap flatCh ch
  where flatCh (tnc -> (PSet, gch)) = gch
        flatCh p = [p]

pchoice :: [K3 Provenance] -> K3 Provenance
pchoice ch = Node (PChoice :@: []) ch

pderived :: [K3 Provenance] -> K3 Provenance
pderived ch = Node (PDerived :@: []) $ nub $ concatMap flatCh ch
  where flatCh (tnc -> (PDerived, gch)) = gch
        flatCh p = [p]

pdata :: Maybe [Identifier] -> [K3 Provenance] -> K3 Provenance
pdata idsOpt ch = Node (PData idsOpt :@: []) ch

precord :: Identifier -> K3 Provenance -> K3 Provenance
precord i p = sing p $ PRecord i

ptuple :: Int -> K3 Provenance -> K3 Provenance
ptuple i p = sing p $ PTuple i

pindirect :: K3 Provenance -> K3 Provenance
pindirect p = sing p $ PIndirection

poption :: K3 Provenance -> K3 Provenance
poption p = sing p $ POption

plambda :: Identifier -> K3 Provenance -> K3 Provenance
plambda i p = sing p $ PLambda i

pclosure :: K3 Provenance -> K3 Provenance
pclosure v = sing v $ PClosure

papply :: K3 Provenance -> K3 Provenance -> K3 Provenance -> K3 Provenance
papply f a r = Node (PApply :@: []) [f, a, r]

pproject :: Identifier -> K3 Provenance -> Maybe (K3 Provenance) -> K3 Provenance
pproject i psrc pvOpt = Node (PProject i :@: []) $ [psrc] ++ maybe [] (:[]) pvOpt

passign :: Identifier -> K3 Provenance -> K3 Provenance
passign i p = sing p $ PAssign i

psend :: K3 Provenance -> K3 Provenance
psend p = sing p $ PSend
