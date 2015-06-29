{-# LANGUAGE ViewPatterns #-}

-- | Provenance data type constructors
module Language.K3.Analysis.Provenance.Constructors where

import Data.Function
import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Analysis.Core
import Language.K3.Analysis.Provenance.Core

leaf :: Provenance -> K3 Provenance
leaf p = Node (p :@: []) []

sing :: K3 Provenance -> Provenance -> K3 Provenance
sing ch p = Node (p :@: []) [ch]

pfvar :: Identifier -> K3 Provenance
pfvar n = leaf $ PFVar n

pbvar :: PMatVar -> K3 Provenance
pbvar mv = leaf $ PBVar mv

ptemp :: K3 Provenance
ptemp = leaf $ PTemporary

pglobal :: Identifier -> K3 Provenance -> K3 Provenance
pglobal i p = sing p $ PGlobal i

simplifyChildren :: (Provenance -> Bool) -> [K3 Provenance] -> [K3 Provenance]
simplifyChildren tagF ch = nub $ filter (\p -> tag p /= PTemporary) $ concatMap flatCh ch
  where flatCh (tnc -> (tagF -> True, gch)) = gch
        flatCh p = [p]

simplifyChildrenTI :: (Provenance -> Bool) -> [TrIndex] -> [K3 Provenance] -> ([TrIndex], [K3 Provenance])
simplifyChildrenTI tagF tich ch =
	unzip $ nubBy ((==) `on` snd)
		  $ filter (\(_,p) -> tag p /= PTemporary)
		  $ concatMap flatCh $ zip tich ch
  where flatCh (children -> tigch, tnc -> (tagF -> True, gch)) = zip tigch gch
        flatCh (ti,p) = [(ti,p)]


pset :: [K3 Provenance] -> K3 Provenance
pset ch = mkNode $ simplifyChildren (== PSet) ch
  where mkNode []  = ptemp
        mkNode chl = Node (PSet :@: []) chl

psetTI :: Int -> [TrIndex] -> [K3 Provenance] -> (TrIndex, K3 Provenance)
psetTI sz tich ch = mkNode $ simplifyChildrenTI (== PSet) tich ch
  where mkNode (_, []) = (tileaf $ zerobv sz, ptemp)
        mkNode (tichl, chl) = (orti tichl, Node (PSet :@: []) chl)

pchoice :: [K3 Provenance] -> K3 Provenance
pchoice ch = Node (PChoice :@: []) ch

pderived :: [K3 Provenance] -> K3 Provenance
pderived ch = mkNode $ simplifyChildren (== PDerived) ch
  where mkNode []  = ptemp
        mkNode chl = Node (PDerived :@: []) chl

pderivedTI :: Int -> [TrIndex] -> [K3 Provenance] -> (TrIndex, K3 Provenance)
pderivedTI sz tich ch = mkNode $ simplifyChildrenTI (== PDerived) tich ch
  where mkNode (_,[]) = (tileaf $ zerobv sz, ptemp)
        mkNode (tichl, chl) = (orti tichl, Node (PDerived :@: []) chl)

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

papply :: Maybe PMatVar -> K3 Provenance -> K3 Provenance -> K3 Provenance -> K3 Provenance
papply mvOpt f a r = Node (PApply mvOpt :@: []) [f, a, r]

papplyExt :: K3 Provenance -> K3 Provenance -> K3 Provenance
papplyExt f a = Node (PApply Nothing :@: []) [f, a]

pmaterialize :: [PMatVar] -> K3 Provenance -> K3 Provenance
pmaterialize mvl p = Node (PMaterialize mvl :@: []) [p]

pproject :: Identifier -> K3 Provenance -> Maybe (K3 Provenance) -> K3 Provenance
pproject i psrc pvOpt = Node (PProject i :@: []) $ [psrc] ++ maybe [] (:[]) pvOpt

passign :: Identifier -> K3 Provenance -> K3 Provenance
passign i p = sing p $ PAssign i

psend :: K3 Provenance -> K3 Provenance
psend p = sing p $ PSend
