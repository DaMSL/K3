-- | Provenance data type constructors
module Language.K3.Analysis.Provenance.Constructors where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Analysis.Provenance.Core

pfvar :: Identifier -> K3 Provenance
pfvar n = Node (PFVar n :@: []) []

pbvar :: PPtr -> K3 Provenance
pbvar i = Node (PBVar i :@: []) []

pglobal :: Identifier -> Maybe (K3 Provenance) -> K3 Provenance
pglobal i chOpt = Node (PGlobal i :@: []) $ maybe [] (:[]) chOpt

plambda :: Identifier -> Maybe (K3 Provenance) -> K3 Provenance
plambda i chOpt = Node (PLambda i :@: []) $ maybe [] (:[]) chOpt

pderived :: [K3 Provenance] -> K3 Provenance
pderived ch = Node (PDerived :@: []) ch