module Language.K3.Analysis.Effects.Constructors where

import Data.Tree
import Data.Maybe

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Analysis.Effects.Core

symbol :: Identifier -> Provenance -> Bool -> Bool -> K3 Symbol
symbol i prov hasCopy hasWb = Node (Symbol {symIdent=i, symProv=prov, symHasCopy=hasCopy, symHasWb=hasWb} :@: []) []

lambda :: Identifier -> K3 Effect -> Maybe (K3 Symbol) -> K3 Symbol
lambda i eff subLam = replaceCh (symbol i (PLambda eff) True False) $ maybeToList subLam

symId :: Int -> K3 Symbol
symId i = Node (SymId i :@: []) []

-- Effects --

read :: K3 Symbol -> K3 Effect
read sym = Node (FRead sym :@: []) []

write :: K3 Symbol -> K3 Effect
write sym = Node (FWrite sym :@: []) []

scope :: [K3 Symbol] -> [K3 Effect] -> K3 Effect
scope syms es = Node (FScope syms :@: []) es

apply :: K3 Symbol -> K3 Symbol -> K3 Effect
apply x y = Node (FApply x y :@: []) []

seq :: [K3 Effect] -> K3 Effect
seq xs = Node (FSeq :@: []) xs

set :: [K3 Effect] -> K3 Effect
set xs = Node (FSeq :@: []) xs

loop :: K3 Effect -> K3 Effect
loop x = Node (FLoop :@: []) [x]

fio :: K3 Effect
fio = Node (FIO :@: []) []

effId :: Int -> K3 Effect
effId i = Node (FEffId i :@: []) []
