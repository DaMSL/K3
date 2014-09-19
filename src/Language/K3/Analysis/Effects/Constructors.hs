module Language.K3.Analysis.Effects.Constructors where

import Data.Tree

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Analysis.Effects.Core

symbol :: Identifier -> Provenance -> K3 Symbol
symbol id prov = Node (Symbol id prov :@: []) []

-- Effects --

read :: K3 Symbol -> K3 Effect
read sym = Node (FRead sym :@: []) []

write :: K3 Symbol -> K3 Effect
write sym = Node (FWrite sym :@: []) []

scope :: [K3 Symbol] -> K3 Effect -> K3 Effect
scope syms x = Node (FScope syms :@: []) [x]

var :: Identifier -> K3 Effect
var id = Node (FVariable id :@: []) []

apply :: K3 Symbol -> K3 Symbol -> K3 Effect
apply x y = Node (FApply x y :@: []) []

seq :: K3 Effect -> K3 Effect -> K3 Effect
seq x y = Node (FSeq :@: []) [x,y]

set :: [K3 Effect] -> K3 Effect
set xs = Node (FSeq :@: []) xs

loop :: K3 Effect -> K3 Effect
loop x = Node (FSeq :@: []) [x]
