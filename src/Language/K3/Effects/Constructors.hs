module Language.K3.Effects.Constructors where

import Language.K3.Effects.Core

symbol :: Identifier -> Provenance -> K3 Symbol
symbol id prov = Node (Symbol id prov :@: []) []

read :: K3 Symbol -> K3 Effect
read sym = Node (FRead sym) []

write :: K3 Symbol -> K3 Effect
write sym = Node (FWrite sym :@: []) []

scope :: K3 Symbol -> K3 Effect -> K3 Effect
scope sym x = Node (FScope sym :@: []) [x]

var :: Identifier -> K3 Effect
var id = Node (FVariable id :@: []) []

subst :: K3 Effect -> K3 Effect -> K3 Effect
subst x y = Node (FSubst :@: []) [x, y]

lambda :: Identifier -> K3 Effect
lambda id x = Node (FLambda is :@: []) [x]

seq :: K3 Effect -> K3 Effect -> K3 Effect
seq x y = Node (FSeq :@: []) [x,y]

set :: [K3 Effect] -> K3 Effect
set xs = Node (FSeq :@: []) xs

loop :: K3 Effect -> K3 Effect
loop x = Node (FSeq :@: []) [x]
