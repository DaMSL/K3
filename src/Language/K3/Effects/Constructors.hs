module Language.K3.Effects.Constructors where

import Language.K3.Effects.Core

symbol id prov = Node (Symbol id prov :@: []) []

read i sym = Node (FRead sym :@: [FId i]) []

write i sym = Node (FWrite sym :@: [FId i]) []

scope i sym x = Node (FScope sym :@: [FId i]) [x]

var i id = Node (FVariable id :@: [FId i]) []

apply i x y = Node (FApply :@: [FId i]) [x, y]

lambda i id x = Node (FLambda is :@: [FId i]) [x]

seq i x y = Node (FSeq :@: [FId i]) [x,y]

set i xs = Node (FSeq :@: [FId i]) xs

loop x = Node (FSeq :@: [FId i]) [x]
