module Language.K3.Effects.Constructors where

import Language.K3.Effects.Core

let symbol id prov = Node (Symbol id prov :@: []) []

let read i sym = Node (FRead sym :@: [FId i]) []

let write i sym = Node (FWrite sym :@: [FId i]) []

let scope i sym x = Node (FScope sym :@: [FId i]) [x]

let var i id = Node (FVariable id :@: [FId i]) []

let apply i x y = Node (FApply :@: [FId i]) [x, y]

let lambda i id x = Node (FLambda is :@: [FId i]) [x]

let seq i x y = Node (FSeq :@: [FId i]) [x,y]

let set i xs = Node (FSeq :@: [FId i]) xs

let loop x = Node (FSeq :@: [FId i]) [x]
