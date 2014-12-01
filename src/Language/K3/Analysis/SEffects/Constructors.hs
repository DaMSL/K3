{-# LANGUAGE ViewPatterns #-}

-- | Provenance data type constructors
module Language.K3.Analysis.SEffects.Constructors where

import Data.List
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common

import Language.K3.Analysis.Provenance.Core
import Language.K3.Analysis.SEffects.Core

leaf :: Effect -> K3 Effect
leaf p = Node (p :@: []) []

sing :: K3 Effect -> Effect -> K3 Effect
sing ch p = Node (p :@: []) [ch]

ffvar :: Identifier -> K3 Effect
ffvar i = leaf $ FFVar i

fbvar :: FMatVar -> K3 Effect
fbvar mv = leaf $ FBVar mv

fread :: K3 Provenance -> K3 Effect
fread p = leaf $ FRead p

fwrite :: K3 Provenance -> K3 Effect
fwrite p = leaf $ FWrite p

fio :: K3 Effect
fio = leaf FIO

fdata :: Maybe [Identifier] -> [K3 Effect] -> K3 Effect
fdata idOpt ch = Node (FData idOpt :@: []) ch

fscope :: FMatVar -> K3 Effect -> K3 Effect
fscope mv f = sing f $ FScope mv 

flambda :: Identifier -> K3 Effect -> K3 Effect -> K3 Effect
flambda i cl f = Node (FLambda i :@: [])  [cl, f]

fapply :: K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect
fapply l a r = Node (FApply :@: []) [l, a, r]

simplifyChildren :: (Effect -> Bool) -> [K3 Effect] -> [K3 Effect]
simplifyChildren tagF ch = nub $ filter (\p -> tag p /= FNone) $ concatMap flatCh ch
  where flatCh (tnc -> (tagF -> True, gch)) = gch
        flatCh p = [p]

fset :: [K3 Effect] -> K3 Effect
fset ch = mkNode $ simplifyChildren (== FSet) ch
  where mkNode []  = fnone
        mkNode chl = Node (FSet :@: []) chl

fseq :: [K3 Effect] -> K3 Effect
fseq ch = mkNode $ simplifyChildren (== FSeq) ch
  where mkNode []  = fnone
        mkNode chl = Node (FSeq :@: []) chl

floop :: [K3 Effect] -> K3 Effect
floop ch = Node (FLoop :@: []) ch

fnone :: K3 Effect
fnone = leaf FNone
