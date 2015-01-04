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
fread (tag -> PTemporary) = fnone
fread p = leaf $ FRead p

fwrite :: K3 Provenance -> K3 Effect
fwrite (tag -> PTemporary) = fnone
fwrite p = leaf $ FWrite p

fio :: K3 Effect
fio = leaf FIO

fdata :: Maybe [Identifier] -> [K3 Effect] -> K3 Effect
fdata idOpt ch = Node (FData idOpt :@: []) ch

fscope :: [FMatVar] -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect
fscope mv prebef bef postbef sf = Node (FScope mv :@: []) [prebef, bef, postbef, sf]

flambda :: Identifier -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect
flambda i cl ef sf = Node (FLambda i :@: [])  [cl, ef, sf]

fapply :: Maybe FMatVar -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect -> K3 Effect
fapply mvOpt lf af ief ef sf = Node (FApply mvOpt :@: []) [lf, af, ief, ef, sf]

fapplyExt :: K3 Effect -> K3 Effect -> K3 Effect
fapplyExt lf af = Node (FApply Nothing :@: []) [lf, af]

simplifyChildren :: (Effect -> Bool) -> [K3 Effect] -> [K3 Effect]
simplifyChildren tagF ch = filter (\p -> tag p /= FNone) $ concatMap flatCh ch
  where flatCh (tnc -> (tagF -> True, gch)) = gch
        flatCh p = [p]

-- Note: we cannot blindly eliminate fnones, otherwise this incorrectly represents scenarios
-- of two paths, only one of which has any concrete effects (e.g., writeback in case-of expressions)
fset :: [K3 Effect] -> K3 Effect
fset ch = Node (FSet :@: []) ch

fseq :: [K3 Effect] -> K3 Effect
fseq ch = mkNode $ simplifyChildren (== FSeq) ch
  where mkNode []  = fnone
        mkNode [x] = x
        mkNode chl = Node (FSeq :@: []) chl

floop :: K3 Effect -> K3 Effect
floop (tag -> FNone) = fnone
floop f = sing f $ FLoop

fnone :: K3 Effect
fnone = leaf FNone
