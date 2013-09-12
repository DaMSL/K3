{-# LANGUAGE TemplateHaskell #-}

{-|
  Contains utilities for manipulating the core K3 data structures.
-}
module Language.K3.Core.Utils
( check0Children
, check1Child
, check2Children
, check3Children
, check4Children
, check5Children
, check6Children
, check7Children
, check8Children
) where

import Control.Applicative
import Data.Tree

import Language.K3.Core.Annotation
import Language.Haskell.TH

-- * Generated routines

-- Defines check0Children through check8Children.  These routines accept a
-- K3 tree and verify that it has a given number of children.  When it does, the
-- result is Just a tuple with that many elements.  When it does not, the result
-- is Nothing.
$(
  let mkCheckChildren :: Int -> Q [Dec]
      mkCheckChildren n = do
        let fname = mkName $ "check" ++ show n ++ "Child" ++
                                (if n /= 1 then "ren" else "")
        ename <- newName "tree"
        elnames <- mapM (newName . ("el" ++) . show) [1::Int .. n]
        typN <- newName "a"
        let typ = varT typN
        let tupTyp = foldl appT (tupleT n) $ replicate n $ [t|K3 $(typ)|]
        ftype <- [t| K3 $(typ) -> Maybe $(tupTyp) |]
        let ftype' = ForallT [PlainTV typN] [] ftype
        let signature = sigD fname $ return ftype'
        let badMatch = match wildP (normalB [| Nothing |]) []
        let goodMatch = match (listP $ map varP elnames) (normalB $
                          appE ([|return|]) $ tupE $ map varE elnames) []
        let bodyExp = caseE ([|subForest $(varE ename)|]) [goodMatch, badMatch]
        let cl = clause [varP ename] (normalB bodyExp) []
        let impl = funD fname [cl]
        sequence [signature,impl]
  in
  concat <$> mapM mkCheckChildren [0::Int .. 8]
 )
