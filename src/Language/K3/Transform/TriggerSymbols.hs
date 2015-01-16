{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Transform.TriggerSymbols where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Utils

import Language.K3.Core.Constructor.Declaration as DC
import Language.K3.Core.Constructor.Expression  as EC
import Language.K3.Core.Constructor.Type        as TC

triggerSymbols :: K3 Declaration -> Either String (K3 Declaration)
triggerSymbols prog = do
  ((trigSyms,_), _)     <- foldProgram declF accIdF accIdF Nothing ([],0) prog
  nProg                 <- mapProgram idF idF (exprF trigSyms) Nothing prog
  case nProg of
    Node (DRole _ :@: _) ch -> return $ replaceCh nProg $ mkSyms trigSyms ++ ch
    _ -> Left $ "Invalid top-level role in program during triggerSymbols"

  where declF (acc,i) d@(tag -> DTrigger n _ _) = return ((acc ++ [(n,i)], i+1), d)
        declF acc d = return (acc, d)

        accIdF acc v = return (acc, v)
        idF        v = return v

        exprF trigSyms e = foldMapIn1RebuildTree trackLambda trackSideways (rewriteTrigVar trigSyms) [] () e
                             >>= return . snd

        trackLambda   shadowed _ (tag -> ELambda i) = return $ shadowed ++ [i]
        trackLambda   shadowed _ _                  = return shadowed

        trackSideways shadowed _ _ (tag -> ELetIn  i) = return (shadowed, [shadowed ++ [i]])
        trackSideways shadowed _ _ (tag -> EBindAs i) = return (shadowed, [shadowed ++ bindingVariables i])
        trackSideways shadowed _ _ (tag -> ECaseOf i) = return (shadowed, [shadowed ++ [i], shadowed])
        trackSideways shadowed _ _ n                  = return (shadowed, replicate (length (children n) - 1) shadowed)

        rewriteTrigVar trigSyms shadowed _ ch e@(tag -> EVariable i)
          | i `notElem` shadowed = return . ((),) $ case lookup i trigSyms of
                                                      Nothing  -> replaceCh e ch
                                                      Just _   -> EC.variable $ symId i

        rewriteTrigVar _ _ _ ch e = return . ((),) $ replaceCh e ch

        mkSyms trigSyms = map (\(n,i) -> mkSymGlobal n i @+ symAnnot) trigSyms
        mkSymGlobal n i = DC.global (symId n) TC.int $ Just $ EC.constant $ CInt i
        symAnnot = DProperty $ Right ("Pinned", Nothing)
        symId n = "__" ++ n ++ "_tid"
