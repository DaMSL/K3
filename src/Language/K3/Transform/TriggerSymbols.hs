{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}

module Language.K3.Transform.TriggerSymbols where

import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.Core.Utils

import Language.K3.Core.Constructor.Declaration as DC
import Language.K3.Core.Constructor.Expression  as EC
import Language.K3.Core.Constructor.Type        as TC
import Language.K3.Core.Constructor.Literal     as LC

-- | Trigger id properties are implemented as Left variants of properties to ensure
--   they are not removed on any further AST annotation cleaning.
--   By implementing this as a declaration property, propagation will ensure the
--   property is available at all usage sites (i.e., send expressions)
triggerIdProperty :: String -> Annotation Declaration
triggerIdProperty i = DProperty $ Left ("TriggerId", Just $ LC.string i)

triggerSymbols :: K3 Declaration -> Either String (K3 Declaration)
triggerSymbols prog = do
  ((trigSyms,_), nProg) <- foldProgram declF accIdF accIdF Nothing ([],0) prog
  case nProg of
    Node (DRole _ :@: _) ch -> return $ replaceCh nProg $ mkSyms trigSyms ++ ch
    _ -> Left $ "Invalid top-level role in program during triggerSymbols"

  where declF (acc,i) d@(tag -> DGlobal n (tag -> TSink) _) =
          return ((acc ++ [(n,i)], i+1), d @+ (triggerIdProperty $ symId n))

        declF (acc,i) d@(tag -> DTrigger n _ _) =
          return ((acc ++ [(n,i)], i+1), d @+ (triggerIdProperty $ symId n))

        declF acc d = return (acc, d)

        accIdF acc v = return (acc, v)
        idF        v = return v

        mkSyms trigSyms = map (\(n,i) -> mkSymGlobal n i @+ symAnnot) trigSyms
        mkSymGlobal n i = DC.global (symId n) TC.int $ Just $ EC.constant $ CInt i
        symAnnot = DProperty $ Left ("Pinned", Nothing)
        symId n = "__" ++ n ++ "_tid"
