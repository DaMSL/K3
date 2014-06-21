{-# LANGUAGE ViewPatterns #-}

-- Program normalization for effectful control expressions.
module Language.K3.Transform.Normalization where

import Control.Monad.Identity
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Common

normalizeProgram :: K3 Declaration -> K3 Declaration
normalizeProgram p = runIdentity $ mapExpression normalizeExpr p

normalizeExpr :: K3 Expression -> Identity (K3 Expression)
normalizeExpr expr = foldMapRebuildTree normalize [] expr >>= return . uncurry rebuildBlock
  where
    normalize _ ch n@(tag -> EConstant c) = return $ ([], Node (EConstant c :@: annotations n) ch)
    normalize _ ch n@(tag -> EVariable i) = return $ ([], Node (EVariable i :@: annotations n) ch)

    normalize acc ch n@(tag -> EOperate OSeq) = return $ (nEffects, last ch)
      where nEffects = head acc ++ [(head ch, annotations n)] ++ last acc

    normalize acc ch n@(liftAll   -> True) = liftAllEffects  acc ch n
    normalize acc ch n@(liftFirst -> True) = liftFirstEffect acc ch n
    normalize acc ch n@(liftNone  -> True) = liftNoEffects   acc ch n
    normalize _ _ n = error $ "Invalid pattern match in effect normalization: " ++ show n

    -- | Fully chained effect normalization, where all children lift and preserve
    --   the order of their effects.
    liftAll (tag -> ESome)         = True
    liftAll (tag -> EIndirect)     = True
    liftAll (tag -> ETuple)        = True
    liftAll (tag -> ERecord _)     = True
    liftAll (tag -> EAssign _)     = True
    liftAll (tag -> EProject _)    = True
    liftAll (tag -> EAddress)      = True
    liftAll (tag -> ESelf)         = True
    liftAll (tag -> EOperate OSeq) = False
    liftAll (tag -> EOperate _)    = True
    liftAll _ = False

    liftFirst (tag -> ELetIn _)  = True
    liftFirst (tag -> EBindAs _) = True
    liftFirst (tag -> ECaseOf _) = True
    liftFirst _ = False

    liftNone (tag -> ELambda _)   = True
    liftNone (tag -> EIfThenElse) = True
    liftNone _ = False

    liftAllEffects  acc ch n = return $ (concat acc, Node (tag n :@: annotations n) ch)
    liftFirstEffect acc ch n = return $ (head acc, Node (tag n :@: annotations n) $ rebuildTailChildren acc ch)
    liftNoEffects   acc ch n = return $ ([], Node (tag n :@: annotations n) $ map (uncurry rebuildBlock) $ zip acc ch)

    rebuildTailChildren acc ch = (head ch):(map (uncurry rebuildBlock) $ zip (tail acc) $ tail ch)
    rebuildBlock effects e = foldr (\(lE, anns) rE -> Node (EOperate OSeq :@: anns) [lE, rE]) e effects
