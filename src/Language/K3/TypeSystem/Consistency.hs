{-|
  This module implements the consistency checking routines from spec sec 4.4.
-}
module Language.K3.TypeSystem.Consistency
( consistent
, inconsistent
) where

import qualified Data.Map as Map
import Data.Maybe
import qualified Data.Set as Set

import Language.K3.TypeSystem.Closure.BinOp
import Language.K3.TypeSystem.Data

consistent :: ConstraintSet -> Bool
consistent = not . inconsistent

inconsistent :: ConstraintSet -> Bool
inconsistent cs = any isInconsistent (csToList cs)
  where
    isInconsistent :: Constraint -> Bool
    isInconsistent c = case c of
      IntermediateConstraint (Left t1) (Left t2) ->
        immInconsistent t1 t2 ||
        (t1 == STop && t2 /= STop) ||
        (t2 /= SBottom && t2 == SBottom) ||
        recordInconsistent t1 t2
      QualifiedIntermediateConstraint (Left q1) (Left q2) ->
        q1 `Set.isProperSubsetOf` q2
      BinaryOperatorConstraint a1 op a2 _ -> or $ do
        t1 <- csQuery cs $ QueryTypeByUVarUpperBound a1
        t2 <- csQuery cs $ QueryTypeByUVarUpperBound a2
        return $ isNothing $ binOpType op t1 t2
      _ -> False
    immInconsistent :: ShallowType -> ShallowType -> Bool
    immInconsistent t1 t2 = case (t1,t2) of
      (SFunction _ _, SFunction _ _) -> False
      (STrigger _, STrigger _) -> False
      (SBool,SBool) -> False
      (SInt,SInt) -> False
      (SFloat,SFloat) -> False
      (SString,SString) -> False
      (SOption _,SOption _) -> False
      (SIndirection _,SIndirection _) -> False
      (STuple xs, STuple xs') -> length xs /= length xs'
      (SRecord _, SRecord _) -> False
      (STop, _) -> False
      (SBottom, _) -> False
      (_, STop) -> False
      (_, SBottom) -> False
      (_, _) -> True -- differently-shaped types!
    recordInconsistent :: ShallowType -> ShallowType -> Bool
    recordInconsistent t1 t2 = case (t1,t2) of
      (SRecord m1, SRecord m2) ->
        not $ Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1
      _ -> False
