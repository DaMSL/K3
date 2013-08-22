{-|
  This module implements the constraint set consistency checking routines from
  the specification.
-}
module Language.K3.TypeSystem.Consistency
( ConsistencyError(..)
, checkConsistent
, checkClosureConsistent
) where

import Control.Applicative
import Control.Monad
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Closure.BinOp
import Language.K3.TypeSystem.Data
import Language.K3.Utils.Either

data ConsistencyError
  = ImmediateInconsistency ShallowType ShallowType
      -- ^Indicates that two types were immediately inconsistent.
  | UnsatisfiedRecordBound ShallowType ShallowType
      -- ^Indicates that a record type lower bound met a record type upper
      --  bound and was not a correct subtype.
  | IncompatibleTypeQualifiers (Set TQual) (Set TQual)
      -- ^Indicates that a set of type qualifiers was insufficient.
  | IncorrectBinaryOperatorArguments BinaryOperator ShallowType ShallowType
      -- ^Indicates that a binary operator was used with incompatible arguments.
  | BottomLowerBound ShallowType
      -- ^Indicates that a type appeared as a lower bound of bottom.
  | TopUpperBound ShallowType
      -- ^Indicates that a type appeared as an upper bound of top.
  deriving (Show)

type ConsistencyCheck = Either (Seq ConsistencyError) ()

-- TODO: add consistency check for opaque bounds

-- |Determines whether a constraint set is consistent or not.  If it is, an
--  appropriate consistency error is generated.
checkConsistent :: ConstraintSet -> ConsistencyCheck
checkConsistent cs =
  mconcat <$> gatherParallelErrors (map checkSingleInconsistency (csToList cs))
  where
    -- |Determines if a single constraint is inconsistent in and of itself.
    checkSingleInconsistency :: Constraint -> ConsistencyCheck
    checkSingleInconsistency c = case c of
      IntermediateConstraint (CLeft t1) (CLeft t2) -> do
        when (isImmediatelyInconsistent t1 t2) $
          genErr $ ImmediateInconsistency t1 t2
        when (t1 == STop && t2 /= STop) $ genErr $ TopUpperBound t2
        when (t1 /= SBottom && t2 == SBottom) $ genErr $ BottomLowerBound t1
        checkRecordInconsistent t1 t2
      QualifiedIntermediateConstraint (CLeft q1) (CLeft q2) ->
        when (q1 `Set.isProperSubsetOf` q2) $
          genErr $ IncompatibleTypeQualifiers q1 q2
      BinaryOperatorConstraint a1 op a2 _ ->
        mconcat <$> gatherParallelErrors (do
          t1 <- csQuery cs $ QueryTypeByUVarUpperBound a1
          t2 <- csQuery cs $ QueryTypeByUVarUpperBound a2
          return $ when (isNothing $ binOpType op t1 t2) $
            genErr $ IncorrectBinaryOperatorArguments op t1 t2)
      _ -> return ()
    isImmediatelyInconsistent :: ShallowType -> ShallowType -> Bool
    isImmediatelyInconsistent t1 t2 = case (t1,t2) of
      (SFunction _ _, SFunction _ _) -> False
      (STrigger _, STrigger _) -> False
      (SBool,SBool) -> False
      (SInt,SInt) -> False
      (SReal,SReal) -> False
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
    checkRecordInconsistent :: ShallowType -> ShallowType -> ConsistencyCheck
    checkRecordInconsistent t1 t2 = case (t1,t2) of
      (SRecord m1, SRecord m2) ->
        unless (Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1) $
          genErr $ UnsatisfiedRecordBound t1 t2
      _ -> return ()
    genErr :: ConsistencyError -> ConsistencyCheck
    genErr = Left . Seq.singleton

-- |A convenience routine to check if the closure of a constraint set is
--  consistent.
checkClosureConsistent :: ConstraintSet -> ConsistencyCheck
checkClosureConsistent cs = checkConsistent $ calculateClosure cs
