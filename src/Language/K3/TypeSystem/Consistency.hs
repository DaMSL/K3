{-# LANGUAGE FlexibleInstances #-}
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
import Data.List.Split
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Closure.BinOp
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Either

data ConsistencyError
  = ImmediateInconsistency ShallowType ShallowType
      -- ^Indicates that two types were immediately inconsistent.
  | UnboundedOpaqueVariable OpaqueVar
      -- ^Indicates that no bounds have been defined for an opaque variable
      --  which appears in the constraint set.
  | MultipleOpaqueBounds OpaqueVar [(TypeOrVar,TypeOrVar)]
      -- ^Indicates that an opaque variable exists in the constraint set and is
      --  bounded by multiple opaque bounding constraints.
  | MultipleLowerBoundsForOpaque OpaqueVar [ShallowType]
      -- ^Indicates that an opaque variable had a bounding constraint which used
      --  a variable for the lower bound and that variable had multiple concrete
      --  lower bounds.
  | MultipleUpperBoundsForOpaque OpaqueVar [ShallowType]
      -- ^Indicates that an opaque variable had a bounding constraint which used
      --  a variable for the upper bound and that variable had multiple concrete
      --  upper bounds.
  | UnsatisfiedRecordBound ShallowType ShallowType
      -- ^Indicates that a record type lower bound met a record type upper
      --  bound and was not a correct subtype.
  | UnconcatenatableRecordType
        ShallowType ShallowType OpaqueVar TypeOrVar RecordConcatenationError
      -- ^Indicates that a given type cannot be concatenated as a lower-bounding
      --  record.  The arguments are, in order: the type which cannot be
      --  concatenated; its upper bound; the opaque component whose bound was
      --  being concatenated; that component's bound; and the error from
      --  record concatenation that occurred.
  | ConflictingRecordConcatenation RecordConcatenationError
      -- ^Indicates that a record type was concatenated in such a way that an
      --  overlap occurred.
  | IncompatibleTypeQualifiers (Set TQual) (Set TQual)
      -- ^Indicates that a set of type qualifiers was insufficient.
  | IncorrectBinaryOperatorArguments BinaryOperator ShallowType ShallowType
      -- ^Indicates that a binary operator was used with incompatible arguments.
  | BottomLowerBound ShallowType
      -- ^Indicates that a type appeared as a lower bound of bottom.
  | TopUpperBound ShallowType
      -- ^Indicates that a type appeared as an upper bound of top.
  deriving (Show)

instance Pretty [ConsistencyError] where
  prettyLines ces = ["["] %$ indent 2 (foldl1 (%$) $ map prettyLines ces)
                          %$ ["]"]

instance Pretty ConsistencyError where
  prettyLines ce = case ce of
    ImmediateInconsistency t1 t2 ->
      ["Immediate inconsistency: "] %+
        prettyLines t1 %+ [" <: "] %+ prettyLines t2
    UnsatisfiedRecordBound t1 t2 ->
      ["Unsatisfied record bound: "] %+
        prettyLines t1 %+ [" <: "] %+ prettyLines t2
    BottomLowerBound t ->
      ["Lower bound of bottom: "] %+ prettyLines t
    _ -> splitOn "\n" $ show ce

type ConsistencyCheck = Either (Seq ConsistencyError) ()

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
        checkOpaqueBounds t1
        checkOpaqueBounds t2
        checkRecordInconsistent t1 t2
        checkConcatenationInconsistent t1
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
      (SAddress,SAddress) -> False
      (SOption _,SOption _) -> False
      (SIndirection _,SIndirection _) -> False
      (STuple xs, STuple xs') -> length xs /= length xs'
      (SRecord _ _, SRecord _ _) -> False
      (STop, _) -> False
      (SBottom, _) -> False
      (SOpaque _, _) -> False
      (_, STop) -> False
      (_, SBottom) -> False
      (_, SOpaque _) -> False
      (_, _) -> True -- differently-shaped types!
    checkRecordInconsistent :: ShallowType -> ShallowType -> ConsistencyCheck
    checkRecordInconsistent t1 t2 = case (t1,t2) of
      (SRecord m1 oas1, SRecord m2 oas2) | Set.null oas1 && Set.null oas2 ->
        unless (Map.keysSet m2 `Set.isSubsetOf` Map.keysSet m1) $
          genErr $ UnsatisfiedRecordBound t1 t2
      (SRecord m oas, _) -> mconcat <$> sequence (do
        oa' <- Set.toList oas
        (_, ta_U) <- csQuery cs $ QueryOpaqueBounds oa'
        t_U <- getUpperBoundsOf cs ta_U
        case recordConcat [t_U, SRecord m $ Set.delete oa' oas] of
          Left err -> return $ Left $ Seq.singleton $ UnconcatenatableRecordType
                        t1 t2 oa' ta_U err
          Right _ -> return $ Right ())
      _ -> return ()
    checkConcatenationInconsistent :: ShallowType -> ConsistencyCheck
    checkConcatenationInconsistent t = case t of
      SRecord m oas ->
        mconcat <$> gatherParallelErrors (do
          oa <- Set.toList oas
          (_,ta_U) <- csQuery cs $ QueryOpaqueBounds oa
          t_U <- getUpperBoundsOf cs ta_U
          case recordConcat [SRecord m oas, t_U] of
            Left err -> return $ genErr $ ConflictingRecordConcatenation err
            Right _ -> return $ Right ()
        )
      _ -> return ()
    checkOpaqueBounds :: ShallowType -> ConsistencyCheck
    checkOpaqueBounds t = case t of
      SOpaque oa ->
        let bounds = csQuery cs $ QueryOpaqueBounds oa in
        case bounds of
          [] -> genErr $ UnboundedOpaqueVariable oa
          _:_:_ -> genErr $ MultipleOpaqueBounds oa bounds
          [(ta_L,ta_U)] ->
            let t_Ls = getLowerBoundsOf cs ta_L in
            let t_Us = getUpperBoundsOf cs ta_U in
            case (length t_Ls, length t_Us) of
              (1,1) -> return ()
              (1,_) -> genErr $ MultipleUpperBoundsForOpaque oa t_Us
              (_,_) -> genErr $ MultipleLowerBoundsForOpaque oa t_Ls
      _ -> return ()
    genErr :: ConsistencyError -> ConsistencyCheck
    genErr = Left . Seq.singleton

-- |A convenience routine to check if the closure of a constraint set is
--  consistent.
checkClosureConsistent :: ConstraintSet -> ConsistencyCheck
checkClosureConsistent cs = checkConsistent $ calculateClosure cs
