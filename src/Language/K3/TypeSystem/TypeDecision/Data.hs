{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances #-}
{-|
  A module containing basic data structures for the type decision procedure.
-}
module Language.K3.TypeSystem.TypeDecision.Data
( StubbedConstraintSet(..)
, Stub(..)
) where

import Control.Arrow
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data

-- |A data structure representing a constraint set which includes stubs.
data StubbedConstraintSet = StubbedConstraintSet ConstraintSet (Set Stub)

-- |A representation of a type stub.  These are used to abstractly represent
--  type cycles before they are resolved; the stub stands in for some currently
--  unknown type or type variable.
newtype Stub = Stub Int
  deriving (Eq, Ord, Show)

instance CSL.ConstraintSetLike (Coproduct Stub Constraint) StubbedConstraintSet
    where
  empty = StubbedConstraintSet csEmpty Set.empty
  singleton e = case e of
    CLeft stub -> StubbedConstraintSet csEmpty $ Set.singleton stub
    CRight c -> StubbedConstraintSet (csSing c) Set.empty
  csingleton = CSL.singleton . CRight
  add e c = c `CSL.union` CSL.singleton e
  fromList = CSL.unions . map CSL.singleton
  toList (StubbedConstraintSet cs ss) =
    map CRight (csToList cs) ++ map CLeft (Set.toList ss)
  isSubsetOf (StubbedConstraintSet cs ss) (StubbedConstraintSet cs' ss') =
    cs `csSubset` cs' && ss `Set.isSubsetOf` ss'
  union (StubbedConstraintSet cs ss) (StubbedConstraintSet cs' ss') =
    StubbedConstraintSet (cs `csUnion` cs') (ss `Set.union` ss')
  unions scs = uncurry StubbedConstraintSet $ csUnions *** Set.unions $
                  unzip $ map extract scs
    where extract (StubbedConstraintSet cs ss) = (cs,ss)

instance CSL.ConstraintSetLikePromotable ConstraintSet StubbedConstraintSet
    where
  promote cs = StubbedConstraintSet cs Set.empty

instance Monoid StubbedConstraintSet where
  mempty = CSL.empty
  mappend = CSL.union
  mconcat = CSL.unions
