{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, TemplateHaskell, FlexibleContexts, UndecidableInstances #-}
{-|
  A module containing basic data structures for the type decision procedure.
-}
module Language.K3.TypeSystem.TypeDecision.Data
( StubbedConstraintSet(..)
, Stub(..)
, stubsOf
, constraintsOf
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set

import Language.K3.Utils.Pretty
import Language.K3.Utils.TemplateHaskell.Transform
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Within
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Morphisms.ReplaceVariables

-- |A data structure representing a constraint set which includes stubs.
data StubbedConstraintSet = StubbedConstraintSet ConstraintSet (Set Stub)
  deriving (Eq, Ord, Show)

-- |A representation of a type stub.  These are used to abstractly represent
--  type cycles before they are resolved; the stub stands in for some currently
--  unknown type or type variable.
newtype Stub = Stub Int
  deriving (Eq, Ord, Show)
  
instance Pretty Stub where
  prettyLines (Stub n) = ["stub " ++ show n]

-- |Obtains all stubs from a stubbed constraint set.
stubsOf :: StubbedConstraintSet -> Set Stub
stubsOf (StubbedConstraintSet _ x) = x

-- |Obtains all constraints from a stubbed constraint set.
constraintsOf :: StubbedConstraintSet -> ConstraintSet
constraintsOf (StubbedConstraintSet x _) = x

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
  query scs = csQuery (constraintsOf scs)

instance CSL.ConstraintSetLikePromotable ConstraintSet StubbedConstraintSet
    where
  promote cs = StubbedConstraintSet cs Set.empty

instance WithinAlignable (Coproduct Stub Constraint) where
  withinAlign sc sc' =
    case (sc,sc') of
      (CLeft s, CLeft s') -> guard $ s == s'
      (CRight c, CRight c') -> withinAlign c c'
      (CLeft _, CRight _) -> mzero
      (CRight _, CLeft _) -> mzero

instance Monoid StubbedConstraintSet where
  mempty = CSL.empty
  mappend = CSL.union
  mconcat = CSL.unions

instance Pretty StubbedConstraintSet where
  prettyLines scs =
    ["{ "] %+
    (sequenceBoxes (max 1 $ maxWidth - 4) ", " $
      map prettyLines $ CSL.toList scs) %+
    [" }"]

$(concat <$> mapM (defineHomInstance ''ReplaceVariables)
                      [''StubbedConstraintSet, ''Stub])
