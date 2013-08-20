module Language.K3.TypeSystem.Subtyping.Error
( SubtypeError(..)
, PrimitiveSubtypeError(..)
, InternalPrimitiveSubtypeError(..)
) where

import Data.List.Split
import Data.Sequence (Seq)
import Data.Set (Set)
import Language.K3.Pretty
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Subtyping.ConstraintMap

  -- TODO: modify constraint set closure so that the specific nature of the
  --       inconsistency can be reported!
-- |A data type describing the ways in which a subtyping check may fail.
data SubtypeError
  = InconsistentSubtypeClosure ConstraintSet
      -- ^Indicates that the preliminary closure of the quantified types'
      --  constraint sets was inconsistent.
  | UnprovablePrimitiveSubtype
        ConstraintMap ConstraintMap Constraint (Seq PrimitiveSubtypeError)
      -- ^Indicates that primitive subtyping for a constraint has failed.  All
      --  available primitive subtyping errors are reported.
  deriving (Show)

instance Pretty SubtypeError where
  prettyLines e = case e of
    _ -> splitOn "\n" $ show e

-- |A data type describing the ways in which a primitive subtyping check may
--  fail.
data PrimitiveSubtypeError
  = InconsistentSubtypeTypeDecomposition ShallowType ShallowType
      -- ^Indicates that an immediate inconsistency was discovered while
      --  attempting to decompose types for a subtyping proof.
  | InconsistentSubtypeQualifierDecomposition (Set TQual) (Set TQual)
      -- ^Indicates that an immediate inconsistency was discovered while
      --  attempting to decompose types for a subtyping proof.
  | DisjunctiveSubtypeProofFailure [PrimitiveSubtypeError]
      -- ^Indicates that every attempt to show a given constraint was implied
      --  has failed.  The provided list of errors determines 
  deriving (Show)

-- |A data type describing how primitive subtype errors can fail due to internal
--  inconsistencies.  If these errors occur, they represent a bug in the K3
--  code.
data InternalPrimitiveSubtypeError
  = InternalPrimitiveSubtypeError_TODO -- TODO
  deriving (Show)