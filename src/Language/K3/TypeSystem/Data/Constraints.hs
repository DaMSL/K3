{-|
  A module defining the types of constraints appearing in the K3 system.
-}
module Language.K3.TypeSystem.Data.Constraints
( Constraint(..)
, TypeOrVar
, QualOrVar
, UVarBound
) where

import Data.Set (Set)

import Language.K3.TypeSystem.Data.Coproduct
import Language.K3.TypeSystem.Data.Types
import Language.K3.Utils.Pretty

-- |A type alias describing a type or a variable.
type TypeOrVar = Coproduct ShallowType UVar
-- |A type alias describing a type qualifier or qualified variable.
type QualOrVar = Coproduct (Set TQual) QVar
-- |A type alias describing a type or any kind of variable.
type UVarBound = Coproduct TypeOrVar QVar

-- |A data type to describe constraints.
data Constraint
  = IntermediateConstraint TypeOrVar TypeOrVar
  | QualifiedLowerConstraint TypeOrVar QVar
  | QualifiedUpperConstraint QVar TypeOrVar
  | QualifiedIntermediateConstraint QualOrVar QualOrVar
  | MonomorphicQualifiedUpperConstraint QVar (Set TQual)
  | PolyinstantiationLineageConstraint QVar QVar
  | OpaqueBoundConstraint OpaqueVar TypeOrVar TypeOrVar
  deriving (Eq, Ord, Show)
  
instance Pretty Constraint where
  prettyLines c =
    case c of
      IntermediateConstraint ta1 ta2 -> binPretty ta1 ta2
      QualifiedLowerConstraint ta qa -> binPretty ta qa
      QualifiedUpperConstraint qa ta -> binPretty qa ta
      QualifiedIntermediateConstraint qv1 qv2 -> binPretty qv1 qv2
      MonomorphicQualifiedUpperConstraint qa qs ->
        prettyLines qa %+ ["<:"] %+ prettyLines qs
      PolyinstantiationLineageConstraint qa1 qa2 ->
        prettyLines qa1 %+ ["<<:"] %+ prettyLines qa2
      OpaqueBoundConstraint oa lb ub ->
        ["("] %+ prettyLines lb %+ ["≤"] %+ prettyLines oa %+ ["≤"] %+
          prettyLines ub %+ [")"]
    where
      binPretty x y = prettyLines x %+ ["<:"] %+ prettyLines y
  
