module Language.K3.TypeSystem.Simplification.Common
( SimplificationConfig(..)
, SimplificationResult(..)
, VariableSubstitution
, Simplifier
, SimplifyM
, runSimplifyM
) where

import Control.Arrow
import Control.Monad.Reader
import Control.Monad.Writer
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Set (Set)

import Language.K3.TypeSystem.Data

-- |The record used to configure common properties of the simplification
--  routines.  The set of variables @preserveVars@ are variables which must be
--  present by the completion of the simplification process.
data SimplificationConfig
  = SimplificationConfig
      { preserveVars :: Set AnyTVar
      }
      
-- |The data type used to describe the result of simplification.
data SimplificationResult
  = SimplificationResult
      { simplificationSubstitutions :: VariableSubstitution 
      }

instance Monoid SimplificationResult where
  mempty = SimplificationResult
            { simplificationSubstitutions = (Map.empty, Map.empty)
            }
  mappend x y =
    SimplificationResult
      { simplificationSubstitutions =
          let (xq,xu) = simplificationSubstitutions x in
          let (yq,yu) = simplificationSubstitutions y in
          (xq `Map.union` yq, xu `Map.union` yu)
      }

-- TODO: consider moving this to somewhere more general
-- |A type for the description of variable substitution.
type VariableSubstitution = (Map QVar QVar, Map UVar UVar)

-- |A type alias describing a routine which simplifies a constraint set.
type Simplifier = ConstraintSet -> SimplifyM ConstraintSet

-- |The monad under which simplification occurs.
type SimplifyM = WriterT SimplificationResult (Reader SimplificationConfig)

-- |Evaluates a computation which simplifies a value.
runSimplifyM :: SimplificationConfig -> SimplifyM a -> (a, SimplificationResult)
runSimplifyM cfg x = runReader (runWriterT x) cfg
