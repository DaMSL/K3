{-# LANGUAGE GADTs #-}

module Language.K3.TypeSystem.Simplification.Common
( SimplificationConfig(..)
, SimplificationResult(..)
, VariableSubstitution
, substitutionLookup
, substitutionLookupAny
, prettySubstitution
, Simplifier
, SimplifyM
, tellSubstitution
, runSimplifyM
, runConfigSimplifyM
) where

import Control.Monad.Reader
import Control.Monad.Writer
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Utils
import Language.K3.Utils.Pretty

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
      { simplificationVarMap :: VariableSubstitution 
      }

instance Monoid SimplificationResult where
  mempty = SimplificationResult
            { simplificationVarMap = (Map.empty, Map.empty)
            }
  mappend x y =
    SimplificationResult
      { simplificationVarMap =
          let (xq,xu) = simplificationVarMap x in
          let (yq,yu) = simplificationVarMap y in
          (xq `Map.union` yq, xu `Map.union` yu)
      }

-- TODO: consider moving this to somewhere more general
-- |A type for the description of variable substitution.
type VariableSubstitution = (Map QVar QVar, Map UVar UVar)

-- |Performs a lookup on a variable substitution.
substitutionLookup :: TVar q -> VariableSubstitution -> TVar q
substitutionLookup var (qvarSubst,uvarSubst) =
  case var of
    a@(UTVar{}) -> doLookup uvarSubst a
    qa@(QTVar{}) -> doLookup qvarSubst qa
  where
    doLookup :: Map (TVar q) (TVar q) -> TVar q -> TVar q
    doLookup m v =
      let f v' = Map.findWithDefault v' v' m in
      leastFixedPoint f v

-- |Performs a lookup on a variable substitution.
substitutionLookupAny :: AnyTVar -> VariableSubstitution -> AnyTVar
substitutionLookupAny sa subst =
  case sa of
    SomeUVar a -> someVar $ substitutionLookup a subst
    SomeQVar qa -> someVar $ substitutionLookup qa subst

-- |Pretty-prints a variable susbtitution pair.
prettySubstitution :: String -> VariableSubstitution -> [String]
prettySubstitution sep (qvarSubsts,uvarSubsts) =
  let prettyElems = map p (Map.toList qvarSubsts) ++
                    map p (Map.toList uvarSubsts) in
  ["{"] %+ sequenceBoxes maxWidth ", " prettyElems %+ ["}"]
  where
    p :: (Pretty k, Pretty v) => (k,v) -> [String]
    p (k,v) = prettyLines k %+ [sep] %+ prettyLines v

-- |A type alias describing a routine which simplifies a constraint set.
type Simplifier = ConstraintSet -> SimplifyM ConstraintSet

-- |The monad under which simplification occurs.
type SimplifyM = WriterT SimplificationResult (Reader SimplificationConfig)

-- |A routine to inform this monad of a substitution.
tellSubstitution :: VariableSubstitution -> SimplifyM ()
tellSubstitution (qvarRepl, uvarRepl) = do
  tell $ SimplificationResult
          { simplificationVarMap = (qvarRepl, uvarRepl)
          }

-- |Evaluates a computation which simplifies a value.  The simplification
--  result will map replaced variables onto the variables which replaced them.
runSimplifyM :: SimplificationConfig -> SimplifyM a -> (a, SimplificationResult)
runSimplifyM cfg x = runReader (runWriterT x) cfg

-- |Evaluates a computation which simplifies a value, producing a writer-monadic
--  result.
runConfigSimplifyM :: SimplificationConfig -> SimplifyM a
                   -> Writer SimplificationResult a
runConfigSimplifyM cfg x =
  let (y,w) = runReader (runWriterT x) cfg in tell w >> return y
