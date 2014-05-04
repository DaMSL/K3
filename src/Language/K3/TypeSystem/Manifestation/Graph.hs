-- | Dependent type-manifestation using graph traversal.
--
-- This module defines an algorithm and supporting machinery to perform dependent type manifestation
-- on a consistent constraint set obtained from a successful typechecking run.
--
-- The algorithm picks types for type variables whilst respecting constraints between them.
-- Equivalent variables are naturally manifested identically, constrained variables are manifested
-- such that the manifest types still satisfy their constraints.
module Language.K3.TypeSystem.Manifestation.Graph where

import Data.Graph.Wrapper

import qualified Data.Set as S

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.TypeSystem
import Language.K3.TypeSystem.Manifestation.Data

type ManifestGraph = Graph (S.Set UID) (BoundType, K3 Type, K3 Type)

-- | Construct a ManifestGraph from the result of typechecking.
--
-- This involves creating a sanitized version of the constraint set (containing only source
-- variables) and turning constraints into edges.
fromTypecheckResult :: TypecheckResult -> Maybe ManifestGraph
fromTypecheckResult = undefined
