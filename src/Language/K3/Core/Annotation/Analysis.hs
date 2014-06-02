-- | Analysis annotations.
--   This annotation category captures all metadata added
--   to the AST as a result of a program analysis.

module Language.K3.Core.Annotation.Analysis where

import Data.Map(Map)

import Language.K3.Core.Common

-- TODO: move ERead, EWrite, EConflict here.
data AnalysisAnnotation
  = BindAlias Identifier
  | BindFreshAlias Identifier
    -- ^ Annotations to mark expressions that are the target of a bind expression.

  | BindAliasExtension Identifier
    -- ^ Annotation to extend record bind targets for record projection.
  
  | ReadOnlyBind (Identifier, Map Identifier (K3 Expression))
    -- ^ Annotation to mark the ids that are never written to in a bind
    --   variable name, (id name, record member name)
  deriving (Eq, Read, Show)
