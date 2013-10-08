-- | Annotations for syntax printing.
--   This represents the metadata needed at AST nodes for correct
--   reconstruction of the user-specified source program.

module Language.K3.Core.Annotation.Syntax where

import Language.K3.Core.Common

data SyntaxAnnotation
  = EndpointDeclaration EndpointSpec EndpointBindings
  | SourceComment Span String
      -- ^ Placeholder for storing comments in the AST.
      --   TODO: this is currently unused.
  deriving (Eq, Read, Show)

-- | A list of triggers ids fed by an endpoint. Only valid for sources.
type EndpointBindings = [Identifier]