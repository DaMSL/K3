-- | Annotations for syntax printing.
--   This represents the metadata needed at AST nodes for correct
--   reconstruction of the user-specified source program.

module Language.K3.Core.Annotation.Syntax where

import Language.K3.Core.Common

data SyntaxAnnotation
  = EndpointDeclaration EndpointSpec EndpointBindings
  | SourceComment Bool Span String
      -- ^ Comment annotations, as multiline, with span and contents.
  | LexicalName Identifier
      -- ^ Alpha-renaming annotations, tracking original variable name.
  deriving (Eq, Read, Show)

-- | A list of triggers ids fed by an endpoint. Only valid for sources.
type EndpointBindings = [Identifier]