-- Type definitions to avoid circular imports.
module Language.K3.Codegen.CPP.Materialization.Hints where

data Method
  = ConstReferenced
  | Referenced
  | Moved
  | Copied
 deriving (Eq, Ord, Read, Show)

-- Decisions as pertaining to an identifier at a given expression specify how the binding is to be
-- populated (initialized), and how it is to be written back (finalized, if necessary).
data Decision = Decision { inD :: Method, outD :: Method } deriving (Eq, Ord, Read, Show)
