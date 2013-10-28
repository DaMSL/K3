{-|
  Defines appropriate aliases for type environments.  This module is separate
  from @Language.K3.TypeSystem.Data.Environments.Common@ to avoid cyclic
  references.
-}
module Language.K3.TypeSystem.Data.Environments.Type
( TypeAliasEntry(..)
, NormalTypeAliasEntry
, TQuantEnvValue
, TAliasEnv
, TNormEnv
, TQuantEnv
, TGlobalQuantEnv
) where

import Language.K3.TypeSystem.Data.Constraints
import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Data.Convenience
import Language.K3.TypeSystem.Data.Environments.Common
import Language.K3.TypeSystem.Data.Types
import Language.K3.Utils.Pretty

-- |Type alias environment entries.  The type parameter is passed to the
--  underlying quantified and annotation types.
data TypeAliasEntry c = QuantAlias (QuantType c) | AnnAlias (AnnType c)
  deriving (Eq, Show)

instance (Pretty c) => Pretty (TypeAliasEntry c) where
  prettyLines tae = case tae of
    QuantAlias qt -> prettyLines qt
    AnnAlias ann -> prettyLines ann
  
-- |A type alias for normal alias entries (those which use normal constraint
--  sets).
type NormalTypeAliasEntry = TypeAliasEntry ConstraintSet

-- |A type alias for the kind of polymorphism information stored in a QuantEnv.
type TQuantEnvValue c = (UVar, TypeOrVar, TypeOrVar, c)

-- |An alias for type alias environments.
type TAliasEnv = TEnv NormalTypeAliasEntry
-- |An alias for normal type environments.
type TNormEnv = TEnv NormalQuantType
-- |An alias for quantified (polymorphism) environments.
type TQuantEnv = TEnv (TQuantEnvValue ConstraintSet)
-- |An alias for global quantified (polymorphism) environments.
type TGlobalQuantEnv = TEnv TQuantEnv
