module Language.K3.TypeSystem.Data.Result where

import Data.Map
import Data.Monoid
import Data.Set

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type

import Language.K3.TypeSystem.Data.Types
import Language.K3.TypeSystem.Data.ConstraintSet
import Language.K3.TypeSystem.Data.Environments.Type

-- |Describes a typechecking result.  Each value in the record is a @Maybe@
--  value so that a partial @TypecheckResult@ can be generated even if an error
--  occurs during typechecking.  @tcAEnv@ is the decided alias environment;
--  @tcEnv@ is the decided type environment.  @tcExprTypes@ is a mapping from
--  subexpression UID to a type and representative constraint set.
data TypecheckResult
  = TypecheckResult
      { tcAEnv :: Maybe TAliasEnv
      , tcEnv :: Maybe TNormEnv
      , tcREnv :: Maybe TGlobalQuantEnv
      , tcExprTypes :: Maybe (Map UID AnyTVar, ConstraintSet)
      , tcExprBounds :: Maybe (Map UID (K3 Type, K3 Type))
      }
  deriving (Eq, Show)

instance Monoid TypecheckResult where
  mempty = TypecheckResult Nothing Nothing Nothing Nothing Nothing
  mappend (TypecheckResult a b c d e) (TypecheckResult a' b' c' d' e') =
    TypecheckResult (a `mappend` a') (b `mappend` b') (c `mappend` c')
      (d `mappend` d') (e `mappend` e')
