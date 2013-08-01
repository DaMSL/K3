{-# LANGUAGE ScopedTypeVariables #-}

{-|
  Contains operations related to typechecking of type expressions.
-}
module Language.K3.TypeSystem.TypeChecking.TypeExpressions
( deriveTypeExpression
) where

import Control.Arrow
import Control.Applicative
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Either
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.TypeChecking.Basis

-- |A function to derive the type of a type expression.
deriveTypeExpression :: forall m. (FreshVarI m)
                     => TAliasEnv -- ^The relevant type alias environment.
                     -> TNormEnv -- ^The relevant type environment.
                     -> K3 Type
                     -> TypecheckM m (UVar, ConstraintSet)
deriveTypeExpression aEnv env expr =
  undefined -- TODO
