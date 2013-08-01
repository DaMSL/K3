{-# LANGUAGE ScopedTypeVariables, TemplateHaskell #-}

{-|
  This module contains basic data types and utilities used in typechecking.
-}
module Language.K3.TypeSystem.TypeChecking.Basis
( TypecheckingError(..)
, InternalTypecheckingError(..)
, TypecheckM
, gatherParallelErrors
, typecheckError
, freshTypecheckingVar

, assertExpr0Children
, assertExpr1Children
, assertExpr2Children
, assertExpr3Children
, assertExpr4Children
, assertExpr5Children
, assertExpr6Children
, assertExpr7Children
, assertExpr8Children
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
import Language.Haskell.TH

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Monad.Iface.FreshVar

-- * Types

-- |A data structure representing typechecking errors.
data TypecheckingError
  = InternalError InternalTypecheckingError
      -- ^Represents an internal typechecking error.  These represent bugs in
      --  the K3 software.
  | UnboundIdentifier Identifier Span
      -- ^Indicates that, at the given location, the provided identifier is used
      --  but unbound.
  deriving (Eq, Show)

-- |A data structure representing /internal/ typechecking errors.  These errors
--  should never be seen by a K3 programmer in practice; they should be excluded
--  by the typechecker or by invariants provided by its callers.
data InternalTypecheckingError
  = DeclarationsDerivationTypeMismatch (K3 Declaration)
      -- ^Indicates that a declaration tree was provided to the global
      --  declaration routine which was not a Role node.
  | ExtraDeclarationsInEnvironments (Set TEnvId) (Set TEnvId)
      -- ^Indicates that there were environment identifiers in the checking
      --  environments which did not match any node in the AST provided during
      --  declaration derivation.  The extra identifiers (type and type alias,
      --  in that order) are included.
  | InvalidSpansInExpression (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had
      --  multiple source span annotations.
  | InvalidQualifiersOnExpression (K3 Expression)
      -- ^Indicates that qualifiers appeared on an expression which should not
      --  have been qualified.
  | InvalidExpressionChildCount (K3 Expression)
      -- ^Indicates that type derivation occurred on an expression which had a
      --  number of children inappropriate for its tag.
  deriving (Eq, Show)
  
-- |A type alias for typechecking environments.
type TypecheckM m a = EitherT (Seq TypecheckingError) m a

-- * Utilities

-- |An operation to execute multiple independent typechecking operations in
--  parallel.  If any operation fails, then the errors from *all* failed
--  operations are collected in the result.  Otherwise, the results of the
--  operations are reported as in @mapM@.
gatherParallelErrors :: forall m a. (FreshVarI m)
                     => [TypecheckM m a]
                     -> TypecheckM m [a]
gatherParallelErrors ops = EitherT $ do -- m, not TypecheckM m
  executed <- mapM runEitherT ops
  let (errs,vals) = partitionEithers executed
  return $ if null errs
            then Right vals
            else Left $ foldl (Seq.><) Seq.empty errs
    
-- |Generates an error for a typechecking operation.
typecheckError :: (FreshVarI m) => TypecheckingError -> TypecheckM m a
typecheckError = EitherT . return . Left . Seq.singleton

-- |A routine to obtain a fresh variable for a source position in a typechecking
--  context.
freshTypecheckingVar :: (FreshVarI m)
                     => Span -> TypecheckM m (TVar a)
freshTypecheckingVar s = lift . freshVar =<< return (TVarSourceOrigin s)

-- * Generated routines

-- Defines assertExpr0Children through assertExpr8Children
$(
  let mkAssertExprChildren :: Int -> Q [Dec]
      mkAssertExprChildren n = do
        let fname = mkName $ "assertExpr" ++ show n ++ "Children"
        let ename = mkName "expr"
        let elnames = map (mkName . ("el" ++) . show) [1::Int .. n]
        let tupTyp = foldl appT (tupleT n) $ replicate n $ [t|K3 Expression|]
        let signature = sigD fname $
              [t| (FreshVarI m) => K3 Expression -> TypecheckM m $(tupTyp) |]
        let badMatch = match wildP (normalB
              [| typecheckError $ InternalError $
                    InvalidExpressionChildCount $(varE ename) |]
              ) []
        let goodMatch = match (listP $ map varP elnames) (normalB $
                          appE ([|return|]) $ tupE $ map varE elnames) []
        let bodyExp = caseE ([|subForest $(varE ename)|]) [goodMatch, badMatch]
        let cl = clause [varP ename] (normalB bodyExp) []
        let impl = funD fname [cl]
        sequence [signature,impl]
  in
  concat <$> mapM mkAssertExprChildren [0::Int .. 8]
 )