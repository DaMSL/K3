{-# LANGUAGE ScopedTypeVariables #-}

{-|
  A module containing the typechecking routines for expressions.  This module is
  separate in part hide operations which should be exclusively internal to
  expression typechecking.
-}
module Language.K3.TypeSystem.TypeChecking.Expressions
( deriveQualifiedExpression
, deriveUnqualifiedExpression
) where

import Control.Arrow
import Control.Applicative
import Control.Error.Util
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
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Basis
import Language.K3.TypeSystem.Utils

-- |A function to derive the type of a qualified expression.
deriveQualifiedExpression ::
      forall m. (FreshVarI m)
   => TAliasEnv -- ^The relevant type alias environment.
   -> TNormEnv -- ^The relevant type environment.
   -> K3 Expression
   -> TypecheckM m (QVar, ConstraintSet)
deriveQualifiedExpression aEnv env expr = do
  (a,cs) <- deriveExpression aEnv env expr
  let quals = qualifiersOfExpr expr
  qa <- freshTypecheckingVar =<< spanOfExpr expr
  return (qa, cs `csUnion` csFromList [a <: qa, quals <: qa])

-- |A function to derive the type of an unqualified expression.  An error is
--  raised if any qualifiers appear.
deriveUnqualifiedExpression ::
      forall m. (FreshVarI m)
   => TAliasEnv -- ^The relevant type alias environment.
   -> TNormEnv -- ^The relevant type environment.
   -> K3 Expression
   -> TypecheckM m (UVar, ConstraintSet)
deriveUnqualifiedExpression aEnv env expr =
  if Set.null $ qualifiersOfExpr expr
    then deriveExpression aEnv env expr
    else typecheckError $ InternalError $ InvalidQualifiersOnExpression expr

-- |A function to derive the type of an expression.  The expression may or may
--  not have outermost qualifiers; this function ignores them.
deriveExpression :: forall m. (FreshVarI m)
                 => TAliasEnv -- ^The relevant type alias environment.
                 -> TNormEnv -- ^The relevant type environment.
                 -> K3 Expression
                 -> TypecheckM m (UVar, ConstraintSet)
deriveExpression aEnv env expr =
  case tag expr of
    EConstant c -> do
      assertExpr0Children expr
      case c of
        CBool _ -> do
          a :: UVar <- freshTypecheckingVar =<< spanOfExpr expr
          return (a, csSing $ SBool <: a)
        CInt _ -> do
          a :: UVar <- freshTypecheckingVar =<< spanOfExpr expr
          return (a, csSing $ SInt <: a)
        CByte _ ->
          error "No Byte type defined in the specification!" -- TODO
        CReal _ -> do
          a :: UVar <- freshTypecheckingVar =<< spanOfExpr expr
          return (a, csSing $ SFloat <: a) -- TODO: naming? (float vs. real)
        CString _ -> do
          a :: UVar <- freshTypecheckingVar =<< spanOfExpr expr
          return (a, csSing $ SString <: a)
        CNone -> do
          a <- freshTypecheckingVar =<< spanOfExpr expr
          qa <- freshTypecheckingVar =<< spanOfExpr expr
          -- TODO: qualifiersOfExpr is probably wrong here; it would confuse the
          --       qualifier inside of a None with e.g. the qualifiers of the
          --       tuple that contained it
          return (a, csFromList [SOption qa <: a, qualifiersOfExpr expr <: qa ])
        CEmpty _ ->
          error "CEmpty does not match specification!" -- TODO
    EVariable x -> do
      assertExpr0Children expr
      s <- spanOfExpr expr
      qt <- EitherT $ return $
              note (Seq.singleton $ UnboundIdentifier x s) $
                Map.lookup (TEnvIdentifier x) env
      (qa,cs) <- EitherT $ Right <$> polyinstantiate s qt
      a <- freshTypecheckingVar s
      return (a, csSing (qa <: a) `csUnion` cs)
    ESome ->
      commonSingleContainer SOption
    EIndirect ->
      commonSingleContainer SIndirection
    ETuple -> do
      let exprs = subForest expr
      (qas, css) <- unzip <$> mapM (deriveQualifiedExpression aEnv env) exprs
      a <- freshTypecheckingVar =<< spanOfExpr expr
      return (a, csUnions css `csUnion` csSing (STuple qas <: a))
    ERecord ids -> do
      let exprs = subForest expr
      (qas, css) <- unzip <$> mapM (deriveQualifiedExpression aEnv env) exprs
      unless (length exprs == length ids) $
        typecheckError $ InternalError $ InvalidExpressionChildCount expr
      a <- freshTypecheckingVar =<< spanOfExpr expr
      return (a, csUnions css `csUnion`
                  csSing (SRecord (Map.fromList $ zip ids qas) <: a))
    ELambda i -> do
      expr' <- assertExpr1Children expr
      qa :: QVar <- freshTypecheckingVar =<< spanOfExpr expr
      let env' = Map.insert (TEnvIdentifier i)(QuantType Set.empty qa csEmpty)
                    env
      (a', cs) <- deriveUnqualifiedExpression aEnv env' expr'
      a <- freshTypecheckingVar =<< spanOfExpr expr
      a'' <- freshTypecheckingVar =<< spanOfExpr expr
      return (a'', cs `csUnion` csFromList[SFunction a a' <: a'', a <: qa])
    EOperate op -> do
      a <- freshTypecheckingVar =<< spanOfExpr expr
      case typeOfOp op of
        SomeBinaryOperator binop -> do
          (expr1, expr2) <- assertExpr2Children expr
          (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr1
          (a2,cs2) <- deriveUnqualifiedExpression aEnv env expr2
          let cs' = csSing $ BinaryOperatorConstraint a1 binop a2 a
          return (a, csUnions [cs1, cs2, cs'])
    EProject i -> do
      expr' <- assertExpr1Children expr
      (a', cs) <- deriveUnqualifiedExpression aEnv env expr'
      a <- freshTypecheckingVar =<< spanOfExpr expr
      qa <- freshTypecheckingVar =<< spanOfExpr expr
      return (a, cs `csUnion` csFromList [ a' <: SRecord (Map.singleton i qa)
                                         , qa <: a ])
    ELetIn i -> do
      (qexpr',expr') <- assertExpr2Children expr
      (qa,cs) <- deriveQualifiedExpression aEnv env qexpr'
      let qt = generalize env qa cs
      let env' = Map.insert (TEnvIdentifier i) qt env
      (a,cs') <- deriveUnqualifiedExpression aEnv env' expr'
      return (a,cs')
    EAssign _ -> undefined -- TODO: what kind of behavior do we want here?
    ECaseOf i -> do
      (expr0,expr1,expr2) <- assertExpr3Children expr
      (a0,cs0) <- deriveUnqualifiedExpression aEnv env expr0
      qa <- freshTypecheckingVar =<< spanOfExpr expr
      let env' = Map.insert (TEnvIdentifier i) (QuantType Set.empty qa csEmpty)
                  env
      (a1,cs1) <- deriveUnqualifiedExpression aEnv env' expr1
      (a2,cs2) <- deriveUnqualifiedExpression aEnv env expr2
      a3 <- freshTypecheckingVar =<< spanOfExpr expr
      return (a3, csUnions [cs0,cs1,cs2,csFromList
                              [ a0 <: SOption qa, a1 <: a3, a2 <: a3]])
    EBindAs binder -> do
      (expr1,expr2) <- assertExpr2Children expr
      (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr1
      let handleBinder :: TypecheckM m (TNormEnv, ConstraintSet)
          handleBinder = case binder of
            BIndirection i -> do
              qa <- freshTypecheckingVar =<< spanOfExpr expr
              return ( Map.singleton (TEnvIdentifier i) $
                          QuantType Set.empty qa csEmpty
                     , csSing $ a1 <: SIndirection qa )
            BTuple is -> do
              qas <- replicateM (length is) $
                        freshTypecheckingVar =<< spanOfExpr expr
              return ( Map.fromList $ zip (map TEnvIdentifier is) $
                          map (\qa -> QuantType Set.empty qa csEmpty) qas
                     , csSing $ a1 <: STuple qas )
            {- FIXME: the implementation does not agree with the spec here on
                      the potential arity of BRecord
            BRecord (i,i') -> do
              qa <- freshTypecheckingVar =<< spanOfExpr expr
              return ( Map.singleton (TEnvIdentifier i') $
                          QuantType Set.empty qa csEmpty
                     , csSing $ a1 <: SRecord (Map.singleton i qa) )
            -}
      (env',cs') <- handleBinder
      (a2,cs2) <- deriveUnqualifiedExpression aEnv (envMerge env env') expr2
      return (a2, csUnions [cs1,cs2,cs'])
    EIfThenElse -> do
      (expr1,expr2,expr3) <- assertExpr3Children expr
      [(a1,cs1),(a2,cs2),(a3,cs3)] <-
          mapM (deriveUnqualifiedExpression aEnv env) [expr1,expr2,expr3]
      a4 <- freshTypecheckingVar =<< spanOfExpr expr
      return (a4, csUnions [ cs1, cs2, cs3, csFromList
                              [ a1 <: SBool, a2 <: a4, a3 <: a4 ] ])
    EAddress -> error "No address expression in specification!" -- TODO
  where
    commonSingleContainer constr = do
      expr' <- assertExpr1Children expr
      (qa, cs) <- deriveQualifiedExpression aEnv env expr'
      a <- freshTypecheckingVar =<< spanOfExpr expr
      return (a, cs `csUnion` csSing (constr qa <: a))

-- |Obtains the type qualifiers of a given expression.
qualifiersOfExpr :: K3 Expression -> Set TQual
qualifiersOfExpr expr = Set.fromList $ mapMaybe unQual $ annotations expr
  where
    unQual eann = case eann of
      EImmutable -> Just TImmut
      EMutable -> Just TMut
      _ -> Nothing

-- |Retrieves the span from the provided expression.  If no such span exists,
--  an error is produced.
spanOfExpr :: (FreshVarI m) => K3 Expression -> TypecheckM m Span
spanOfExpr expr =
  let spans = mapMaybe unSpan $ annotations expr in
  if length spans /= 1
    then typecheckError $ InternalError $ InvalidSpansInExpression expr
    else return $ head spans
  where
    unSpan eann = case eann of
      ESpan s -> Just s
      _ -> Nothing

