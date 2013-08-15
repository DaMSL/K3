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

import Control.Applicative
import Control.Monad
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Constructor.Type as TC
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree

-- |A function to derive the type of a qualified expression.
deriveQualifiedExpression ::
      TAliasEnv -- ^The relevant type alias environment.
   -> TNormEnv -- ^The relevant type environment.
   -> K3 Expression
   -> TypecheckM (QVar, ConstraintSet)
deriveQualifiedExpression aEnv env expr = do
  (a,cs) <- deriveExpression aEnv env expr
  let quals = qualifiersOfExpr expr
  if length quals /= 1
    then internalTypeError $ InvalidQualifiersOnExpression expr
    else do
      qa <- freshTypecheckingQVar =<< uidOf expr
      return (qa, cs `csUnion`
                  csFromList [a <: qa, Set.fromList (concat quals) <: qa])

-- |A function to derive the type of an unqualified expression.  An error is
--  raised if any qualifiers appear.
deriveUnqualifiedExpression ::
      TAliasEnv -- ^The relevant type alias environment.
   -> TNormEnv -- ^The relevant type environment.
   -> K3 Expression
   -> TypecheckM (UVar, ConstraintSet)
deriveUnqualifiedExpression aEnv env expr =
  if null $ qualifiersOfExpr expr
    then deriveExpression aEnv env expr
    else typecheckError $ InternalError $ InvalidQualifiersOnExpression expr

-- |A function to derive the type of an expression.  The expression may or may
--  not have outermost qualifiers; this function ignores them.
deriveExpression :: TAliasEnv -- ^The relevant type alias environment.
                 -> TNormEnv -- ^The relevant type environment.
                 -> K3 Expression
                 -> TypecheckM (UVar, ConstraintSet)
deriveExpression aEnv env expr =
  case tag expr of
    EConstant c -> do
      assert0Children expr
      case c of
        CBool _ -> do
          a <- freshTypecheckingUVar =<< uidOf expr
          return (a, csSing $ SBool <: a)
        CInt _ -> do
          a <- freshTypecheckingUVar =<< uidOf expr
          return (a, csSing $ SInt <: a)
        CByte _ ->
          error "No Byte type defined in the specification!" -- TODO
        CReal _ -> do
          a <- freshTypecheckingUVar =<< uidOf expr
          return (a, csSing $ SReal <: a)
        CString _ -> do
          a <- freshTypecheckingUVar =<< uidOf expr
          return (a, csSing $ SString <: a)
        CNone nm -> do
          a <- freshTypecheckingUVar =<< uidOf expr
          qa <- freshTypecheckingQVar =<< uidOf expr
          let qs = case nm of
                      NoneMut -> Set.singleton TMut
                      NoneImmut -> Set.singleton TImmut
          return (a, csFromList [SOption qa <: a, qs <: qa ])
        CEmpty recType -> do
          -- The EAnnotation items on this expression are the annotation
          -- identifiers.
          let getAnnIdent ann = case ann of
                EAnnotation i -> Just i
                _ -> Nothing
          let anns = mapMaybe getAnnIdent $ annotations expr
          let tExpr = foldl (@+) (TC.collection recType) $ map TAnnotation anns
          deriveUnqualifiedTypeExpression aEnv tExpr
    EVariable x -> do
      assert0Children expr
      s <- uidOf expr
      qt <- lookupOrFail $ TEnvIdentifier x
      (qa,cs) <- polyinstantiate s qt
      a <- freshTypecheckingUVar s
      return (a, csSing (qa <: a) `csUnion` cs)
    ESome ->
      commonSingleContainer SOption
    EIndirect ->
      commonSingleContainer SIndirection
    ETuple -> do
      let exprs = subForest expr
      (qas, css) <- unzip <$> mapM (deriveQualifiedExpression aEnv env) exprs
      a <- freshTypecheckingUVar =<< uidOf expr
      return (a, csUnions css `csUnion` csSing (STuple qas <: a))
    ERecord ids -> do
      let exprs = subForest expr
      (qas, css) <- unzip <$> mapM (deriveQualifiedExpression aEnv env) exprs
      unless (length exprs == length ids) $
        typecheckError $ InternalError $ InvalidExpressionChildCount expr
      a <- freshTypecheckingUVar =<< uidOf expr
      return (a, csUnions css `csUnion`
                  csSing (SRecord (Map.fromList $ zip ids qas) <: a))
    ELambda i -> do
      expr' <- assert1Child expr
      qa <- freshTypecheckingQVar =<< uidOf expr
      let env' = Map.insert (TEnvIdentifier i)(QuantType Set.empty qa csEmpty)
                    env
      (a', cs) <- deriveUnqualifiedExpression aEnv env' expr'
      a <- freshTypecheckingUVar =<< uidOf expr
      a'' <- freshTypecheckingUVar =<< uidOf expr
      return (a'', cs `csUnion` csFromList[SFunction a a' <: a'', a <: qa])
    EOperate op -> do
      a <- freshTypecheckingUVar =<< uidOf expr
      case typeOfOp op of
        SomeBinaryOperator binop -> do
          (expr1, expr2) <- assert2Children expr
          (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr1
          (a2,cs2) <- deriveUnqualifiedExpression aEnv env expr2
          let cs' = csSing $ BinaryOperatorConstraint a1 binop a2 a
          return (a, csUnions [cs1, cs2, cs'])
    EProject i -> do
      expr' <- assert1Child expr
      (a', cs) <- deriveUnqualifiedExpression aEnv env expr'
      a <- freshTypecheckingUVar =<< uidOf expr
      qa <- freshTypecheckingQVar =<< uidOf expr
      return (a, cs `csUnion` csFromList [ a' <: SRecord (Map.singleton i qa)
                                         , qa <: a ])
    ELetIn i -> do
      (qexpr',expr') <- assert2Children expr
      (qa,cs) <- deriveQualifiedExpression aEnv env qexpr'
      let qt = generalize env qa cs
      let env' = Map.insert (TEnvIdentifier i) qt env
      (a,cs') <- deriveUnqualifiedExpression aEnv env' expr'
      return (a,cs')
    EAssign i -> do
      expr' <- assert1Child expr
      (a,cs) <- deriveUnqualifiedExpression aEnv env expr'
      (QuantType _ qa _) <- lookupOrFail $ TEnvIdentifier i
      return (a,cs `csUnion` csFromList [a <: qa,
                  MonomorphicQualifiedUpperConstraint qa $ Set.singleton TMut])
    ECaseOf i -> do
      (expr0,expr1,expr2) <- assert3Children expr
      (a0,cs0) <- deriveUnqualifiedExpression aEnv env expr0
      qa <- freshTypecheckingQVar =<< uidOf expr
      let env' = Map.insert (TEnvIdentifier i) (QuantType Set.empty qa csEmpty)
                  env
      (a1,cs1) <- deriveUnqualifiedExpression aEnv env' expr1
      (a2,cs2) <- deriveUnqualifiedExpression aEnv env expr2
      a3 <- freshTypecheckingUVar =<< uidOf expr
      return (a3, csUnions [cs0,cs1,cs2,csFromList
                              [ a0 <: SOption qa, a1 <: a3, a2 <: a3]])
    EBindAs binder -> do
      (expr1,expr2) <- assert2Children expr
      (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr1
      let handleBinder :: TypecheckM (TNormEnv, ConstraintSet)
          handleBinder = case binder of
            BIndirection i -> do
              qa <- freshTypecheckingQVar =<< uidOf expr
              return ( Map.singleton (TEnvIdentifier i) $
                          QuantType Set.empty qa csEmpty
                     , csSing $ a1 <: SIndirection qa )
            BTuple is -> do
              qas <- replicateM (length is) $
                        freshTypecheckingQVar =<< uidOf expr
              return ( Map.fromList $ zip (map TEnvIdentifier is) $
                          map (\qa -> QuantType Set.empty qa csEmpty) qas
                     , csSing $ a1 <: STuple qas )
            BRecord ips -> do
              let (is,i's) = unzip ips
              qas <- replicateM (length ips) $
                        freshTypecheckingQVar =<< uidOf expr
              return ( Map.fromList $ zipWith
                          (\i' qa -> ( TEnvIdentifier i'
                                     , QuantType Set.empty qa csEmpty ))
                          i's qas
                     , csSing $ a1 <: SRecord (Map.fromList $ zip is qas) )
      (env',cs') <- handleBinder
      (a2,cs2) <- deriveUnqualifiedExpression aEnv (envMerge env env') expr2
      return (a2, csUnions [cs1,cs2,cs'])
    EIfThenElse -> do
      (expr1,expr2,expr3) <- assert3Children expr
      [(a1,cs1),(a2,cs2),(a3,cs3)] <-
          mapM (deriveUnqualifiedExpression aEnv env) [expr1,expr2,expr3]
      a4 <- freshTypecheckingUVar =<< uidOf expr
      return (a4, csUnions [ cs1, cs2, cs3, csFromList
                              [ a1 <: SBool, a2 <: a4, a3 <: a4 ] ])
    EAddress -> error "No address expression in specification!" -- TODO
    ESelf -> do
      assert0Children expr
      qt@(QuantType sas qa cs) <- lookupOrFail TEnvIdSelf
      u <- uidOf expr
      unless (Set.null sas) $
        typecheckError $ InternalError $ PolymorphicSelfBinding qt u
      a <- freshTypecheckingUVar u
      return (a, csSing (qa <: a) `csUnion` cs)
  where
    commonSingleContainer constr = do
      expr' <- assert1Child expr
      (qa, cs) <- deriveQualifiedExpression aEnv env expr'
      a <- freshTypecheckingUVar =<< uidOf expr
      return (a, cs `csUnion` csSing (constr qa <: a))
    lookupOrFail :: TEnvId -> TypecheckM NormalQuantType
    lookupOrFail envId =
      envRequireM (UnboundEnvironmentIdentifier <$> uidOf expr
                                                <*> return envId)
        envId env

-- |Obtains the type qualifiers of a given expression.  If no type qualifier
--  tags appear, a type error is generated.  The qualifiers are obtained as a
--  list of lists; each outer list represents the results from a single
--  qualifier annotation while each inner list represents the qualifiers which
--  are derived from that annotation.
qualifiersOfExpr :: K3 Expression -> [[TQual]]
qualifiersOfExpr expr =
  mapMaybe unQual $ annotations expr
  where
    unQual eann = case eann of
      EImmutable -> Just [TImmut]
      EMutable -> Just [TMut]
      _ -> Nothing
