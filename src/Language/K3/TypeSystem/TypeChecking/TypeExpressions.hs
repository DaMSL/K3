{-# LANGUAGE ScopedTypeVariables, TupleSections, FlexibleContexts #-}

{-|
  Contains operations related to typechecking of type expressions.  These
  operations are defined in terms of the @ConstraintSetLike@ typeclass and
  similar abstractions so that they can also be used in the environment decision
  procedure.
-}
module Language.K3.TypeSystem.TypeChecking.TypeExpressions
( deriveUnqualifiedTypeExpression
, deriveQualifiedTypeExpression
) where

import Control.Applicative
import qualified Data.Map as Map
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.TypeSystem.Annotations
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Basis

-- |A function to derive the type of a qualified expression.
deriveQualifiedTypeExpression ::
      forall m el c. ( FreshVarI m, CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c)
   => TAliasEnv -- ^The relevant type alias environment.
   -> K3 Type
   -> TypecheckM m (QVar, c)
deriveQualifiedTypeExpression aEnv tExpr = do
  (a,cs) <- deriveTypeExpression aEnv tExpr
  let quals = qualifiersOfType tExpr
  qa <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
  return (qa, cs `CSL.union` CSL.promote (csFromList [a <: qa, quals <: qa]))

-- |A function to derive the type of an unqualified expression.  An error is
--  raised if any qualifiers appear.
deriveUnqualifiedTypeExpression ::
      forall m el c. ( FreshVarI m, CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c)
   => TAliasEnv -- ^The relevant type alias environment.
   -> K3 Type
   -> TypecheckM m (UVar, c)
deriveUnqualifiedTypeExpression aEnv tExpr =
  if Set.null $ qualifiersOfType tExpr
    then deriveTypeExpression aEnv tExpr
    else typecheckError $ InternalError $ InvalidQualifiersOnType tExpr


-- |A function to derive the type of a type expression.
deriveTypeExpression ::
      forall m el c. ( FreshVarI m, CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c)
   => TAliasEnv -- ^The relevant type alias environment.
   -> K3 Type
   -> TypecheckM m (UVar, c)
deriveTypeExpression aEnv tExpr =
  case tag tExpr of
    TBool -> deriveTypePrimitive SBool
    TByte -> undefined "No Byte type in spec!"
    TInt -> deriveTypePrimitive SInt
    TReal -> deriveTypePrimitive SReal
    TString -> deriveTypePrimitive SString
    TFunction -> do
      (tExpr1, tExpr2) <- assert2Children tExpr
      (a1,cs1) <- deriveUnqualifiedTypeExpression aEnv tExpr1
      (a2,cs2) <- deriveUnqualifiedTypeExpression aEnv tExpr2
      a0 <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a0, CSL.unions [cs1, cs2, CSL.csingleton $ SFunction a1 a2 <: a0])
    TOption -> commonSingleContainer SOption
    TIndirection -> commonSingleContainer SIndirection
    TTuple -> do
      (qas,css) <- unzip <$>
                    mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, CSL.unions css `CSL.union` CSL.csingleton (STuple qas <: a))
    TRecord ids -> do
      (qas,css) <- unzip <$>
                    mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
      a' <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a', CSL.unions css `CSL.union`
                  CSL.csingleton ((SRecord $ Map.fromList $ zip ids qas) <: a'))
    TCollection -> do
      tExpr' <- assert1Children tExpr
      let ais = mapMaybe toAnnotationId $ annotations tExpr
      (a_c,cs_c) <- deriveUnqualifiedTypeExpression aEnv tExpr'
      s <- spanOfTypeExpr tExpr
      namedAnns <- mapM (\i -> aEnvLookup (TEnvIdentifier i) s) ais
      anns <- mapM (uncurry toAnnAlias) namedAnns
      -- Concatenate the annotations
      ann <- either (\err -> typecheckError =<<
                        InvalidAnnotationConcatenation <$>
                          spanOfTypeExpr tExpr <*> return err)
                    return
                  $ concatAnnTypes anns
      einstcol <- instantiateCollection ann a_c
      (a_s,cs_s) <- either (\err -> typecheckError =<<
                        InvalidCollectionInstantiation <$>
                          spanOfTypeExpr tExpr <*> return err)
                    return
                    einstcol
      return (a_s, cs_c `CSL.union` CSL.promote cs_s)
    TAddress -> error "Address type not in specification!" -- TODO
    TSource -> error "Source type is deprecated (should be annotation)!" -- TODO
    TSink -> error "Sink type is deprecated (should be annotation)!" -- TODO
    TTrigger -> do
      tExpr' <- assert1Children tExpr
      (a,cs) <- deriveUnqualifiedTypeExpression aEnv tExpr'
      a' <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a', cs `CSL.union` CSL.csingleton (STrigger a <: a'))
    TBuiltIn b -> do
      assert0Children tExpr
      let ei = case b of
                  TSelf -> TEnvIdSelf
                  TStructure -> TEnvIdFinal
                  THorizon -> TEnvIdHorizon
                  TContent -> TEnvIdContent
      s <- spanOfTypeExpr tExpr
      qt <- uncurry toQuantType =<< aEnvLookup ei s
      (qa,cs) <- polyinstantiate s qt
      a <- freshTypecheckingVar s
      return (a, CSL.promote cs `CSL.union` CSL.csingleton (qa <: a))
  where
    deriveTypePrimitive p = do
      assert0Children tExpr
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, CSL.csingleton $ p <: a)
    commonSingleContainer constr = do
      tExpr' <- assert1Children tExpr
      (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr'
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, cs `CSL.union` CSL.csingleton (constr qa <: a))
    toAnnotationId tann = case tann of
      TAnnotation i -> Just i
      _ -> Nothing
    toAnnAlias :: TEnvId -> TypeAliasEntry -> TypecheckM m AnnType
    toAnnAlias ei entry = case entry of
      AnnAlias ann -> return ann
      _ -> typecheckError =<< NonAnnotationAlias <$> spanOfTypeExpr tExpr
                                                 <*> return ei
    toQuantType :: TEnvId -> TypeAliasEntry -> TypecheckM m QuantType
    toQuantType ei entry = case entry of
      QuantAlias qt -> return qt
      _ -> typecheckError =<< NonQuantAlias <$> spanOfTypeExpr tExpr
                                            <*> return ei
    aEnvLookup :: TEnvId -> Span -> TypecheckM m (TEnvId, TypeAliasEntry)
    aEnvLookup ei s =
      (ei,) <$> envRequire (UnboundTypeEnvironmentIdentifier s ei) ei aEnv

-- |Obtains the type qualifiers of a given expression.
qualifiersOfType :: K3 Type -> Set TQual
qualifiersOfType tExpr = Set.fromList $ mapMaybe unQual $ annotations tExpr
  where
    unQual eann = case eann of
      TImmutable -> Just TImmut
      TMutable -> Just TMut
      _ -> Nothing

-- |Retrieves the span from the provided expression.  If no such span exists,
--  an error is produced.
spanOfTypeExpr :: (FreshVarI m) => K3 Type -> TypecheckM m Span
spanOfTypeExpr tExpr =
  let spans = mapMaybe unSpan $ annotations tExpr in
  if length spans /= 1
    then typecheckError $ InternalError $ InvalidSpansInTypeExpression tExpr
    else return $ head spans
  where
    unSpan eann = case eann of
      TSpan s -> Just s
      _ -> Nothing
