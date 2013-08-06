{-# LANGUAGE ScopedTypeVariables, TupleSections #-}

{-|
  Contains operations related to typechecking of type expressions.
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
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Basis

-- |A function to derive the type of a qualified expression.
deriveQualifiedTypeExpression ::
      forall m. (FreshVarI m)
   => TAliasEnv -- ^The relevant type alias environment.
   -> K3 Type
   -> TypecheckM m (QVar, ConstraintSet)
deriveQualifiedTypeExpression aEnv tExpr = do
  (a,cs) <- deriveTypeExpression aEnv tExpr
  let quals = qualifiersOfType tExpr
  qa <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
  return (qa, cs `csUnion` csFromList [a <: qa, quals <: qa])

-- |A function to derive the type of an unqualified expression.  An error is
--  raised if any qualifiers appear.
deriveUnqualifiedTypeExpression ::
      forall m. (FreshVarI m)
   => TAliasEnv -- ^The relevant type alias environment.
   -> K3 Type
   -> TypecheckM m (UVar, ConstraintSet)
deriveUnqualifiedTypeExpression aEnv tExpr =
  if Set.null $ qualifiersOfType tExpr
    then deriveTypeExpression aEnv tExpr
    else typecheckError $ InternalError $ InvalidQualifiersOnType tExpr


-- |A function to derive the type of a type expression.
deriveTypeExpression :: forall m. (FreshVarI m)
                     => TAliasEnv -- ^The relevant type alias environment.
                     -> K3 Type
                     -> TypecheckM m (UVar, ConstraintSet)
deriveTypeExpression aEnv tExpr =
  case tag tExpr of
    TBool -> deriveTypePrimitive SBool
    TByte -> undefined "No Byte type in spec!"
    TInt -> deriveTypePrimitive SInt
    TReal -> deriveTypePrimitive SReal
    TString -> deriveTypePrimitive SString
    TFunction -> do
      (tExpr1, tExpr2) <- assertTExpr2Children tExpr
      (a1,cs1) <- deriveUnqualifiedTypeExpression aEnv tExpr1
      (a2,cs2) <- deriveUnqualifiedTypeExpression aEnv tExpr2
      a0 <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a0, csUnions [cs1,cs2,csSing $ SFunction a1 a2 <: a0])
    TOption -> commonSingleContainer SOption
    TIndirection -> commonSingleContainer SIndirection
    TTuple -> do
      (qas,css) <- unzip <$>
                    mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, csUnions css `csUnion` csSing (STuple qas <: a))
    TRecord ids -> do
      (qas,css) <- unzip <$>
                    mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
      a' <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a', csUnions css `csUnion`
                  csSing ((SRecord $ Map.fromList $ zip ids qas) <: a'))
    TCollection -> do
      tExpr' <- assertTExpr1Children tExpr
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
      return (a_s, cs_c `csUnion` cs_s)
    TAddress -> error "Address type not in specification!" -- TODO
    TSource -> error "Source type not in specification!" -- TODO
    TSink -> error "Sink type not in specification!" -- TODO
    TTrigger -> error "Trigger type expression not in specification!" -- TODO
    TBuiltIn b -> do
      assertTExpr0Children tExpr
      let ei = case b of
                  TSelf -> TEnvIdSelf
                  TStructure -> TEnvIdFinal
                  THorizon -> TEnvIdHorizon
                  TContent -> TEnvIdContent
      s <- spanOfTypeExpr tExpr
      qt <- uncurry toQuantType =<< aEnvLookup ei s
      (qa,cs) <- polyinstantiate s qt
      a <- freshTypecheckingVar s
      return (a, cs `csUnion` csSing (qa <: a))
  where
    deriveTypePrimitive p = do
      assertTExpr0Children tExpr
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, csSing $ p <: a)
    commonSingleContainer constr = do
      tExpr' <- assertTExpr1Children tExpr
      (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr'
      a <- freshTypecheckingVar =<< spanOfTypeExpr tExpr
      return (a, cs `csUnion` csSing (constr qa <: a))
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
      fromMaybe <$> typecheckError (UnboundTypeEnvironmentIdentifier s ei)
                <*> return ((ei,) <$> Map.lookup ei aEnv)

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
