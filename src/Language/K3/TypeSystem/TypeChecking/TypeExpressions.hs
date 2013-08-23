{-# LANGUAGE ScopedTypeVariables, TupleSections, FlexibleContexts, ConstraintKinds, TemplateHaskell #-}

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
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Logger
import Language.K3.Pretty
import Language.K3.TemplateHaskell.Transform
import Language.K3.TypeSystem.Annotations
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.Utils.K3Tree

$(loggingFunctions)

-- |A function to derive the type of a qualified expression.
deriveQualifiedTypeExpression ::
      forall m el c. ( Applicative m, Monad m, TypeErrorI m, FreshVarI m
                     , CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , Transform ReplaceVariables c, ConstraintSetType c)
   => TEnv (TypeAliasEntry c) -- ^The relevant type alias environment.
   -> K3 Type
   -> m (QVar, c)
deriveQualifiedTypeExpression aEnv tExpr = do
  (a,cs) <- deriveTypeExpression aEnv tExpr
  let quals = qualifiersOfType tExpr
  if length quals /= 1
    then internalTypeError $ InvalidQualifiersOnType tExpr
    else do
      qa <- freshTypecheckingQVar =<< uidOf tExpr
      return (qa, CSL.unions [cs, a ~= qa, Set.fromList (concat quals) ~= qa])

-- |A function to derive the type of an unqualified expression.  An error is
--  raised if any qualifiers appear.
deriveUnqualifiedTypeExpression ::
      forall m el c. ( Applicative m, Monad m, TypeErrorI m, FreshVarI m
                     , CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , Transform ReplaceVariables c, ConstraintSetType c)
   => TEnv (TypeAliasEntry c) -- ^The relevant type alias environment.
   -> K3 Type
   -> m (UVar, c)
deriveUnqualifiedTypeExpression aEnv tExpr =
  if null $ qualifiersOfType tExpr
    then deriveTypeExpression aEnv tExpr
    else internalTypeError $ InvalidQualifiersOnType tExpr

-- |A function to derive the type of a type expression.
deriveTypeExpression ::
      forall m el c. ( Applicative m, Monad m, TypeErrorI m, FreshVarI m
                     , CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , Transform ReplaceVariables c, ConstraintSetType c)
   => TEnv (TypeAliasEntry c) -- ^The relevant type alias environment.
   -> K3 Type
   -> m (UVar, c)
deriveTypeExpression aEnv tExpr = do
  _debug $ boxToString $ ["Interpreting type expression:"] %+ prettyLines tExpr
  (a, cs) <-
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
          a0 <- freshTypecheckingUVar =<< uidOf tExpr
          return (a0, CSL.unions [cs1, cs2, SFunction a1 a2 ~= a0])
        TOption -> commonSingleContainer SOption
        TIndirection -> commonSingleContainer SIndirection
        TTuple -> do
          (qas,css) <- unzip <$>
                        mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
          a <- freshTypecheckingUVar =<< uidOf tExpr
          return (a, CSL.unions css `CSL.union` (STuple qas ~= a))
        TRecord ids -> do
          (qas,css) <- unzip <$>
                        mapM (deriveQualifiedTypeExpression aEnv) (subForest tExpr)
          a' <- freshTypecheckingUVar =<< uidOf tExpr
          return (a', CSL.unions css `CSL.union`
                      ((SRecord $ Map.fromList $ zip ids qas) ~= a'))
        TCollection -> do
          tExpr' <- assert1Child tExpr
          let ais = mapMaybe toAnnotationId $ annotations tExpr
          (a_c,cs_c) <- deriveUnqualifiedTypeExpression aEnv tExpr'
          u <- uidOf tExpr
          namedAnns <- mapM (\i -> aEnvLookup (TEnvIdentifier i) u) ais
          anns <- mapM (uncurry toAnnAlias) namedAnns
          -- Concatenate the annotations
          ann <- either (\err -> typeError =<<
                            InvalidAnnotationConcatenation <$>
                              uidOf tExpr <*> return err)
                        return
                      $ concatAnnTypes anns
          einstcol <- instantiateCollection ann a_c
          (a_s,cs_s) <- either (\err -> typeError =<<
                            InvalidCollectionInstantiation <$>
                              uidOf tExpr <*> return err)
                        return
                        einstcol
          return (a_s, cs_c `CSL.union` cs_s)
        TAddress -> error "Address type not in specification!" -- TODO
        TSource -> error "Source type is deprecated (should be annotation)!" -- TODO
        TSink -> error "Sink type is deprecated (should be annotation)!" -- TODO
        TTrigger -> do
          tExpr' <- assert1Child tExpr
          (a,cs) <- deriveUnqualifiedTypeExpression aEnv tExpr'
          a' <- freshTypecheckingUVar =<< uidOf tExpr
          return (a', cs `CSL.union` (STrigger a ~= a'))
        TBuiltIn b -> do
          assert0Children tExpr
          let ei = case b of
                      TSelf -> TEnvIdSelf
                      TStructure -> TEnvIdFinal
                      THorizon -> TEnvIdHorizon
                      TContent -> TEnvIdContent
          u <- uidOf tExpr
          qt <- uncurry toQuantType =<< aEnvLookup ei u
          (qa,cs) <- polyinstantiate u qt
          a <- freshTypecheckingUVar u
          return (a, cs `CSL.union` (qa ~= a))
  _debug $ boxToString $
    ["Interpreted type expression:"] %$ indent 2 (
        ["Type expression: "] %+ prettyLines tExpr %$
        ["Interpreted type: "] %+ prettyLines a %+ ["\\"] %+ prettyLines cs
      )
  return (a,cs)
  where
    deriveTypePrimitive p = do
      assert0Children tExpr
      a <- freshTypecheckingUVar =<< uidOf tExpr
      return (a, p ~= a)
    commonSingleContainer constr = do
      tExpr' <- assert1Child tExpr
      (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr'
      a <- freshTypecheckingUVar =<< uidOf tExpr
      return (a, cs `CSL.union` (constr qa ~= a))
    toAnnotationId tann = case tann of
      TAnnotation i -> Just i
      _ -> Nothing
    toAnnAlias :: TEnvId -> TypeAliasEntry c -> m (AnnType c)
    toAnnAlias ei entry = case entry of
      AnnAlias ann -> return ann
      _ -> typeError =<< NonAnnotationAlias <$> uidOf tExpr <*> return ei
    toQuantType :: TEnvId -> TypeAliasEntry c -> m (QuantType c)
    toQuantType ei entry = case entry of
      QuantAlias qt -> return qt
      _ -> typeError =<< NonQuantAlias <$> uidOf tExpr <*> return ei
    aEnvLookup :: TEnvId -> UID -> m (TEnvId, TypeAliasEntry c)
    aEnvLookup ei u =
      (ei,) <$> envRequire (UnboundTypeEnvironmentIdentifier u ei) ei aEnv

-- |Obtains the type qualifiers of a given expression.  Each inner list
--  represents the results for a single annotation.
qualifiersOfType :: K3 Type -> [[TQual]]
qualifiersOfType tExpr = mapMaybe unQual $ annotations tExpr
  where
    unQual eann = case eann of
      TImmutable -> Just [TImmut]
      TMutable -> Just [TMut]
      _ -> Nothing
