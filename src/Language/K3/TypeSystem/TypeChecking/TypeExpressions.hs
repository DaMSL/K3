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
, derivePolymorphicTypeExpression
) where

import Control.Applicative
import Control.Monad
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Utils.Pretty
import Language.K3.TemplateHaskell.Reduce
import Language.K3.TemplateHaskell.Transform
import Language.K3.TypeSystem.Annotations
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Morphisms.ExtractVariables
import Language.K3.TypeSystem.Morphisms.ReplaceVariables
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |A function to derive the type of a polymorphic type expression.
derivePolymorphicTypeExpression ::
      forall m el c. ( Applicative m, Monad m, TypeErrorI m, FreshVarI m
                     , FreshOpaqueI m
                     , CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , Transform ReplaceVariables c
                     , Reduce ExtractVariables c (Set AnyTVar)
                     , ConstraintSetType c)
   => TEnv (TypeAliasEntry c) -- ^The relevant type alias environment.
   -> K3 Type
   -> m (QVar, c, TEnv (TQuantEnvValue c))
derivePolymorphicTypeExpression aEnv tExpr =
  case tag tExpr of
    TForall vdecls -> do
      tExpr' <- assert1Child tExpr
      -- Determine type form of variable declarations
      tvdecls <- mapM typeOfVarDecl vdecls
      -- Allocate variables for each declaration's environment entry
      entryVars <- mconcat <$> mapM (\i -> Map.singleton i <$>
                        (freshTypecheckingUVar =<< uidOf tExpr))
                      (map fst tvdecls)
      -- Extend environment
      aEnv' <- envMerge aEnv <$> mconcat <$>
                  mapM toQuantBinding (Map.toList entryVars)
      -- Now recurse
      (qa, cs) <- deriveQualifiedTypeExpression aEnv' tExpr'
      -- Next, calculate the extension for Q and C
      (cs', qEnv) <- foldM (extendResult entryVars) (cs, Map.empty) tvdecls
      -- And we're done!
      return (qa, cs', qEnv)
    _ -> noPoly
  where
    noPoly = do
      (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr
      return (qa, cs, Map.empty)
    typeOfVarDecl :: TypeVarDecl -> m (Identifier,(UVar,c))
    typeOfVarDecl (TypeVarDecl i mtExpr') =
      case mtExpr' of
        Nothing -> do
          a <- freshTypecheckingUVar =<< uidOf tExpr
          return (i, (a,a ~= STop))
        Just tExpr' ->
          (i,) <$> deriveTypeExpression aEnv tExpr'
    toQuantBinding :: (Identifier, UVar) -> m (TEnv (TypeAliasEntry c))
    toQuantBinding (i,a) = do
      qa <- freshTypecheckingQVar =<< uidOf tExpr
      return $ Map.singleton (TEnvIdentifier i) $ QuantAlias $
        QuantType Set.empty qa $ qa ~= a
    -- |Builds up the C and Q for this rule.
    extendResult :: Map Identifier UVar
                 -> (c, TEnv (TQuantEnvValue c))
                 -> (Identifier,(UVar,c))
                 -> m (c, TEnv (TQuantEnvValue c))
    extendResult entryVars (cs,qEnv) (i,(a',cs')) = do
      let ta_U :: TypeOrVar = CRight a'
      let ta_L :: TypeOrVar = CLeft SBottom
      u <- uidOf tExpr
      oa <- freshOVar (OpaqueSourceOrigin u)
      let a = fromJust $ Map.lookup i entryVars
      let csOpaque = cs' `CSL.union` CSL.promote
                        ((SOpaque oa ~= a) `csUnion`
                          csSing (OpaqueBoundConstraint oa ta_L ta_U))
      return ( CSL.unions [cs, cs',
                              CSL.promote (csFromList [ta_L <: a, a <: ta_U])]
             , Map.insert (TEnvIdentifier i) (a, ta_L, ta_U, csOpaque) qEnv)

-- |A function to derive the type of a qualified expression.
deriveQualifiedTypeExpression ::
      forall m el c. ( Applicative m, Monad m, TypeErrorI m, FreshVarI m
                     , CSL.ConstraintSetLike el c
                     , CSL.ConstraintSetLikePromotable ConstraintSet c
                     , Transform ReplaceVariables c
                     , Reduce ExtractVariables c (Set AnyTVar)
                     , ConstraintSetType c)
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
                     , Transform ReplaceVariables c
                     , Reduce ExtractVariables c (Set AnyTVar)
                     , ConstraintSetType c)
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
                     , Transform ReplaceVariables c
                     , Reduce ExtractVariables c (Set AnyTVar)
                     , ConstraintSetType c)
   => TEnv (TypeAliasEntry c) -- ^The relevant type alias environment.
   -> K3 Type
   -> m (UVar, c)
deriveTypeExpression aEnv tExpr = do
  _debug $ boxToString $ ["Interpreting type expression:"] %+ prettyLines tExpr
  (a, cs) <-
      case tag tExpr of
        TBool -> deriveTypePrimitive SBool
        TByte -> error "No Byte type in spec!"
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
          t <- either (\err -> typeError =<< RecordSignatureError <$>
                                                  uidOf tExpr <*> return err)
                  return
                $ recordConcat (map (\ (i, qa) ->
                    SRecord (Map.singleton i qa) Set.empty) $ zip ids qas)
          return (a', CSL.unions css `CSL.union` (t ~= a'))
        TCollection -> do
          tExpr' <- assert1Child tExpr
          let ais = mapMaybe toAnnotationId $ annotations tExpr
          (a_c',cs_c) <- deriveUnqualifiedTypeExpression aEnv tExpr'
          u <- uidOf tExpr
          namedAnns <- mapM (\i -> aEnvLookup (TEnvIdentifier i) u) ais
          anns <- mapM (uncurry toAnnAlias) namedAnns
          -- Concatenate the annotations
          anns' <- mapM (freshenAnnotation u) anns
          concattedAnns <- concatAnnTypes anns'
          AnnType p (AnnBodyType ms1 ms2) cs <-
            either (\err -> typeError =<<
                        InvalidAnnotationConcatenation <$>
                          uidOf tExpr <*> return err)
              return
              concattedAnns
          (a_c,a_f,a_s) <- readAnnotationSpecialParameters p
          (t_s, cs_s) <- depolarizeOrError u ms1
          (t_f, cs_f) <- depolarizeOrError u ms2
          let cs' = csUnions [a_c ~= a_c', a_f ~= t_f, a_s ~= t_s]
          return (a_s, CSL.unions [cs, cs_c, cs_f, cs_s, CSL.promote cs'])
        TAddress -> error "Address type not in specification!" -- TODO
        TSource -> error "Source type is deprecated (should be annotation)!" -- TODO
        TSink -> error "Sink type is deprecated (should be annotation)!" -- TODO
        TTrigger -> do
          tExpr' <- assert1Child tExpr
          (a,cs) <- deriveUnqualifiedTypeExpression aEnv tExpr'
          a' <- freshTypecheckingUVar =<< uidOf tExpr
          return (a', cs `CSL.union` (STrigger a ~= a'))
        TBuiltIn b ->
          let ei = case b of
                      TSelf -> TEnvIdSelf
                      TStructure -> TEnvIdFinal
                      THorizon -> TEnvIdHorizon
                      TContent -> TEnvIdContent
          in
          environIdType ei
        TForall _ -> error "Forall type is invalid for deriveTypeExpression!"
          -- TODO: possibly make the above error less crash-y?
        TDeclaredVar i ->
          environIdType $ TEnvIdentifier i
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
    environIdType :: TEnvId -> m (UVar, c)
    environIdType ei = do
      assert0Children tExpr
      u <- uidOf tExpr
      qt <- uncurry toQuantType =<< aEnvLookup ei u
      (qa,cs) <- polyinstantiate u qt
      a <- freshTypecheckingUVar u
      return (a, cs `CSL.union` (qa ~= a))

-- |Obtains the type qualifiers of a given expression.  Each inner list
--  represents the results for a single annotation.
qualifiersOfType :: K3 Type -> [[TQual]]
qualifiersOfType tExpr = mapMaybe unQual $ annotations tExpr
  where
    unQual eann = case eann of
      TImmutable -> Just [TImmut]
      TMutable -> Just [TMut]
      _ -> Nothing
