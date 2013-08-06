{-# LANGUAGE ScopedTypeVariables #-}

{-|
  A module containing operations which typecheck declarations.
-}
module Language.K3.TypeSystem.TypeChecking.Declarations
( deriveDeclarations
, deriveDeclaration
, deriveAnnotationMember
) where

import Control.Arrow
import Control.Applicative
import Control.Monad
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Basis
import Language.K3.TypeSystem.TypeChecking.Expressions
import Language.K3.TypeSystem.TypeChecking.TypeExpressions

-- |A function to check whether a given pair of environments correctly describes
--  an AST.  The provided AST must be a Role declaration.
deriveDeclarations :: forall m. (FreshVarI m)
                   => TAliasEnv -- ^The existing type alias environment.
                   -> TNormEnv -- ^The existing type environment.
                   -> TAliasEnv -- ^The type alias environment to check.
                   -> TNormEnv -- ^The type environment to check.
                   -> K3 Declaration -- ^The AST of global declarations to use
                                     --  in the checking process.
                   -> TypecheckM m ()
deriveDeclarations aEnv env aEnv' env' decls =
  case tag &&& subForest $ decls of
    (DRole _, globals) -> do
      let aEnv'' = aEnv `envMerge` aEnv' 
      let env'' = env `envMerge` env'
      () <- mconcat <$> gatherParallelErrors
              (map (deriveDeclaration aEnv'' env'') globals)
      let (gIds, _, aIds) = mconcat $ map (idOf . tag) globals
      let xgIds = Set.map TEnvIdentifier gIds `Set.difference` envKeys env'
      let xaIds = Set.map TEnvIdentifier aIds `Set.difference` envKeys aEnv'
      unless (Set.null xgIds && Set.null xaIds) $
        typecheckError $ InternalError $
          ExtraDeclarationsInEnvironments xgIds xaIds 
    (_, _) -> typecheckError $ InternalError $
                DeclarationsDerivationTypeMismatch decls
  where
    envKeys m = Set.fromList $ Map.keys m
    idOf decl = case decl of
      DGlobal i _ _ -> (Set.singleton i, Set.empty, Set.empty)
      DRole i -> (Set.empty, Set.singleton i, Set.empty)
      DAnnotation i _ -> (Set.empty, Set.empty, Set.singleton i)

-- |A function to check whether a global has the type described in the provided
--  type environments.
deriveDeclaration :: forall m. (FreshVarI m)
                  => TAliasEnv -- ^The type alias environment in which to check.
                  -> TNormEnv -- ^The type environment in which to check.
                  -> K3 Declaration -- ^The AST of the declaration to check.
                  -> TypecheckM m ()
deriveDeclaration aEnv env decl =
  case tag decl of
    DRole _ -> typecheckError $ InternalError $ NestedRole decl
    DGlobal i tExpr mexpr -> do
      (qa2,cs2) <- deriveQualifiedTypeExpression aEnv tExpr
      let qt2 = generalize env qa2 cs2
      -- TODO: the environment entry should be a *subtype* of qt2, not equal;
      --       equal is more work to check because it really means "equivalent
      --       w.r.t. alpha renaming of variables"
      undefined -- TODO
    DAnnotation i mems -> do
      s <- spanOfDecl decl
      entry <- fromMaybe
                 <$> (typecheckError $
                        UnboundTypeEnvironmentIdentifier s $ TEnvIdentifier i) 
                 <*> return (Map.lookup (TEnvIdentifier i) aEnv)
      declared@(AnnType p (AnnBodyType ms1' ms2') _) <- case entry of
                            AnnAlias ann -> return ann
                            QuantAlias _ -> typecheckError $
                              NonAnnotationAlias s $ TEnvIdentifier i
      (t_h,cs_h) <- either
                      (typecheckError . AnnotationDepolarizationFailure s)
                      return
                    $ depolarize ms2'
      let getTVar :: TEnvId -> TypecheckM m UVar
          getTVar ei = fromMaybe <$> typecheckError (InternalError $
                                                      MissingTypeParameter p ei)
                                 <*> return (Map.lookup ei p)
      let singEntry ei a qa = Map.singleton ei $ QuantAlias $
                                QuantType Set.empty qa $ csSing $ a <: qa 
      aEnv' <- mappend
                  <$> (mconcat <$> mapM
                        (\ei -> singEntry ei <$> getTVar ei
                                             <*> freshTypecheckingVar s)
                        [TEnvIdContent, TEnvIdSelf, TEnvIdFinal] )
                  <*> ((\qa -> Map.singleton TEnvIdHorizon $ QuantAlias $
                                QuantType Set.empty qa $ csSing $ t_h <: qa)
                          <$> freshTypecheckingVar s)
      let env' = mconcat $ map
                    (\(AnnMemType i' _ qa) -> Map.singleton (TEnvIdentifier i')
                  $ QuantType Set.empty qa csEmpty) ms1'
      let (aEnv'',env'') = (aEnv `envMerge` aEnv', env `envMerge` env')
      (bs,css) <- unzip <$> mapM (deriveAnnotationMember aEnv'' env'') mems
      (b'',cs'') <- either
                        (typecheckError . AnnotationConcatenationFailure s)
                        return
                      $ concatAnnBodies bs
      -- TODO: update spec to reflect the following call
      inst <- instantiateCollection (AnnType p b'' csEmpty) =<<
                fromMaybe <$> typecheckError (InternalError $
                                MissingTypeParameter p TEnvIdContent)
                          <*> return (Map.lookup TEnvIdContent p)
      (a_s,cs_s) <- either
                      (typecheckError . InvalidCollectionInstantiation s)
                      return
                      inst
      aP_f <- getTVar TEnvIdFinal
      aP_h <- getTVar TEnvIdHorizon
      aP_s <- getTVar TEnvIdSelf
      let inferred = AnnType p b'' $ csUnions
                      [ cs''
                      , cs_h
                      , cs_s
                      , csFromList [aP_f <: t_h, t_h <: aP_h, a_s <: aP_s]
                      , csUnions css
                      ]
      join $ unless <$> (inferred `isAnnotationSubtypeOf` declared) <*> 
                typecheckError (AnnotationSubtypeFailure s inferred declared)

-- |A function to derive a type for an annotation member.
deriveAnnotationMember :: forall m. (FreshVarI m)
                       => TAliasEnv -- ^The relevant type alias environment.
                       -> TNormEnv -- ^The relevant type environment.
                       -> AnnMemDecl -- ^The member to typecheck.
                       -> TypecheckM m (AnnBodyType, ConstraintSet)
deriveAnnotationMember aEnv env decl =
  case decl of
    Lifted pol i tExpr mexpr s ->
      let constr x = AnnBodyType [x] [] in
      deriveMember pol i tExpr mexpr s constr
    Attribute pol i tExpr mexpr s ->
      let constr x = AnnBodyType [] [x] in
      deriveMember pol i tExpr mexpr s constr
    MAnnotation pol i s -> do
      p <- mconcat <$>
        mapM (\ei -> Map.singleton ei <$> lookupSpecialVar ei)
          [TEnvIdContent, TEnvIdFinal, TEnvIdSelf]
      mann <- fromMaybe <$> typecheckError (UnboundTypeEnvironmentIdentifier s
                                              $ TEnvIdentifier i)
                        <*> return (Map.lookup (TEnvIdentifier i) aEnv)
      ann <- case mann of
                QuantAlias _ -> typecheckError (NonAnnotationAlias s
                                                  $ TEnvIdentifier i)
                AnnAlias ann -> return ann
      let (AnnType p' b cs) = instantiateAnnotation p ann
      unless (Map.null p') $
        typecheckError $ InternalError $ UnresolvedTypeParameters p'
      let b' = case pol of
                Provides -> b
                Requires ->
                  let AnnBodyType m1 m2 = b in
                  let negatize (AnnMemType i' _ qa) =
                        AnnMemType i' Negative qa in
                  AnnBodyType (map negatize m1) (map negatize m2)
      return (b', cs)
  where
    deriveMember pol = case pol of
                          Provides -> derivePositiveMember
                          Requires -> deriveNegativeMember
    derivePositiveMember i tExpr mexpr s constr = do
      (qa,cs) <- deriveQualifiedTypeExpression aEnv tExpr
      expr <- fromMaybe <$> typecheckError
                            (NoInitializerForPositiveAnnotationMember s)
                        <*> return mexpr
      (a',cs') <- deriveUnqualifiedExpression aEnv env expr
      return ( constr $ AnnMemType i Positive qa
             , csUnions [cs,cs',csSing $ a' <: qa] )
    deriveNegativeMember i tExpr mexpr s constr = do
      (qa,cs)<- deriveQualifiedTypeExpression aEnv tExpr
      unless (isNothing mexpr) $
        typecheckError $ InitializerForNegativeAnnotationMember s
      return ( constr $ AnnMemType i Negative qa, cs )
    lookupSpecialVar :: TEnvId -> TypecheckM m UVar
    lookupSpecialVar ei = do
      mqt <- fromMaybe <$> badForm Nothing
                       <*> return (Map.lookup ei aEnv)
      case mqt of
        QuantAlias (QuantType sas qa cs) -> do
          unless (Set.null sas) $ badForm $ Just mqt
          case csToList cs of
            [QualifiedLowerConstraint (CRight a) qa'] | qa == qa' ->
              return a
            _ -> badForm $ Just mqt
        AnnAlias _ -> badForm $ Just mqt
      where
        badForm mqt = typecheckError $ InternalError $
                        InvalidSpecialBinding ei mqt

-- |Retrieves the span from the provided expression.  If no such span exists,
--  an error is produced.
spanOfDecl :: (FreshVarI m) => K3 Declaration -> TypecheckM m Span
spanOfDecl decl =
  let spans = mapMaybe unSpan $ annotations decl in
  if length spans /= 1
    then typecheckError $ InternalError $ InvalidSpansInDeclaration decl
    else return $ head spans
  where
    unSpan dann = case dann of
      DSpan s -> Just s
      _ -> Nothing
