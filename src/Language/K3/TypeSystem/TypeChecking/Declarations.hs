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
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.Subtyping
import Language.K3.TypeSystem.TypeChecking.Expressions
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Conditional

-- |A function to check whether a given pair of environments correctly describes
--  an AST.  The provided AST must be a Role declaration.
deriveDeclarations :: TAliasEnv -- ^The existing type alias environment.
                   -> TNormEnv -- ^The existing type environment.
                   -> TAliasEnv -- ^The type alias environment to check.
                   -> TNormEnv -- ^The type environment to check.
                   -> K3 Declaration -- ^The AST of global declarations to use
                                     --  in the checking process.
                   -> TypecheckM ()
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
                TopLevelDeclarationNonRole decls
  where
    envKeys m = Set.fromList $ Map.keys m
    idOf decl = case decl of
      DGlobal i _ _ -> (Set.singleton i, Set.empty, Set.empty)
      DRole i -> (Set.empty, Set.singleton i, Set.empty)
      DAnnotation i _ -> (Set.empty, Set.empty, Set.singleton i)
      DTrigger i _ _ -> (Set.singleton i, Set.empty, Set.empty)

-- |A function to check whether a global has the type described in the provided
--  type environments.
deriveDeclaration :: TAliasEnv -- ^The type alias environment in which to check.
                  -> TNormEnv -- ^The type environment in which to check.
                  -> K3 Declaration -- ^The AST of the declaration to check.
                  -> TypecheckM ()
deriveDeclaration aEnv env decl =
  case tag decl of

    DRole _ -> typecheckError $ InternalError $ NonTopLevelDeclarationRole decl

    DGlobal i tExpr mexpr -> do
      assert0Children decl
      u <- uidOf decl
      (qa2,cs2) <- deriveQualifiedTypeExpression aEnv tExpr
      let qt2 = generalize env qa2 cs2
      qt3 <- requireQuantType u i env
      unlessM (qt2 `isSubtypeOf` qt3) $
        typecheckError $ InternalError $
          TypeInEnvironmentDoesNotMatchSignature (TEnvIdentifier i) qt2 qt3
      whenJust mexpr $ \expr -> do
        (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr
        qa1 <- freshTypecheckingQVar u
        let qt1 = generalize env qa1 $ csUnion cs1 $ csSing $ a1 <: qa1
        unlessM (qt1 `isSubtypeOf` qt2) $ typecheckError $
          DeclarationSubtypeFailure u qt1 qt2

    DTrigger i tExpr expr -> do
      u <- uidOf decl
      (a1,cs1) <- deriveUnqualifiedExpression aEnv env expr
      (a2,cs2) <- deriveUnqualifiedTypeExpression aEnv tExpr
      let f a cs = do
                    qa <- freshTypecheckingQVar u
                    a' <- freshTypecheckingUVar u
                    a'' <- freshTypecheckingUVar u
                    return $ generalize env qa $ csUnion cs $ csFromList
                      [ a <: SFunction a' a''
                      , a'' <: STuple []
                      , STrigger a' <: qa]
      qt1 <- f a1 cs1
      qt2 <- f a2 cs2
      qt3 <- requireQuantType u i env
      unlessM (qt2 `isSubtypeOf` qt3) $
        typecheckError $ InternalError $
          TypeInEnvironmentDoesNotMatchSignature (TEnvIdentifier i) qt2 qt3
      unlessM (qt1 `isSubtypeOf` qt2) $ typecheckError $
        DeclarationSubtypeFailure u qt1 qt2

    DAnnotation i mems -> do
      u <- uidOf decl
      entry <- envRequire
                  (UnboundTypeEnvironmentIdentifier u $ TEnvIdentifier i)
                  (TEnvIdentifier i) aEnv
      declared@(AnnType p (AnnBodyType ms1' ms2') _) <- case entry of
                            AnnAlias ann -> return ann
                            QuantAlias _ -> typecheckError $
                              NonAnnotationAlias u $ TEnvIdentifier i
      (t_h,cs_h) <- liftEither (AnnotationDepolarizationFailure u) $
                      depolarize ms2'
      let getTVar :: TEnvId -> TypecheckM UVar
          getTVar ei = envRequire (InternalError $ MissingTypeParameter p ei)
                          ei p
      let singEntry ei a qa = Map.singleton ei $ QuantAlias $
                                QuantType Set.empty qa $ csSing $ a <: qa 
      aEnv' <- mappend
                  <$> (mconcat <$> mapM
                        (\ei -> singEntry ei <$> getTVar ei
                                             <*> freshTypecheckingQVar u)
                        [TEnvIdContent, TEnvIdSelf, TEnvIdFinal] )
                  <*> ((\qa -> Map.singleton TEnvIdHorizon $ QuantAlias $
                                QuantType Set.empty qa $ csSing $ t_h <: qa)
                          <$> freshTypecheckingQVar u)
      let env' = mconcat $ map
                    (\(AnnMemType i' _ qa) -> Map.singleton (TEnvIdentifier i')
                  $ QuantType Set.empty qa csEmpty) ms1'
      let aEnv'' = aEnv `envMerge` aEnv'
      let env'' = env `envMerge` env'
      (bs,css) <- unzip <$> mapM (deriveAnnotationMember aEnv'' env'') mems
      (b'',cs'') <- liftEither (AnnotationConcatenationFailure u) $
                      concatAnnBodies bs
      inst <- instantiateCollection (AnnType p b'' csEmpty) =<<
                envRequire
                  (InternalError $ MissingTypeParameter p TEnvIdContent)
                  TEnvIdContent p
      (a_s,cs_s) <- liftEither (InvalidCollectionInstantiation u) inst 
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
      unlessM (inferred `isAnnotationSubtypeOf` declared) $ 
                typecheckError (AnnotationSubtypeFailure u inferred declared)
  where
    liftEither :: (a -> TypeError) -> Either a b -> TypecheckM b
    liftEither errF val = case val of
      Left err -> typecheckError $ errF err
      Right x -> return x

-- |A function to derive a type for an annotation member.
deriveAnnotationMember :: TAliasEnv -- ^The relevant type alias environment.
                       -> TNormEnv -- ^The relevant type environment.
                       -> AnnMemDecl -- ^The member to typecheck.
                       -> TypecheckM (AnnBodyType, ConstraintSet)
deriveAnnotationMember aEnv env decl =
  case decl of
  
    Lifted pol i tExpr mexpr u ->
      let constr x = AnnBodyType [x] [] in
      deriveMember pol i tExpr mexpr u constr
      
    Attribute pol i tExpr mexpr u ->
      let constr x = AnnBodyType [] [x] in
      deriveMember pol i tExpr mexpr u constr
      
    MAnnotation pol i u -> do
      p <- mconcat <$>
        mapM (\ei -> Map.singleton ei <$> lookupSpecialVar ei)
          [TEnvIdContent, TEnvIdFinal, TEnvIdSelf]
      mann <- envRequire (UnboundTypeEnvironmentIdentifier u $ TEnvIdentifier i)
                (TEnvIdentifier i) aEnv
      ann <- case mann of
                QuantAlias _ -> typecheckError (NonAnnotationAlias u
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
    lookupSpecialVar :: TEnvId -> TypecheckM UVar
    lookupSpecialVar ei = do
      mqt <- envRequire (badFormErr Nothing) ei aEnv
      let badForm = typecheckError $ badFormErr $ Just mqt 
      case mqt of
        QuantAlias (QuantType sas qa cs) -> do
          unless (Set.null sas) badForm
          case csToList cs of
            [QualifiedLowerConstraint (CRight a) qa'] | qa == qa' ->
              return a
            _ -> badForm
        AnnAlias _ -> badForm
      where
        badFormErr :: Maybe NormalTypeAliasEntry -> TypeError
        badFormErr mqt = InternalError $ InvalidSpecialBinding ei mqt

-- |Obtains a quantified type entry from the type environment, generating an
--  error if it cannot be found.
requireQuantType :: UID -> Identifier -> TNormEnv
                 -> TypecheckM NormalQuantType
requireQuantType u i env =
  envRequire (UnboundTypeEnvironmentIdentifier u $ TEnvIdentifier i)
             (TEnvIdentifier i) env
