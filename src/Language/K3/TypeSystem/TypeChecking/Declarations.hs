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
import qualified Data.Foldable as Foldable
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Expressions
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.Utils.K3Tree

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
      ids <- (Set.fromList . map TEnvIdentifier) <$> gatherParallelErrors
              (map (deriveDeclaration aEnv'' env'') globals)
      let namedIds =
            Set.fromList (Map.keys aEnv'') `Set.union`
            Set.fromList (Map.keys env'')
      unless (ids == namedIds) $
        typecheckError $ InternalError $ ExtraDeclarationsInEnvironments $
          namedIds Set.\\ ids
    (_, _) -> typecheckError $ InternalError $
                TopLevelDeclarationNonRole decls

-- |A function to check whether a global has the type described in the provided
--  type environments.
deriveDeclaration :: TAliasEnv -- ^The type alias environment in which to check.
                  -> TNormEnv -- ^The type environment in which to check.
                  -> K3 Declaration -- ^The AST of the declaration to check.retr
                  -> TypecheckM Identifier
deriveDeclaration aEnv env decl =
  case tag decl of

    DRole _ -> typecheckError $ InternalError $ NonTopLevelDeclarationRole decl

    DGlobal i _ Nothing -> do
      assert0Children decl
      return i 

    DGlobal i _ (Just expr) ->
      basicDeclaration i expr deriveQualifiedExpression
        $ \qa1 qa2 -> return $ csSing $ qa1 <: qa2

    DTrigger i _ expr ->
      basicDeclaration i expr deriveUnqualifiedExpression
        $ \a1 qa2 -> do
            u <- uidOf decl
            a3 <- freshTypecheckingUVar u
            a4 <- freshTypecheckingUVar u
            return $ csFromList
              [ a1 <: SFunction a3 a4
              , a4 <: STuple []
              , qa2 <: STrigger a3 ]

    DAnnotation i mems -> do
      u <- uidOf decl
      ann@(AnnType p (AnnBodyType ms1 ms2) cs) <-
          aEnvRequireAnn u (TEnvIdentifier i) aEnv
      a_C <- pEnvRequire TEnvIdContent p
      a_F <- pEnvRequire TEnvIdFinal p
      a_S <- pEnvRequire TEnvIdSelf p
      a_H <- freshTypecheckingUVar u
      inst <- instantiateCollection ann a_C
      (t_S,cs_S) <- either (typecheckError . InvalidCollectionInstantiation u)
                        return inst
      (t_H,cs_H) <- either (typecheckError . AnnotationDepolarizationFailure u)
                        return $ depolarize ms2
      aEnv' <- mconcat <$> mapM (\(i,a) ->
                    (\qa -> Map.singleton i $ QuantType Set.empty qa $
                            csFromList [qa <: a, a <: qa]) <$>
                                freshTypecheckingQVar u)
                  [ (TEnvIdContent, a_C), (TEnvIdFinal, a_F)
                  , (TEnvIdHorizon, a_H), (TEnvIdSelf, a_S) ]
      qa_S' <- freshTypecheckingQVar u
      let env' = Map.singleton TEnvIdSelf
                    (QuantType Set.empty qa_S' $ csSing $ a_S <: qa_S')
                 `mappend`
                 mconcat (map (\(AnnMemType i _ qa') ->
                                  Map.singleton (TEnvIdentifier i) $
                                    QuantType Set.empty qa' csEmpty) ms1)
--      let cs' = csUnions
--                  [ cs , cs_S , cs_H , csSing $ a_F <: a_H
--                  , a_H ~= t_H, t_S ~= a_S,  
      undefined -- TODO: annotation typechecking implementation
  where
    basicDeclaration i expr deriv csf = do
      assert0Children decl
      u <- uidOf decl
      (v1,cs1) <- deriv aEnv env expr
      qt2 <- requireQuantType u i env
      (v2,cs2) <- polyinstantiate u qt2
      cs' <- csf v1 v2
      let cs'' = calculateClosure $ csUnions [cs1,cs2,cs']
      either (typecheckError . DeclarationClosureInconsistency i cs''
                                  (someVar v1) (someVar v2) . Foldable.toList)
             return
           $ checkConsistent cs''
      return i

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
      (qa',cs') <- deriveQualifiedExpression aEnv env expr
      return ( constr $ AnnMemType i Positive qa
             , csUnions [cs,cs',csSing $ qa' <: qa] )
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
  envRequire (UnboundEnvironmentIdentifier u $ TEnvIdentifier i)
             (TEnvIdentifier i) env
