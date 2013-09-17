{-# LANGUAGE ScopedTypeVariables, TemplateHaskell #-}

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
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Traversable as Trav
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Logger
import Language.K3.Pretty
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Expressions
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Conditional

$(loggingFunctions)

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
            Set.fromList (Map.keys aEnv'') `Set.union` -- TODO: disjoint union
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

    DAnnotation iAnn mems -> do
      _debug $ boxToString $ ["Checking type for annotation "++iAnn++":"] %$
                             indent 2 (prettyLines decl)
      u <- uidOf decl
      ann@(AnnType p (AnnBodyType ms1 ms2) cs) <-
          aEnvRequireAnn u (TEnvIdentifier iAnn) aEnv
      _debug $ boxToString $ ["Environment type for annotation " ++ iAnn ++
                              ":"] %$ indent 2 (prettyLines ann)

      -- First, get the relevant variables.
      a_c <- pEnvRequire TEnvIdContent p
      a_f <- pEnvRequire TEnvIdFinal p
      a_s <- pEnvRequire TEnvIdSelf p
      a_h <- freshTypecheckingUVar u

      -- Build the environments.
      aEnv' <- mconcat <$> mapM (\(i,a) ->
                    (\qa -> Map.singleton i $ QuantAlias $
                            QuantType Set.empty qa $
                            csFromList [qa <: a, a <: qa]) <$>
                                freshTypecheckingQVar u)
                  [ (TEnvIdContent, a_c), (TEnvIdFinal, a_f)
                  , (TEnvIdHorizon, a_h), (TEnvIdSelf, a_s) ]
      qa_s' <- freshTypecheckingQVar u
      let env' = Map.singleton TEnvIdSelf
                    (QuantType Set.empty qa_s' $ csSing $ a_s <: qa_s')
                 `mappend`
                 mconcat (map (\(AnnMemType i _ qa') ->
                                  Map.singleton (TEnvIdentifier i) $
                                    QuantType Set.empty qa' csEmpty) ms1)

      -- Depolarize the members to get the self and horizon schema types.
      (t_s, cs_s) <- depolarizeOrError u ms1
      (t_h', cs_h) <- depolarizeOrError u ms2
      _debug $ boxToString $ ["Annotation " ++ iAnn ++ " self part: "] %+
            prettyLines t_s
      _debug $ boxToString $ ["Annotation " ++ iAnn ++ " horizon part: "] %+
            prettyLines t_h'
      
      -- NOTE: here, we'd normally enforce that a set of constraints existed in
      --       @cs@.  But that performs poorly.  Instead, we will rely on the
      --       type decision process to ensure that they are provided.
      
      -- Get the opaque types.
      oa_c <- freshOVar $ OpaqueAnnotationOrigin u
      oa_f <- freshOVar $ OpaqueAnnotationOrigin u
      oa_s <- freshOVar $ OpaqueAnnotationOrigin u
      
      -- Construct the full horizon type.
      t_h <- either
                  (typecheckError . InternalError .
                      HorizonTypeConstructionError decl)
                  return
                $ recordConcat [t_h', SRecord Map.empty $ Set.singleton oa_c]

      -- Build the set of constraints to connect the opaque types to their
      -- corresponding type variables.
      let cs' = csUnions [ SOpaque oa_c ~= a_c
                         , SOpaque oa_f ~= a_f
                         , SOpaque oa_s ~= a_s
                         , csFromList
                             [ OpaqueBoundConstraint oa_c (SOpaque oa_f) $
                                  SRecord Map.empty Set.empty
                             , OpaqueBoundConstraint oa_f SBottom t_h
                             , OpaqueBoundConstraint oa_s SBottom t_s
                             ] ]
      
      -- Derive appropriate types for the members.
      (bs,cs''s) <- unzip <$> mapM (deriveAnnotationMember (envMerge aEnv aEnv')
                            (envMerge env env')) mems
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " has inferred bodies:"] %$
          indent 2 (
            ["["] %$ indent 2 (
              sequenceBoxes (maxWidth - 4) "," $ map prettyLines bs
            ) %$ ["]"]
          )
      (b'@(AnnBodyType ms1' ms2'), cs''')
          <- either (typecheckError . AnnotationConcatenationFailure u) return $
                concatAnnBodies bs
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " inferred bodies concatenate to:"] %$
          indent 2 (prettyLines b' +% [" \\ "] %$ prettyLines cs''') 
      let allCs = csUnions $ cs:cs_s:cs_h:cs':cs''':cs''s
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " complete inferred constraint set C*:"] %$
          indent 2 (prettyLines allCs)

      {-
        It remains to show the two forall conditions at the end of the
        annotation rule.  These conditions can be considerably simplified due to
        some of the guarantees that we can get out of other rules.  Comments
        below detail the assumptions on which we rely.
      -}
      -- For negative members, annotation member inference *always* produces
      -- an unconstrained type.  It therefore suffices to reduce this problem to
      -- (1) consistency-checking the constraints and (2) ensuring that each
      -- inferred negative identifier is reported in the environment.
      either (typecheckError . AnnotationClosureInconsistencyInternal iAnn
                  . Foldable.toList) return $ checkClosureConsistent allCs
      let negIdentsFor ms =
            Set.fromList $ map fst $ mapMaybe (digestMemFromPol Negative) ms
      let missingNegatives = (negIdentsFor ms1 `Set.union` negIdentsFor ms2)
                      Set.\\ (negIdentsFor ms1' `Set.union` negIdentsFor ms2')
      unless (Set.null missingNegatives) $
        typecheckError $ InternalError $
          MissingNegativeAnnotationMemberInEnvironment iAnn $
            Set.toList missingNegatives
      -- For the positive members, we know that each identifier can only have
      -- one positive inferred type; otherwise, the annotation concatenation
      -- above would've failed.  So we can treat the positives like a
      -- dictionary.  Then we just verify consistency.
      let genConstraints ms ms' = do
            let mkDict (AnnMemType i Positive qa) = Just $ Map.singleton i qa
                mkDict (AnnMemType _ Negative _) = Nothing
            let d = mconcat $ mapMaybe mkDict ms'
            let sig = mapMaybe (digestMemFromPol Positive) ms
            let mkCs i qa = case Map.lookup i d of
                  Just qa' -> return $ qa' <: qa
                  Nothing -> typecheckError $ InternalError $
                    MissingPositiveAnnotationMemberInInferredType iAnn i
            csFromList <$> gatherParallelErrors (map (uncurry mkCs) sig)
      wiringCs <- csUnion <$> genConstraints ms1 ms1'
                          <*> genConstraints ms2 ms2'
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " positive wiring constraints:"] %$
          indent 2 (prettyLines wiringCs)
      either (typecheckError . AnnotationClosureInconsistency iAnn .
                Foldable.toList) return $ checkClosureConsistent $
                  csUnion allCs wiringCs
      return iAnn
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
    digestMemFromPol :: TPolarity -> AnnMemType -> Maybe (Identifier,QVar)
    digestMemFromPol pol' (AnnMemType i pol qa) =
      if pol' == pol then Just (i,qa) else Nothing

-- |A function to derive a type for an annotation member.
deriveAnnotationMember :: TAliasEnv -- ^The relevant type alias environment.
                       -> TNormEnv -- ^The relevant type environment.
                       -> AnnMemDecl -- ^The member to typecheck.
                       -> TypecheckM (AnnBodyType, ConstraintSet)
deriveAnnotationMember aEnv env decl = do
  _debug $ boxToString $ ["Deriving type for annotation member: "] %$
                            indent 2 (prettyLines decl)
  (b,cs) <-
      case decl of
      
        Lifted pol i _ mexpr u ->
          let constr x = AnnBodyType [x] [] in
          deriveMember pol i mexpr u constr
          
        Attribute pol i _ mexpr u ->
          let constr x = AnnBodyType [] [x] in
          deriveMember pol i mexpr u constr
          
        MAnnotation pol i u -> do
          p <- mconcat <$>
            mapM (\ei -> Map.singleton ei <$> lookupSpecialVar ei)
              [TEnvIdContent, TEnvIdFinal, TEnvIdSelf]
          mann <- envRequire (UnboundTypeEnvironmentIdentifier u $
                                  TEnvIdentifier i) (TEnvIdentifier i) aEnv
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
  _debug $ boxToString $ ["Derived type for annotation member: "] %$
                            indent 2 (
                              ["Member: "] %$
                                indent 2 (prettyLines decl) %$
                              ["Type:   "] %$
                                indent 2 (
                                  prettyLines b +% [" \\ "] %$ prettyLines cs
                                )
                            )
  return (b,cs)
  where
    deriveMember pol = case pol of
                          Provides -> derivePositiveMember
                          Requires -> deriveNegativeMember
    derivePositiveMember i mexpr u constr = do
      expr <- maybe (typecheckError $
                        NoInitializerForPositiveAnnotationMember u)
                return mexpr
      (qa,cs) <- deriveQualifiedExpression aEnv env expr
      return (constr $ AnnMemType i Positive qa, cs)
    deriveNegativeMember i mexpr u constr = do
      unless (isNothing mexpr) $
        typecheckError $ InitializerForNegativeAnnotationMember u
      qa <- freshTypecheckingQVar u
      return ( constr $ AnnMemType i Negative qa, csEmpty )
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
