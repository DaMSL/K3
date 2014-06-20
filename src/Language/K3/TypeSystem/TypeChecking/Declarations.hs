{-# LANGUAGE ScopedTypeVariables, TemplateHaskell, TupleSections #-}

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
import Control.Monad.Writer
import qualified Data.Foldable as Foldable
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Maybe
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Closure
import Language.K3.TypeSystem.Consistency
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Environment
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.TypeAttribution
import Language.K3.TypeSystem.Monad.Utils
import Language.K3.TypeSystem.Polymorphism
import Language.K3.TypeSystem.TypeChecking.Expressions
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Logger

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
                  -> K3 Declaration -- ^The AST of the declaration to check.
                  -> TypecheckM Identifier
deriveDeclaration aEnv env decl =
  case tag decl of

    DRole _ -> typecheckError $ InternalError $ NonTopLevelDeclarationRole decl

    DGlobal i _ Nothing -> do
      assert0Children decl
      return i

    DGlobal i _ (Just expr) -> do
      rEnv <- globalREnv <$> typecheckingContext
      qEnv <- envRequire (InternalError $
                MissingIdentifierInGlobalQuantifiedEnvironment rEnv i)
                (TEnvIdentifier i) rEnv
      {-
      NOTE: Technically, we are obliged to check that the type decision
            procedure has placed certain upper bounds on the type variables in
            qEnv.  We are skipping that step here for performance reasons under
            the assumption that the type decision process will have done this
            correctly.
      -}
      let cs2'' = csUnions [cs'' | (_, _, _, cs'') <- Map.elems qEnv]
      let addedConstraints qa1 qa2 =
            return $ csSing (qa1 <: qa2)
      basicDeclaration i expr deriveQualifiedExpression cs2'' addedConstraints

    DTrigger i _ expr ->
      basicDeclaration i expr deriveUnqualifiedExpression csEmpty
        $ \a1 qa2 -> do
            u <- uidOf decl
            a3 <- freshTypecheckingUVar u
            a4 <- freshTypecheckingUVar u
            return $ csUnions
              [ csSing $ a1 <: SFunction a3 a4
              , csSing $ a4 <: STuple []
              , qa2 ~= STrigger a3 ]

    DAnnotation iAnn _ mems -> do
      rEnv <- globalREnv <$> typecheckingContext
      qEnv <- envRequire (InternalError $
                MissingIdentifierInGlobalQuantifiedEnvironment rEnv iAnn)
                (TEnvIdentifier iAnn) rEnv
      {-
      NOTE: Technically, we are obliged to check that the type decision
            procedure has placed certain upper bounds on the type variables in
            qEnv.  We are skipping that step here for performance reasons under
            the assumption that the type decision process will have done this
            correctly.
      -}
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
      aEnv'1 <- mconcat <$> mapM (\(i,a) ->
                    (\qa -> Map.singleton i $ QuantAlias $
                            QuantType Set.empty qa $ qa ~= a) <$>
                                freshTypecheckingQVar u)
                  [ (TEnvIdContent, a_c), (TEnvIdFinal, a_f)
                  , (TEnvIdHorizon, a_h), (TEnvIdSelf, a_s) ]
      -- NOTE: Not generalizing the two environments because polymorphism will
      --       change the construction function in the latter.
      aEnv'2 <- mconcat <$>
                  mapM (\(i,a) ->
                          (\qa -> Map.singleton i $ QuantAlias $
                            QuantType Set.empty qa $ qa ~= a) <$>
                                freshTypecheckingQVar u)
                  (map (\(ei,(a,_,_,_)) -> (ei, a)) $
                    Map.toList qEnv)
      qa_s' <- freshTypecheckingQVar u
      let env'1 = Map.singleton TEnvIdSelf
                    (QuantType Set.empty qa_s' $ csSing $ a_s <: qa_s')
      let env'2 = mconcat (map (\(AnnMemType i _ _ qa' cs') ->
                    Map.singleton (TEnvIdentifier i) $
                      generalize (envMerge env env'1) qa' cs') ms1)

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
                $ recordConcat [t_h', SRecord Map.empty (Set.singleton oa_c) Nothing]

      -- Build the set of constraints to connect the opaque types to their
      -- corresponding type variables.
      let cs'1 = csUnions [ SOpaque oa_c ~= a_c
                          , SOpaque oa_f ~= a_f
                          , SOpaque oa_s ~= a_s
                          , csFromList
                             [ OpaqueBoundConstraint oa_c
                                  (CLeft $ SOpaque oa_f) $
                                  CLeft $ SRecord Map.empty Set.empty Nothing
                             , OpaqueBoundConstraint oa_f
                                  (CLeft SBottom) (CLeft t_h)
                             , OpaqueBoundConstraint oa_s
                                  (CLeft SBottom) (CLeft t_s)
                             ] ]
      let cs'2 = csUnions $ map (\(_,_,_,cs_i') -> cs_i') $ Map.elems qEnv

      -- Derive appropriate types for the members.
      let arityMaps =
            let f = Map.fromList . map (\(AnnMemType i _ ar _ _) -> (i,ar)) in
            (f ms1, f ms2)

      (bs,cs''s) <- unzip <$> mapM (deriveAnnotationMember arityMaps
                                (envMerge (envMerge aEnv aEnv'1) aEnv'2)
                                (envMerge (envMerge env env'1) env'2)) mems
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " has inferred bodies:"] %$
          indent 2 (
            ["["] %$ indent 2 (
              sequenceBoxes (maxWidth - 4) "," $ map prettyLines bs
            ) %$ ["]"]
          )
      b'@(AnnBodyType ms1' ms2')
          <- either (typecheckError . AnnotationConcatenationFailure u) return $
                concatAnnBodies bs
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " inferred bodies concatenate to:"] %$
          indent 2 (prettyLines b')
      let allCs = csUnions $ cs:cs_s:cs_h:cs'1:cs'2:cs''s
      _debug $ boxToString $
        ["Annotation " ++ iAnn ++ " complete inferred constraint set C*:"] %$
          indent 2 (prettyLines allCs)

      -- Perform an early attribution just in case something goes wrong.
      attributeConstraints allCs

      -- Now creating and recording our learned constraint sets for each member
      -- for the typechecking result.  See below for rationale.  We're doing
      -- this now so that we can attribute as much as possible before we
      -- generate a type error.
      -- TODO: refactor for code reuse re: checkPositiveMatches below
      let getMatchingPositiveConstraints ms ms' = do
            let posInferredDict = Map.fromList $
                                  mapMaybe (digestMemFromPol Positive) ms'
            let posSignaturePairs = mapMaybe (digestMemFromPol Positive) ms
            let extractConstraints (i',(qa1',cs1''')) =
                  case Map.lookup i' posInferredDict of
                    Nothing -> typecheckError $ InternalError $
                                MissingPositiveAnnotationMemberInInferredType
                                  iAnn i'
                    Just (qa2',cs2''') ->
                      return $ csUnions [ cs1''', cs2'''
                                        , csSing $ qa2' <: qa1']
            csUnions <$> (allCs:) <$> mapM extractConstraints posSignaturePairs
      externalCs <- getMatchingPositiveConstraints ms1 ms1'
      attributeConstraints $
        loggedClosure ("annotation " ++ iAnn ++ " ascription") $
          externalCs `csUnion` allCs

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
                  . Foldable.toList) return $
        checkConsistent $
          loggedClosure ("annotation " ++ iAnn ++ " negative member checking")
            allCs
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
      -- dictionary.  Then we just verify consistency over each member.
      let checkPositiveMatches ms ms' = do
            let posInferredDict = Map.fromList $
                                  mapMaybe (digestMemFromPol Positive) ms'
            let posSignaturePairs = mapMaybe (digestMemFromPol Positive) ms
            let verifySignaturePair (i',(qa1',cs1''')) =
                  case Map.lookup i' posInferredDict of
                    Nothing -> typecheckError $ InternalError $
                                MissingPositiveAnnotationMemberInInferredType
                                  iAnn i'
                    Just (qa2',cs2''') -> do
                      let csToClose = csUnions [ allCs, cs1''', cs2'''
                                               , csSing $ qa2' <: qa1']
                      _debug $ boxToString $
                        ["Annotation " ++ iAnn ++ " member " ++ i' ++
                         " positive wiring constraints:"] %$
                          indent 2 (prettyLines csToClose)
                      either (typecheckError . AnnotationClosureInconsistency
                                                  iAnn i' .
                        Foldable.toList) return $ checkConsistent $
                          loggedClosure
                            ("annotation " ++ iAnn ++ " positive member " ++
                              i' ++ " checking")
                            csToClose
            mconcat <$> gatherParallelErrors
                          (map verifySignaturePair posSignaturePairs)
      mconcat <$> gatherParallelErrors
                    (map (uncurry checkPositiveMatches) [(ms1,ms1'),(ms2,ms2')])
      return iAnn
  where
    -- |A common implementation of both initialized variables and triggers.
    --  These rules only vary by (1) the derivation used on the type expression
    --  and (2) the constraint sets which are added to the constraint closure.
    --  The constraint sets are defined by @csPre@, a set of constraints to be
    --  added to the type before polyinstantiation, and @csPostF@, a function
    --  from the variables inferred during type expression and expression
    --  derivation onto a set of constraints for consistency checking.
    basicDeclaration i expr deriv csPre csPostF = do -- TypecheckM
      assert0Children decl
      u <- uidOf decl
      (v1,cs1) <- deriv aEnv env expr
      QuantType sas qa' cs2' <- requireQuantType u i env
      (v2,cs2) <- polyinstantiate u $ QuantType sas qa' $ csUnion cs2' csPre
      csPost <- csPostF v1 v2
      let cs'' = loggedClosure ("declaration " ++ i ++ " checking") $
                    csUnions [cs1,cs2,csPost]
      attributeConstraints cs''
      -- We've decided upon the type, so now check for consistency.
      either (typecheckError . DeclarationClosureInconsistency i cs''
                                  (someVar v1) (someVar v2) . Foldable.toList)
             return
           $ checkConsistent cs''
      return i
    digestMemFromPol :: TPolarity -> NormalAnnMemType
                     -> Maybe (Identifier,(QVar,ConstraintSet))
    digestMemFromPol pol' (AnnMemType i pol _ qa cs) =
      if pol' == pol then Just (i,(qa,cs)) else Nothing

-- |A function to derive a type for an annotation member.
deriveAnnotationMember :: ( Map Identifier MorphismArity
                          , Map Identifier MorphismArity )
                       -> TAliasEnv  -- ^The relevant type alias environment.
                       -> TNormEnv   -- ^The relevant type environment.
                       -> AnnMemDecl -- ^The member to typecheck.
                       -> TypecheckM (NormalAnnBodyType, ConstraintSet)
deriveAnnotationMember (ars1,ars2) aEnv env decl = do
  _debug $ boxToString $ ["Deriving type for annotation member: "] %$
                            indent 2 (prettyLines decl)
  declUid <- uidOfAnnMem decl
  (b,cs)  <-
      case decl of

        Lifted pol i _ mexpr _ ->
          let constr x = AnnBodyType [x] [] in
          deriveMember ars1 pol i mexpr declUid constr

        Attribute pol i _ mexpr _ ->
          let constr x = AnnBodyType [] [x] in
          deriveMember ars2 pol i mexpr declUid constr

        MAnnotation pol i _ -> do
          p <- mconcat <$>
            mapM (\ei -> Map.singleton ei <$> lookupSpecialVar ei)
              [TEnvIdContent, TEnvIdFinal, TEnvIdSelf]
          mann <- envRequire (UnboundTypeEnvironmentIdentifier declUid $
                                  TEnvIdentifier i) (TEnvIdentifier i) aEnv
          ann <- case mann of
                    QuantAlias _ -> typecheckError (NonAnnotationAlias declUid
                                                      $ TEnvIdentifier i)
                    AnnAlias ann -> return ann
          let (AnnType p' b cs) = instantiateAnnotation p ann
          unless (Map.null p') $
            typecheckError $ InternalError $ UnresolvedTypeParameters p'
          let b' = case pol of
                    Provides -> b
                    Requires ->
                      let AnnBodyType m1 m2 = b in
                      let negatize (AnnMemType i' _ ar qa cs') =
                            AnnMemType i' Negative ar qa cs' in
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
    deriveMember ars pol i mexpr u constr = do
      ar <- maybe (typecheckError $ InternalError $
                      UndeterminedArityForAnnotationMember decl i)
              return $ Map.lookup i ars
      case pol of
        Provides -> derivePositiveMember ar i mexpr u constr
        Requires -> deriveNegativeMember ar i mexpr u constr
    derivePositiveMember ar i mexpr u constr =
      case mexpr of
        Nothing -> do
          qa <- freshTypecheckingQVar u
          return (constr $ AnnMemType i Positive ar qa csEmpty, csEmpty)
        Just expr -> do
          (qa,cs) <- deriveQualifiedExpression aEnv env expr
          return (constr $ AnnMemType i Positive ar qa cs, csEmpty)
    deriveNegativeMember ar i mexpr u constr = do
      unless (isNothing mexpr) $
        typecheckError $ InitializerForNegativeAnnotationMember u
      qa <- freshTypecheckingQVar u
      return (constr $ AnnMemType i Negative ar qa csEmpty, csEmpty)
    lookupSpecialVar :: TEnvId -> TypecheckM UVar
    lookupSpecialVar ei = do
      mqt <- envRequire (badFormErr Nothing) ei aEnv
      let badForm = typecheckError $ badFormErr $ Just mqt
      case mqt of
        QuantAlias (QuantType sas qa cs) -> do
          unless (Set.null sas) badForm
          case csToList cs of
            [QualifiedLowerConstraint (CRight a) qa']
              | qa == qa' -> return a

            [QualifiedLowerConstraint (CRight a) qa', QualifiedUpperConstraint qa'' (CRight a') ]
              | qa == qa' && qa == qa'' && a == a' -> return a

            _ -> badForm
        AnnAlias _ -> badForm
      where
        badFormErr :: Maybe NormalTypeAliasEntry -> TypeError
        badFormErr mqt = InternalError $ InvalidSpecialBinding ei mqt

-- |Obtains a quantified type entry from the type environment, generating an
--  error if it cannot be found.
requireQuantType :: UID -> Identifier -> TNormEnv
                 -> TypecheckM NormalQuantType
requireQuantType u i =
  envRequire (UnboundEnvironmentIdentifier u $ TEnvIdentifier i)
             (TEnvIdentifier i)

loggedClosure :: String -> ConstraintSet -> ConstraintSet
loggedClosure msg cs =
  _debugI ("Calculating closure for " ++ msg) $
  let ans = calculateClosure cs in
  let errs = either Seq.length (const 0) $ checkConsistent ans in
  _debugI ("Calculated closure for " ++ msg ++ " (" ++
            show (Set.size $ csToSet ans) ++ " constraint(s), " ++
            show errs ++ " consistency error(s))")
  ans
