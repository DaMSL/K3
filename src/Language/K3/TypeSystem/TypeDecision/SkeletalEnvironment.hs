{-# LANGUAGE ScopedTypeVariables, TypeSynonymInstances, FlexibleInstances, TemplateHaskell #-}
{-|
  A module containing the routines necessary to construct a skeletal environment
  for the type decision prodecure.
-}
module Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
( TSkelAliasEnv
, TSkelGlobalQuantEnv
, TSkelQuantEnv
, TypeDecideSkelM
, StubInfo(..)
, constructSkeletalEnvs
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.Trans
import Control.Monad.Trans.Writer
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import qualified Data.Traversable as Trav
import Data.Tree as Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Utils.Pretty
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshOpaque
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.TypeChecking.TypeExpressions
import Language.K3.TypeSystem.TypeDecision.AnnotationInlining
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.Utils
import Language.K3.TypeSystem.Utils.K3Tree
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |A type alias for skeletal type alias environments.
type TSkelAliasEnv = TEnv (TypeAliasEntry StubbedConstraintSet)
-- |A type alias for skeletal global quantified environments.
type TSkelGlobalQuantEnv = TEnv TSkelQuantEnv
-- |A type alias for skeletal quantified environments.
type TSkelQuantEnv = TEnv (TQuantEnvValue StubbedConstraintSet)

-- |A type alias for the skeletal construction monad.  The writer output maps
--  each used stub to the information associated with it.
type TypeDecideSkelM = WriterT (Map Stub StubInfo) TypeDecideM

-- |A data type for tracking the information suspended by a stub.
data StubInfo
  = StubInfo
    { stubTypeExpr :: K3 Type
        -- ^ The type expression deferred by this stub.
    , stubVar :: QVar
        -- ^ The type variable to be bounded by the type expression.
    , stubParamEnv :: TParamEnv
        -- ^ The type parameter environment for the annotation in this stub.
    , stubMemRepr :: AnnMemRepr
        -- ^ The representation of the annotation member which inspired this
        --   stub.
    , stubFreshUID :: UID
        -- ^ The UID to which fresh type variables may be attributed.
    }
  deriving (Show)

instance TypeErrorI TypeDecideSkelM where
  typeError = lift . typeError

-- |A function which constructs a skeletal type environment for the type
--  decision procedure.
constructSkeletalEnvs :: FlatAnnotationDecls
                      -> TypeDecideSkelM (TSkelAliasEnv, TSkelGlobalQuantEnv)
constructSkeletalEnvs anns = do
  -- First, get a structure for each declaration.
  base <- Trav.mapM constructTypeForDecl anns
  _debug $ boxToString $
    ["Base skeletal environment: "] %+ indent 2 (
        vconcats $ map (\(k,(b,p,scs)) -> [k] %+ [" → "] %+
                          prettyLines (AnnType p b scs)) $
                      Map.toList base
      )
      
  -- Next, join all of the constraint sets together (because each annotation can
  -- refer to the others) and make an environment from it.  This environment
  -- lacks bounds on member type parameters but is otherwise complete.
  let scs = CSL.unions $ map ((\(_,_,scs') -> scs') . snd) $ Map.toList base
  let aPreEnv = Map.fromList $
                  map (\(i,(b,p,_)) ->
                      (TEnvIdentifier i, AnnAlias $ AnnType p b scs)) $
                    Map.toList base
                
  -- Next, each annotation's type parameters should be processed.  We must
  -- create both the skeletal global quantified environment entry as well as
  -- the constraints which are required to be present within the annotation by
  -- the Annotation rule.  We are calculating these required constraints later
  -- than we otherwise would because processing their type signatures requires
  -- the skeletal environment from above.
  -- NOTE: The typechecker implementation, rather than checking for these
  -- constraints, simply assumes that this code will insert them here.  This set
  -- of constraints has to do with declared type parameters for annotation
  -- members and not the annotation type parameters themselves (which are added
  -- in the constructTypeForDecl calls above).
  tpdata <- Trav.mapM (digestTypeParameterInfo aPreEnv) anns
  let rEnv = Map.mapKeys TEnvIdentifier $ Map.map snd tpdata
  let tpscs = CSL.unions $ Map.elems $ Map.map fst tpdata
  _debug $ boxToString $ ["Type parameter constraints: "] %+ prettyLines tpscs
  let aEnv = Map.map (addConstraints tpscs) aPreEnv

  -- Finally, present some debugging info and give back the results.
  _debug $ boxToString $
    ["Skeletal environment: "] %+ indent 2 (
        vconcats $ map (\(k,v) -> prettyLines k %+ [" → "] %+ prettyLines v) $
                      Map.toList aEnv
      )
  return (aEnv, rEnv)
  where
    addConstraints :: StubbedConstraintSet
                   -> TypeAliasEntry StubbedConstraintSet
                   -> TypeAliasEntry StubbedConstraintSet
    addConstraints scs entry = case entry of
      QuantAlias _ -> error $ "constructTypeForDecl produced QuantAlias: " ++
                                  show entry
      AnnAlias (AnnType p mems scs') ->
        AnnAlias (AnnType p (addBodyConstraints mems) $ CSL.union scs scs')
      where
        addBodyConstraints :: AnnBodyType StubbedConstraintSet
                           -> AnnBodyType StubbedConstraintSet
        addBodyConstraints (AnnBodyType ms1 ms2) =
          AnnBodyType (map addMemConstraints ms1) (map addMemConstraints ms2)
        addMemConstraints :: AnnMemType StubbedConstraintSet
                          -> AnnMemType StubbedConstraintSet
        addMemConstraints (AnnMemType i pol ar qa scs') =
          AnnMemType i pol ar qa $ CSL.union scs scs'

-- |Constructs a skeletal environment entry for a single declaration.
constructTypeForDecl :: (FlatAnnotation, K3 Declaration)
                     -> TypeDecideSkelM ( AnnBodyType StubbedConstraintSet
                                        , TParamEnv
                                        , StubbedConstraintSet )
constructTypeForDecl ((_,lAtts,sAtts),decl) = do
  u <- uidOf decl
  -- Prepare an appropriate environment
  a_c <- lift $ freshUVar $ TVarSourceOrigin u
  a_f <- lift $ freshUVar $ TVarSourceOrigin u
  a_s <- lift $ freshUVar $ TVarSourceOrigin u
  a_h <- lift $ freshUVar $ TVarSourceOrigin u
  let p = Map.fromList [ (TEnvIdContent, a_c)
                       , (TEnvIdFinal, a_f)
                       , (TEnvIdSelf, a_s) ]
  
  -- Calculate the horizon part of the schema and the constraints generated by
  -- it.
  sAttTs <- gatherParallelSkelErrors (map (memReprToMemType p) sAtts)
  (a_h', cs_h) <- liftEither (AnnotationDepolarizationFailure u) $
                    depolarize sAttTs
                    
  -- Calculate the type of self and the corresponding constraints
  lAttTs <- gatherParallelSkelErrors (map (memReprToMemType p) lAtts)
  (a_s', cs_s) <- liftEither (AnnotationDepolarizationFailure u) $
                    depolarize lAttTs

  -- NOTE: The typechecker is required by specification to ensure that certain
  -- constraints exist in the annotation's constraint set.  For the sake of
  -- efficiency, we instead allow the typechecker implementation to assume that
  -- the decision process (this code) has inserted those constraints.  The first
  -- set of constraints relates only to annotation parametric bounds and is
  -- handled here.
  let cs = csFromList [a_f <: a_h, a_h <: a_c, a_h <: a_h', a_s <: a_s']

  -- Construct an appropriate stubbed constraint set
  let scs = CSL.unions [cs_h, cs_s, CSL.promote cs]
  
  -- We now have a result for a single annotation
  return (AnnBodyType lAttTs sAttTs,p,scs)
  where
    liftEither :: (a -> TypeError) -> Either a b -> TypeDecideSkelM b
    liftEither f = either (typeError . f) return
    memReprToMemType :: TParamEnv -> AnnMemRepr
                     -> TypeDecideSkelM (AnnMemType StubbedConstraintSet)
    memReprToMemType p repr = do
      let decl' = reprMem repr
      u <- uidOfAnnMem decl'
      case decl' of
        Lifted pol i' tExpr _ _    -> genMemType pol i' tExpr u
        Attribute pol i' tExpr _ _ -> genMemType pol i' tExpr u
        MAnnotation {} ->
          -- These should've been inlined and forgotten!
          error $ "Member annotation declaration found during skeletal " ++
                  "environment construction!"
      where
        genMemType pol i' tExpr s = do
          qa <- lift $ freshQVar $ TVarSourceOrigin s
          stub <- lift nextStub
          let scs = CSL.singleton $ CLeft stub
          u <- uidOf decl
          tell $ Map.singleton stub StubInfo
                    { stubTypeExpr = tExpr, stubVar = qa, stubParamEnv = p
                    , stubMemRepr = repr, stubFreshUID = u }
          -- Determine appropriate arity ascription.  Whether the member has a
          -- polymorphic signature is equivalent to whether the signature
          -- contains any declared type variable references, since this is
          -- necessary (declared polymorphism only comes from such variables)
          -- and sufficient (the only declared type variables in scope are those
          -- on the annotation declaration since all such declarations are top
          -- level).
          let ar =
                if any isDeclaredVar $ map tag $ flatten tExpr
                  then PolyArity else MonoArity 
          return $ AnnMemType i' (typeOfPol pol) ar qa scs
          where
            isDeclaredVar :: Type -> Bool
            isDeclaredVar (TDeclaredVar _) = True
            isDeclaredVar _ = False

-- |Calculates for a representation of a single annotation the constraints
--  required by the Annotation rule of typechecking as well as the entry which
--  should appear in the global quantified environment.  These results are
--  skeletal, making use of stubs to defer constraint decisions.
digestTypeParameterInfo :: TSkelAliasEnv
                        -> (FlatAnnotation, K3 Declaration)
                        -> TypeDecideSkelM
                              ( StubbedConstraintSet , TSkelQuantEnv )
digestTypeParameterInfo aEnv ((cxt,_,_),decl) = do
  digested <- Trav.mapM digestSingleTypeParameterInfo cxt
  return ( CSL.unions $ map fst $ Map.elems digested
         , Map.mapKeys TEnvIdentifier $ Map.map snd digested )
  where
    digestSingleTypeParameterInfo :: TypeParameterContextEntry
                                  -> TypeDecideSkelM
                                        ( StubbedConstraintSet
                                        , TQuantEnvValue StubbedConstraintSet )
    digestSingleTypeParameterInfo (a,qa,mtExpr') = do
      let (ta_L,scs_L) = (CLeft SBottom :: TypeOrVar,CSL.empty)
      (ta_U,scs_U) <- case mtExpr' of
            Nothing -> return (CLeft STop :: TypeOrVar,CSL.empty)
            Just tExpr' ->
              first CRight <$> deriveUnqualifiedTypeExpression aEnv tExpr'
      -- Create opaques for annotation types.  (Non-annotation types are handled
      -- later once the environments are constructed.)
      u <- uidOf decl
      oa <- freshOVar $ OpaqueSourceOrigin u
      let scsOpaque = CSL.promote $ (SOpaque oa ~= a) `csUnion`
                        csSing (OpaqueBoundConstraint oa ta_L ta_U)
      return ( CSL.unions [ qa ~= a
                          , CSL.promote $ csFromList [ta_L <: a, a <: ta_U]
                          , scs_L, scs_U ]
             , (a, ta_L, ta_U, CSL.unions [scs_L, scs_U, scsOpaque]) )

-- |Performs error gathering for @TypeDecideSkelM@.
gatherParallelSkelErrors :: forall a. [TypeDecideSkelM a] -> TypeDecideSkelM [a]
gatherParallelSkelErrors xs =
  WriterT $
    second mconcat <$> unzip <$> gatherParallelErrors (map runWriterT xs)
