{-# LANGUAGE ScopedTypeVariables, TypeSynonymInstances, FlexibleInstances #-}
{-|
  A module containing the routines necessary to construct a skeletal environment
  for the type decision prodecure.
-}
module Language.K3.TypeSystem.TypeDecision.SkeletalEnvironment
( TSkelAliasEnv
, TypeDecideSkelM
, StubInfo(..)
, constructSkeletalAEnv
) where

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Writer
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Monoid
import Data.Tree

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import qualified Language.K3.TypeSystem.ConstraintSetLike as CSL
import Language.K3.TypeSystem.Annotations
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.FreshVar
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.TypeDecision.Data
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.TypeSystem.Utils.K3Tree

-- |A type alias for skeletal type alias environments.
type TSkelAliasEnv = TEnv (TypeAliasEntry StubbedConstraintSet)

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
    }
  deriving (Show)

instance TypeErrorI TypeDecideSkelM where
  typeError = lift . typeError

-- |A function which constructs a skeletal type environment for the type
--  decision procedure.
constructSkeletalAEnv :: K3 Declaration -> TypeDecideSkelM TSkelAliasEnv
constructSkeletalAEnv decl =
  case tag decl of
    DRole _ ->
      let decl's = subForest decl in
      mconcat <$> gatherParallelSkelErrors
                    (map constructSkeletalAEnvForDecl decl's)
    _ -> lift $ decisionError $ InternalError $ TopLevelDeclarationNonRole decl

-- |Constructs a skeletal type environment for a single declaration.
constructSkeletalAEnvForDecl :: K3 Declaration -> TypeDecideSkelM TSkelAliasEnv
constructSkeletalAEnvForDecl decl =
  case tag decl of
    DAnnotation i mems -> do
      assert0Children decl
      s <- spanOf decl
      p <- Map.fromList <$>
              zip [TEnvIdContent, TEnvIdFinal, TEnvIdSelf] <$>
                replicateM 3 (lift $ freshUVar $ TVarSourceOrigin s)
      (bs, scss) <- unzip <$> gatherParallelSkelErrors
                        (map (constructBodyTypeForAnnotationMember decl p) mems)
      (b, cs) <- either (typeError . InvalidAnnotationConcatenation s)
                    return
                  $ concatAnnBodies bs
      let scs = CSL.unions scss `CSL.union` CSL.promote cs
      return $ Map.singleton (TEnvIdentifier i) $ AnnAlias $ AnnType p b scs

    DTrigger{} -> assert0Children decl >> return Map.empty
    
    DGlobal{} -> assert0Children decl >> return Map.empty
    
    DRole{} -> lift $ decisionError $
                  InternalError $ NonTopLevelDeclarationRole decl

-- |Constructs an annotation member type for a single annotation member.
constructBodyTypeForAnnotationMember :: K3 Declaration -> TParamEnv
                                     -> AnnMemDecl
                                     -> TypeDecideSkelM
                                          (AnnBodyType, StubbedConstraintSet)
constructBodyTypeForAnnotationMember decl p mem =
  case mem of
    Lifted pol i tExpr _ s ->
      first (\ memT -> AnnBodyType [memT] []) <$>
        constructMemberType pol i tExpr s
    Attribute pol i tExpr _ s ->
      first (\memT -> AnnBodyType [] [memT]) <$>
        constructMemberType pol i tExpr s
    MAnnotation {} ->
      -- These should've been inlined by now.
      internalTypeError $ UnexpectedMemberAnnotationDeclaration decl mem
  where
    constructMemberType pol i tExpr s = do
      qa <- lift $ freshQVar $ TVarSourceOrigin s
      stub <- lift nextStub
      let polT = case pol of { Provides -> Positive; Requires -> Negative }
      tell $ Map.singleton stub
        StubInfo { stubTypeExpr = tExpr, stubVar = qa, stubParamEnv = p }
      return (AnnMemType i polT qa, CSL.singleton (CLeft stub))

-- |Performs error gathering for @TypeDecideSkelM@.
gatherParallelSkelErrors :: forall a. [TypeDecideSkelM a] -> TypeDecideSkelM [a]
gatherParallelSkelErrors xs =
  WriterT $
    second mconcat <$> unzip <$> gatherParallelErrors (map runWriterT xs)
