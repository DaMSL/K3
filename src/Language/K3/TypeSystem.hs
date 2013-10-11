{-# LANGUAGE ViewPatterns, GeneralizedNewtypeDeriving, TemplateHaskell #-}

module Language.K3.TypeSystem
( typecheck
, TypecheckResult(..)
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.Writer
import Control.Monad.Trans.Either
import qualified Data.Foldable as Foldable
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Set as Set

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Type
import Language.K3.Utils.Pretty
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Manifestation
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Sanity
import Language.K3.TypeSystem.Simplification
import Language.K3.TypeSystem.TypeChecking
import Language.K3.TypeSystem.TypeDecision
import Language.K3.TypeSystem.TypeDecision.Monad
import Language.K3.Utils.Logger

$(loggingFunctions)

-- |Describes a typechecking result.  Each value in the record is a @Maybe@
--  value so that a partial @TypecheckResult@ can be generated even if an error
--  occurs during typechecking.  @tcAEnv@ is the decided alias environment;
--  @tcEnv@ is the decided type environment.  @tcExprTypes@ is a mapping from
--  subexpression UID to a type and representative constraint set.
data TypecheckResult
  = TypecheckResult
      { tcAEnv :: Maybe TAliasEnv
      , tcEnv :: Maybe TNormEnv
      , tcExprTypes :: Maybe (Map UID (AnyTVar, ConstraintSet))
      , tcExprBounds :: Maybe (Map UID (K3 Type, K3 Type))
      }
  deriving (Eq, Show)

instance Monoid TypecheckResult where
  mempty = TypecheckResult Nothing Nothing Nothing Nothing
  mappend (TypecheckResult a b c d) (TypecheckResult a' b' c' d') =
    TypecheckResult (a `mappend` a') (b `mappend` b') (c `mappend` c')
      (d `mappend` d')

-- |The top level of typechecking in K3.  This routine accepts a role
--  declaration and typechecks it.  The result is a pair between the
--  typechecking result and a sequence of errors which occurred.  The result is
--  in the form of a record of @Maybe@ values; if no errors are reported, then
--  the result will contain only @Just@ values.
typecheck :: TAliasEnv -- ^The environment defining existing type bindings.
          -> TNormEnv -- ^The environment defining existing bindings.
          -> K3 Declaration -- ^The top-level AST to check.
          -> (Seq TypeError, TypecheckResult)
typecheck aEnv env decl =
  let (errs, result) =
        first (either id (const Seq.empty)) $
          runWriter $ runEitherT $ doTypecheck aEnv env decl
  in
  let errsBox =
        if Seq.null errs
          then ["Succeeded in typechecking AST:"] %$ indent 4 (prettyLines decl)
          else ["Errors while typechecking AST:"] %$ indent 4 (prettyLines decl)
                  %$ indent 2 (["Errors were:"] %$ indent 2
                        (vconcats $ map prettyLines $ Foldable.toList errs))
  in
  let exprTypesBox =
        case tcExprTypes result of
          Nothing -> ["(No expression types inferred.)"]
          Just ts -> ["Inferred expression types:"] %$ indent 2
                        (vconcats $ map prettyExprType $ Map.toList ts)
  in
  let exprBoundsBox =
        case tcExprBounds result of
          Nothing -> ["(No expression bounds inferred.)"]
          Just ts -> ["Inferred expression bounds:"] %$ indent 2
                        (vconcats $ map prettyExprBounds $ Map.toList ts)
  in
  let outputBox = errsBox %$ indent 2 exprTypesBox %$ indent 2 exprBoundsBox in
  _debugI (boxToString outputBox) (errs, result)
  where
    prettyExprType :: (UID,(AnyTVar, ConstraintSet)) -> [String]
    prettyExprType (u,(sa,cs)) =
      let n = take 10 $ show u ++ repeat ' ' in
      let simpConfig =
            SimplificationConfig { preserveVars = Set.singleton sa } in
      let simpCs = runSimplifyM simpConfig $
                      simplifyByGarbageCollection =<<
                      simplifyByUnification cs in
      [n ++ " → "] %+ prettyLines sa %+ ["\\"] %+ prettyLines simpCs
    prettyExprBounds :: (UID,(K3 Type, K3 Type)) -> [String]
    prettyExprBounds (u,(lb,ub)) =
      let n = take 10 $ show u ++ repeat ' ' in
      [n ++ " ≤ "] %+ prettyLines lb %$ [n ++ " ≥ "] %+ prettyLines ub

-- |The actual heavy lifting of @typecheck@.
doTypecheck :: TAliasEnv -- ^The environment defining existing type bindings.
            -> TNormEnv -- ^The environment defining existing bindings.
            -> K3 Declaration -- ^The top-level AST to check.
            -> EitherT (Seq TypeError) (Writer TypecheckResult) ()
doTypecheck aEnv env decl = do
  _debug $ boxToString $ ["Performing typechecking for AST:"] %$
                            indent 2 (prettyLines decl)
  -- 1. Simple sanity checks for consistency.
  hoistEither $ either (Left . Seq.singleton) Right $
    unSanityM (sanityCheck decl)
  -- 2. Decide the types that should be assigned.
  ((aEnv',env',rEnv),idx) <- hoistEither $ runDecideM 0 $ typeDecision decl
  tell $ mempty { tcAEnv = Just aEnv', tcEnv = Just env' }
  -- 3. Check that types inferred for the declarations match these types.
  let (eErrs, attribs) = runDeclTypecheckM idx
                            (deriveDeclarations aEnv env aEnv' env' rEnv decl)
  tell $ mempty { tcExprTypes = Just attribs }
  ((), _) <- hoistEither eErrs
  -- 4. Post-process all of the attributions so we can extract meaningful type
  --    definitions from them.
  tell $ mempty { tcExprBounds = Just $ Map.map manifestBounds attribs }
  return ()
  where
    manifestBounds :: (AnyTVar, ConstraintSet) -> (K3 Type, K3 Type)
    manifestBounds (sa,cs) =
      case sa of
        SomeQVar qa -> manifestBounds' qa
        SomeUVar a -> manifestBounds' a
      where
        manifestBounds' var =
          (manifestType lowerBound cs var, manifestType upperBound cs var)

-- |A simple monad type for sanity checking.
newtype SanityM a = SanityM { unSanityM :: Either TypeError a }
  deriving (Monad, Functor, Applicative)

instance TypeErrorI SanityM where
  typeError = SanityM . Left
