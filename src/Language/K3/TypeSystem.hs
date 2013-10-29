{-# LANGUAGE ViewPatterns, GeneralizedNewtypeDeriving, TemplateHaskell, TupleSections #-}

module Language.K3.TypeSystem
( typecheck
, typecheckProgram
, TypecheckResult(..)
) where

import Control.Applicative
import Control.Arrow
import Control.Monad.Writer
import Control.Monad.Trans.Either
import qualified Data.Foldable as Foldable
import Data.Functor.Identity
import Data.List
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import Data.Traversable

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import qualified Language.K3.Core.Constructor.Declaration as DC
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
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
      , tcREnv :: Maybe TGlobalQuantEnv
      , tcExprTypes :: Maybe (Map UID AnyTVar, ConstraintSet)
      , tcExprBounds :: Maybe (Map UID (K3 Type, K3 Type))
      }
  deriving (Eq, Show)

instance Monoid TypecheckResult where
  mempty = TypecheckResult Nothing Nothing Nothing Nothing Nothing
  mappend (TypecheckResult a b c d e) (TypecheckResult a' b' c' d' e') =
    TypecheckResult (a `mappend` a') (b `mappend` b') (c `mappend` c')
      (d `mappend` d') (e `mappend` e')

-- |The top level of typechecking in K3.  This routine accepts a role
--  declaration and typechecks it.  The result is a pair between the
--  typechecking result and a sequence of errors which occurred.  The result is
--  in the form of a record of @Maybe@ values; if no errors are reported, then
--  the result will contain only @Just@ values.
typecheck :: TAliasEnv -- ^The environment defining existing type bindings.
          -> TNormEnv -- ^The environment defining existing bindings.
            -> TGlobalQuantEnv -- ^The polymorphism bounding info bindings.
          -> K3 Declaration -- ^The top-level AST to check.
          -> (Seq TypeError, TypecheckResult)
typecheck aEnv env rEnv decl =
  let (errs, result) =
        first (either id (const Seq.empty)) $
          runWriter $ runEitherT $ doTypecheck aEnv env rEnv decl
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
          Just (vars,cs) ->
            let ts = Map.map (,cs) vars in
            ["Inferred expression types:"] %$ indent 2
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
                      simplifyByConstraintEquivalenceUnification cs in
      [n ++ " → "] %+ prettyLines sa %+ ["\\"] %+ prettyLines simpCs
    prettyExprBounds :: (UID,(K3 Type, K3 Type)) -> [String]
    prettyExprBounds (u,(lb,ub)) =
      let n = take 10 $ show u ++ repeat ' ' in
      [n ++ " ≥ "] %+ prettyLines lb %$ [n ++ " ≤ "] %+ prettyLines ub

-- |The actual heavy lifting of @typecheck@.
doTypecheck :: TAliasEnv -- ^The environment defining existing type bindings.
            -> TNormEnv -- ^The environment defining existing bindings.
            -> TGlobalQuantEnv -- ^The polymorphism bounding info bindings.
            -> K3 Declaration -- ^The top-level AST to check.
            -> EitherT (Seq TypeError) (Writer TypecheckResult) ()
doTypecheck aEnv env rEnv ast = do
  _debug $ boxToString $ ["Performing typechecking for AST:"] %$
                            indent 2 (prettyLines ast)
  -- 0. Strip out things that the type system isn't specified to do (e.g. sink
  --    and source declarations).
  let ast' = fromJust $ stripUnspecifiedNodes ast
  -- 1. Simple sanity checks for consistency.
  hoistEither $ either (Left . Seq.singleton) Right $
    unSanityM (sanityCheck ast')
  -- 2. Decide the types that should be assigned.
  ((aEnv',env',rEnv'),idx) <- hoistEither $ runDecideM 0 $ typeDecision ast'
  tell $ mempty { tcAEnv = Just aEnv', tcEnv = Just env', tcREnv = Just rEnv' }
  -- 3. Check that types inferred for the declarations match these types.
  let (eErrs, attribs@(attVars,attCs)) =
        runTypecheckM (rEnv' `mappend` rEnv) idx
          (deriveDeclarations aEnv env aEnv' env' ast')
  tell $ mempty { tcExprTypes = Just attribs }
  ((), _) <- hoistEither eErrs
  -- 4. Post-process all of the attributions so we can extract meaningful type
  --    definitions from them.
  tell $ mempty { tcExprBounds = Just $ Map.map (manifestBounds attCs) attVars }
  return ()
  where
    manifestBounds :: ConstraintSet -> AnyTVar -> (K3 Type, K3 Type)
    manifestBounds cs sa =
      case sa of
        SomeQVar qa -> manifestBounds' qa
        SomeUVar a -> manifestBounds' a
      where
        manifestBounds' var =
          (manifestType lowerBound cs var, manifestType upperBound cs var)
    stripUnspecifiedNodes :: K3 Declaration -> Maybe (K3 Declaration)
    stripUnspecifiedNodes decl =
      case tag decl of
        DRole i -> Just $ DC.role i $
                    mapMaybe stripUnspecifiedNodes $ children decl
        DGlobal _ tExpr _ ->
          case tag tExpr of
            TSink -> Nothing
            TSource -> Nothing
            _ -> Just decl
        _ -> Just decl


-- |Driver wrapper function for typechecking
-- NOTE: this function should not be used once a module system is in place
typecheckProgram :: K3 Declaration -> (Seq TypeError, TypecheckResult, K3 Declaration)
typecheckProgram p = 
  let (errs, result) = typecheck Map.empty Map.empty Map.empty p
  in (errs, result, case tcExprBounds result of
        Nothing     -> p
        Just bounds -> annotateProgramTypes p bounds)

-- | Attaches type bounds as expression annotations to a program.
annotateProgramTypes :: K3 Declaration -> Map UID (K3 Type, K3 Type) -> K3 Declaration
annotateProgramTypes p typeBounds = runIdentity $ traverse annotateDecl p
  where
    annotateDecl (dt :@: anns) = return . (:@: anns) $ case dt of
      DGlobal n t (Just e)   -> DGlobal n t . Just . runIdentity $ traverse annotateExpr e
      DTrigger n t e         -> DTrigger n t . runIdentity $ traverse annotateExpr e
      DAnnotation n tis mems -> DAnnotation n tis $ map annotateAnnMem mems
      _ -> dt
    
    annotateExpr e@(_ :@: anns) = return $ case partition isEType anns of
      ([], rest) -> maybe e (\(lb,ub) -> (e @+ ETypeLB lb) @+ ETypeUB ub) $ 
                      Map.lookup (getEUID rest) typeBounds

      (_,_)      -> e

    annotateAnnMem (Lifted p' n t me uid) =
      flip (Lifted p' n t) uid $ (runIdentity . traverse annotateExpr) <$> me
    
    annotateAnnMem (Attribute p' n t me uid) =
      flip (Lifted p' n t) uid $ (runIdentity . traverse annotateExpr) <$> me
    
    annotateAnnMem x = x    

    getEUID anns = case filter isEUID anns of
      [EUID uid] -> uid
      []         -> error "No uid found for expression"
      _          -> error "Multiple UIDs found for expression"


-- |A simple monad type for sanity checking.
newtype SanityM a = SanityM { unSanityM :: Either TypeError a }
  deriving (Monad, Functor, Applicative)

instance TypeErrorI SanityM where
  typeError = SanityM . Left
