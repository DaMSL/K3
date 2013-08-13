{-# LANGUAGE ViewPatterns, GeneralizedNewtypeDeriving #-}

module Language.K3.TypeSystem
( typecheck
) where

import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

import Control.Applicative
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Error
import Language.K3.TypeSystem.Monad.Iface.TypeError
import Language.K3.TypeSystem.Sanity
import Language.K3.TypeSystem.TypeChecking
import Language.K3.TypeSystem.TypeChecking.Monad
import Language.K3.TypeSystem.TypeDecision
import Language.K3.TypeSystem.TypeDecision.Monad

-- |Describes a typechecking result.  This result carries an annotated
--  declaration tree and a set of constraints over those declarations.  It also
--  provides the type environments which were decided upon and checked.
data TypecheckResult
  = TypecheckResult
      { tcAnnotatedDecl :: K3 Declaration
      , tcConstraints :: ConstraintSet
      , tcAEnv :: TAliasEnv
      , tcEnv :: TNormEnv
      }
  deriving (Eq, Show)

-- |The top level of typechecking in K3.  This routine accepts a role
--  declaration and typechecks it.  Upon success, an updated version of the
--  declaration is returned; this version includes an annotation at each node
--  describing the type of that subtree.  These types are described with respect
--  to a global set of constraints, which is also returned.
typecheck :: TAliasEnv -- ^The environment defining existing type bindings.
          -> TNormEnv -- ^The environment defining existing bindings.
          -> K3 Declaration -- ^The top-level AST to check.
          -> Either (Seq TypeError) TypecheckResult
typecheck aEnv env decl = do
  -- 1. Simple sanity checks for consistency.
  either (Left . Seq.singleton) Right $ unSanityM (sanityCheck decl)
  -- 2. Decide the types that should be assigned.
  ((aEnv',env'),idx) <- runDecideM 0 $ typeDecision decl
  -- 3. Check that types inferred for the declarations match these types.
  ((),_) <- runTypecheckM idx (deriveDeclarations aEnv env aEnv' env' decl)
  -- TODO: annotate the declaration tree with type information
  -- TODO: some form of type simplification on the output types
  return TypecheckResult
            { tcAnnotatedDecl = decl -- TODO
            , tcConstraints = csEmpty
            , tcAEnv = aEnv'
            , tcEnv = env'
            }

-- |A simple monad type for sanity checking.
newtype SanityM a = SanityM { unSanityM :: Either TypeError a }
  deriving (Monad, Functor, Applicative)

instance TypeErrorI SanityM where
  typeError = SanityM . Left
