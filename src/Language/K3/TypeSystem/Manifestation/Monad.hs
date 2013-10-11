{-# LANGUAGE GeneralizedNewtypeDeriving #-}

{-|
  A module defining a monad (and associated data types) for type manifestation.
  In particular, this monad is used for managing how mu-recursive type
  structures are defined.  Each time a group of type variables is inspected,
  they are used with the bounding direction to produce a "signature".  If the
  same signature appears twice in the same path on the type tree, a mu-recursive
  variable is declared to represent that signature.
-}

module Language.K3.TypeSystem.Manifestation.Monad
( ManifestM(..)
, runManifestM

, askConstraints
, askBoundType
, envQuery

, tryBindSig
, catchSigUse
, dualizeBoundType
) where

import Control.Applicative
import Control.Monad.RWS
import Data.Char (ord,chr)
import qualified Data.Map as Map
import Data.Map (Map)
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.Data

-- * Monad definitions

-- |A type for the monad under which manifestation occurs.
newtype ManifestM a
  = ManifestM
      { unManifestM :: RWS ManifestEnviron ManifestLog ManifestState a
      }
  deriving ( Monad, Functor, Applicative, MonadState ManifestState
           , MonadReader ManifestEnviron, MonadWriter ManifestLog)
  
-- |Executes a type manifesting computation.
runManifestM :: BoundType -> ConstraintSet -> ManifestM a -> a
runManifestM bt cs x =
  let initialEnviron = ManifestEnviron { envConstraints = cs
                                       , envBoundType = bt
                                       , definedSignatures = Set.empty } in
  let initialState = ManifestState { unusedNames = initialNames
                                   , variableNameMap = Map.empty } in
  let (result, _, _) = runRWS (unManifestM x) initialEnviron initialState in
  result
  
-- |A structure which defines the identifier used when creating bindings for
--  mu-recursive types.
data VariableSignature = VariableSignature (Set AnyTVar) DelayedOperationTag
  deriving (Eq, Ord, Show)

-- |A structure for the manifestation environment.
data ManifestEnviron
  = ManifestEnviron
      { envConstraints :: ConstraintSet
      , envBoundType :: BoundType
      , definedSignatures :: Set VariableSignature
      }

-- |A structure for the generated manifestation info.
data ManifestLog
  = ManifestLog
      { usedSignatures :: Set VariableSignature
      }

instance Monoid ManifestLog where
  mempty = ManifestLog { usedSignatures = Set.empty }
  mappend (ManifestLog a) (ManifestLog a') =
    ManifestLog (a `Set.union` a') 

-- |A structure for the manifestation state.
data ManifestState
  = ManifestState
      { unusedNames :: [String]
      , variableNameMap :: Map VariableSignature String
      }

initialNames :: [String]
initialNames = iterate nextName "a"
  where
    nextName = reverse . advanceStr . reverse
    advanceChar c = if c == 'z' then ('a', True) else (chr $ 1 + ord c, False)
    advanceStr s = case s of
                    [] -> "a"
                    h:t -> let (h',b) = advanceChar h in
                           h' : if b then advanceStr t else t
                           
-- * Monad operations

-- |Retrieves the constraints in context.
askConstraints :: ManifestM ConstraintSet
askConstraints = envConstraints <$> ask

-- |Retrieves the bound type in context.
askBoundType :: ManifestM BoundType
askBoundType = envBoundType <$> ask

-- |Runs a query on the in-context constraint set.
envQuery :: (Ord r) => ConstraintSetQuery r -> ManifestM [r]
envQuery query = (`csQuery` query) <$> askConstraints

-- |Attempts to bind a variable for mu recursion.  If the variable has already
--  been bound in the environment, it is marked as used and the @alreadyDefined@
--  computation runs.  If it has not yet been bound, it is marked as bound and
--  the @justDefined@ computation runs.
tryBindSig :: VariableSignature
           -> (String -> ManifestM a)
                -- ^Accepts the variable name for the definition.
           -> ManifestM a -- ^Runs if the variable was not yet bound.
           -> ManifestM a -- ^The result
tryBindSig sig alreadyDefined justDefined = do
  dsigs <- definedSignatures <$> ask
  if Set.member sig dsigs
    then do
      name <- getVarName sig
      tell ManifestLog{ usedSignatures = Set.singleton sig }
      alreadyDefined name
    else
      local defSig justDefined
  where
    defSig :: ManifestEnviron -> ManifestEnviron
    defSig e = e { definedSignatures = Set.insert sig $ definedSignatures e }
    getVarName :: VariableSignature -> ManifestM String
    getVarName sig' = do
      s <- get
      case Map.lookup sig' $ variableNameMap s of
        Just name -> return name
        Nothing -> do
          let name:rest = unusedNames s
          put s{ unusedNames = rest
               , variableNameMap = Map.insert sig' name $ variableNameMap s }
          return name

-- |Determines whether a given variable was used or not.
catchSigUse :: VariableSignature -> ManifestM a -> ManifestM (a, Bool)
catchSigUse sig x = do
  (v,w) <- ManifestM $ listen (unManifestM x)
  tell w
  return (v, Set.member sig $ usedSignatures w)

-- |Performs a computation in the dual bound type.  This is used when a type
--  must be constructed in a contravariant position.
dualizeBoundType :: ManifestM a -> ManifestM a
dualizeBoundType x = do
  env <- ask
  let env' = env { envBoundType = getDualBoundType $ envBoundType env}
  local (const env') x
