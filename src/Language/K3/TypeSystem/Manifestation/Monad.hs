{-# LANGUAGE GeneralizedNewtypeDeriving, TupleSections, TemplateHaskell #-}

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

, askDictionary
, askBoundType

, tryVisitVar
, catchVarUse
, nameOpaque
, getNamedOpaques
, clearNamedOpaques
, usingBoundType
, dualizeBoundType
, typeComputationCache
) where

import Control.Applicative
import Control.Monad.RWS
import Data.Char (ord,chr)
import qualified Data.Map as Map
import Data.Map (Map)
import qualified Data.Set as Set
import Data.Set (Set)

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Type
import Language.K3.TypeSystem.Data
import Language.K3.TypeSystem.Manifestation.Data
import Language.K3.Utils.Logger
import Language.K3.Utils.Pretty

$(loggingFunctions)

-- * Monad definitions

-- |A type for the monad under which manifestation occurs.
newtype ManifestM a
  = ManifestM
      { unManifestM :: RWS ManifestEnviron ManifestLog ManifestState a
      }
  deriving ( Monad, Functor, Applicative, MonadState ManifestState
           , MonadReader ManifestEnviron, MonadWriter ManifestLog)
  
-- |Executes a type manifesting computation.
runManifestM :: BoundType -> BoundDictionary -> ManifestM a -> a
runManifestM bt dict x =
  let initialEnviron = ManifestEnviron { envDict = dict
                                       , envBoundType = bt
                                       , visitedVariables = Set.empty } in
  let initialState = ManifestState { unusedNames = initialNames
                                   , variableNameMap = Map.empty
                                   , opaqueNameMap = Map.empty
                                   , resultCache = Map.empty } in
  let (result,_,_) = runRWS (unManifestM x) initialEnviron initialState in
  result
  
-- |A structure for the manifestation environment.
data ManifestEnviron
  = ManifestEnviron
      { envDict :: BoundDictionary
      , envBoundType :: BoundType
      , visitedVariables :: Set AnyTVar
      }

-- |A structure for the generated manifestation info.
data ManifestLog
  = ManifestLog
      { usedVariables :: Set AnyTVar
      }

instance Monoid ManifestLog where
  mempty = ManifestLog { usedVariables = Set.empty }
  mappend (ManifestLog a) (ManifestLog a') =
    ManifestLog (a `Set.union` a') 

-- |A structure for the manifestation state.
data ManifestState
  = ManifestState
      { unusedNames :: [String]
      , variableNameMap :: Map AnyTVar String
      , opaqueNameMap :: Map OpaqueVar String
      , resultCache :: Map AnyTVar (K3 Type)
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
askDictionary :: ManifestM BoundDictionary
askDictionary = envDict <$> ask

-- |Retrieves the bound type in context.
askBoundType :: ManifestM BoundType
askBoundType = envBoundType <$> ask

-- |Attempts to bind a variable for mu recursion.  If the variable has already
--  been bound in the environment, it is marked as used and the @alreadyDefined@
--  computation runs.  If it has not yet been bound, it is marked as bound and
--  the @justDefined@ computation runs.
tryVisitVar :: AnyTVar
            -> (Identifier -> ManifestM a)
                 -- ^Accepts the variable name for the definition.
            -> ManifestM a -- ^Runs if the variable was not yet bound.
            -> ManifestM a -- ^The result
tryVisitVar var alreadyDefined justDefined = do
  visitedVars <- visitedVariables <$> ask
  if Set.member var visitedVars
    then do
      name <- getVarName var
      tell ManifestLog{ usedVariables = Set.singleton var }
      alreadyDefined name
    else
      local defVar justDefined
  where
    defVar :: ManifestEnviron -> ManifestEnviron
    defVar e = e { visitedVariables = Set.insert var $ visitedVariables e }
    getVarName :: AnyTVar -> ManifestM Identifier
    getVarName var' = do
      s <- get
      case Map.lookup var' $ variableNameMap s of
        Just name -> return name
        Nothing -> do
          let name:rest = unusedNames s
          put s{ unusedNames = rest
               , variableNameMap = Map.insert var' name $ variableNameMap s }
          return name

-- |Determines whether a given variable signature was used or not.  If it was
--  used, the return value contains the name it was assigned; otherwise, the
--  return value contains a @Nothing@.
catchVarUse :: AnyTVar -> ManifestM a -> ManifestM (a, Maybe Identifier)
catchVarUse var x = do
  (v,w) <- ManifestM $ listen (unManifestM x)
  name <- if Set.member var $ usedVariables w
            then Map.lookup var <$> variableNameMap <$> get
            else return Nothing
  return (v, name)
  
-- |Defines a name for the provided opaque variable if one does not already
--  exist.  In either case, returns the name for the given opaque variable.
nameOpaque :: OpaqueVar -> ManifestM Identifier
nameOpaque oa = do
  s <- get
  case Map.lookup oa $ opaqueNameMap s of
    Just name -> return name
    Nothing -> do
      let newName:rest = unusedNames s
      put s{ unusedNames = rest
           , opaqueNameMap = Map.insert oa newName $ opaqueNameMap s }
      return newName

-- |Retrieves all declared opaque variables.
getNamedOpaques :: ManifestM (Map OpaqueVar Identifier)
getNamedOpaques = opaqueNameMap <$> get

-- |Clears all declared opaque variables.
clearNamedOpaques :: ManifestM ()
clearNamedOpaques = do
  s <- get
  put s{ opaqueNameMap = Map.empty }
  
-- |Performs a computation under the current monad but using a specific bound
--  type.
usingBoundType :: BoundType -> ManifestM a -> ManifestM a
usingBoundType bt x = do
  env <- ask
  let env' = env { envBoundType = bt }
  local (const env') x

-- |Performs a computation in the dual bound type.  This is used when a type
--  must be constructed in a contravariant position.
dualizeBoundType :: ManifestM a -> ManifestM a
dualizeBoundType x = do
  bt <- getDualBoundType <$> envBoundType <$> ask
  usingBoundType bt x

-- |Either performs a computation or retrieves a cached version based upon the
--  provided variable signature.
typeComputationCache :: AnyTVar
                     -> ManifestM (K3 Type)
                     -> ManifestM (K3 Type)
typeComputationCache var x = do
  cache <- resultCache <$> get
  case Map.lookup var cache of
    Just result -> do
      _debug $ boxToString $
        ["Manifestation cache hit: "] %+ prettyLines var %+ [" => "] %+
          prettyLines result 
      return result
    Nothing -> do
      _debug $ boxToString $
        ["Manifestation cache miss: "] %+ prettyLines var
      result <- x
      s <- get
      put $ s {resultCache = Map.insert var result $ resultCache s}
      return result
