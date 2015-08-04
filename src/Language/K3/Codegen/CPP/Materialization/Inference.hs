{-# LANGUAGE TypeOperators #-}

module Language.K3.Codegen.CPP.Materialization.Inference (
  inferMaterialization
) where

import Data.Foldable

import Control.Monad.Except
import Control.Monad.Identity
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Writer

import qualified Data.Map as M

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Provenance.Core (Provenance(..))

import Language.K3.Analysis.Provenance.Inference (PIEnv)
import Language.K3.Analysis.SEffects.Inference (FIEnv)

import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Hints

-- * Entry-Point
inferMaterialization :: K3 Expression -> K3 Expression
inferMaterialization = undefined

-- * Types

-- ** Monads
type InferT m = StateT IState (ReaderT IScope (WriterT IReport (ExceptT IError m)))
type InferM = InferT Identity

-- ** Non-scoping State
data IState = IState { cTable :: M.Map (Contextual Identifier) MExpr }

-- ** Scoping state
data IScope = IScope { downstreams :: [Downstream], nearestBind :: UID, pEnv :: PIEnv, fEnv :: FIEnv }

-- ** Reporting
type IReport = ()

-- ** Errors
newtype IError = IError String

-- * Helpers
newtype Contextual a = Contextual (a, Maybe UID)
type Downstream = Contextual (K3 Expression)

eUID :: K3 Expression -> InferM UID
eUID e = maybe (throwError $ IError "Invalid UID") (\(EUID u) -> return u) (e @~ isEUID)

eProv :: K3 Expression -> InferM (K3 Provenance)
eProv e = maybe (throwError $ IError "Invalid Provenance") (\(EProvenance p) -> return p) (e @~ isEProvenance)

contextualizeNow :: a -> InferM (Contextual a)
contextualizeNow a = asks nearestBind >>= \n -> return $ Contextual (a, n)

withDownstreams :: [Downstream] -> InferM a -> InferM a
withDownstreams nds m = local (\s -> s { downstreams = nds ++ downstreams s }) m

bindPoint :: Contextual (K3 Provenance) -> InferM (Maybe Juncture)
bindPoint (Contextual (p, u)) = case tag p of
  PFVar i | Just u' <- u -> return $ Just $ Juncture (u', i)
  _ -> return Nothing

-- * Inference Algorithm

-- Nothing special to do at declarations, proxy to expressions.
-- TODO: Add constraint between decision for declaration and root expression of initializer.
materializeD :: K3 Declaration -> InferM ()
materializeD d = case tag d of
  DGlobal _ _ (Just e) -> materializeE e
  DTrigger _ _ e -> materializeE e
  _ -> traverse_ materializeD (children d)

materializeE :: K3 Expression -> InferM ()
materializeE e = case tag e of
  _ -> traverse_ materializeE (children e)

-- * Queries
hasRead :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
hasRead = undefined

hasWrite :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
hasWrite = undefined

occursIn :: Contextual (K3 Provenance) -> Contextual (K3 Provenance) -> InferM (K3 MPred)
occursIn = undefined
