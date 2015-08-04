{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TypeOperators #-}

module Language.K3.Codegen.CPP.Materialization.Inference (
  inferMaterialization
) where

import qualified Data.List as L

import Data.Foldable
import Data.Maybe
import Data.Traversable
import Data.Tree

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

import Language.K3.Analysis.Provenance.Core (Provenance(..), PMatVar(..))

import Language.K3.Analysis.Provenance.Inference (PIEnv)
import Language.K3.Analysis.SEffects.Inference (FIEnv)

import Language.K3.Codegen.CPP.Materialization.Constructors
import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Hints

-- * Entry-Point
inferMaterialization :: (PIEnv, FIEnv) -> K3 Declaration -> IO (Either String (K3 Declaration))
inferMaterialization (p, f) d = case runInferM (materializeD d) defaultIState defaultIScope of
  Left (IError msg) -> return $ Left msg
  Right ((_, _), r) -> formatIReport r >> return (Right d)
 where
  defaultIState = IState { cTable = M.empty }
  defaultIScope = IScope { downstreams = [], nearestBind = Nothing, pEnv = p, fEnv = f }

-- * Types

-- ** Monads
type InferT m = StateT IState (ReaderT IScope (WriterT [IReport] (ExceptT IError m)))
type InferM = InferT Identity

runInferM :: InferM a -> IState -> IScope -> Either IError ((a, IState), [IReport])
runInferM m st sc = runIdentity $ runExceptT $ runWriterT $ flip runReaderT sc $ runStateT m st

-- ** Non-scoping State
data IState = IState { cTable :: M.Map Juncture (K3 MExpr) }

constrain :: UID -> Identifier -> K3 MExpr -> InferM ()
constrain u i m = let j = Juncture (u, i) in logR j m >> modify (\s -> s { cTable = M.insert j m (cTable s) })

-- ** Scoping state
data IScope = IScope { downstreams :: [Downstream], nearestBind :: Maybe UID, pEnv :: PIEnv, fEnv :: FIEnv }

-- ** Reporting
data IReport = IReport { juncture :: Juncture, constraint :: K3 MExpr }

logR :: Juncture -> K3 MExpr -> InferM ()
logR j m = tell [IReport { juncture = j, constraint = m }]

formatIReport :: [IReport] -> IO ()
formatIReport = traverse_ $ \ir -> do
  putStrLn $ "At juncture " ++ show (juncture ir) ++ ": constrained " ++ show (constraint ir)

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
