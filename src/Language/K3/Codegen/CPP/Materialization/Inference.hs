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

import Text.Printf

import qualified Data.IntMap as I
import qualified Data.Map as M

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Provenance.Core (Provenance(..), PMatVar(..), PPtr(..))

import Language.K3.Analysis.Provenance.Inference (PIEnv, ppenv)
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
formatIReport = traverse_ $ \(IReport j m) -> do
  printf "Juncture %d/%s: %s\n" (gUID $ fst $ jLoc j) (snd $ jLoc j) (simpleShowE m)

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

chasePPtr :: PPtr -> InferM (K3 Provenance)
chasePPtr p = do
  ppEnv <- asks $ ppenv . pEnv
  case I.lookup p ppEnv of
    Nothing -> throwError $ IError "Invalid pointer in provenance chase"
    Just p' -> return p'

bindPoint :: Contextual (K3 Provenance) -> InferM (Maybe Juncture)
bindPoint (Contextual (p, u)) = case tag p of
  PFVar i | Just u' <- u -> return $ Just $ Juncture (u', i)
  PBVar (PMatVar i u' _) -> return $ Just $ Juncture (u', i)
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
materializeE e@(Node (t :@: _) cs) = case t of
  ERecord is -> do
    u <- eUID e

    -- Each field decision for a record needs to be made in the context where each of the subsequent
    -- children of the record are downstreams for the current field.
    for_ (zip3 is cs (tail $ L.tails cs)) $ \(i, c, os) -> do
      os' <- traverse contextualizeNow os
      withDownstreams os' $ do
        materializeE c

        -- Determine if the field argument is moveable within the current expression.
        moveableNow <- eProv c >>= contextualizeNow >>= isMoveableNow

        -- Determine if the field argument is owned by the containing expression.
        bindingContext <- eProv c >>= contextualizeNow >>= bindPoint
        let moveableInContext = maybe (mBool True) (\bc -> mOneOf (mVar bc) [Moved, Copied]) bindingContext

        constrain u i $ mITE (moveableNow -&&- moveableInContext) (mAtom Moved) (mAtom Copied)

  _ -> traverse_ materializeE (children e)

-- * Queries
hasReadIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred, K3 MPred)
hasReadIn (Contextual (tag -> PFVar _, cp)) (Contextual (_, ce)) | cp /= ce = return (mBool False, mBool False)
hasReadIn (Contextual (p, cp)) (Contextual (e, ce)) = case tag e of
  ELambda _ -> do
    cls <- ePrv e >>= \case
      (tag -> PLambda _ cls) -> return cls
      _ -> do
        u <- eUID e
        throwError $ IError $ printf "Expected lambda provenance on lambda expression at UID %s." (show u)

    -- In order for a write effect on `p` to occur at this lambda node, the following must hold true
    -- for at least one closure variable `c`: `p` must occur in `c`, and `c` must be materialized by
    -- either a copy or a move.
    closurePs <- for cls $ \m@(PMatVar n u _) -> do
      occurs <- occursIn (Contextual (p, cp)) (Contextual ((pbvar m), ce))
      return $ occurs -&&- mOneOf (mVar $ Juncture (u, n)) [Copied, Moved]
    return (foldr (-||-) (mBool False) closurePs, mBool False)

  EOperate OApp -> do
    let [f, x] = children e
    (fehr, fihr) <- hasReadIn (Contextual (p, cp)) (Contextual (f, ce))
    (xehr, xihr) <- hasReadIn (Contextual (p, cp)) (Contextual (x, ce))

hasWrite :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
hasWrite = undefined

occursIn :: Contextual (K3 Provenance) -> Contextual (K3 Provenance) -> InferM (K3 MPred)
occursIn = undefined

isGlobalP :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isGlobalP (Contextual (p, k)) = case tag p of
  (PGlobal _) -> return (mBool True)
  (PBVar (PMatVar n u p)) -> do
    -- TODO: k isn't the right context for the recursive call to isGlobalP. What is?
    parent <- chasePPtr p >>= \p' -> isGlobalP (Contextual (p', k))
    return $ mOneOf (mVar (Juncture (u, n))) [Referenced, ConstReferenced] -&&- parent
  -- (tag &&& children -> (PProject _, [pp])) -> isGlobalP pp
  --   _ -> return False

isMoveable :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveable (Contextual (p, c)) = undefined

isMoveableIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
isMoveableIn (Contextual (p, cp)) (Contextual (e, ce)) = undefined

isMoveableNow :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveableNow cp = do
  ds <- asks downstreams
  isMoveable1 <- isMoveable cp
  allMoveable <- foldr (-&&-) (mBool True) <$> traverse (isMoveableIn cp) ds
  return $ isMoveable1 -&&- allMoveable
