{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Materialization.Inference (
  optimizeMaterialization
) where

import qualified Data.List as L

import Data.Foldable
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
import qualified Data.Set as S

import Language.K3.Core.Common

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression

import Language.K3.Analysis.Provenance.Core (Provenance(..), PMatVar(..), PPtr)
import Language.K3.Analysis.Provenance.Constructors (pbvar, pfvar)

import Language.K3.Analysis.SEffects.Core (Effect(..))

import Language.K3.Analysis.Provenance.Inference (PIEnv, ppenv)
import Language.K3.Analysis.SEffects.Inference (FIEnv)

import Language.K3.Codegen.CPP.Materialization.Constructors
import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Hints
import Language.K3.Codegen.CPP.Materialization.Common

import Language.K3.Utils.Pretty

-- * Entry-Point

optimizeMaterialization :: (PIEnv, FIEnv) -> K3 Declaration -> IO (Either String (K3 Declaration))
optimizeMaterialization (p, f) d = runExceptT $ inferMaterialization >>= solveMaterialization >>= attachMaterialization d
 where
  inferMaterialization = case runInferM (materializeD d) defaultIState defaultIScope of
    Left (IError msg) -> throwError msg
    Right ((_, IState ct), r) -> liftIO (formatIReport reportVerbosity r) >> return ct
   where defaultIState = IState { cTable = M.empty }
         defaultIScope = IScope { downstreams = [], nearestBind = Nothing, pEnv = p, fEnv = f }

  solveMaterialization ct = case runSolverM solveAction defaultSState of
    Left (SError msg) -> throwError msg
    Right (_, SState mp) -> return mp
   where
    solveAction = (mkDependencyList ct >>= traverse_ (\k -> solveForE (ct M.! k) >>= setMethod k))

  attachMaterialization k m = return $ attachD <$> k
   where
     attachD g = case g of
       DGlobal i t (Just e) :@: as -> (DGlobal i t (Just $ attachE <$> e)) :@: as
       DTrigger i t e :@: as -> (DTrigger i t (attachE <$> e)) :@: as
       t :@: as -> t :@: as

     attachE e = case e of
       (t :@: as) -> (t :@: (EMaterialization as' : as))
      where
        Just u = e @~ isEUID >>= getUID
        as' = M.fromList [((i, r), q) | ((Juncture u' i, r), q) <- M.toList m, u' == u]

-- * Types

-- ** Monads
type InferT m = StateT IState (ReaderT IScope (WriterT [IReport] (ExceptT IError m)))
type InferM = InferT Identity

runInferM :: InferM a -> IState -> IScope -> Either IError ((a, IState), [IReport])
runInferM m st sc = runIdentity $ runExceptT $ runWriterT $ flip runReaderT sc $ runStateT m st

-- ** Non-scoping State
data IState = IState { cTable :: M.Map DKey (K3 MExpr) }

type DKey = (Juncture, Direction)

constrain :: UID -> Identifier -> Direction -> K3 MExpr -> InferM ()
constrain u i d m = let j = (Juncture u i) in logR j d m >> modify (\s -> s { cTable = M.insert (j, d) m (cTable s) })

-- ** Scoping state
data IScope = IScope { downstreams :: [Downstream], nearestBind :: Maybe UID, pEnv :: PIEnv, fEnv :: FIEnv }

data Contextual a = Contextual a (Maybe UID) deriving (Eq, Ord, Read, Show)
type Downstream = Contextual (K3 Expression)

contextualizeNow :: a -> InferM (Contextual a)
contextualizeNow a = asks nearestBind >>= \n -> return $ Contextual a n

withDownstreams :: [Downstream] -> InferM a -> InferM a
withDownstreams nds = local (\s -> s { downstreams = nds ++ downstreams s })

withNearestBind :: UID -> InferM a -> InferM a
withNearestBind u = local (\s -> s { nearestBind = Just u })

-- ** Reporting
data IReport = IReport { juncture :: Juncture, direction :: Direction, constraint :: K3 MExpr }

logR :: Juncture -> Direction -> K3 MExpr -> InferM ()
logR j d m = tell [IReport { juncture = j, direction = d, constraint = m }]

data ReportVerbosity = None | Short | Long

reportVerbosity :: ReportVerbosity
reportVerbosity = Long

formatIReport :: ReportVerbosity -> [IReport] -> IO ()
formatIReport rv ir = do
  putStrLn "--- Begin Materialization Inference Report ---"
  for_ ir $ \(IReport (Juncture u i) d m) -> case reportVerbosity of
    None -> return ()
    Short -> printf "J%d/%s/%s: %s\n" (gUID u) i (show d) (ppShortE m)
    Long -> printf "--- Juncture %d/%s/%s:\n%s" (gUID u) i (show d) (pretty m)
  putStrLn "--- End Materialization Inference Report ---"

-- ** Errors
newtype IError = IError String

-- * Helpers

anon :: Identifier
anon = "!"

eUID :: K3 Expression -> InferM UID
eUID e = maybe (throwError $ IError "Invalid UID") (\(EUID u) -> return u) (e @~ isEUID)

ePrv :: K3 Expression -> InferM (K3 Provenance)
ePrv e = maybe (throwError $ IError "Invalid Provenance") (\(EProvenance p) -> return p) (e @~ isEProvenance)

eEff :: K3 Expression -> InferM (K3 Effect)
eEff e = maybe (throwError $ IError "Invalid Provenance") (\(ESEffect f) -> return f) (e @~ isESEffect)

chasePPtr :: PPtr -> InferM (K3 Provenance)
chasePPtr p = do
  ppEnv <- asks $ ppenv . pEnv
  case I.lookup p ppEnv of
    Nothing -> throwError $ IError "Invalid pointer in provenance chase"
    Just p' -> return p'

bindPoint :: Contextual (K3 Provenance) -> InferM (Maybe Juncture)
bindPoint (Contextual p u) = case tag p of
  PFVar i | Just u' <- u -> return $ Just $ Juncture u' i
  PBVar (PMatVar i u' _) -> return $ Just $ Juncture u' i
  PProject _ -> bindPoint (Contextual (head $ children p) u)
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
        moveableNow <- ePrv c >>= contextualizeNow >>= isMoveableNow

        ownContext <- ePrv c >>= contextualizeNow >>= bindPoint >>= \case
          Just (Juncture u i) -> return $ mOneOf (mVar u i In) [Moved, Copied] -??- "Owned by containing context?"
          Nothing -> return $ mBool True -??- "Temporary."

        constrain u i In $ mITE (moveableNow -&&- ownContext) (mAtom Moved) (mAtom Copied)

  ELambda i -> do
    let [body] = cs

    u <- eUID e

    (ci, cb) <- withNearestBind u $ do
      materializeE body
      ci' <- contextualizeNow (pfvar i)
      cb' <- contextualizeNow body
      return (ci', cb')

    fProv <- ePrv e >>= contextualizeNow
    nrvo <- occursIn ci fProv

    (ehw, _) <- hasWriteIn ci cb

    constrain u i In $ mITE ehw (mAtom Copied) (mAtom ConstReferenced)
    constrain u i Ex $ mITE nrvo (mAtom Moved) (mAtom Copied)

    cls <- ePrv e >>= \case
      (tag -> PLambda _ cls) -> return cls
      _ -> do
        u <- eUID e
        throwError $ IError $ printf "Expected lambda provenance on lambda expression at UID %s." (show u)

    for_ cls $ \m@(PMatVar name loc ptr) -> do
      needsOwn <- withNearestBind u $ do
        innerP <- contextualizeNow (pbvar m)
        (ehw, _) <- contextualizeNow body >>= hasWriteIn innerP
        return $ ehw -??- printf "Lambda needs ownership of closure variable %s?" name

      outerP <- chasePPtr ptr
      ownContext <- contextualizeNow outerP >>= bindPoint >>= \case
        Just (Juncture u i) -> return $ mOneOf (mVar u i In) [Moved, Copied] -??- "Owned by containing context?"
        Nothing -> return $ mBool True -??- "Temporary."

      constrain u name In $ mITE (needsOwn -&&- ownContext) (mAtom Moved) (mAtom Copied)

  EOperate OApp -> do
    let [f, x] = cs
    contextualizeNow x >>= \x' -> withDownstreams [x'] $ materializeE f
    materializeE x

    moveable <- ePrv x >>= contextualizeNow >>= isMoveableNow

    ownContext <- ePrv x >>= contextualizeNow >>= bindPoint >>= \case
      Just (Juncture u i) -> return $ mOneOf (mVar u i In) [Moved, Copied] -??- "Owned by containing context?"
      Nothing -> return $ mBool True -??- "Temporary"

    u <- eUID e
    constrain u anon In $ mITE (moveable -&&- ownContext) (mAtom Moved) (mAtom Copied)

  EOperate OSnd -> do
    let [h, m] = cs
    contextualizeNow m >>= \m' -> withDownstreams [m'] $ materializeE h
    materializeE m

    moveable <- ePrv m >>= contextualizeNow >>= isMoveableNow
    ownContext <- ePrv m >>= contextualizeNow >>= bindPoint >>= \case
      Just (Juncture u i) -> return $ mOneOf (mVar u i In) [Moved, Copied]
      Nothing -> return $ mBool True

    u <- eUID e
    constrain u anon In $ mITE (moveable -&&- ownContext) (mAtom Moved) (mAtom Copied)

  EOperate _ -> case cs of
    [x] -> materializeE x
    [x, y] -> contextualizeNow y >>= \y' -> withDownstreams [y'] (materializeE x) >> materializeE y
    _ -> throwError $ IError "Unreachable: More than two operators for operator."


  EBindAs binder -> do
    let [initB, body] = cs
    contextualizeNow body >>= \body' -> withDownstreams [body'] (materializeE initB)
    materializeE body

    bindings <- ePrv e >>= \case
      (tag -> PMaterialize bindings) -> return bindings
      _ -> throwError $ IError "Unknown provenance form on EBindAs."

    sourceMoveableNow <- ePrv initB >>= contextualizeNow >>= isMoveableNow
    sourceOwnContext <- ePrv initB >>= contextualizeNow >>= bindPoint >>= \case
      Just (Juncture u i) -> return $ mOneOf (mVar u i In) [Moved, Copied] -??- "Owned by containing context?"
      Nothing -> return $ mBool True -??- "Temporary."

    let sourceMoveable = sourceMoveableNow -&&- sourceOwnContext

    u <- eUID e

    for_ bindings $ \m@(PMatVar name loc ptr) -> do
      cpBody <- contextualizeNow body
      cpBinding <- contextualizeNow (pbvar m)
      (ehw, _) <- hasWriteIn cpBinding cpBody

      let bindNeedsOwn = ehw
      constrain u name In $ mITE bindNeedsOwn (mITE sourceMoveable (mAtom Moved) (mAtom Copied)) (mAtom Referenced)
      constrain u name Ex $ mITE (mOneOf (mVar u name In) [Copied, Moved]) (mAtom Moved) (mAtom Referenced)


  ELetIn i -> do
    let [initB, body] = cs
    contextualizeNow body >>= \body' -> withDownstreams [body'] (materializeE initB)
    materializeE body

    u <- eUID e
    constrain u i In $ mAtom Referenced -??- "Default let materialization strategy."

  ECaseOf _ -> do
    let [initB, some, none] = cs
    some' <- contextualizeNow some
    none' <- contextualizeNow none

    withDownstreams [some', none'] $ materializeE initB
    materializeE some
    materializeE none

  EIfThenElse -> do
    let [cond, thenB, elseB] = cs
    thenB' <- contextualizeNow thenB
    elseB' <- contextualizeNow elseB

    withDownstreams [thenB', elseB'] $ materializeE cond
    materializeE thenB
    materializeE elseB

  _ -> traverse_ materializeE (children e)

-- * Queries
hasReadIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred, K3 MPred)
hasReadIn (Contextual (tag -> PFVar _) cp) (Contextual _ ce) | cp /= ce = return (mBool False, mBool False)
hasReadIn (Contextual p cp) (Contextual e ce) = case tag e of
  ELambda _ -> do
    cls <- ePrv e >>= \case
      (tag -> PLambda _ cls) -> return cls
      _ -> do
        u <- eUID e
        throwError $ IError $ printf "Expected lambda provenance on lambda expression at UID %s." (show u)

    -- In order for a read effect on `p` to occur at this lambda node, the following must hold true
    -- for at least one closure variable `c`: `p` must occur in `c`, and `c` must be materialized by
    -- either a copy or a move.
    closurePs <- for cls $ \m@(PMatVar n u _) -> do
      occurs <- occursIn (Contextual p cp) (Contextual (pbvar m) ce)
      return $ occurs -&&- mOneOf (mVar u n In) [Copied, Moved] -??- "Owned by closure?"
    return (mBool False, foldr (-||-) (mBool False) closurePs)

  EOperate OApp -> do
    let [f, x] = children e
    (fehr, fihr) <- hasReadIn (Contextual p cp) (Contextual f ce)
    (xehr, xihr) <- hasReadIn (Contextual p cp) (Contextual x ce)

    u <- eUID e
    xp <- ePrv x

    occurs <- occursIn (Contextual p cp) (Contextual xp ce)

    let aihr = occurs -&&- mOneOf (mVar u anon In) [Copied, Moved]
    return (fehr -||- xehr, fihr -||- xihr -||- aihr)

  _ -> do
    eff <- eEff e
    ehr <- hasReadInF (Contextual p cp) (Contextual eff ce)
    return (ehr, mBool False)

hasReadInF :: Contextual (K3 Provenance) -> Contextual (K3 Effect) -> InferM (K3 MPred)
hasReadInF p (Contextual f cf) = case f of
  (tag -> FRead p') -> occursIn p (Contextual p' cf)
  (tag -> FScope _) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $ children f)
  (tag -> FSeq) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $ children f)
  (tag -> FSet) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $ children f)
  _ -> return (mBool False)

hasWriteIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred, K3 MPred)
hasWriteIn (Contextual (tag -> PFVar _) cp) (Contextual _ ce) | cp /= ce = return (mBool False, mBool False)
hasWriteIn (Contextual p cp) (Contextual e ce) = case tag e of
  ELambda _ -> do
    cls <- ePrv e >>= \case
      (tag -> PLambda _ cls) -> return cls
      _ -> do
        u <- eUID e
        throwError $ IError $ printf "Expected lambda provenance on lambda expression at UID %s." (show u)

    -- In order for a write effect on `p` to occur at this lambda node, the following must hold true
    -- for at least one closure variable `c`: `p` must occur in `c`, and `c` must be materialized by
    -- a move.
    closurePs <- for cls $ \m@(PMatVar n u _) -> do
      occurs <- occursIn (Contextual p cp) (Contextual (pbvar m) ce)
      return $ occurs -&&- mOneOf (mVar u n In) [Moved]
    return (mBool False, foldr (-||-) (mBool False) closurePs)

  EOperate OApp -> do
    let [f, x] = children e
    (fehw, fihw) <- hasWriteIn (Contextual p cp) (Contextual f ce)
    (xehw, xihw) <- hasWriteIn (Contextual p cp) (Contextual x ce)

    u <- eUID e
    xp <- ePrv x

    occurs <- occursIn (Contextual p cp) (Contextual xp ce)

    let aihw = occurs -&&- mOneOf (mVar u anon In) [Moved]

    aEff <- eEff e
    aehw <- hasWriteInF (Contextual p cp) (Contextual aEff ce)

    return ( mOr [ fehw -??- "Explicit write in function?"
                 , xehw -??- "Explicit write in argument?"
                 , aehw -??- "Explicit write in application?"
                 ]
           , mOr [ fihw -??- "Implicit write in function?"
                 , xihw -??- "Implicit write in argument?"
                 , aihw -??- "Implicit write in application?"
                 ]
           )

  _ -> do
    eff <- eEff e
    ehw <- hasWriteInF (Contextual p cp) (Contextual eff ce)
    return (ehw, mBool False)

hasWriteInF :: Contextual (K3 Provenance) -> Contextual (K3 Effect) -> InferM (K3 MPred)
hasWriteInF p (Contextual f cf) = case f of
  (tag -> FWrite p') -> occursIn p (Contextual p' cf)
  (tag -> FScope _) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $ children f)
  (tag -> FSeq) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $ children f)
  (tag -> FSet) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $ children f)
  _ -> return (mBool False)

isGlobal :: K3 Provenance -> InferM (K3 MPred)
isGlobal p = case tag p of
  (PGlobal _) -> return (mBool True)
  (PBVar (PMatVar n u ptr)) -> do
    parent <- chasePPtr ptr >>= isGlobal
    return $ mOneOf (mVar u n In) [Referenced, ConstReferenced] -&&- parent
  (PProject _) -> isGlobal (head $ children p)
  _ -> return $ mBool False

occursIn :: Contextual (K3 Provenance) -> Contextual (K3 Provenance) -> InferM (K3 MPred)
occursIn a@(Contextual pa ca) b@(Contextual pb cb) = case tag pb of
  PFVar i -> case tag pa of
    PFVar j | i == j && ca == cb -> return (mBool True)
    _ -> return (mBool False)
  PBVar (PMatVar n u ptr) -> case tag pa of
    PBVar (PMatVar n' u' _) | n' == n && u' == u -> return (mBool True)
    _ -> do
      pOccurs <- chasePPtr ptr >>= \p' -> occursIn a (Contextual p' cb)
      return $ mOneOf (mVar u n In) [Referenced, ConstReferenced] -&&- pOccurs
  _ -> return (mBool False)

isMoveable :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveable (Contextual p _) = isGlobal p

isMoveableIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
isMoveableIn cp ce = do
  (ehr, ihr) <- hasReadIn cp ce
  (ehw, ihw) <- hasWriteIn cp ce

  eu <- let (Contextual e _) = ce in eUID e
  return $ foldr1 (-||-) ([ ehr -??- (printf "Explicit reads in downstream %d?" (gUID eu))
                          , ihr -??- (printf "Implicit reads in downstream %d?" (gUID eu))
                          , ehw -??- (printf "Explicit writes in downstream %d?" (gUID eu))
                          , ihw -??- (printf "Explicit writes in downstream %d?" (gUID eu))
                          ] :: [K3 MPred]) -??- printf "Moveable in downstream %d?" (gUID eu)

isMoveableNow :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveableNow cp = do
  ds <- asks downstreams
  isMoveable1 <- isMoveable cp
  allMoveable <- foldr (-&&-) (mBool True) <$> traverse (isMoveableIn cp) ds
  return $ (isMoveable1 -??- "Global?" -&&- allMoveable -??- "Moveable in all downstreams?") -??- "Moveable?"

-- * Solver

type SolverT m = StateT SState (ExceptT SError m)
type SolverM a = SolverT Identity a

data SState = SState { assignments :: M.Map DKey Method }
newtype SError = SError String

defaultSState :: SState
defaultSState = SState []

runSolverM :: SolverM a -> SState -> Either SError (a, SState)
runSolverM m st = runIdentity $ runExceptT $ runStateT m st

setMethod :: DKey -> Method -> SolverM ()
setMethod k m = modify $ \s -> s { assignments = M.insert k m (assignments s) }

getMethod :: DKey -> SolverM Method
getMethod k = gets assignments >>= maybe (throwError $ SError $ "Unconfirmed decision for " ++ show k) return . M.lookup k

-- ** Sorting
mkDependencyList :: M.Map DKey (K3 MExpr) -> SolverM [DKey]
mkDependencyList m = mkDependencySets m >>= topoSortWithDependencySets

mkDependencySets :: M.Map DKey (K3 MExpr) -> SolverM (M.Map DKey (S.Set DKey))
mkDependencySets = traverse findDependenciesE

findDependenciesE :: K3 MExpr -> SolverM (S.Set DKey)
findDependenciesE e = case tag e of
  MVar j d -> return [(j, d)]
  MAtom _ -> return []
  MIfThenElse p -> findDependenciesP p

findDependenciesP :: K3 MPred -> SolverM (S.Set DKey)
findDependenciesP p = case tag p of
  MOneOf e _ -> findDependenciesE e
  _ -> S.unions <$> traverse findDependenciesP (children p)

topoSortWithDependencySets :: M.Map DKey (S.Set DKey) -> SolverM [DKey]
topoSortWithDependencySets m
  | M.null m = return []
  | M.null rootSet = throwError $ SError $ "cycle in materialization dependencies." ++ show remaining ++ show missingVariables
  | otherwise = (M.keys rootSet ++) <$> topoSortWithDependencySets reducedMap
 where
  (rootSet, remaining) = M.partition S.null m
  reducedMap = M.map (S.\\ (M.keysSet rootSet)) remaining
  missingVariables = S.unions (M.elems m) S.\\ M.keysSet m

-- ** Solving
solveForE :: K3 MExpr -> SolverM Method
solveForE m = case tag m of
  MVar j d -> getMethod (j, d)
  MAtom a -> return a
  MIfThenElse p -> let [t, e] = children m in solveForP p >>= \r -> if r then solveForE t else solveForE e

solveForP :: K3 MPred -> SolverM Bool
solveForP p = case tag p of
  MNot -> let [x] = children p in not <$> solveForP x
  MAnd -> and <$> traverse solveForP (children p)
  MOr -> or <$> traverse solveForP (children p)
  MOneOf e m -> flip elem m <$> solveForE e
  MBool b -> return b
