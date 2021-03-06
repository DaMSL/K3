{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Codegen.CPP.Materialization.Inference (
  prepareInitialIState,
  optimizeMaterialization,
  MZFlags(..),
  IState(..),
  DKey,
  defaultIState,
  defaultMZFlags
) where

import GHC.Generics (Generic)
import Data.Binary (Binary)
import Data.Serialize (Serialize)

import qualified Data.Graph as G
import qualified Data.List as L

import Control.Arrow
import Control.DeepSeq

import Data.Foldable
import Data.Maybe (fromJust, fromMaybe, mapMaybe)
import Data.Traversable
import Data.Tree

import Debug.Trace

import Control.Monad.Except
import Control.Monad.Identity
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Writer

import Text.Printf

import qualified Data.IntMap as I
import qualified Data.HashMap.Strict as M
import qualified Data.Map.Strict as MM
import qualified Data.Set as S

import Language.K3.Core.Common
import Language.K3.Core.Utils

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Literal
import Language.K3.Core.Type

import Language.K3.Analysis.Provenance.Core (Provenance(..), PMatVar(..), PPtr)
import Language.K3.Analysis.Provenance.Constructors (pbvar, pfvar, ptemp)

import Language.K3.Analysis.SEffects.Core (Effect(..))

import Language.K3.Analysis.Provenance.Inference (PIEnv, ppenv)
import Language.K3.Analysis.SEffects.Inference (FIEnv)

import Language.K3.Codegen.CPP.Materialization.Constructors
import Language.K3.Codegen.CPP.Materialization.Core
import Language.K3.Codegen.CPP.Materialization.Hints
import Language.K3.Codegen.CPP.Materialization.Common

import Language.K3.Utils.Pretty

-- * Entry-Point

data MZFlags =
  MZFlags { isolateRuntimeMZ     :: !Bool
          , isolateApplicationMZ :: !Bool
          , isolateQueryMZ       :: !Bool
          }
 deriving (Eq, Generic, Ord, Read, Show)

defaultMZFlags :: MZFlags
defaultMZFlags = MZFlags { isolateRuntimeMZ = False, isolateApplicationMZ = False, isolateQueryMZ = False }

instance NFData MZFlags
instance Binary MZFlags
instance Serialize MZFlags

prepareInitialIState :: Bool -> K3 Declaration -> IState
prepareInitialIState dbg dr = IState (M.fromList initCTable) M.empty initNames

 where
  (initCTable, initNames) = foldl genHack ([], I.singleton anon anonS) $! children dr

  genHack (cacc, iacc) d@(tag -> DGlobal i _ _) = debugHack d $
    let p = hashJunctureName i in
    (cacc ++ [((Juncture (gUID d) p, In), (mAtom Referenced -??- "Hack"))], I.insert p i iacc)

  genHack acc _ = acc

  gUID d = let Just (DUID u) = d @~ isDUID in u
  gPrv d = maybe ("N",ptemp) (\(DProvenance provE) -> either (("U"),) (("I"),) provE) $! d @~ isDProvenance

  debugHack d r@([], _) = r
  debugHack d r@((last . fst) -> ((j, _), _)) =
    if dbg
      then let (a,p) = gPrv d
           in flip trace r $ boxToString $ ["Init IState " ++ a ++ " global " ++ show j]
                          %$ prettyLines p
      else r

  debugHack _ r = r

  hasPhaseBoundary :: K3 Declaration -> Maybe Identifier
  hasPhaseBoundary x = case x @~ isDProperty of
    Just (DProperty dp) -> flip onDProperty dp $! \case
      ("ExpiresAt", Just (tag -> LString mt)) -> Just mt
      _ -> Nothing
    _ -> Nothing

  getMaybePhaseBoundary :: K3 Declaration -> Maybe (Identifier, Identifier)
  getMaybePhaseBoundary x = case tag x of
    DGlobal i _ _ -> (i,) <$> hasPhaseBoundary x
    _ -> Nothing

optimizeMaterialization :: Bool -> MZFlags -> IState -> (PIEnv, FIEnv) -> K3 Declaration -> IO (Either String (K3 Declaration))
optimizeMaterialization dbg mzfs is (p, f) d = runExceptT $! inferMaterialization >>= solveMaterialization >>= attachMaterialization d
 where
  inferMaterialization = case runInferM (materializeD d) defaultIState defaultIScope of
    Left (IError msg) -> throwError msg
    Right ((_, IState ct _ ins), r) -> liftIO (formatIReport reportVerbosity r) >> (debugInfer ct $ return (ct, ins))
   where defaultIState = is
         defaultIScope = IScope { downstreams = []
                                , nearestBind = Nothing
                                , pEnv = p
                                , fEnv = f
                                , topLevel = False
                                , currentTrigger = Nothing
                                , flags = mzfs
                                }
         debugInfer ct r = if False
                            then flip trace r $ boxToString $ M.foldlWithKey' debugCTEntry ["Mat CT"] ct
                            else r

         debugCTEntry acc k v = acc ++ [show k ++ " => "] %$ (indent 2 $ prettyLines v)

  solveMaterialization (ct, ins) = case runSolverM solveAction defaultSState of
    Left (SError msg) -> throwError msg
    Right (_, SState mp) -> return (mp, ins)
   where
    solveAction = (mkDependencyList ct d >>= flip solveForAll ct)

  attachMaterialization k (m, ins) = return $! attachD <$> k
   where
     attachD g = case g of
       DGlobal i t (Just e) :@: as -> (DGlobal i t (Just $! attachE <$> e)) :@: as
       DTrigger i t e :@: as -> (DTrigger i t (attachE <$> e)) :@: as
       t :@: as -> t :@: as

     attachE e = case e of
       (EOperate OApp :@: as) -> (EOperate OApp :@: (EMaterialization as' : as))
       (t :@: as) -> (t :@: (EMaterialization as' : as))
      where
        Just u = e @~ isEUID >>= getUID
        as' = MM.fromList [((fromJust $! I.lookup i ins, r), q) | ((Juncture u' i, r), q) <- M.toList m, u' == u]

-- * Types

-- ** Monads
type InferT m = StateT IState (ReaderT IScope (WriterT [IReport] (ExceptT IError m)))
type InferM = InferT Identity

runInferM :: InferM a -> IState -> IScope -> Either IError ((a, IState), [IReport])
runInferM m st sc = runIdentity $! runExceptT $! runWriterT $! flip runReaderT sc $! runStateT m st

-- ** Non-scoping State
data IState = IState { cTable                :: !(M.HashMap DKey (K3 MExpr))
                     , globalPhaseBoundaries :: !(M.HashMap Identifier (S.Set Identifier))
                     , internedNames         :: !(I.IntMap Identifier)
                     } deriving (Eq, Read, Show, Generic)

defaultIState :: IState
defaultIState = IState  M.empty M.empty I.empty

type DKey = (Juncture, Direction)

constrain :: UID -> Int -> Direction -> K3 MExpr -> InferM ()
constrain u i d m = let j = Juncture u i in
  logR j d m >> modify (\s -> s { cTable = M.insertWith (flip const) (j, d) (simplifyE m) (cTable s) })

addGlobalPhaseBoundary :: M.HashMap Identifier (S.Set Identifier) -> (Identifier, Identifier) -> M.HashMap Identifier (S.Set Identifier)
addGlobalPhaseBoundary m (gName, tName) = M.insertWith S.union tName [gName] m

isPseudoLocalInContext :: Identifier -> InferM Bool
isPseudoLocalInContext i = do
  ct <- asks currentTrigger
  tm <- gets globalPhaseBoundaries
  return $! maybe False (\tn -> i `S.member` (M.lookupDefault S.empty tn tm)) ct

-- ** Scoping state
data IScope = IScope { downstreams    :: ![Downstream]
                     , nearestBind    :: !(Maybe UID)
                     , pEnv           :: !PIEnv
                     , fEnv           :: !FIEnv
                     , topLevel       :: !Bool
                     , currentTrigger :: !(Maybe Identifier)
                     , flags          :: !MZFlags
                     }

data Contextual a = Contextual !a !(Maybe UID) deriving (Eq, Ord, Read, Show)
type Downstream = Contextual (K3 Expression)

contextualizeNow :: a -> InferM (Contextual a)
contextualizeNow a = asks nearestBind >>= \n -> return $! Contextual a n

withDownstreams :: [Downstream] -> InferM a -> InferM a
withDownstreams nds = local (\s -> s { downstreams = nds ++ downstreams s })

withNearestBind :: UID -> InferM a -> InferM a
withNearestBind u = local (\s -> s { nearestBind = Just u })

withTopLevel :: Bool -> InferM a -> InferM a
withTopLevel b = local (\s -> s { topLevel = b })

withCurrentTrigger :: Identifier -> InferM a -> InferM a
withCurrentTrigger ct = local (\s -> s { currentTrigger = Just ct })

-- ** Reporting
data IReport = IReport { juncture :: !Juncture, direction :: !Direction, constraint :: !(K3 MExpr) }

logR :: Juncture -> Direction -> K3 MExpr -> InferM ()
logR j d m = if reportVerbosity == None
               then return ()
               else tell [IReport { juncture = j, direction = d, constraint = m }]

data ReportVerbosity = None | Short | Long
                     deriving (Eq, Read, Show)

reportVerbosity :: ReportVerbosity
reportVerbosity = None

formatIReport :: ReportVerbosity -> [IReport] -> IO ()
formatIReport rv ir = do
  putStrLn "--- Begin Materialization Inference Report ---"
  for_ ir $ \(IReport (Juncture u i) d m) -> case rv of
    None -> return ()
    Short -> printf "J%d/%d/%s: %s / %s\n" (gUID u) i (show d) (ppShortE m) (ppShortE $ simplifyE m)
    Long -> printf "--- Juncture %d/%d/%s:\n%s" (gUID u) i (show d) (boxToString $ prettyLines m %+ prettyLines (simplifyE m))
  putStrLn "--- End Materialization Inference Report ---"

-- ** Errors
newtype IError = IError String

-- * Helpers

dUID :: K3 Declaration -> InferM UID
dUID d = maybe (throwError $ IError "Invalid DUID") (\(DUID u) -> return u) (d @~ isDUID)

eUID :: K3 Expression -> InferM UID
eUID e = maybe (throwError $ IError "Invalid EUID") (\(EUID u) -> return u) (e @~ isEUID)

ePrv :: K3 Expression -> InferM (K3 Provenance)
ePrv e = maybe (throwError $ IError "Invalid EProvenance") (\(EProvenance p) -> return p) (e @~ isEProvenance)

eEff :: K3 Expression -> InferM (K3 Effect)
eEff e = maybe (throwError $ IError "Invalid ESEffect") (\(ESEffect f) -> return f) (e @~ isESEffect)

internName :: Identifier -> InferM Int
internName n = (modify $! \s -> s { internedNames = extend n $! internedNames s }) >> return ptr
  where extend n ns = I.alter (const $ Just n) ptr ns
        ptr = hashJunctureName n

uninternName :: Int -> InferM Identifier
uninternName i = get >>= \s -> maybe err return $! I.lookup i $! internedNames s
  where err = throwError $ IError $ "Could not unintern " ++ show i

chasePPtr :: PPtr -> InferM (K3 Provenance)
chasePPtr p = do
  ppEnv <- asks $! ppenv . pEnv
  case I.lookup p ppEnv of
    Nothing -> throwError $ IError "Invalid pointer in provenance chase"
    Just p' -> return p'

bindPoint :: Contextual (K3 Provenance) -> InferM (Maybe Juncture)
bindPoint (Contextual p u) = case tag p of
  PFVar i _ | Just u' <- u -> internName i >>= \ptr -> return $! Just $! Juncture u' ptr
  PBVar (PMatVar i u' _) -> internName i >>= \ptr -> return $! Just $! Juncture u' ptr
  PProject _ -> bindPoint (Contextual (head $! children p) u)
  _ -> return Nothing

-- * Inference Algorithm

-- Nothing special to do at declarations, proxy to expressions.
-- TODO: Add constraint between decision for declaration and root expression of initializer.
materializeD :: K3 Declaration -> InferM ()
materializeD d = case tag d of
  DGlobal i _ (Just e) -> materializeE e
  DTrigger i _ e -> withCurrentTrigger i $ withTopLevel True $ materializeE e
  _ -> traverse_ materializeD (children d)

materializeE :: K3 Expression -> InferM ()
materializeE e@(Node (t :@: _) cs) = case t of
  EProject _ -> do
    let [super] = cs
    materializeE super
    moveableNow <- ePrv super >>= contextualizeNow >>= isMoveableNow

    u <- eUID e

    -- Need to be very careful here, semantics probably say that default method is copy, but we
    -- treat it as reference almost everywhere. Check projection decisions carefully, usually on
    -- collection transformers.
    constrain u anon In $! mITE moveableNow (mAtom Moved) (mAtom Referenced)

  ERecord is -> do
    u <- eUID e

    -- Each field decision for a record needs to be made in the context where each of the subsequent
    -- children of the record are downstreams for the current field.
    for_ (zip3 is cs (tail $ L.tails cs)) $! \(i, c, os) -> do
      os' <- traverse contextualizeNow os
      withDownstreams os' $ do
        materializeE c

        ila <- asks (isolateApplicationMZ . flags)

        -- Determine if the field argument is moveable within the current expression.
        moveableNow <- ePrv c >>= contextualizeNow >>= isMoveableNow
        iptr <- internName i
        constrain u iptr In $! mITE (moveableNow -&&- (mNot $! mBool ila)) (mAtom Moved) (mAtom Copied)

  ELambda i -> do
    let [body] = cs

    u <- eUID e

    (ci, cb) <- withNearestBind u $ do
      withTopLevel False $ materializeE body
      ci' <- contextualizeNow (pfvar i $! Just u)
      cb' <- contextualizeNow body
      return (ci', cb')

    fProv <- ePrv e >>= contextualizeNow

    (ehw, _) <- hasWriteIn ci cb

    topL <- asks topLevel
    iRun <- asks (isolateRuntimeMZ . flags)

    let argShouldBeMoved = case (e @~ \case { EType _ -> True; _ -> False }) of
          Just (EType (tag &&& children -> (TFunction, [t, _]))) -> isNonScalarType t
          _ -> False

    isolateApplication <- asks (isolateApplicationMZ . flags)
    let fSourceOverride = mITE (mBool $! isolateApplication && e @:? "FusionSource") (mAtom Copied)
    let standardPath = mITE ehw (mITE (mBool $! argShouldBeMoved) (mAtom Moved) (mAtom Copied)) (mAtom Forwarded)
    let topLPath = mITE (mBool topL -&&- mBool iRun) (mAtom Copied) (fSourceOverride standardPath)

    iptr <- internName i
    constrain u iptr In topLPath

    cls <- ePrv e >>= \case
      (tag -> PLambda _ cls) -> return cls
      _ -> do
        u <- eUID e
        throwError $ IError $ printf "Expected lambda provenance on lambda expression at UID %s." (show u)

    for_ cls $! \m@(PMatVar name loc ptr) -> do
      needsOwn <- withNearestBind u $ do
        innerP <- contextualizeNow (pbvar m)
        (ehw, _) <- contextualizeNow body >>= hasWriteIn innerP
        return $! ehw -??- printf "Lambda needs ownership of closure variable %s?" name

      outerP <- chasePPtr ptr
      moveable <- contextualizeNow outerP >>= isMoveableNow


      nptr <- internName name
      constrain u nptr In $! mITE needsOwn (mITE moveable (mAtom Moved) (mAtom Copied)) (mAtom ConstReferenced)

    clps <- sequence [withNearestBind u (contextualizeNow $! pbvar m) | m <- cls]
    nrvo <- mOr <$> traverse (`occursIn` fProv) (ci:clps)
    constrain u anon Ex $! mITE nrvo (mAtom Moved) (mAtom Copied)

  EOperate OApp -> do
    let [f, x] = cs
    case (f, x) of
      (tag -> EProject "fold", tag &&& children -> (ELambda i, [il@(tag &&& children -> (ELambda ele, _))])) -> do
        xu <- eUID x
        ilu <- eUID il
        iptr <- internName i
        constrain xu  iptr In (mAtom Moved -??- "Fold Override")
        constrain ilu iptr In (mAtom Moved -??- "Fold Override")
      _ -> return ()

    contextualizeNow x >>= \x' -> withDownstreams [x'] $ materializeE f
    materializeE x

    moveable <- ePrv x >>= contextualizeNow >>= isMoveableNow

    fwdContext <- ePrv x >>= contextualizeNow >>= bindPoint >>= \case
      Just (Juncture u i) -> return $! mOneOf (mVar u i In) [Forwarded] -??- "Forwarded by containing context?"
      Nothing -> return $! mBool True -??- "Temporary."

    u <- eUID e
    constrain u anon In $! mITE moveable (mAtom Moved) (mAtom Copied)

  EOperate OSnd -> do
    let [h, m] = cs
    contextualizeNow m >>= \m' -> withDownstreams [m'] $ materializeE h
    materializeE m

    ir <- asks (isolateRuntimeMZ . flags)

    moveable <- ePrv m >>= contextualizeNow >>= isMoveableNow

    u <- eUID e
    constrain u anon In $! mITE (moveable -&&- mNot (mBool ir)) (mAtom Moved) (mAtom Copied)

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

    let sourceMoveable = sourceMoveableNow

    u <- eUID e

    for_ bindings $ \m@(PMatVar name loc ptr) -> do
      cpBody <- contextualizeNow body
      cpBinding <- contextualizeNow (pbvar m)
      (ehw, _) <- hasWriteIn cpBinding cpBody

      cpBindSource <- chasePPtr ptr >>= contextualizeNow
      (bsehw, _) <- hasWriteIn cpBindSource cpBody
      (bsehr, _) <- hasReadIn cpBindSource cpBody

      let bindNeedsOwn = ehw
      let bindConflict = bsehr -||- bsehw
      nptr <- internName name
      constrain u nptr In $! mITE bindConflict (mITE bindNeedsOwn (mAtom Copied) (mAtom Referenced)) (mAtom Referenced)
      constrain u nptr Ex $! mITE (mOneOf (mVar u nptr In) [Copied, Moved]) (mAtom Moved) (mAtom Referenced)

  ELetIn i -> do
    let [initL, body] = cs

    (imn, ico) <- do
      body' <- contextualizeNow body

      withDownstreams [body'] $ do
        materializeE initL
        imn <- ePrv initL >>= contextualizeNow >>= isMoveableNow
        ico <- ePrv initL >>= contextualizeNow >>= bindPoint >>= \case
          Nothing -> return $! mBool True -??- "Temporary."
          Just (Juncture u i) -> return $! mOneOf (mVar u i In) [Moved, Copied] -??- "Owned by containing context?"

        return (imn, ico)

    materializeE body

    u <- eUID e
    let moveable = imn -&&- ico
    iptr <- internName i
    constrain u iptr In $! mITE moveable (mAtom Moved) (mAtom Copied)

  ECaseOf i -> do
    let [initB, some, none] = cs
    some' <- contextualizeNow some
    none' <- contextualizeNow none

    withDownstreams [some', none'] $ materializeE initB
    materializeE some
    materializeE none

    sourceMoveableNow <- ePrv initB >>= contextualizeNow >>= isMoveableNow

    u <- eUID e
    ip <- contextualizeNow (pbvar $! PMatVar i u (-1))

    let sourceMoveable = sourceMoveableNow

    (ehw, _) <- hasWriteIn ip some'
    let caseNeedsOwn = ehw
    iptr <- internName i
    constrain u iptr In $! mITE caseNeedsOwn (mITE sourceMoveable (mAtom Moved) (mAtom Copied)) (mAtom Referenced)

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
hasReadIn (Contextual (tag -> PFVar _ _) cp) (Contextual _ ce) | cp /= ce = return (mBool False, mBool False)
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
      return $! occurs -??- "Captured by Lambda?"
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
  (tag -> FScope _) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $! children f)
  (tag -> FSeq) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $! children f)
  (tag -> FSet) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $! children f)
  (tag -> FLoop) -> foldr (-||-) (mBool False) <$> traverse (hasReadInF p) (map (flip Contextual cf) $! children f)
  _ -> return (mBool False)

hasWriteIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred, K3 MPred)
hasWriteIn (Contextual (tag -> PFVar _ _) cp) (Contextual _ ce) | cp /= ce = return (mBool False, mBool False)
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
      nptr <- internName n
      return $! occurs -&&- mOneOf (mVar u nptr In) [Moved]
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
  (tag -> FScope _) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $! children f)
  (tag -> FSeq) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $! children f)
  (tag -> FSet) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $! children f)
  (tag -> FLoop) -> foldr (-||-) (mBool False) <$> traverse (hasWriteInF p) (map (flip Contextual cf) $! children f)
  _ -> return (mBool False)

isPseudoGlobal :: K3 Provenance -> InferM (K3 MPred)
isPseudoGlobal p = case tag p of
  (PGlobal n) -> isPseudoLocalInContext n >>= \pl -> return (mBool (not pl) -??- "Is Pseudo Local?")
  (PBVar (PMatVar n u ptr)) -> do
    parent <- chasePPtr ptr >>= isPseudoGlobal
    nptr <- internName n
    return $! mOneOf (mVar u nptr In) [Referenced, ConstReferenced, Forwarded] -&&- parent
  (PProject _) -> isPseudoGlobal (head $! children p)
  PSet -> mOr <$> traverse isPseudoGlobal (children p)
  (PRecord _) -> mOr <$> traverse isPseudoGlobal (children p)
  (PTuple _) -> mOr <$> traverse isPseudoGlobal (children p)
  POption -> mOr <$> traverse isPseudoGlobal (children p)
  _ -> return $! mBool False

occursIn :: Contextual (K3 Provenance) -> Contextual (K3 Provenance) -> InferM (K3 MPred)
occursIn a@(Contextual pa ca) b@(Contextual pb cb) = case tag pb of
  PFVar i m -> case tag pa of
    PFVar j n | i == j && m == n -> return (mBool True)
    _ -> return (mBool False)
  PBVar (PMatVar n u ptr) -> case tag pa of
    PBVar (PMatVar n' u' _) | n' == n && u' == u -> return (mBool True)
    _ -> do
      pOccurs <- chasePPtr ptr >>= \p' -> occursIn a (Contextual p' cb)
      nptr <- internName n
      return $! (mOneOf (mVar u nptr In) [Referenced, ConstReferenced, Forwarded] -&&- pOccurs)
        -??- (if u == (UID 43168) then boxToString $ ["occursIn RCRF"] %$ prettyLines pa %$ prettyLines pb else "")

  PSet -> mOr <$> traverse (\pb' -> occursIn a (Contextual pb' cb)) (children pb)
  PLambda _ _ -> mOr <$> traverse (\pb' -> occursIn a (Contextual pb' cb)) (children pb)
  PProject _ -> mOr <$> traverse (\pb' -> occursIn a (Contextual pb' cb)) (children pb)
  _ -> return (mBool False)

ownedByContext :: Contextual (K3 Provenance) -> InferM (K3 MPred)
ownedByContext (Contextual p c) = case tag p of
  PFVar i _ | Just u' <- c -> internName i >>= \iptr -> return $! mOneOf (mVar u' iptr In) [Copied, Moved]
  PBVar (PMatVar i u' ptr) -> do
    transitive <- chasePPtr ptr >>= \p' -> ownedByContext (Contextual p' c)
    iptr <- internName i
    return $! mOneOf (mVar u' iptr In) [Copied, Moved]
              -||- (mOneOf (mVar u' iptr In) [Referenced] -&&- transitive)
  PProject _ -> ownedByContext (Contextual (head $! children p) c)
  _ -> return (mBool True)

isMoveable :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveable (Contextual p _) = mNot <$> isPseudoGlobal p

isMoveableIn :: Contextual (K3 Provenance) -> Contextual (K3 Expression) -> InferM (K3 MPred)
isMoveableIn cp ce = do
  (ehr, ihr) <- hasReadIn cp ce
  (ehw, ihw) <- hasWriteIn cp ce

  eu <- let (Contextual e _) = ce in eUID e
  return $! (mNot $! foldr1 (-||-) ([ ehr -??- (printf "Explicit reads in downstream %d?" (gUID eu))
                                    , ihr -??- (printf "Implicit reads in downstream %d?" (gUID eu))
                                    , ehw -??- (printf "Explicit writes in downstream %d?" (gUID eu))
                                    , ihw -??- (printf "Explicit writes in downstream %d?" (gUID eu))
                                    ] :: [K3 MPred])) -??- printf "Moveable in downstream %d?" (gUID eu)

isMoveableNow :: Contextual (K3 Provenance) -> InferM (K3 MPred)
isMoveableNow cp = do
  ds <- asks downstreams
  isMoveable1 <- isMoveable cp
  allMoveable <- foldr (-&&-) (mBool True) <$> traverse (isMoveableIn cp) ds
  let noDownstreamConflicts = (isMoveable1 -??- "Global?" -&&- allMoveable -??- "Moveable in all downstreams?") -??- "Moveable?"

  ownedInContext <- ownedByContext cp

  return $! noDownstreamConflicts -&&- ownedInContext


-- * Solver

type SolverT m = StateT SState (ExceptT SError m)
type SolverM a = SolverT Identity a

data SState = SState { assignments :: !(M.HashMap DKey Method) }
newtype SError = SError String

defaultSState :: SState
defaultSState = SState []

runSolverM :: SolverM a -> SState -> Either SError (a, SState)
runSolverM m st = runIdentity $! runExceptT $! runStateT m st

setMethod :: DKey -> Method -> SolverM ()
setMethod k m = modify $! \s -> s { assignments = M.insert k m (assignments s) }

getMethod :: DKey -> SolverM Method
getMethod k = gets assignments >>= maybe (err k) return . M.lookup k
  where err k' = throwError $ SError $ "Materialization lookup failed on " ++ show k'

-- ** Sorting
mkDependencyList :: M.HashMap DKey (K3 MExpr) -> K3 Declaration -> SolverM [Either DKey DKey]
mkDependencyList m p = return (buildHybridDepList graph)
 where
  graph = [(k, k, S.toList (findDependenciesE $ m M.! k)) | k <- M.keys m]
  collapseSCCs scc = case scc of
    G.AcyclicSCC (v, _, _) -> [Right v]
    G.CyclicSCC vs -> let ((v', _, _):vs') = sortByProgramUID vs p
                      in Left v' : (buildHybridDepList vs')
  buildHybridDepList g = concatMap collapseSCCs (G.stronglyConnCompR g)

  sortByProgramUID :: [(DKey, DKey, [DKey])] -> K3 Declaration -> [(DKey, DKey, [DKey])]
  sortByProgramUID ks d = fst $! fst $! foldProgramUID' moveToFront ([], ks) d

  moveToFront (ns, os) u = let (ns', os') = L.partition (\(_, (Juncture k _, _), _) -> k == u) os in (ns' ++ ns, os')

  foldProgramUID' :: (a -> UID -> a) -> a -> K3 Declaration -> (a, K3 Declaration)
  foldProgramUID' uidF z d = runIdentity $! foldProgram onDecl onMem onExpr (Just onType) z d
    where onDecl a n = return $! (dUID a n, n)
          onExpr a n = foldTree' (\a' n' -> return $! eUID a' n') a n >>= return . (,n)
          onType a n = foldTree' (\a' n' -> return $! tUID a' n') a n >>= return . (,n)

          onMem a mem@(Lifted    _ _ _ _ anns) = return $! (dMemUID a anns, mem)
          onMem a mem@(Attribute _ _ _ _ anns) = return $! (dMemUID a anns, mem)
          onMem a mem@(MAnnotation   _ _ anns) = return $! (dMemUID a anns, mem)

          dUID a n = maybe a (\case {DUID b -> uidF a b; _ -> a}) $! n @~ isDUID
          eUID a n = maybe a (\case {EUID b -> uidF a b; _ -> a}) $! n @~ isEUID
          tUID a n = maybe a (\case {TUID b -> uidF a b; _ -> a}) $! n @~ isTUID

          dMemUID a anns = maybe a (\case {DUID b -> uidF a b; _ -> a}) $! find isDUID anns

  foldTree' f x n@(Node _ []) = f x n
  foldTree' f x n@(Node _ ch) = f x n >>= flip (foldM (foldTree' f)) ch

findDependenciesE :: K3 MExpr -> S.Set DKey
findDependenciesE e = case tag e of
  MVar j d -> [(j, d)]
  MAtom _ -> []
  MIfThenElse p -> S.union (S.unions $ fmap findDependenciesE (children e)) (findDependenciesP p)

findDependenciesP :: K3 MPred -> S.Set DKey
findDependenciesP p = case tag p of
  MOneOf e _ -> findDependenciesE e
  _ -> S.unions $ fmap findDependenciesP (children p)

-- ** Solving
solveForAll :: [Either DKey DKey] -> M.HashMap DKey (K3 MExpr) -> SolverM ()
solveForAll eks m =
  let debugConstraint dtg origE simpleE m = flip trace m $ boxToString
                                              $ [dtg] ++
                                              ["Unsimplified:"]
                                              ++ (indent 2 $ prettyLines origE)
                                              ++ ["Simplified:"]
                                              ++ (indent 2 $ prettyLines simpleE)
      onFail dtg mexpr smexpr (SError msg) = debugConstraint dtg mexpr smexpr $ throwError $ SError msg
  in for_ eks $ \case
      Left fk -> do
        progress <- gets (S.fromList . M.keys . assignments)
        let mexpr  = m M.! fk
        let smexpr = simplifyE mexpr
        if progress S.\\ (findDependenciesE mexpr) == [fk]
          then (tryResolveSelfCycle fk smexpr) `catchError` (onFail (unwords ["SCYC", show fk]) mexpr smexpr)
                  >>= setMethod fk . fromMaybe Copied
          else setMethod fk Copied

      Right rk -> let mexpr  = m M.! rk
                      smexpr = simplifyE mexpr
                  in (solveForE smexpr) `catchError` (onFail (unwords ["ACYC", show rk]) mexpr smexpr) >>= setMethod rk

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

tryResolveSelfCycle :: DKey -> K3 MExpr -> SolverM (Maybe Method)
tryResolveSelfCycle k e = do
  g <- get
  mms <- forM [ConstReferenced, Referenced, Moved, Copied] $ \m -> do
    setMethod k m
    m' <- solveForE e
    if m' == m
      then return (First $! Just m)
      else return (First Nothing)
  put g
  return $! getFirst $! mconcat mms

-- * Independent Simplification Routines
-- | The following routines perform simplification on MExprs/MPreds independent of the binding
--   values of MVars; it is in essence performing just the propagation stage of constraint solvers.
--   The constraints still need to be "solved" as above, but hopefully they will be a _lot_ smaller,
--   and will not include spurious dependencies.
simplifyE :: K3 MExpr -> K3 MExpr
simplifyE expr = case expr of
  (tag &&& children -> (MIfThenElse p, [t, e])) -> case simplifyP p of
    (tag -> MBool b) -> simplifyE $! if b then t else e
    p' -> mITE p' (simplifyE t) (simplifyE e)
  _ -> expr

simplifyP :: K3 MPred -> K3 MPred
simplifyP pred = case pred of
  (tag &&& children -> (MNot, [p])) -> case simplifyP p of
    (tag -> MBool b) -> mBool (not b)
    p' -> mNot p'
  (tag &&& children -> (MAnd, cs)) -> case foldl andFold (Just []) cs of
    Nothing -> mBool False
    Just [] -> mBool True
    Just xs -> case L.nub xs of
      [x] -> x
      xs' -> mAnd xs'
  (tag &&& children -> (MOr, cs)) -> case foldl orFold (Just []) cs of
    Nothing -> mBool True
    Just [] -> mBool False
    Just xs -> case L.nub xs of
      [x] -> x
      xs' -> mOr xs'
  (tag -> (MOneOf e ms)) -> case simplifyE e of
    (tag -> MAtom m) -> mBool (m `elem` ms)
    e' -> mOneOf e' ms
  (tag -> MBool b) -> mBool b
 where
  andFold :: Maybe [K3 MPred] -> K3 MPred -> Maybe [K3 MPred]
  andFold acc p = case (acc, simplifyP p) of
    (Nothing, _) -> Nothing
    (Just as, tag -> MBool b)
      | b -> Just as
      | otherwise -> Nothing
    (Just as, tag &&& children -> (MAnd, cs)) -> Just (as ++ cs)
    (Just as, q) -> Just (as ++ [q])

  orFold :: Maybe [K3 MPred] -> K3 MPred -> Maybe [K3 MPred]
  orFold acc p = case (acc, simplifyP p) of
    (Nothing, _) -> Nothing
    (Just as, tag -> MBool b)
      | b -> Nothing
      | otherwise -> Just as
    (Just as, tag &&& children -> (MOr, cs)) -> Just (as ++ cs)
    (Just as, q) -> Just (as ++ [q])
