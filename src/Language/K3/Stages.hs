{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE NoMonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ParallelListComp #-}

-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Arrow hiding ( left )
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.State
import Control.Monad.Trans.Except

import Control.Concurrent

import Criterion.Measurement
import Criterion.Types

import Data.Binary ( Binary )
import Data.Serialize ( Serialize )

import Data.Function
import Data.Functor.Identity
import Data.List
import Data.List.Split
import Data.Map ( Map )
import Data.Monoid
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Tree
import Data.Tuple

import Debug.Trace

import GHC.Generics ( Generic )

import qualified Text.Printf as TPF

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Analysis.Core
import Language.K3.Analysis.HMTypes.Inference hiding ( tenv, inferDeclTypes )

import qualified Language.K3.Analysis.Properties           as Properties
import qualified Language.K3.Analysis.CArgs                as CArgs
import qualified Language.K3.Analysis.Provenance.Inference as Provenance
import qualified Language.K3.Analysis.SEffects.Inference   as SEffects

import qualified Language.K3.Analysis.SendGraph as SG

import Language.K3.Transform.Simplification
import Language.K3.Transform.TriggerSymbols (triggerSymbols)

import Language.K3.Utils.Pretty
import qualified Language.K3.Utils.PrettyText as PT

import Language.K3.Codegen.CPP.Materialization.Inference
import Language.K3.Codegen.CPP.Preprocessing

-- | Snapshot specifications are a list of pass names to capture per declaration.
type SnapshotSpec = Map String [String]

-- | Stage specifications are a pair of pass identifiers, and a snapshot spec.
data StageSpec = StageSpec { passesToRun    :: Maybe [Identifier]
                           , passesToFilter :: Maybe [Identifier]
                           , snapshotSpec   :: SnapshotSpec }
                 deriving (Eq, Ord, Read, Show, Generic)

-- | Configuration metadata for compiler stages
data CompilerSpec = CompilerSpec { blockSize :: Int
                                 , stageSpec :: StageSpec }
                    deriving (Eq, Ord, Read, Show, Generic)

-- | Compilation profiling
data TransformReport = TransformReport { statistics :: Map String [Measured]
                                       , snapshots  :: Map String [K3 Declaration] }
                      deriving (Eq, Read, Show, Generic)

-- | The program transformation composition monad
data TransformSt = TransformSt { nextuid    :: ParGenSymS
                               , cseCnt     :: ParGenSymS
                               , tenv       :: TIEnv
                               , prenv      :: Properties.PIEnv
                               , penv       :: Provenance.PIEnv
                               , fenv       :: SEffects.FIEnv
                               , report     :: TransformReport }
                  deriving (Eq, Read, Show, Generic)

-- | Monoid instance for transform reports.
instance Monoid TransformReport where
  mempty = TransformReport Map.empty Map.empty

  mappend (TransformReport ast asn) (TransformReport bst bsn) =
    TransformReport (ast <> bst) (asn <> bsn)

data TransformStSymS = TransformStSymS { uidSym :: ParGenSymS, cseSym :: ParGenSymS
                                       , prvSym :: ParGenSymS, effSym :: ParGenSymS }
                      deriving (Eq, Ord, Read, Show, Generic)

-- | Stage-based transformation monad.
type TransformM = ExceptT String (StateT TransformSt IO)

{- Stage-based transform instances -}
instance Binary StageSpec
instance Binary CompilerSpec
instance Binary TransformReport
instance Binary TransformSt
instance Binary TransformStSymS

instance Serialize StageSpec
instance Serialize CompilerSpec
instance Serialize TransformReport
instance Serialize TransformSt
instance Serialize TransformStSymS

ss0 :: StageSpec
ss0 = StageSpec Nothing Nothing Map.empty

cs0 :: CompilerSpec
cs0 = CompilerSpec 16 ss0

rp0 :: TransformReport
rp0 = TransformReport Map.empty Map.empty

st0 :: Maybe ParGenSymS -> K3 Declaration -> IO (Either String TransformSt)
st0 symSOpt prog =
  return $ mkEnv >>= \(stpe, stfe) ->
    return $ TransformSt uidSymS cseSymS tienv0 Properties.pienv0 stpe stfe rp0

  where puid = let UID i = maxProgramUID prog in i + 1
        uidSymS = maybe (contigsymAtS puid) (lowerboundsymS puid) symSOpt
        [cseSymS, prvSymS, effSymS] = replicate 3 $ resetsymS uidSymS
        mkEnv = do
          vpenv <- variablePositions prog
          let pe = Provenance.pienv0 (Just prvSymS) vpenv
          return (pe, SEffects.fienv0 (Just effSymS) (Provenance.ppenv pe) $ lcenv vpenv)

runTransformM :: TransformSt -> TransformM a -> IO (Either String (a, TransformSt))
runTransformM st m = do
  (a, s) <- runStateT (runExceptT m) st
  return $ either Left (Right . (,s)) a

evalTransformM :: TransformSt -> TransformM a -> IO (Either String a)
evalTransformM st m = do
  e <- runTransformM st m
  return $ either Left (Right . fst) e

liftEitherM :: Either String a -> TransformM a
liftEitherM = either throwE return

{- TransformSt utilities -}
-- | Left-associative merge for transform states.
mergeTransformSt :: Maybe Identifier -> TransformSt -> TransformSt -> TransformSt
mergeTransformSt d agg new = rewindTransformStSyms new $
  agg { penv    = Provenance.mergePIEnv d (penv agg) (penv new)
      , fenv    = SEffects.mergeFIEnv   d (fenv agg) (fenv new)
      , report  = (report agg) <> (report new) }

-- | A merge function that propagates only the transform report.
mergeTransformStReport :: TransformSt -> TransformSt -> TransformSt
mergeTransformStReport st1 st2 = rewindTransformStSyms st2 $ st1 { report  = (report st1) <> (report st2) }

getTransformSyms :: TransformSt -> TransformStSymS
getTransformSyms s = TransformStSymS (nextuid s) (cseCnt s) (Provenance.pcnt $ penv s) (SEffects.fcnt $ fenv s)

putTransformSyms :: TransformStSymS -> TransformSt -> TransformSt
putTransformSyms syms s =
    s { nextuid = uidSym syms
      , cseCnt  = cseSym syms
      , penv    = (penv s) { Provenance.pcnt = prvSym syms }
      , fenv    = (fenv s) { SEffects.fcnt   = effSym syms } }

mapTransformStSyms :: (Monad m) => (TransformStSymS -> m TransformStSymS) -> TransformSt -> m TransformSt
mapTransformStSyms f s = do
    rsyms <- f $ getTransformSyms s
    return $ putTransformSyms rsyms s

advanceTransformStSyms :: Int -> TransformSt -> Maybe TransformSt
advanceTransformStSyms delta s = mapTransformStSyms advance s
  where adv = advancesymS delta
        advance (TransformStSymS a b c d) = TransformStSymS <$> adv a <*> adv b <*> adv c <*> adv d

rewindTransformStSyms :: TransformSt -> TransformSt -> TransformSt
rewindTransformStSyms s t = runIdentity $ mapTransformStSyms (rewind $ getTransformSyms s) t
  where rw a b = return $ rewindsymS a b
        rewind (TransformStSymS a b c d) (TransformStSymS a' b' c' d') =
          TransformStSymS <$> rw a a' <*> rw b b' <*> rw c c' <*> rw d d'

forkTransformStSyms :: Int -> TransformSt -> TransformSt
forkTransformStSyms factor s = runIdentity $ mapTransformStSyms fork s
  where f = return . forksymS factor
        fork (TransformStSymS a b c d) = TransformStSymS <$> f a <*> f b <*> f c <*> f d

partitionTransformStSyms :: Int -> Int -> TransformSt -> Maybe TransformSt
partitionTransformStSyms forkFactor assignOffset s = mapTransformStSyms part s
  where p = advancesymS assignOffset . forksymS forkFactor
        part (TransformStSymS a b c d) = TransformStSymS <$> p a <*> p b <*> p c <*> p d

ensureParallelStSyms :: Int -> TransformM ()
ensureParallelStSyms parFactor = modify $ \s -> runIdentity $ mapTransformStSyms ensure s
  where e sym@(ParGenSymS str off cur) | str < parFactor = return $ ParGenSymS parFactor off cur
                                       | otherwise = return sym
        ensure (TransformStSymS a b c d) = TransformStSymS <$> e a <*> e b <*> e c <*> e d

{-- Transform utilities --}
type ProgramTransform = K3 Declaration -> TransformM (K3 Declaration)

-- | Stateful program transforms.
type ProgramTransformSt a = a -> K3 Declaration -> TransformM (a, K3 Declaration)

-- | Transforms returning whether they modified the program.
type ProgramTransformD = K3 Declaration -> TransformM (Bool, K3 Declaration)

-- | Transforms with only a declaration argument.
type TrF  = K3 Declaration -> K3 Declaration
type TrE  = K3 Declaration -> Either String (K3 Declaration)
type TrED = K3 Declaration -> Either String (Bool, K3 Declaration)

-- | Transforms with an accumulating argument.
type TrTF = TransformSt -> K3 Declaration -> K3 Declaration
type TrTE = TransformSt -> K3 Declaration -> Either String (K3 Declaration)

type TrSF a = a -> K3 Declaration -> (a, K3 Declaration)
type TrSE a = a -> K3 Declaration -> Either String (a, K3 Declaration)

runPasses :: [ProgramTransform] -> ProgramTransform
runPasses passes p = foldM (flip ($)) p passes

bracketPasses :: String -> ProgramTransform -> [ProgramTransform] -> [ProgramTransform]
bracketPasses n f l = pass "preBracket" f ++ l ++ pass "postBracket" f
  where pass tg f' = if True then [f'] else [displayPass (unwords [n, tg]) $ f']

type PrpTrE = Properties.PIEnv -> K3 Declaration -> Either String (K3 Declaration, Properties.PIEnv)
type TypTrE = TIEnv -> K3 Declaration -> Either String (K3 Declaration, TIEnv)
type EffTrE = Provenance.PIEnv -> SEffects.FIEnv -> K3 Declaration
              -> Either String (K3 Declaration, Provenance.PIEnv, SEffects.FIEnv)

-- | Forces evaluation of the program transformation, by fully evaluating its result.
reifyPass :: ProgramTransform -> ProgramTransform
reifyPass f p = do
  np <- f p
  return $!! np

reifyPassD :: ProgramTransformD -> ProgramTransformD
reifyPassD f p = do
  (r,np) <- f p
  return $!! (r,np)

-- | Show the program after applying the given program transform.
displayPass :: String -> ProgramTransform -> ProgramTransform
displayPass n f p = f p >>= \np -> mkTg n np (return np)
  where mkTg str p' = trace (boxToString $ [str] %$ prettyLines p')

-- | Show the program both before and after applying the given program transform.
debugPass :: String -> ProgramTransform -> ProgramTransform
debugPass n f p = mkTg ("Before " ++ n) p (f p) >>= \np -> mkTg ("After " ++ n) np (return np)
  where mkTg str p' = trace (boxToString $ [str] %$ prettyLines p')

-- | Measure the execution time of a transform
timePass :: String -> ProgramTransform -> ProgramTransform
timePass n f prog = do
  (npE, sample) <- get >>= \st -> liftIO (profile $ const $ runTransformM st $ f prog)
  (np, nst)     <- liftEitherM npE
  void $ put $ addMeasurement n sample nst
  return np

  where
    addMeasurement n' sample st =
      let rp  = report st
          nrp = rp {statistics = Map.insertWith (++) n' [sample] $ statistics rp}
      in st {report = nrp}

-- This is a reimplementation of Criterion.Measurement.measure that returns the value computed.
profile :: (MonadIO m) => (() -> m a) -> m (a, Measured)
profile m = do
  startStats     <- liftIO getGCStats
  startTime      <- liftIO getTime
  startCpuTime   <- liftIO getCPUTime
  startCycles    <- liftIO getCycles
  resultE        <- m () >>= liftIO . evaluate
  endTime        <- liftIO getTime
  endCpuTime     <- liftIO getCPUTime
  endCycles      <- liftIO getCycles
  endStats       <- liftIO getGCStats
  let msr = applyGCStats endStats startStats $ measured {
              measTime    = max 0 (endTime - startTime)
            , measCpuTime = max 0 (endCpuTime - startCpuTime)
            , measCycles  = max 0 (fromIntegral (endCycles - startCycles))
            , measIters   = 1
            }
  return (resultE, msr)


-- | Take a snapshot of the result of a transform
type SnapshotCombineF = [K3 Declaration] -> [K3 Declaration] -> [K3 Declaration]

lastSnapshot :: SnapshotCombineF
lastSnapshot a _ = a

snapshotPass :: String -> SnapshotCombineF -> ProgramTransform -> ProgramTransform
snapshotPass n combineF f prog = do
  np <- f prog
  modify $ addSnapshot n np
  return np

  where addSnapshot n' np st =
          let rp  = report st
              nrp = rp {snapshots = Map.insertWith combineF n' [np] $ snapshots rp}
          in st {report = nrp}

{-- Stateful transformations --}
withStateTransform :: (TransformSt -> a) -> (a -> TransformSt -> TransformSt) -> TrSE a -> ProgramTransform
withStateTransform getStF modifyStF f p = do
  st <- get
  (na, np) <- liftEitherM $ f (getStF st) p
  void $ put $ modifyStF na st
  return np

withPropertyTransform :: PrpTrE -> ProgramTransform
withPropertyTransform f p = do
  st <- get
  (np, pre) <- liftEitherM $ f (prenv st) p
  void $ put $ st {prenv=pre}
  return np

withTypeTransform :: TypTrE -> ProgramTransform
withTypeTransform f p = do
  --void $ ensureNoDuplicateUIDs p
  st <- get
  (np, te) <- liftEitherM $ f (tenv st) p
  void $ put $ st {tenv=te}
  return np

withEffectTransform :: EffTrE -> ProgramTransform
withEffectTransform f p = do
  st <- get
  (np,pe,fe) <- liftEitherM (f (penv st) (fenv st) p)
  void $ put (st {penv=pe, fenv=fe})
  return np

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg f prog = do
  np <- f prog
  st <- get
  let (i,np') = repairProgram msg (Just $ nextuid st) np
  void $ put $ st {nextuid = i}
  return np'

withRepairD :: String -> ProgramTransformD -> ProgramTransformD
withRepairD msg f prog = do
  (r,np) <- f prog
  st <- get
  let (i,np') = repairProgram msg (Just $ nextuid st) np
  void $ put $ st {nextuid = i}
  return (r,np')


{-- Transform constructors --}
transformFromDelta :: ProgramTransformD -> ProgramTransform
transformFromDelta f p = f p >>= return . snd

transformF :: TrF -> ProgramTransform
transformF f p = return $ f p

transformE :: TrE -> ProgramTransform
transformE f p = liftEitherM $ f p

transformEDbg :: String -> TrE -> ProgramTransform
transformEDbg tg f p = do
  p' <- mkTg "Before " p $ transformE f p
  mkTg "After " p' $ return p'
  where mkTg pfx p' = trace (boxToString $ [pfx ++ tg] %$ prettyLines p')

transformED :: TrED -> ProgramTransformD
transformED f p = liftEitherM $ f p

transformEDDbg :: String -> TrED -> ProgramTransformD
transformEDDbg tg f p = do
  (changed, p') <- mkTg "Before " p $ transformED f p
  mkTg (after changed) p' $ return (changed, p')
  where
    after b = unwords ["After", "(", show b, ")"]
    mkTg pfx p' = trace (boxToString $ [pfx ++ tg] %$ prettyLines p')

transformTF :: TrTF -> ProgramTransform
transformTF f p = get >>= return . flip f p

transformTE :: TrTE -> ProgramTransform
transformTE f p = get >>= liftEitherM . flip f p

transformSF :: TrSF a -> ProgramTransformSt a
transformSF f z p = return $ f z p

transformSE :: TrSE a -> ProgramTransformSt a
transformSE f z p = liftEitherM $ f z p

{- Fixpoint transform constructors -}
transformFixpoint :: ProgramTransform -> ProgramTransform
transformFixpoint f p = do
  np <- f p
  (if compareDAST np p then return else transformFixpoint f) np

transformFixpointI :: [ProgramTransform] -> ProgramTransform -> ProgramTransform
transformFixpointI interF f p = do
  np <- f p
  if compareDAST np p then return np
  else runPasses interF np >>= transformFixpointI interF f

transformFixpointSt :: ProgramTransformSt a -> ProgramTransformSt a
transformFixpointSt f z p = do
  (nacc, np) <- f z p
  (if compareDAST np p then return . (nacc,) else transformFixpointSt f nacc) np

transformFixpointD :: ProgramTransformD -> ProgramTransform
transformFixpointD f p = do
  (changed, np) <- f p
  (if not changed && compareDAST np p then return else transformFixpointD f) np

transformFixpointID :: [ProgramTransform] -> ProgramTransformD -> ProgramTransform
transformFixpointID interF f p = do
  (changed, np) <- f p
  if not changed && compareDAST np p then return np
  else runPasses interF np >>= transformFixpointID interF f

fixpointF :: TrF -> ProgramTransform
fixpointF f = transformFixpoint $ transformF f

fixpointE :: TrE -> ProgramTransform
fixpointE f = transformFixpoint $ transformE f

fixpointTF :: TrTF -> ProgramTransform
fixpointTF f = transformFixpoint $ transformTF f

fixpointTE :: TrTE -> ProgramTransform
fixpointTE f = transformFixpoint $ transformTE f

fixpointSF :: TrSF a -> ProgramTransformSt a
fixpointSF f = transformFixpointSt $ transformSF f

fixpointSE :: TrSE a -> ProgramTransformSt a
fixpointSE f = transformFixpointSt $ transformSE f

-- Fixpoint constructors with intermediate transformations between rounds.
fixpointIF :: [ProgramTransform] -> TrF -> ProgramTransform
fixpointIF interF f = transformFixpointI interF $ transformF f

fixpointIE :: [ProgramTransform] -> TrE -> ProgramTransform
fixpointIE interF f = transformFixpointI interF $ transformE f

fixpointITF :: [ProgramTransform] -> TrTF -> ProgramTransform
fixpointITF interF f = transformFixpointI interF $ transformTF f

fixpointITE :: [ProgramTransform] -> TrTE -> ProgramTransform
fixpointITE interF f = transformFixpointI interF $ transformTE f


{- Whole program analyses -}
ensureNoDuplicateUIDs :: ProgramTransform
ensureNoDuplicateUIDs p =
  let dupUids = duplicateProgramUIDs p
  in if null dupUids then return p
     else throwE $ T.unpack $ PT.boxToString $ [T.pack "Found duplicate uids:"]
                                         PT.%$ [T.pack $ show dupUids]
                                         PT.%$ PT.prettyLines p

inferTypes :: ProgramTransform
inferTypes prog = do
  --void $ ensureNoDuplicateUIDs prog
  (p, tienv) <- liftEitherM $ inferProgramTypes prog
  p' <- liftEitherM $ translateProgramTypes p
  void $ modify $ \st -> st {tenv = tienv}
  return p'

inferEffects :: ProgramTransform
inferEffects prog = do
  st <- get
  (p,  pienv) <- liftEitherM $ Provenance.inferProgramProvenance (Just $ Provenance.pcnt $ penv st) prog
  (p', fienv) <- liftEitherM $ SEffects.inferProgramEffects Nothing (Just $ SEffects.fcnt $ fenv st)
                                 (Provenance.ppenv pienv) (debugEffects "After provenance" p)
  void $ modify $ \st' -> st' {penv = pienv, fenv = fienv}
  return (debugEffects "After effects" p')

  where debugEffects tg p = if True then p else flip trace p $ boxToString $ [tg] %$ prettyLines p

inferProperties :: ProgramTransform
inferProperties prog = do
  (p, prienv) <- liftEitherM $ Properties.inferProgramUsageProperties prog
  void $ modify $ \st -> st {prenv = prienv}
  return p

inferFreshProperties :: ProgramTransform
inferFreshProperties = inferProperties . stripProperties

inferTypesAndEffects :: ProgramTransform
inferTypesAndEffects = runPasses [inferTypes, inferEffects]

inferFreshTypes :: ProgramTransform
inferFreshTypes = inferTypes . stripTypeAnns

inferFreshEffects :: ProgramTransform
inferFreshEffects = inferEffects . stripEffectAnns

inferFreshTypesAndEffects :: ProgramTransform
inferFreshTypesAndEffects = inferTypesAndEffects . stripTypeAndEffectAnns

-- | Recomputes a program's inferred properties, types and effects.
refreshProgram :: ProgramTransform
refreshProgram prog = runPasses [inferFreshTypesAndEffects, inferFreshProperties] prog


{- Whole program pass aliases -}

cgPasses :: [ProgramTransform]
cgPasses = [ withRepair "TID" $ transformE $ triggerSymbols
           , \d -> (liftIO (SG.generateSendGraph d) >> return d)
           , \d -> return (mangleReservedNames d)
           , refreshProgram
           , transformF CArgs.runAnalysis
           , transformE markProgramLambdas
           , \d -> get >>= \s -> liftIO (optimizeMaterialization (penv s, fenv s) d) >>= either throwE return
           ]

runCGPassesM :: ProgramTransform
runCGPassesM prog = runPasses cgPasses prog


{- Declaration-at-a-time analyses and optimizations. -}

-- | Accumulating declaration-at-a-time program traversal.
foldProgramDecls :: (a -> K3 Declaration -> (a, [ProgramTransform])) -> ProgramTransformSt a
foldProgramDecls passesF z prog = foldProgram declFixpoint idF idF Nothing z prog
  where idF a b = return (a, b)
        declFixpoint acc d = do
          let (nacc, passes) = passesF acc d
          nd <- runPasses passes d
          return (nacc, nd)

mapProgramDecls :: (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
mapProgramDecls passesF prog =
  foldProgramDecls (\_ d -> ((), passesF d)) () prog >>= return . snd

parmapProgramDeclsBlock :: (K3 Declaration -> [ProgramTransform]) -> [K3 Declaration] -> TransformM [K3 Declaration]
parmapProgramDeclsBlock declPassesF block = do
    initState <- get
    result <- debugBlock block $ liftIO $ runParallelBlock initState block
    case result of
      Left s -> throwE s
      Right (st', nblock) -> put st' >> return nblock

  where
    runParallelBlock :: TransformSt -> [K3 Declaration] -> IO (Either String (TransformSt, [K3 Declaration]))
    runParallelBlock st ds = do
      locks <- sequence $ newEmptyMVar <$ ds
      sequence_ [runParallelDecl i lock d st | lock <- locks | d <- ds | i <- [0..]]
      newSDs <- mapM takeMVar locks
      return $ foldl mergeEitherStateDecl (Right (st, [])) newSDs

    stateError :: IO a
    stateError = throwIO $ userError "Invalid parallel compilation state."

    runParallelDecl :: Int -> MVar (Either String (TransformSt, K3 Declaration)) -> K3 Declaration -> TransformSt -> IO ThreadId
    runParallelDecl i m d s = flip (maybe stateError) (advanceTransformStSyms i s) $ \s' ->
      forkIO $ runTransformM s' (fixD (mapProgramDecls declPassesF) compareDAST d) >>= putMVar m . fmap swap

    mergeEitherStateDecl :: Either String (TransformSt, [K3 Declaration]) -> Either String (TransformSt, K3 Declaration)
                         -> Either String (TransformSt, [K3 Declaration])
    mergeEitherStateDecl (Left s) _ = (Left s)
    mergeEitherStateDecl _ (Left s) = (Left s)
    mergeEitherStateDecl (Right (aggState, aggDecls)) (Right (newState, newDecl)) =
      let resultSt = mergeTransformSt (declName newDecl) aggState newState
      in debugMergeReport (statistics $ report aggState) (statistics $ report newState) (statistics $ report resultSt)
          $ Right (resultSt, aggDecls++[newDecl])

    debugMergeReport srp1 srp2 srprest r = if True then r else
      flip trace r $ boxToString $ ["Report 1"]      ++ (indent 2 $ map fst (Map.toList srp1)) ++
                                   ["Report 2"]      ++ (indent 2 $ map fst (Map.toList srp2)) ++
                                   ["Report result"] ++ (indent 2 $ map fst (Map.toList srprest))

    fixD f (===) d = f d >>= \d' -> if d === d' then return d else fixD f (===) d'

    debugBlock :: [K3 Declaration] -> a -> a
    debugBlock ds = trace ("Compiling a block: " ++ intercalate ", " (map showDuid ds))

    showDuid d = maybe invalidUid duid $ d @~ isDUID
    duid (DUID (UID i)) = "DUID " ++ show i
    duid _ = invalidUid
    invalidUid = "<no duid>"

blockMapProgramDecls :: Int -> [ProgramTransform] -> (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
blockMapProgramDecls blockSz blockPassesF declPassesF prog =
  let chunks = chunksOf blockSz $ topLevelDecls prog
  in (rebuild . concat <$> mapM (parmapProgramDeclsBlock declPassesF) chunks) >>= runPasses blockPassesF

  where
    rebuild :: [K3 Declaration] -> K3 Declaration
    rebuild = Node (DRole "__global" :@: [])

    topLevelDecls :: K3 Declaration -> [K3 Declaration]
    topLevelDecls d =
      case d of
        (tag -> DRole _) -> children prog
        _ -> error "Top level declaration is not a role."

inferDeclProperties :: Identifier -> ProgramTransform
inferDeclProperties n = withPropertyTransform $ \pre p ->
  Properties.reinferProgDeclUsageProperties pre n p

inferFreshDeclProperties :: Identifier -> ProgramTransform
inferFreshDeclProperties n = inferDeclProperties n . stripDeclProperties n

inferDeclTypes :: Identifier -> ProgramTransform
inferDeclTypes n = withTypeTransform $ \te p -> reinferProgDeclTypes te n p

inferDeclEffects :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
inferDeclEffects extInfOpt n = withEffectTransform $ \pe fe p -> do
  (nvp, _)   <- variablePositionsDecl n (Provenance.pvpenv pe) p
  let pe'     = pe {Provenance.pvpenv = nvp}
  (np,  npe) <- {-debugPretty ("Reinfer P\n" ++ show nvp) p $-} Provenance.reinferProgDeclProvenance pe' n p
  let fe'     = fe {SEffects.fppenv = Provenance.ppenv npe, SEffects.flcenv = lcenv nvp}
  (np', nfe) <- {-debugPretty "Reinfer F" fe' $-} SEffects.reinferProgDeclEffects extInfOpt fe' n np
  return (np', npe, nfe)
  where debugPretty tg a b = trace (T.unpack $ PT.boxToString $ [T.pack tg] PT.%$ PT.prettyLines a) b

inferFreshDeclTypesAndEffects :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
inferFreshDeclTypesAndEffects extInfOpt n =
  runPasses [inferDeclTypes n, inferDeclEffects extInfOpt n] . stripDeclTypeAndEffectAnns n

-- | Recomputes property propagation, types and effects on a declaration
refreshDecl :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
refreshDecl extInfOpt n =
  runPasses [inferFreshDeclTypesAndEffects extInfOpt n, inferFreshDeclProperties n]

-- | Returns a map of transformations, treating passes as data.
--   We could build a TH-DSL based on this map to define compilers.
declTransforms :: StageSpec -> Maybe (SEffects.ExtInferF a, a) -> Identifier -> Map Identifier ProgramTransform
declTransforms stSpec extInfOpt n = topLevel
  where
    topLevel  = (Map.fromList $ fPf fst $ [
        second mkFix $
        mkT $ mkSeqRep "Optimize" highLevel $ fPf fst $ prepend [ ("refreshI",      False) ]
                                                              $ [ ("Decl-Simplify", True)
                                                                , ("Decl-Fuse",     True)
                                                                , ("Decl-Simplify", True) ]

      ]) `Map.union` highLevel

    highLevel = (Map.fromList $ fPf fst $ [
        second mkFix $
        mkT $ mkSeq "Decl-Simplify" lowLevel $ fP $ intersperse "refreshI" $ [ "Decl-CF"
                                                                             , "Decl-BR"
                                                                             , "Decl-DCE"
                                                                             , "Decl-CSE" ]
      , mkT $ mkSeq "Decl-Fuse" lowLevel $ fP [ "Decl-FE"
                                              , "typEffI"
                                              , "Decl-FT" ]
      ]) `Map.union` lowLevel

    lowLevel = Map.fromList $ fPf fst $ [
              mk  foldConstants            "Decl-CF"  False True False True  (Just [typEffI])
      ,       betaReduce                   "Decl-BR"  False True False True
      ,       mk  eliminateDeadCode        "Decl-DCE" False True False True  (Just [typEffI])
      ,       mkW cseTransform             "Decl-CSE" False True False True  (Just [typEffI])
      , mkT $ mkD encodeTransformers       "Decl-FE"  False True False True  (Just [typEffI])
      , mkT $ mk  fuseFoldTransformers     "Decl-FT"  False True False True  (Just fusionI)
      , mkT $ mkDebug False $ fusionReduce
      , mkT $ mkDebug False $ ("typEffI",)  $ typEffI
      , mkT $ mkDebug False $ ("refreshI",) $ refreshI
      ]

    -- Build a transform with additional debugging/repair/reification functionality.
    mk f i asReified asRepair asDebug asFixT fixPassOpt = mkSS $ (i,)
      $ (maybe id (if asFixT then mkFixIT i else mkFixI) fixPassOpt)
      $ (if asReified then reifyPass else id)
      $ (if asRepair then withRepair i else id)
      $ (if asDebug then transformEDbg i else transformE)
      $ mapNamedDeclExpression n f

    -- Build a delta transform
    mkD f i asReified asRepair asDebug asFixT fixPassOpt = mkSS $ (i,)
      $ (maybe transformFromDelta (if asFixT then mkFixIDT i else mkFixID) fixPassOpt)
      $ (if asReified then reifyPassD else id)
      $ (if asRepair then withRepairD i else id)
      $ (if asDebug then transformEDDbg i else transformED)
      $ foldNamedDeclExpression n f False

    -- Wrap an existing transform
    mkW tr i asReified asRepair asDebug asFixT fixPassOpt = mkSS $ (i,)
      $ (maybe id (if asFixT then mkFixIT i else mkFixI) fixPassOpt)
      $ (if asReified then reifyPass else id)
      $ (if asRepair then withRepair i else id)
      $ (if asDebug then debugPass i else id)
      $ tr

    mkDebug asDebug (i,tr) = mkSS $ (i,) $ (if asDebug then debugPass i else id) tr

    -- Pass filtering
    fPf :: (a -> Identifier) -> [a] -> [a]
    fPf f = maybe (id) (\l -> filter (\x -> (f x) `notElem` l)) $ passesToFilter stSpec
    fP    = fPf id

    -- Fixpoint pass construction
    mkFix             = transformFixpoint
    mkFixI     interF = transformFixpointI  interF
    mkFixID    interF = transformFixpointID interF
    mkFixIT  i interF = transformFixpointI  [timePass (mkN $ i ++ "-FPI") $ runPasses interF]
    mkFixIDT i interF = transformFixpointID [timePass (mkN $ i ++ "-FPI") $ runPasses interF]

    -- Timing and snapshotting.
    mkN i = unwords [n, i]
    mkT (i,f)  = (i, timePass (mkN i) f)
    mkS (i,f)  = (i, snapshotPass (mkN i) lastSnapshot f)
    mkSS (i,f) = maybe (i,f) (\l -> if i `elem` l then mkS (i,f) else (i,f))
                   $ Map.lookup n $ snapshotSpec stSpec

    -- Custom, and shared passes
    betaReduce i asReified asRepair asDebug asFixT =
      (\f -> mkW f i asReified asRepair asDebug asFixT (Just [typEffI]))
        $ withEffectTransform
        $ \p f d -> mapNamedDeclExpression n (betaReduction p) d >>= return . (,p,f)

    fusionReduce = mkT $ betaReduce "Decl-FR" False True False True

    cseTransform = withStateTransform (Just . cseCnt)
                                      (\ncntOpt st -> st {cseCnt=maybe (cseCnt st) id ncntOpt})
                                      (foldNamedDeclExpression n commonSubexprElim)

    -- Fixpoint intermediates
    typEffI  = inferFreshDeclTypesAndEffects extInfOpt n
    refreshI = refreshDecl extInfOpt n
    fusionI  = bracketPasses "fusionReduce" typEffI [snd fusionReduce]

    -- Derived transforms
    mkSeq i m l = mkSS (i, runPasses $ map (flip getTransform m) l)

    mkSeqRep i m lWRep = mkSS (i, runPasses $ map (getWRep m) lWRep)
    getWRep m (i, asRep) = (if asRep then withRepair i else id) $ getTransform i m

    prepend l1 l2 = concatMap ((l1 ++) . (:[])) l2


getTransform :: Identifier -> Map Identifier ProgramTransform -> ProgramTransform
getTransform i m = maybe err id $ Map.lookup i m
  where err = error $ "Invalid compiler transformation: " ++ i

declOptPasses :: StageSpec -> Maybe (SEffects.ExtInferF a, a) -> K3 Declaration -> [ProgramTransform]
declOptPasses stSpec extInfOpt d = case nameOfDecl d of
  Nothing -> []
  Just n -> maybe (tltransforms n) (map $ flip getTransform (transforms n)) $ passesToRun stSpec

  where transforms n = declTransforms stSpec extInfOpt n
        tltransforms n = maybe (defaultPasses n) (\l -> if "Optimize" `elem` l then [] else defaultPasses n) $ passesToFilter stSpec
        defaultPasses n = [getTransform "Optimize" $ transforms n]

        nameOfDecl (tag -> DGlobal  n _ (Just _)) = Just n
        nameOfDecl (tag -> DTrigger n _ _) = Just n
        nameOfDecl _ = Nothing

runDeclPreparePassesM :: ProgramTransform
runDeclPreparePassesM = runPasses [refreshProgram]

runDeclOptPassesM :: CompilerSpec -> Maybe (SEffects.ExtInferF a, a) -> ProgramTransform
runDeclOptPassesM cSpec extInfOpt prog = do
  ensureParallelStSyms $ blockSize cSpec
  runPasses [blockMapProgramDecls (blockSize cSpec) [refreshProgram] passes] prog
  where passes = declOptPasses (stageSpec cSpec) extInfOpt

runDeclOptPassesBLM :: CompilerSpec -> Maybe (SEffects.ExtInferF a, a)
                    -> [K3 Declaration] -> TransformM [K3 Declaration]
runDeclOptPassesBLM cSpec extInfOpt block =
  parmapProgramDeclsBlock (declOptPasses (stageSpec cSpec) extInfOpt) block


{- Instances -}
instance Pretty TransformReport where
  prettyLines (TransformReport st sn) =
    tlines %$ [unwords ["Total observed compilation time:", secs tsum]]
           %$ Map.foldlWithKey prettySnapshot [] sn
    where
      maxnst = maximum $ map length $ Map.keys st
      maxnsn = maximum $ map length $ Map.keys sn
      padst s = replicate (maxnst - length s) ' '
      padsn s = replicate (maxnsn - length s) ' '

      stagg = map (second $ (sum &&& length) . map measTime) $ Map.assocs st
      tsum  = sum $ map (fst . snd) stagg
      trep  = map (second $ (\(tt,l) -> ((tt, percent tt), l))) stagg
      percent t = TPF.printf "%.2f" $ 100 * t / tsum

      tlines = concatMap prettyStats $ sortBy (compare `on` (fst . fst . snd)) trep
      prettyStats (n,((t,pt), l)) = [unwords [n ++ padst n, ":", secs t, pt, show l, "iters"]]

      prettySnapshot acc n l = acc %$ (flip concatMap l $ \p -> [n ++ padsn n ++ " "] %$ (prettyLines $ stripTypeAndEffectAnns p))
