{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | High-level API to K3 toolchain stages.
module Language.K3.Stages where

import Control.Arrow hiding ( left )
import Control.Exception
import Control.Monad
import Control.Monad.State
import Control.Monad.Trans.Either

import Criterion.Measurement
import Criterion.Types

import Data.Function
import Data.List
import Data.Map ( Map )
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import Debug.Trace

import qualified Text.Printf as TPF

import Language.K3.Core.Common
import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import Language.K3.Core.Utils

import Language.K3.Analysis.HMTypes.Inference hiding ( liftEitherM, tenv, inferDeclTypes )

import qualified Language.K3.Analysis.Properties           as Properties
import qualified Language.K3.Analysis.CArgs                as CArgs
import qualified Language.K3.Analysis.Provenance.Inference as Provenance
import qualified Language.K3.Analysis.SEffects.Inference   as SEffects

import Language.K3.Transform.Simplification
import Language.K3.Transform.TriggerSymbols (triggerSymbols)

import Language.K3.Utils.Pretty
import qualified Language.K3.Utils.PrettyText as PT

import Language.K3.Codegen.CPP.Materialization

-- | Snapshot specifications are a list of pass names to capture per declaration.
type SnapshotSpec = Map String [String]

-- | Configuration metadata for compiler stages
data CompilerSpec = CompilerSpec { blockSize :: Int
                                 , snapshotSpec :: SnapshotSpec }
                    deriving (Eq, Ord, Read, Show)

-- | Compilation profiling
data TransformReport = TransformReport { statistics :: Map String [Measured]
                                       , snapshots  :: Map String [K3 Declaration] }

-- | The program transformation composition monad
data TransformSt = TransformSt { nextuid    :: Int
                               , tenv       :: TIEnv
                               , prenv      :: Properties.PIEnv
                               , penv       :: Provenance.PIEnv
                               , fenv       :: SEffects.FIEnv
                               , report     :: TransformReport }

type TransformM = EitherT String (StateT TransformSt IO)

cs0 :: CompilerSpec
cs0 = CompilerSpec 16 Map.empty

rp0 :: TransformReport
rp0 = TransformReport Map.empty Map.empty

st0 :: K3 Declaration -> IO (Either String TransformSt)
st0 prog = do
  return $ mkEnv >>= \(stpe, stfe) ->
    return $ TransformSt puid tienv0 Properties.pienv0 stpe stfe rp0

  where puid = let UID i = maxProgramUID prog in i + 1
        mkEnv = do
          lcenv <- lambdaClosures prog
          let pe = Provenance.pienv0 lcenv
          return (pe, SEffects.fienv0 (Provenance.ppenv pe) lcenv)

runTransformStM :: TransformSt -> TransformM a -> IO (Either String (a, TransformSt))
runTransformStM st m = do
  (a, s) <- runStateT (runEitherT m) st
  return $ either Left (Right . (,s)) a

runTransformM :: TransformSt -> TransformM a -> IO (Either String a)
runTransformM st m = do
  e <- runTransformStM st m
  return $ either Left (Right . fst) e

liftEitherM :: Either String a -> TransformM a
liftEitherM = either left return


{-- Transform utilities --}
type ProgramTransform = K3 Declaration -> TransformM (K3 Declaration)
type ProgramTransformSt a = a -> K3 Declaration -> TransformM (a, K3 Declaration)

type TrF  = K3 Declaration -> K3 Declaration
type TrE  = K3 Declaration -> Either String (K3 Declaration)
type TrSF = TransformSt -> K3 Declaration -> K3 Declaration
type TrSE = TransformSt -> K3 Declaration -> Either String (K3 Declaration)

runPasses :: [ProgramTransform] -> ProgramTransform
runPasses passes p = foldM (flip ($)) p passes

bracketPasses :: String -> ProgramTransform -> [ProgramTransform] -> [ProgramTransform]
bracketPasses n f l = pass "preBracket" f ++ l ++ pass "postBracket" f
  where pass tg f' = if True then [f'] else [displayPass (unwords [n, tg]) $ f']

type PrpTrE = Properties.PIEnv -> K3 Declaration -> Either String (K3 Declaration, Properties.PIEnv)
type TypTrE = TIEnv -> K3 Declaration -> Either String (K3 Declaration, TIEnv)
type EffTrE = Provenance.PIEnv -> SEffects.FIEnv -> K3 Declaration
              -> Either String (K3 Declaration, Provenance.PIEnv, SEffects.FIEnv)

-- | Show the program after applying the given program transform.
displayPass :: String -> ProgramTransform -> ProgramTransform
displayPass n f p = f p >>= \np -> mkTg n np (return np)
  where mkTg str p' = trace (boxToString $ [str] %$ prettyLines p')

-- | Measure the execution time of a transform
timePass :: String -> ProgramTransform -> ProgramTransform
timePass n f prog = do
  (npE, sample) <- get >>= liftIO . profile f prog
  (np, nst)     <- liftEitherM npE
  void $ put $ addMeasurement n sample nst
  return np

  where
    addMeasurement n sample st =
      let rp  = report st
          nrp = rp {statistics = Map.insertWith (++) n [sample] $ statistics rp}
      in st {report = nrp}

    -- This is a reimplementation of Criterion.Measurement.measure
    -- while actually returning the value computed.
    profile tr arg st = do
      startStats     <- getGCStats
      startTime      <- getTime
      startCpuTime   <- getCPUTime
      startCycles    <- getCycles
      resultE        <- runTransformStM st $ tr arg
      wresultE       <- evaluate resultE
      endTime        <- getTime
      endCpuTime     <- getCPUTime
      endCycles      <- getCycles
      endStats       <- getGCStats
      let !m = applyGCStats endStats startStats $ measured {
                 measTime    = max 0 (endTime - startTime)
               , measCpuTime = max 0 (endCpuTime - startCpuTime)
               , measCycles  = max 0 (fromIntegral (endCycles - startCycles))
               , measIters   = 1
               }
      return (wresultE, m)

-- | Take a snapshot of the result of a transform
type SnapshotCombineF = [K3 Declaration] -> [K3 Declaration] -> [K3 Declaration]

lastSnapshot :: SnapshotCombineF
lastSnapshot a _ = a

snapshotPass :: String -> SnapshotCombineF -> ProgramTransform -> ProgramTransform
snapshotPass n combineF f prog = do
  np <- f prog
  modify $ addSnapshot n np
  return np

  where addSnapshot n np st =
          let rp  = report st
              nrp = rp {snapshots = Map.insertWith combineF n [np] $ snapshots rp}
          in st {report = nrp}

{-- Stateful transformations --}
withPropertyTransform :: PrpTrE -> ProgramTransform
withPropertyTransform f p = do
  st <- get
  (np, pre) <- liftEitherM $ f (prenv st) p
  void $ put $ st {prenv=pre}
  return np

withTypeTransform :: TypTrE -> ProgramTransform
withTypeTransform f p = do
  void $ ensureNoDuplicateUIDs p
  st <- get
  (np, te) <- liftEitherM $ f (tenv st) p
  void $ put $ st {tenv=te}
  return np

withEffectTransform :: EffTrE -> ProgramTransform
withEffectTransform f p = do
  st <- get
  (np,pe,fe) <- liftEitherM (f (penv st) (fenv st) p)
  void $ {-debugEffectTransform (penv st) (fenv st) pe fe $-} put (st {penv=pe, fenv=fe})
  return np

  where debugEffectTransform oldp oldf newp newf r =
          trace (T.unpack $ PT.boxToString $ [T.pack "Effect xform"]
                  PT.%$ [T.pack "Old P"] PT.%$ PT.prettyLines oldp
                  PT.%$ [T.pack "New P"] PT.%$ PT.prettyLines newp
                  PT.%$ [T.pack "Old F"] PT.%$ PT.prettyLines oldf
                  PT.%$ [T.pack "New F"] PT.%$ PT.prettyLines newf)
            $ r

withRepair :: String -> ProgramTransform -> ProgramTransform
withRepair msg f prog = do
  np <- f prog
  st <- get
  let (i,np') = repairProgram msg (Just $ nextuid st) np
  void $ put $ st {nextuid = i}
  return np'


{-- Transform constructors --}
transformF :: TrF -> ProgramTransform
transformF f p = return $ f p

transformE :: TrE -> ProgramTransform
transformE f p = liftEitherM $ f p

transformEDbg :: String -> TrE -> ProgramTransform
transformEDbg tg f p = do
  p' <- mkTg "Before " p $ transformE f p
  mkTg "After " p' $ return p'
  where mkTg pfx p' = trace (boxToString $ [pfx ++ tg] %$ prettyLines p')

transformSF :: TrSF -> ProgramTransform
transformSF f p = get >>= return . flip f p

transformSE :: TrSE -> ProgramTransform
transformSE f p = get >>= liftEitherM . flip f p

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

fixpointF :: TrF -> ProgramTransform
fixpointF f = transformFixpoint $ transformF f

fixpointE :: TrE -> ProgramTransform
fixpointE f = transformFixpoint $ transformE f

fixpointSF :: TrSF -> ProgramTransform
fixpointSF f = transformFixpoint $ transformSF f

fixpointSE :: TrSE -> ProgramTransform
fixpointSE f = transformFixpoint $ transformSE f

-- Fixpoint constructors with intermediate transformations between rounds.
fixpointIF :: [ProgramTransform] -> TrF -> ProgramTransform
fixpointIF interF f = transformFixpointI interF $ transformF f

fixpointIE :: [ProgramTransform] -> TrE -> ProgramTransform
fixpointIE interF f = transformFixpointI interF $ transformE f

fixpointISF :: [ProgramTransform] -> TrSF -> ProgramTransform
fixpointISF interF f = transformFixpointI interF $ transformSF f

fixpointISE :: [ProgramTransform] -> TrSE -> ProgramTransform
fixpointISE interF f = transformFixpointI interF $ transformSE f


{- Whole program analyses -}
ensureNoDuplicateUIDs :: ProgramTransform
ensureNoDuplicateUIDs p =
  let dupUids = duplicateProgramUIDs p
  in if null dupUids then return p
     else left $ T.unpack $ PT.boxToString $ [T.pack "Found duplicate uids:"]
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
  (p,  pienv) <- liftEitherM $ Provenance.inferProgramProvenance prog
  (p', fienv) <- liftEitherM $ SEffects.inferProgramEffects Nothing (Provenance.ppenv pienv) p
  void $ modify $ \st -> st {penv = pienv, fenv = fienv}
  return p'

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
refreshProgram = runPasses [inferFreshTypesAndEffects, inferFreshProperties]

{- Whole program optimizations -}
simplify :: ProgramTransform
simplify = transformFixpoint $ runPasses simplifyPasses
  where simplifyPasses = intersperse refreshProgram $
                           map (mkXform False) [ ("CF", foldProgramConstants)
                                               , ("BR", betaReductionOnProgram)
                                               , ("DCE", eliminateDeadProgramCode) ]
        mkXform asDebug (i,f) = withRepair i $ (if asDebug then transformEDbg i else transformE) f

-- TODO: debug and revise fixpoints
simplifyWCSE :: ProgramTransform
simplifyWCSE p = simplify p >>= transformE commonProgramSubexprElim

streamFusion :: ProgramTransform
streamFusion = runPasses [fusionEncodeFixpoint, inferFreshTypesAndEffects, fusionTransformFixpoint]
  where
    mkXform asDebug i f     = withRepair i $ (if asDebug then transformEDbg i else transformE) f
    fusionEncode            = mkXform False "fusionEncode"    encodeProgramTransformers
    fusionTransform         = mkXform False "fusionTransform" fuseProgramFoldTransformers
    fusionReduce            = mkXform False "fusionReduce"    betaReductionOnProgram
    fusionEncodeFixpoint    = transformFixpointI [inferFreshTypesAndEffects] fusionEncode
    fusionTransformFixpoint = transformFixpointI fusionInterF fusionTransform
    fusionInterF            = bracketPasses "fusion" inferFreshTypesAndEffects [fusionReduce]


{- Whole program pass aliases -}
optPasses :: [ProgramTransform]
optPasses = map prepareOpt [ (simplify,     "opt-simplify-prefuse")
                           , (streamFusion, "opt-fuse")
                           , (simplify,     "opt-simplify-final") ]
  where prepareOpt (f,i) = runPasses [refreshProgram, withRepair i f]

cgPasses :: Int -> [ProgramTransform]
cgPasses _ = [ withRepair "TID" $ transformE triggerSymbols
             , refreshProgram
             , transformF CArgs.runAnalysis
             , \d -> get >>= \s -> return $ (optimizeMaterialization (penv s, fenv s)) d
             ]

runOptPassesM :: ProgramTransform
runOptPassesM prog = runPasses optPasses $ stripTypeAndEffectAnns prog

runCGPassesM :: Int -> ProgramTransform
runCGPassesM lvl prog = runPasses (cgPasses lvl) prog

-- Legacy methods.
runOptPasses :: K3 Declaration -> IO (Either String (K3 Declaration, TransformReport))
runOptPasses prog = st0 prog >>= either (return . Left) run
  where run st = do
          resE <- runTransformStM st $ runOptPassesM prog
          return (resE >>= return . second report)

runCGPasses :: Int -> K3 Declaration -> IO (Either String (K3 Declaration))
runCGPasses lvl prog = do
  stE <- st0 prog
  either (return . Left) (\st -> runTransformM st $ runCGPassesM lvl prog) stE


{- Declaration-at-a-time analyses and optimizations. -}

-- | Program traversal with fixpoints per declaration.
foldProgramDecls :: Bool -> (a -> K3 Declaration -> (a, [ProgramTransform])) -> ProgramTransformSt a
foldProgramDecls asFixpoint passesF z prog =
  foldProgram declFixpoint idF idF Nothing z prog
  where idF a b = return (a, b)
        declFixpoint acc d = do
          let (nacc, passes) = passesF acc d
          nd <- ((if asFixpoint then transformFixpoint else id) $ runPasses passes) d
          return (nacc, nd)

mapProgramDecls :: (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
mapProgramDecls passesF prog =
  foldProgramDecls False (\_ d -> ((), passesF d)) () prog >>= return . snd

mapProgramDeclFixpoint :: (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
mapProgramDeclFixpoint passesF prog =
  foldProgramDecls True (\_ d -> ((), passesF d)) () prog >>= return . snd

blockMapProgramDeclFixpoint :: Int -> [ProgramTransform] -> (K3 Declaration -> [ProgramTransform]) -> ProgramTransform
blockMapProgramDeclFixpoint blockSize blockPassesF declPassesF prog = blockDriver emptySeen prog
  where
    emptySeen              = (Set.empty, [])
    addNamedSeen   (n,u) i = (Set.insert i n, u)
    addUnnamedSeen (n,u) i = (n, i:u)
    seenNamed      (n,_) i = Set.member i n
    seenUnnamed    (_,u) i = i `elem` u

    blockDriver seen p = do
      ((nseen, anyNew), np) <- blockedPass seen p
      np' <- trace "Finished a block" $ runPasses blockPassesF np
      if not anyNew && compareDAST np' p then return np'
      else blockDriver nseen np'

    blockedPass seen p = do
      let z = (seen, blockSize, False)
      ((nseen, _, anyNew), np) <- foldProgramDecls True passesWSkip z p
      return ((nseen, anyNew), np)

    passesWSkip (seen, cnt, anyNew) d@(nameOfDecl -> nOpt) =
      let thisSeen        = maybe (seenUnnamed seen d) (seenNamed seen) nOpt
          nAnyNew         = anyNew || not thisSeen
          (nseen, passes) = if thisSeen then (seen, []) else (case nOpt of
                              Nothing -> (addUnnamedSeen seen d, declPassesF d)
                              Just n  -> if cnt == 0 then ({-trace ("Skip " ++ n)-} (seen, []))
                                         else (trace ("Process " ++ n) $ (addNamedSeen seen n, declPassesF d)))
      in
      let ncnt = maybe cnt (const $ if cnt == 0 || thisSeen then cnt else (cnt-1)) nOpt
          nacc = (nseen, ncnt, nAnyNew)
      in (nacc, passes)

    nameOfDecl (tag -> DGlobal  n _ _) = Just n
    nameOfDecl (tag -> DTrigger n _ _) = Just n
    nameOfDecl _ = Nothing

inferDeclProperties :: Identifier -> ProgramTransform
inferDeclProperties n = withPropertyTransform $ \pre p ->
  Properties.reinferProgDeclUsageProperties pre n p

inferFreshDeclProperties :: Identifier -> ProgramTransform
inferFreshDeclProperties n = inferDeclProperties n . stripDeclProperties n

inferDeclTypes :: Identifier -> ProgramTransform
inferDeclTypes n = withTypeTransform $ \te p -> reinferProgDeclTypes te n p

inferDeclEffects :: Maybe (SEffects.ExtInferF a, a) -> Identifier -> ProgramTransform
inferDeclEffects extInfOpt n = withEffectTransform $ \pe fe p -> do
  (nlc, _)   <- lambdaClosuresDecl n (Provenance.plcenv pe) p
  let pe'     = pe {Provenance.plcenv = nlc}
  (np,  npe) <- {-debugPretty ("Reinfer P\n" ++ show nlc) p $-} Provenance.reinferProgDeclProvenance pe' n p
  let fe'     = fe {SEffects.fppenv = Provenance.ppenv npe, SEffects.flcenv = nlc}
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
declTransforms :: SnapshotSpec -> Maybe (SEffects.ExtInferF a, a) -> Identifier -> Map Identifier ProgramTransform
declTransforms snSpec extInfOpt n = topLevel
  where
    topLevel  = Map.fromList [
        mkSeqRep "Optimize" highLevel $ prepend [ ("refreshI",      False) ]
                                              $ [ ("Decl-Simplify", True)
                                                , ("Decl-Fuse",     True)
                                                , ("Decl-Simplify", True) ]
      ] `Map.union` highLevel

    highLevel = Map.fromList [
        mkT $ mkSeq "Decl-Simplify" lowLevel $ intersperse "refreshI" $ [ "Decl-CF"
                                                                        , "Decl-BR"
                                                                        , "Decl-DCE" ]
      , mkT $ mkSeq "Decl-Fuse" lowLevel [ "Decl-FE"
                                         , "typEffI"
                                         , "Decl-FT" ]
      ] `Map.union` lowLevel

    -- TODO: CF,BR,DCE should be a local fixpoint.
    lowLevel = Map.fromList [
        mk foldConstants        "Decl-CF"  True False Nothing
      , mk betaReduction        "Decl-BR"  True False Nothing
      , mk eliminateDeadCode    "Decl-DCE" True False Nothing
      , mk encodeTransformers   "Decl-FE"  True False (Just [typEffI])
      , mk fuseFoldTransformers "Decl-FT"  True False (Just fusionI)
      , fusionReduce
      , ("typEffI",)  $ typEffI
      , ("refreshI",) $ refreshI
      ]

    mk f i asRepair asDebug fixPassOpt = mkSS $ (i,)
      $ (maybe id mkFix fixPassOpt)
      $ (if asRepair then withRepair i else id)
      $ (if asDebug then transformEDbg i else transformE)
      $ mapNamedDeclExpression n f

    mkFix interF = transformFixpointI interF

    -- Timing and snapshotting.
    mkN i = unwords [n, i]
    mkT (i,f)  = (i, timePass (mkN i) f)
    mkS (i,f)  = (i, snapshotPass (mkN i) lastSnapshot f)
    mkSS (i,f) = maybe (i,f) (\l -> if i `elem` l then mkS (i,f) else (i,f))
                   $ Map.lookup n snSpec

    -- Shared passes
    fusionReduce = mk betaReduction "Decl-FR" True False Nothing

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

declOptPasses :: SnapshotSpec -> Maybe (SEffects.ExtInferF a, a) -> K3 Declaration -> [ProgramTransform]
declOptPasses snSpec extInfOpt d = case nameOfDecl d of
  Nothing -> []
  Just n -> trace ("Optimizing " ++ n) $ [getTransform "Optimize" $ declTransforms snSpec extInfOpt n]

  where nameOfDecl (tag -> DGlobal  n _ (Just _)) = Just n
        nameOfDecl (tag -> DTrigger n _ _) = Just n
        nameOfDecl _ = Nothing

runDeclOptPassesM :: CompilerSpec -> Maybe (SEffects.ExtInferF a, a) -> ProgramTransform
runDeclOptPassesM cSpec extInfOpt =
  runPasses [refreshProgram, blockMapProgramDeclFixpoint (blockSize cSpec) [refreshProgram] passes]
  where passes = declOptPasses (snapshotSpec cSpec) extInfOpt

runDeclOptPasses :: CompilerSpec -> Maybe (SEffects.ExtInferF a, a)
                 -> K3 Declaration -> IO (Either String (K3 Declaration, TransformReport))
runDeclOptPasses cSpec extInfOpt prog = st0 prog >>= either (return . Left) run
  where run st = do
          resE <- runTransformStM st $ runDeclOptPassesM cSpec extInfOpt prog
          return (resE >>= return . second report)


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

      prettySnapshot acc n l = acc %$ (flip concatMap l $ \p -> [n ++ padsn n ++ " "] %$ prettyLines p)
