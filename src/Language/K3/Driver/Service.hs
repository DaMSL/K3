{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Service where

import Control.Lens hiding ( transform, each )
import Control.Exception
import Control.Monad
import Control.Concurrent hiding ( yield )
import Control.Concurrent.Async
import Control.Monad.Trans.Except
import Control.Monad.Trans.State.Strict

import Data.List.Split
import Data.Maybe
import Data.Heap ( Heap )
import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Heap as Heap
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.ByteString.Char8 as BC

import GHC.Generics ( Generic )

import Network.Simple.TCP
import Pipes
import Pipes.Binary hiding ( get )
import Pipes.Concurrent
import Pipes.Network.TCP
import Pipes.Parse

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser ( parseK3 )
import qualified Language.K3.Stages as ST

import Language.K3.Driver.Common
import Language.K3.Driver.Options
import Language.K3.Driver.Driver hiding ( reasonM )
import qualified Language.K3.Driver.CompilerTarget.CPP     as CPPC

import Language.K3.Utils.Pretty

-- | Compiler service protocol, supporting compile tasks (Program*, Block* messages)
--   and worker tracking (Register*, Heartbeat*)
data CProtocol = Register     String
               | RegisterAck  CompileOptions
               | Heartbeat    Integer
               | HeartbeatAck Integer
               | Program      Request ByteString RemoteJobOptions (Maybe RequestID)
               | ProgramDone  Request ByteString
               | Block        ProgramID CompileStages ByteString [(BlockID, [ByteString])]
               | BlockDone    WorkerID ProgramID [(BlockID, [ByteString])]
               | Quit
               deriving (Eq, Read, Show, Generic)

instance Binary CProtocol

-- | Service thread state, encapsulating task workers and connection workers.
data ThreadState = ThreadState { sthread   :: Maybe ThreadId
                               , ttworkers :: Set (Async ())
                               , tcworkers :: Set ThreadId }
                  deriving ( Eq, Ord )

-- | Mailbox handles
type MBHandle = Maybe (Output CProtocol, Input CProtocol)

-- | Compilation progress state.
type Request   = String
type RequestID = Int
type ClientID  = SockAddr
type WorkerID  = String
type BlockID   = Int
type ProgramID = Int

data JobState = JobState { jrid         :: RequestID
                         , jrq          :: Request
                         , pending      :: BlockSet
                         , completed    :: BlockSourceMap }
                deriving (Eq, Read, Show)

type BlockSet       = Set BlockID
type BlockSourceMap = Map BlockID   [K3 Declaration]
type JobsMap        = Map ProgramID JobState
type AssignmentsMap = Map WorkerID  BlockSet
type WorkersMap     = Map WorkerID  (Socket, SockAddr)

type RequestSet     = Set RequestID
type ClientMap      = Map ClientID  (Socket, RequestSet)
type RequestMap     = Map RequestID ClientID

-- | Common state for the compiler service.
data ServiceState a = ServiceState { threadSt      :: ThreadState
                                   , mbhndl        :: MBHandle
                                   , stcompileOpts :: CompileOptions
                                   , compileSt     :: a }

-- | Compiler service master state.
data SMST = SMST { cworkers     :: WorkersMap
                 , cclients     :: ClientMap
                 , crequests    :: RequestMap
                 , bcnt         :: Int
                 , pcnt         :: Int
                 , rcnt         :: Int
                 , hbcnt        :: Integer
                 , jobs         :: JobsMap
                 , assignments  :: AssignmentsMap }

type ServiceMState = ServiceState SMST

-- | Compiler service worker state.
data SWST = SWST { cmaster :: Maybe (Socket, SockAddr) }

type ServiceWState = ServiceState SWST

-- | Mutable service state variable.
type ServiceSTVar a = MVar a
type ServiceST    a = MVar (ServiceState a)
type ServiceMSTVar  = ServiceST SMST
type ServiceWSTVar  = ServiceST SWST

-- | A K3 compiler service monad.
type ServiceM a = ExceptT String (StateT (ServiceSTVar a) IO)

-- | Type definitions for service master and worker monads.
type ServiceMM = ServiceM ServiceMState
type ServiceWM = ServiceM ServiceWState

-- | Producers and consumers of the compiler protocol.
type SProducer a b = Producer CProtocol (ServiceM a) b
type SConsumer a b = Consumer CProtocol (ServiceM a) b

type SocketHandler a = Socket -> SockAddr -> ServiceM a ()
type WorkHandler a = Int -> CProtocol -> SConsumer a ()

{- Initial states -}

sthread0 :: ThreadState
sthread0 = ThreadState Nothing Set.empty Set.empty

sst0 :: CompileOptions -> a -> ServiceState a
sst0 cOpts st = ServiceState sthread0 Nothing cOpts st

sstm0 :: CompileOptions -> ServiceMState
sstm0 cOpts = sst0 cOpts $ SMST Map.empty Map.empty Map.empty 0 0 0 0 Map.empty Map.empty

svm0 :: CompileOptions -> IO ServiceMSTVar
svm0 cOpts = newMVar $ sstm0 cOpts

sstw0 :: CompileOptions -> ServiceWState
sstw0 cOpts = sst0 cOpts $ SWST Nothing

svw0 :: CompileOptions -> IO ServiceWSTVar
svw0 cOpts = newMVar $ sstw0 cOpts


{- Service monad utilities -}

runServiceM :: ServiceSTVar a -> ServiceM a b -> IO (Either String b)
runServiceM st m = evalStateT (runExceptT m) st

runServiceM_ :: ServiceSTVar a -> ServiceM a b -> IO ()
runServiceM_ st m = runServiceM st m >>= either putStrLn (const $ return ())

liftI :: IO b -> ServiceM a b
liftI = lift . liftIO

liftIE :: IO (Either String b) -> ServiceM a b
liftIE m = ExceptT $ liftIO m

liftIC :: IO b -> SConsumer a b
liftIC = lift . liftI

liftIP :: IO b -> SProducer a b
liftIP = lift . liftI

liftEitherM :: Either String b -> ServiceM a b
liftEitherM e = liftIE $ return e

reasonM :: String -> ServiceM a b -> ServiceM a b
reasonM msg m = withExceptT (msg ++) m

modifyM :: (a -> (a, b)) -> ServiceM a b
modifyM f = getV >>= \sv -> liftI (modifyMVar sv (return  . f))

modifyM_ :: (a -> a) -> ServiceM a ()
modifyM_ f = getV >>= \sv -> liftI (modifyMVar_ sv (return  . f))

getV :: ServiceM a (ServiceSTVar a)
getV = lift get

getSt :: ServiceM a a
getSt = getV >>= liftIO . readMVar

{- Thread and async task management -}
getTS :: ServiceM (ServiceState a) ThreadState
getTS = getSt >>= return . threadSt

modifyTS :: (ThreadState -> (ThreadState, b)) -> ServiceM (ServiceState a) b
modifyTS f = modifyM $ \st -> let (nt,r) = f $ threadSt st in (st {threadSt = nt}, r)

modifyTS_ :: (ThreadState -> ThreadState) -> ServiceM (ServiceState a) ()
modifyTS_ f = modifyM_ $ \st -> st { threadSt = f $ threadSt st }

{- Mailbox handle accessors -}
getQ :: ServiceM (ServiceState a) (Output CProtocol, Input CProtocol)
getQ = getSt >>= return . fromJust . mbhndl

cgetQ :: SConsumer (ServiceState a) (Output CProtocol, Input CProtocol)
cgetQ = lift $ getQ

pgetQ :: SProducer (ServiceState a) (Output CProtocol, Input CProtocol)
pgetQ = lift $ getQ

putQ :: Output CProtocol -> Input CProtocol -> ServiceM (ServiceState a) ()
putQ e d = modifyM_ $ \st -> st { mbhndl = Just (e, d) }

getCO :: ServiceM (ServiceState a) CompileOptions
getCO = getSt >>= return . stcompileOpts

putCO :: CompileOptions -> ServiceM (ServiceState a) ()
putCO cOpts = modifyM_ $ \st -> st { stcompileOpts = cOpts }

modifyCO_ :: (CompileOptions -> CompileOptions) -> ServiceM (ServiceState a) ()
modifyCO_ f = modifyM_ $ \st -> st { stcompileOpts = f $ stcompileOpts st }

{- Service master accessors -}
modifyMM :: (SMST -> (SMST, a)) -> ServiceMM a
modifyMM f = modifyM $ \st -> let (ncst, r) = f $ compileSt st in (st { compileSt = ncst }, r)

modifyMM_ :: (SMST -> SMST) -> ServiceMM ()
modifyMM_ f = modifyM_ $ \st -> st { compileSt = f $ compileSt st }

getM :: ServiceMM SMST
getM = getSt >>= return . compileSt

putM :: SMST -> ServiceMM ()
putM ncst = modifyM_ $ \st -> st { compileSt = ncst }

{- Counter accessors -}
progIDM :: ServiceMM ProgramID
progIDM = modifyMM $ \cst -> let i = pcnt cst + 1 in (cst {pcnt = i}, i)

blockIDM :: ServiceMM BlockID
blockIDM = modifyMM $ \cst -> let i = bcnt cst + 1 in (cst {bcnt = i}, i)

requestIDM :: ServiceMM BlockID
requestIDM = modifyMM $ \cst -> let i = rcnt cst + 1 in (cst {rcnt = i}, i)

heartbeatM :: ServiceMM Integer
heartbeatM = modifyMM $ \cst -> let i = hbcnt cst + 1 in (cst {hbcnt = i}, i)

{- Assignments accessors -}
getMA :: ServiceMM AssignmentsMap
getMA = getM >>= return . assignments

putMA :: AssignmentsMap -> ServiceMM ()
putMA nassigns = modifyMM_ $ \cst -> cst { assignments = nassigns }

modifyMA :: (AssignmentsMap -> (AssignmentsMap, a)) -> ServiceMM a
modifyMA f = modifyMM $ \cst -> let (nassigns, r) = f $ assignments cst in (cst { assignments = nassigns }, r)

modifyMA_ :: (AssignmentsMap -> AssignmentsMap) -> ServiceMM ()
modifyMA_ f = modifyMM_ $ \cst -> cst { assignments = f $ assignments cst }

mapMA :: (WorkerID -> BlockSet -> a) -> ServiceMM (Map WorkerID a)
mapMA f = getMA >>= return . Map.mapWithKey f

foldMA :: (a -> WorkerID -> BlockSet -> a) -> a -> ServiceMM a
foldMA f z = getMA >>= return . Map.foldlWithKey f z

{- Job state accessors -}
getMJ :: ServiceMM JobsMap
getMJ = getM >>= return . jobs

putMJ :: JobsMap -> ServiceMM ()
putMJ njobs = modifyMM_ $ \cst -> cst { jobs = njobs }

modifyMJ :: (JobsMap -> (JobsMap, a)) -> ServiceMM a
modifyMJ f = modifyMM $ \cst -> let (njobs, r) = f $ jobs cst in (cst { jobs = njobs }, r)

modifyMJ_ :: (JobsMap -> JobsMap) -> ServiceMM ()
modifyMJ_ f = modifyMM_ $ \cst -> cst { jobs = f $ jobs cst }

{- Service request, client and worker accessors -}
getMC :: ClientID -> ServiceMM (Maybe (Socket, RequestSet))
getMC c = getM >>= return . Map.lookup c . cclients

putMC :: ClientID -> Socket -> RequestSet -> ServiceMM ()
putMC c s r = modifyMM_ $ \cst -> cst { cclients = Map.insert c (s,r) $ cclients cst }

modifyMC :: (ClientMap -> (ClientMap, a)) -> ServiceMM a
modifyMC f = modifyMM $ \cst -> let (ncc, r) = f $ cclients cst in (cst {cclients = ncc}, r)

modifyMC_ :: (ClientMap -> ClientMap) -> ServiceMM ()
modifyMC_ f = modifyMM_ $ \cst -> cst {cclients = f $ cclients cst}

getMR :: RequestID -> ServiceMM (Maybe ClientID)
getMR r = getM >>= return . Map.lookup r . crequests

putMR :: RequestID -> ClientID -> ServiceMM ()
putMR r c = modifyMM_ $ \cst -> cst { crequests = Map.insert r c $ crequests cst }

modifyMR :: (RequestMap -> (RequestMap, a)) -> ServiceMM a
modifyMR f = modifyMM $ \cst -> let (nrq, r) = f $ crequests cst in (cst {crequests = nrq}, r)

modifyMR_ :: (RequestMap -> RequestMap) -> ServiceMM ()
modifyMR_ f = modifyMM_ $ \cst -> cst {crequests = f $ crequests cst}

getMW :: ServiceMM WorkersMap
getMW = getM >>= return . cworkers

getMWI :: WorkerID -> ServiceMM (Maybe (Socket, SockAddr))
getMWI wid = getM >>= return . Map.lookup wid . cworkers

putMWI :: WorkerID -> Socket -> SockAddr -> ServiceMM ()
putMWI wid s a = modifyMM_ $ \cst -> cst { cworkers = Map.insert wid (s,a) $ cworkers cst }

{- Worker state accessors -}
modifyWM :: (SWST -> (SWST, a)) -> ServiceWM a
modifyWM f = modifyM $ \st -> let (ncst, r) = f $ compileSt st in (st { compileSt = ncst }, r)

modifyWM_ :: (SWST -> SWST) -> ServiceWM ()
modifyWM_ f = modifyM_ $ \st -> st { compileSt = f $ compileSt st }

getW :: ServiceWM SWST
getW = getSt >>= return . compileSt

putW :: SWST -> ServiceWM ()
putW ncst = modifyM_ $ \st -> st { compileSt = ncst }

getWC :: ServiceWM (Maybe (Socket, SockAddr))
getWC = getW >>= return . cmaster

putWC :: Socket -> SockAddr -> ServiceWM ()
putWC s a = modifyWM_ $ \cst -> cst { cmaster = Just (s, a) }

{- Misc -}
putStrLnM :: String -> ServiceM a ()
putStrLnM = liftIO . putStrLn

-- | Service utilities.
runServer :: ServiceOptions -> ServiceSTVar a
          -> (ThreadId -> ServiceM a ())
          -> (ThreadId -> ServiceM a ())
          -> ServiceM a () -> SocketHandler a -> SocketHandler a
          -> IO ()
runServer sOpts sv0 onThreadF onCThreadF onShutdown onListenF onConnF = withSocketsDo $ do
  stid <- myThreadId
  runServiceM_ sv0 $ onThreadF stid
  flip finally (runServiceM_ sv0 onShutdown) $
    listen (Host $ serviceHost sOpts) (show $ servicePort sOpts) $ \(lsock, laddr) -> runServiceM_ sv0 $ do
      putStrLnM $ "Listening for TCP connections at " ++ show laddr
      onListenF lsock laddr
      void $ forever $ acceptConnection lsock
      putStrLnM $ "Closing listener at " ++ show laddr

  where
    acceptConnection lsock = do
      ctid <- acceptFork lsock runConnection
      onCThreadF ctid

    runConnection (csock, caddr) = do
      putStrLn $ "Accepted incoming connection from " ++ show caddr
      runServiceM_ sv0 $ onConnF csock caddr
      putStrLn $ "Closing connection from " ++ show caddr


runClient :: ServiceOptions -> ServiceSTVar a -> ServiceM a () -> SocketHandler a -> IO ()
runClient sOpts sv0 onShutdown onConnF = withSocketsDo $
  flip finally (runServiceM_ sv0 onShutdown) $
    connect (serviceHost sOpts) (show $ servicePort sOpts) $ \(sock, addr) -> do
      putStrLn $ "Connection established to " ++ show addr
      runServiceM_ sv0 $ onConnF sock addr

terminate :: ServiceM (ServiceState a) ()
terminate = do
    tst <- getTS
    liftIO $ killThread $ fromJust $ sthread $ tst

shutdown :: ServiceM (ServiceState a) ()
shutdown = do
    tst <- getTS
    liftIO $ forM_ (Set.toList $ ttworkers $ tst) $ \as -> cancel as
    liftIO $ forM_ (Set.toList $ tcworkers $ tst) $ \tid -> killThread tid

workqueue :: ServiceOptions -> WorkHandler (ServiceState a) -> ServiceM (ServiceState a) ()
workqueue sOpts workerF = do
    (qEnq, qDeq) <- liftIO $ spawn unbounded
    putQ qEnq qDeq
    workers (serviceThreads sOpts) workerF

workers :: Int -> WorkHandler (ServiceState a) -> ServiceM (ServiceState a) ()
workers numWorkers workerF = do
    sv <- getV
    (_, qDeq) <- getQ
    forM_ [1..numWorkers] $ asyncWorker sv qDeq
  where asyncWorker sv qDeq i = do
          as <- liftIO $ async $ runServiceM_ sv $ runEffect $ fromInput qDeq >-> worker i workerF
          modifyTS_ $ \tst -> tst { ttworkers = Set.insert as $ ttworkers tst }

worker :: Int -> WorkHandler (ServiceState a) -> SConsumer (ServiceState a) ()
worker workerId workerF = forever $ do
    job <- await
    workerF workerId job

-- | Send a message over the given socket.
sendC :: Socket -> CProtocol -> ServiceM (ServiceState a) ()
sendC sock m = runEffect $ encode m >-> toSocket sock

sendCs :: [(Socket, CProtocol)] -> ServiceM (ServiceState a) ()
sendCs msgs = forM_ msgs $ uncurry sendC

-- | Process messages from the socket with the given handler.
messages :: Socket -> (CProtocol -> ServiceM (ServiceState a) Bool) -> ServiceM (ServiceState a) ()
messages sock handler = runmsg $ (fromSocket sock 4096) ^. decoded
  where runmsg p = runStateT untilEmpty p >>= \(quit, np) -> unless quit (runmsg np)
        untilEmpty = draw >>= maybe (return True) (lift . handler)

-- | Compiler service master.
runServiceMaster :: ServiceOptions -> ServiceMasterOptions -> Options -> IO ()
runServiceMaster sOpts smOpts opts = svm0 (scompileOpts sOpts) >>= \sv ->
    runServer sOpts sv onInit onConnInit onShutdown (onMasterListen sv) (processMasterConn sOpts smOpts)
  where
    -- | Thread management.
    onInit     tid = modifyTS_ $ \tst -> tst { sthread = Just tid }
    onConnInit tid = modifyTS_ $ \tst -> tst { tcworkers = Set.insert tid $ tcworkers tst }

    onShutdown = do
      wm <- getMW
      forM_ (map fst $ Map.elems wm) $ \wsock -> sendC wsock Quit
      shutdown

    -- | Create a workqueue and a timer thread when the master's server socket is up.
    onMasterListen sv _ _ = do
      workqueue sOpts (goSMaster sOpts smOpts opts)
      liftIO $ void $ async $ heartbeatLoop sv

    heartbeatLoop sv = forever $ runServiceM_ sv $ do
      liftIO $ threadDelay heartbeatPeriod
      hbid <- heartbeatM
      wsockets <- getMW >>= return . Map.elems
      forM_ wsockets $ \(sock, addr) -> do
        putStrLnM $ unwords ["Pinging", show addr]
        sendC sock $ Heartbeat hbid

    heartbeatPeriod = seconds 10
    seconds x = x * 1000000

-- | Compiler service worker.
runServiceWorker :: ServiceOptions -> IO ()
runServiceWorker sOpts = svw0 (scompileOpts sOpts) >>= \sv ->
  runClient sOpts sv shutdown (processWorkerConn sOpts)

-- | Compiler service protocol handlers.
processMasterConn :: ServiceOptions -> ServiceMasterOptions -> Socket -> SockAddr -> ServiceM ServiceMState ()
processMasterConn sOpts smOpts sock addr = messages sock mHandler
  where
    -- | Client and worker messages forwarded out of connection thread.
    mHandler (Program rq s jo Nothing) = continue $ do
      rid <- trackRequest
      enqueueMsg (Program rq s jo $ Just rid)

    mHandler m@(BlockDone _ _ _) = continue $ enqueueMsg m

    -- | Remote worker messages
    mHandler (Register wid) = continue $ do
      putStrLnM $ unwords ["Registering worker", wid]
      putMWI wid sock addr
      cOpts <- getCO
      sendC sock $ RegisterAck cOpts

    -- TODO: detect worker changes.
    mHandler (HeartbeatAck hbid) = continue $ putStrLnM $ unwords ["Got a heartbeat ack", show hbid]

    -- | Service termination.
    mHandler Quit = halt $ terminate

    mHandler m = halt $ putStrLnM $ boxToString $ ["Invalid message for compiler master:"] %$ [show m]

    continue m = m >> return False
    halt     m = m >> return True

    enqueueMsg m = getQ >>= \(qEnq,_) -> runEffect (yield m >-> toOutput qEnq)

    trackRequest = requestIDM >>= \rid ->
      putMC addr sock (Set.singleton rid) >> putMR rid addr >> return rid

processWorkerConn :: ServiceOptions -> Socket -> SockAddr -> ServiceM ServiceWState ()
processWorkerConn sOpts sock addr = do
    putWC sock addr
    workqueue sOpts $ goSWorker sOpts
    sendC sock $ Register $ serviceId sOpts
    messages sock mHandler

  where
    -- | Work messages forwarded out of connection thread.
    mHandler m@(Block _ _ _ _) = continue $ enqueueMsg m

    -- | Connection messages processed in-band.
    mHandler (RegisterAck cOpts) = continue $ do
      putStrLnM $ unwords ["Worker registered", show cOpts]
      modifyCO_ $ mergeCompileOpts cOpts

    mHandler (Heartbeat hbid) = continue $ do
      putStrLnM $ unwords ["Worker got heartbeat", show hbid]
      sendC sock $ HeartbeatAck hbid

    mHandler Quit = halt $ terminate

    mHandler m = halt $ putStrLnM $ boxToString $ ["Invalid message for compiler worker:"] %$ [show m]

    continue m = m >> return False
    halt     m = m >> return True

    enqueueMsg m = getQ >>= \(qEnq,_) -> runEffect (yield m >-> toOutput qEnq)

    -- | Synchronizes relevant master and worker compiler options.
    --   These are defaults that can be overridden per compile job.
    mergeCompileOpts mcopts wcopts = wcopts { outLanguage = outLanguage $ mcopts
                                            , programName = programName $ mcopts
                                            , outputFile  = outputFile  $ mcopts
                                            , useSubTypes = useSubTypes $ mcopts
                                            , optimizationLevel = optimizationLevel $ mcopts }


-- | Work handling function for the service master.
goSMaster :: ServiceOptions -> ServiceMasterOptions -> Options -> Int -> CProtocol -> SConsumer ServiceMState ()
goSMaster sOpts@(serviceId -> msid) smOpts opts workerId msg = do
    case msg of
      Program rq prog jobOpts (Just rid) -> process (BC.unpack prog) jobOpts rq rid
      BlockDone wid pid blocksByBID -> completeBlocks wid pid blocksByBID
      m -> lift $ putStrLnM $ boxToString $ ["Invalid message for compiler master thread:"] %$ [show m]

  where
    nfP        = noFeed $ input opts
    includesP  = (includes $ paths opts)

    process prog jobOpts rq rid = lift $ do
      putStrLnM $ "Master thread " ++ (show workerId) ++ " got a program"
      (blocks, initSt) <- preprocess prog jobOpts
      assignBlocks rid rq jobOpts blocks initSt

    preprocess prog jobOpts = do
      putStrLnM $ "Parsing with paths " ++ show includesP
      ((initP, _), initSt) <- prepareProgram prog
      initParts <- partitionProgram (jobBlockSize jobOpts) initP
      return (initParts, initSt)

    -- | Parse, evaluate metaprogram, and apply prepare transform.
    prepareProgram prog = do
      pP <- reasonM parseError . liftIE $ parseK3 nfP includesP prog
      mP <- liftIE . runDriverM $ metaprogram opts pP
      liftIE $ runTransform (coStages $ scompileOpts $ sOpts) mP

    partitionProgram n (tnc -> (DRole _, ch)) =
      let blocks = chunksOf n ch
      in mapM (\bl -> blockIDM >>= return . (, map (BC.pack . show) bl)) blocks

    partitionProgram _ _ = throwE "Top level declaration is not a role."

    {- Job management -}
    js0 rid rq = JobState rid rq Set.empty Map.empty

    assignBlocks rid rq jobOpts blocks initSt = do
      pid <- progIDM
      putStrLnM $ unwords ["Assigning blocks for program", show pid]
      putStrLnM $ unwords ["State size", show $ BC.length $ BC.pack $ show initSt]
      waCounts <- countAssignments pid
      (_, blocksByWID, nassigns, js) <- foldM assignBlock (waCounts, Map.empty, Map.empty, js0 rid rq) blocks
      putStrLnM $ unwords ["Assignments:", show $ Map.toList nassigns]
      modifyMJ_ $ \jbs -> Map.insert pid js jbs
      modifyMA_ $ \assigns -> Map.unionWith Set.union assigns nassigns
      blockMsgs <- mkMessages pid jobOpts initSt blocksByWID
      sendCs blockMsgs

    assignBlock (waCounts, msgacc, asacc, jsacc) (bid, block) = do
      (wid, nwaCounts) <- assignToMin waCounts $ length block
      return $ ( nwaCounts
               , Map.insertWith (\new old -> old ++ new) wid [(bid, block)] msgacc
               , Map.insertWith Set.union wid (Set.singleton bid) asacc
               , jsacc {pending = Set.insert bid $ pending jsacc} )

    -- | Compute a min-heap of assignment counts, instantiating a zero-heap if no assignments have been made.
    countAssignments pid = do
      countsByWid <- flip foldMA Map.empty $ \acc wid block -> Map.insertWith (+) wid (length block) acc
      if Map.null countsByWid
        then defaultCounts pid
        else return $ Map.foldlWithKey (\acc wid sz -> Heap.insert (sz, wid) acc) Heap.empty countsByWid

    -- | Construct a zero-heap of all available woerkers.
    defaultCounts pid = do
      cw <- getMW
      if Map.null cw
        then assignError pid
        else return $ Map.foldlWithKey (\acc wid _ -> Heap.insert (0, wid) acc) Heap.empty cw

    assignToMin waCounts sz = do
      let Just ((wsz, wid), nwaCounts) = Heap.uncons waCounts
      return $ (wid, Heap.insert (wsz + sz, wid) nwaCounts)

    -- | Grouped block messages construction.
    mkMessages pid (rcStages -> rstg) (BC.pack . show -> pst) blocksByWID =
      (\f -> Map.foldlWithKey f (return []) blocksByWID) $ \m wid bbl -> do
        msgacc <- m
        wsock <- getMWI wid >>= maybe (workerError wid) (return . fst)
        return $ msgacc ++ [(wsock, Block pid rstg pst bbl)]


    -- | Block completion processing. This garbage collects jobs and assignment state.
    completeBlocks wid pid blocksByBID = lift $ do
      forM_ blocksByBID $ \(bid, map (read . BC.unpack) -> block) -> do
        modifyMA_ $ \assigns -> Map.adjust (Set.delete bid) wid assigns
        progDone <- modifyMJ $ \sjobs -> tryCompleteJS pid bid block sjobs $ Map.lookup pid sjobs
        maybe (return ()) completeProgram progDone

    tryCompleteJS pid bid block sjobs jsOpt =
      maybe (sjobs, Nothing) (either (completeJS pid sjobs) (incompleteJS pid sjobs) . completeJobBlock bid block) jsOpt

    completeJS pid sjobs result = (Map.delete pid sjobs, Just result)
    incompleteJS pid sjobs partials = (Map.insert pid partials sjobs, Nothing)

    completeJobBlock bid block js =
      let nc = Map.insert bid block $ completed js
          np = Set.delete bid $ pending js
      in if Set.null np then Left $ (jrid js, jrq js, nc)
                        else Right $ js { pending = np, completed = nc }

    -- | Program completion processing. This garbage collects client request state.
    completeProgram (rid, rq, sources) = do
      let prog = DC.role "__global" $ concatMap snd $ Map.toAscList sources
      (nprog,_) <- liftIE $ evalTransform (sfinalStages $ smOpts) prog
      clOpt <- getMR rid
      case clOpt of
        Nothing -> requestError rid
        Just cid -> completeRequest cid rid rq nprog

    completeRequest cid rid rq prog = do
      modifyMR_ $ \rm -> Map.delete rid rm
      sockOpt <- modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      let packedProg = BC.pack $ show prog
      putStrLnM $ "Packed result program size " ++ (show $ BC.length packedProg)
      maybe (return ()) (\sock -> sendC sock $ ProgramDone rq packedProg) sockOpt

    tryCompleteCL cid rid cm srOpt = maybe (cm, Nothing) (completeClient cid rid cm) srOpt
    completeClient cid rid cm (sock, rs) =
      let nrs = Set.delete rid rs in
      if null nrs then (Map.delete cid cm, Just sock)
                  else (Map.insert cid (sock, nrs) cm, Just sock)

    parseError = "Could not parse input: "

    assignError  pid = throwE $ unwords ["Could not assign program", show pid, "(no workers available)"]
    workerError  wid = throwE $ "No worker named " ++ show wid
    requestError rid = throwE $ "No request found: " ++ show rid


-- | Work handling function for the service worker.
goSWorker :: ServiceOptions -> Int -> CProtocol -> SConsumer ServiceWState ()
goSWorker sOpts@(serviceId -> wid) tid msg =  do
    case msg of
      Block pid cstages st blocksByBID -> processBlock pid cstages st blocksByBID
      _ -> lift $ putStrLnM $ boxToString $ ["Invalid message for compiler worker thread:"] %$ [show msg]

  where
    processBlock :: ProgramID -> CompileStages -> ByteString -> [(BlockID, [ByteString])] -> SConsumer ServiceWState ()
    processBlock pid cstages@([SDeclOpt cSpec]) (read . BC.unpack -> st) blocksByBID = lift $ do
      Just (sock, _) <- getWC
      cBlocksByBID <- forM blocksByBID $ \(bid, map (read . BC.unpack) -> block) -> do
        (nblock, _) <- debugCompileBlock pid bid (unwords [show $ length block, show cstages])
                         $ liftIE $ ST.runTransformM st $ ST.runDeclOptPassesBLM cSpec Nothing block
        return (bid, map (BC.pack . show) nblock)
      sendC sock $ BlockDone wid pid cBlocksByBID

    processBlock _ _ _ _ = lift $ putStrLnM $ "Invalid worker compile stages"

    debugCompileBlock pid bid str m = do
      wlogM $ unwords ["got block", show pid, show bid, str]
      result <- m
      wlogM $ unwords ["finished block", show pid, show bid]
      return result

    wlogM str = putStrLnM $ unwords ["Worker", show wid, "thread", show tid, str]

-- | One-shot connection to submit a remote job and wait for compilation to complete.
submitJob :: ServiceOptions -> RemoteJobOptions -> Options -> IO ()
submitJob sOpts@(serviceId -> rq) rjOpts opts = svw0 (scompileOpts sOpts) >>= \sv ->
    runClient sOpts sv (return ()) processClientConn

  where
    processClientConn :: Socket -> SockAddr -> ServiceM ServiceWState ()
    processClientConn sock addr = do
      putWC sock addr
      prog <- liftIE $ runDriverM $ k3read opts
      sendC sock $ Program rq (BC.pack $ concat $ prog) rjOpts Nothing
      putStrLnM $ boxToString $ ["Client waiting for compilation"]
      messages sock mHandler

    mHandler (ProgramDone rrq bytes) | rq == rrq = halt $ liftIO $ do
      putStrLn $ "Client compiling request " ++ rq
      putStrLn $ "Client received program of size " ++ (show $ BC.length bytes)
      CPPC.compile opts (scompileOpts $ sOpts) ($) (read $ BC.unpack bytes)

    mHandler m = halt $ putStrLnM $ boxToString $ ["Invalid message for compiler client:"] %$ [show m]

    continue m = m >> return False
    halt     m = m >> return True

command :: ServiceOptions -> CProtocol -> IO ()
command sOpts msg = svw0 (scompileOpts sOpts) >>= \sv ->
  runClient sOpts sv (return ()) $ \sock _ -> sendC sock msg

shutdownService :: ServiceOptions -> IO ()
shutdownService sOpts = command sOpts Quit

