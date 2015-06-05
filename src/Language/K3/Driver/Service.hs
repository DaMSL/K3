{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Service where

import Control.Concurrent hiding ( yield )
import Control.Concurrent.Async hiding ( wait )
import Control.Exception
import Control.Lens hiding ( transform, each )
import Control.Monad
import Control.Monad.Trans.Except
import Control.Monad.Trans.State.Strict

import Data.List
import Data.List.Split
import Data.Maybe
import Data.Heap ( Heap )
import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Heap as Heap
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.ByteString.Char8 as BC

import Data.Time.Format
import Data.Time.Clock
import Data.Time.LocalTime

import GHC.Generics ( Generic )

import Network.Simple.TCP
import Pipes
import Pipes.Binary hiding ( get )
import Pipes.Concurrent
import Pipes.Network.TCP
import Pipes.Parse

import System.IO ( Handle, stdout )
import qualified System.Log.Logger         as Log
import qualified System.Log.Formatter      as LogF
import qualified System.Log.Handler        as LogH
import qualified System.Log.Handler.Simple as LogHS

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
data CProtocol = Register       String
               | RegisterAck    CompileOptions
               | Heartbeat      Integer
               | HeartbeatAck   Integer
               | Program        Request ByteString RemoteJobOptions (Maybe RequestID)
               | ProgramDone    Request ByteString
               | ProgramAborted Request String
               | Block          ProgramID CompileStages ByteString [(BlockID, [ByteString])]
               | BlockDone      WorkerID ProgramID [(BlockID, [ByteString])]
               | BlockAborted   WorkerID ProgramID [BlockID] String
               | Quit
               deriving (Eq, Read, Show, Generic)

instance Binary CProtocol

-- | Service thread state, encapsulating task workers and connection workers.
data ThreadState = ThreadState { sthread    :: Maybe ThreadId
                               , ttworkers  :: Set (Async ())
                               , tcworkers  :: Set ThreadId }
                  deriving ( Eq )

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
                         , completed    :: BlockSourceMap
                         , aborted      :: [String] }
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
data ServiceState a = ServiceState { sterminate    :: MVar ()
                                   , threadSt      :: ThreadState
                                   , mbhndl        :: MBHandle
                                   , stcompileOpts :: CompileOptions
                                   , compileSt     :: a }

-- | Compiler service master state.
data SMST = SMST { ssocket      :: Maybe Socket
                 , cworkers     :: WorkersMap
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
type SocketMHandler  = Socket -> SockAddr -> ServiceMM ()
type SocketWHandler  = Socket -> SockAddr -> ServiceWM ()

type WorkHandler a = Int -> CProtocol -> SConsumer a ()

{- Initial states -}

sthread0 :: ThreadState
sthread0 = ThreadState Nothing Set.empty Set.empty

sst0 :: MVar () -> CompileOptions -> a -> ServiceState a
sst0 trmv cOpts st = ServiceState trmv sthread0 Nothing cOpts st

sstm0 :: MVar () -> CompileOptions -> ServiceMState
sstm0 trmv cOpts = sst0 trmv cOpts $ SMST Nothing Map.empty Map.empty Map.empty 0 0 0 0 Map.empty Map.empty

svm0 :: CompileOptions -> IO ServiceMSTVar
svm0 cOpts = newEmptyMVar >>= \trmv0 -> newMVar $ sstm0 trmv0 cOpts

sstw0 :: MVar () -> CompileOptions -> ServiceWState
sstw0 trmv cOpts = sst0 trmv cOpts $ SWST Nothing

svw0 :: CompileOptions -> IO ServiceWSTVar
svw0 cOpts = newEmptyMVar >>= \trmv0 -> newMVar $ sstw0 trmv0 cOpts


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

{- Server socket accessors -}
getMSS :: ServiceMM (Maybe Socket)
getMSS = getM >>= return . ssocket

putMSS :: Socket -> ServiceMM ()
putMSS ssock = modifyMM_ $ \cst -> cst { ssocket = Just ssock }

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

logFormat :: String -> String -> LogF.LogFormatter a
logFormat timeFormat format h (prio, msg) loggername = LogF.varFormatter vars format h (prio,msg) loggername
  where vars = [("time", formatTime defaultTimeLocale timeFormat <$> getZonedTime)
               ,("utcTime", formatTime defaultTimeLocale timeFormat <$> getCurrentTime)
               ,("rprio", rprio)
               ]
        rprio = return $ replicate rpad ' ' ++ sprio
        sprio = show prio
        rpad  = length "EMERGENCY" - length sprio

streamLogging :: Handle -> Log.Priority -> IO ()
streamLogging hndl lvl = do
  lh <- LogHS.streamHandler hndl lvl
  h  <- return $ LogH.setFormatter lh (logFormat "%s.%q" "[$time $rprio] $msg")
  Log.updateGlobalLogger Log.rootLoggerName (Log.setLevel lvl . Log.setHandlers [h])

fileLogging :: FilePath -> Log.Priority -> IO ()
fileLogging path lvl = do
  lh <- LogHS.fileHandler path lvl
  h  <- return $ LogH.setFormatter lh (logFormat "%s.%q" "[$time $rprio] $msg")
  Log.updateGlobalLogger Log.rootLoggerName (Log.setLevel lvl . Log.setHandlers [h])

logN :: String
logN = "Language.K3.Driver.Service"

{- Logging helpers. Bypass L.K3.Utils.Logger to avoid need for -DDEBUG -}
debugM :: (Monad m, MonadIO m) => String -> m ()
debugM = liftIO . Log.debugM logN

infoM :: (Monad m, MonadIO m) => String -> m ()
infoM = liftIO . Log.infoM logN

noticeM :: (Monad m, MonadIO m) => String -> m ()
noticeM = liftIO . Log.noticeM logN

warningM :: (Monad m, MonadIO m) => String -> m ()
warningM = liftIO . Log.warningM logN

errorM :: (Monad m, MonadIO m) => String -> m ()
errorM = liftIO . Log.errorM logN

cshow :: CProtocol -> String
cshow = \case
          Register wid                -> "Register " ++ wid
          RegisterAck _               -> "RegisterAck"
          Heartbeat hbid              -> "Heartbeat " ++ show hbid
          HeartbeatAck hbid           -> "HeartbeatAck " ++ show hbid
          Program rq _ _ ridOpt       -> unwords ["Program", rq, maybe "" show ridOpt]
          ProgramDone rq _            -> "ProgramDone " ++ rq
          ProgramAborted rq _         -> "ProgramAborted " ++ rq
          Block pid _ _ bids          -> unwords ["Block", show pid, intercalate "," $ map (show . fst) bids]
          BlockDone wid pid bids      -> unwords ["BlockDone", show wid, show pid, intercalate "," $ map (show . fst) bids]
          BlockAborted wid pid bids _ -> unwords ["BlockAborted", show wid, show pid, intercalate "," $ map show bids]
          Quit                        -> "Quit"


-- | Service utilities.
initServiceThread :: ServiceSTVar a -> (ThreadId -> ServiceM a ()) -> IO ()
initServiceThread sv0 onThreadF = do
  streamLogging stdout Log.INFO
  stid <- myThreadId
  runServiceM_ sv0 $ onThreadF stid

runServer :: ServiceOptions -> ServiceSTVar a -> ServiceM a ()
          -> (ThreadId -> ServiceM a ())
          -> (ThreadId -> ServiceM a ())
          -> (ThreadId -> ServiceM a ())
          -> ServiceM a () -> SocketHandler a -> SocketHandler a
          -> IO ()
runServer sOpts sv0 waitM onThreadF onCThreadInitF onCThreadFinalF onShutdown onListenF onConnF = do
  waitTID <- forkFinally (runServiceM_ sv0 $ waitM) (either throwIO $ const $ runServiceM_ sv0 onShutdown)
  void $ runListener
  killThread waitTID

  where
    runListener = withSocketsDo $ do
      initServiceThread sv0 onThreadF
      listen (Host $ serviceHost sOpts) (show $ servicePort sOpts) $ \(lsock, laddr) ->
        bracket_ (lInit laddr) (lFinal laddr) $ do
          runServiceM_ sv0 $ onListenF lsock laddr
          forever $ acceptFork lsock runConnection

    runConnection (csock, caddr) =
      bracket_ (cInit caddr) (cFinal caddr) $ runServiceM_ sv0 $ onConnF csock caddr

    cInit caddr = do
      ctid <- myThreadId
      runServiceM_ sv0 $ onCThreadInitF ctid
      noticeM $ "Accepted incoming connection from " ++ show caddr

    cFinal caddr = do
      ctid <- myThreadId
      runServiceM_ sv0 $ onCThreadFinalF ctid
      noticeM $ "Closing connection from " ++ show caddr

    lInit  laddr = noticeM $ "Listening for TCP connections at " ++ show laddr
    lFinal laddr = noticeM $ "Closing listener at " ++ show laddr


runClient :: ServiceOptions -> ServiceSTVar a -> (ThreadId -> ServiceM a ())
          -> ServiceM a () -> SocketHandler a -> IO ()
runClient sOpts sv0 onThreadF onShutdown onConnF = withSocketsDo $ do
  initServiceThread sv0 onThreadF
  flip finally (runServiceM_ sv0 onShutdown) $
    connect (serviceHost sOpts) (show $ servicePort sOpts) $ \(sock, addr) -> do
      bracket_ (cStart addr) (cEnd addr) $ runServiceM_ sv0 $ onConnF sock addr

  where cStart addr = noticeM $ "Connection established to " ++ show addr
        cEnd   addr = noticeM $ "Connection finished for " ++ show addr

simpleClient :: ServiceOptions -> Maybe (ServiceWM ()) -> SocketWHandler -> IO ()
simpleClient sOpts onShutdownOpt onConnF = do
    sv <- svw0 (scompileOpts sOpts)
    runClient sOpts sv onInit (maybe (return ()) id onShutdownOpt) onConnF
  where
    -- | Thread management.
    onInit tid = modifyTS_ $ \tst -> tst { sthread = Just tid }

wait :: ServiceM (ServiceState a) ()
wait = do
  st <- getSt
  liftIO $ readMVar $ sterminate st

terminate :: ServiceM (ServiceState a) ()
terminate = do
    st <- getSt
    liftIO $ void $ tryPutMVar (sterminate st) ()

shutdown :: ServiceM (ServiceState a) ()
shutdown = do
    tst <- getTS
    liftIO $ forM_ (Set.toList $ ttworkers $ tst) $ \as -> stopAS as
    liftIO $ forM_ (Set.toList $ tcworkers $ tst) $ \tid -> stopCW tid

  where stopAS as = do
          tid <- myThreadId
          if tid /= asyncThreadId as then cancel as else return ()

        stopCW tid = do
          mtid <- myThreadId
          if mtid /= tid then killThread tid else return ()

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
messages sock handler = runmsg $ ((fromSocket sock 4096) ^. decoded)
  where runmsg p = runStateT untilEmpty p >>= \(quit, np) -> unless quit (runmsg np)
        untilEmpty = draw >>= maybe (return True) (lift . handler)

-- | Compiler service master.
runServiceMaster :: ServiceOptions -> ServiceMasterOptions -> Options -> IO ()
runServiceMaster sOpts@(serviceId -> msid) smOpts opts = do
    sv <- svm0 (scompileOpts sOpts)
    runServer sOpts sv wait onInit onConnInit onConnStop onShutdown (onMasterListen sv) (processMasterConn sOpts smOpts)
  where
    mPfxM     = "[" ++ msid ++ "] "
    mlogM msg = noticeM $ mPfxM ++ msg
    mdbgM msg =  debugM $ mPfxM ++ msg

    -- | Thread management.
    onInit     tid = modifyTS_ $ \tst -> tst { sthread = Just tid }
    onConnInit tid = modifyTS_ $ \tst -> tst { tcworkers = Set.insert tid $ tcworkers tst }
    onConnStop tid = modifyTS_ $ \tst -> tst { tcworkers = Set.delete tid $ tcworkers tst }

    onShutdown = do
      wm <- getMW
      forM_ (Map.elems wm) $ \(wsock, waddr) -> do
        mlogM $ "Shutting down worker " ++ show waddr
        sendC wsock Quit
      shutdown
      ssock <- getMSS
      closeSock $ fromJust ssock

    -- | Create a workqueue and a timer thread when the master's server socket is up.
    onMasterListen sv ssock _ = do
      putMSS ssock
      workqueue sOpts (goSMaster sOpts smOpts opts)
      liftIO $ void $ async $ heartbeatLoop sv

    heartbeatLoop sv = forever $ runServiceM_ sv $ do
      liftIO $ threadDelay heartbeatPeriod
      hbid <- heartbeatM
      wsockets <- getMW >>= return . Map.elems
      forM_ wsockets $ \(sock, addr) -> do
        mdbgM $ unwords ["Pinging", show addr]
        sendC sock $ Heartbeat hbid

    heartbeatPeriod = seconds 10
    seconds x = x * 1000000

-- | Compiler service worker.
runServiceWorker :: ServiceOptions -> IO ()
runServiceWorker sOpts = simpleClient sOpts (Just shutdown) $ processWorkerConn sOpts


-- | Compiler service protocol handlers.
processMasterConn :: ServiceOptions -> ServiceMasterOptions -> Socket -> SockAddr -> ServiceM ServiceMState ()
processMasterConn sOpts@(serviceId -> msid) smOpts sock addr = messages sock logmHandler
  where
    mPfxM     = "[" ++ msid ++ "] "
    mlogM msg = noticeM $ mPfxM ++ msg
    mdbgM msg =  debugM $ mPfxM ++ msg
    merrM msg =  errorM $ mPfxM ++ msg

    logmHandler msg = do
      mlogM $ cshow msg
      mHandler msg

    -- | Client and worker messages forwarded out of connection thread.
    mHandler (Program rq s jo Nothing) = continue $ do
      rid <- trackRequest
      enqueueMsg (Program rq s jo $ Just rid)

    mHandler m@(BlockDone _ _ _) = continue $ enqueueMsg m
    mHandler m@(BlockAborted _ _ _ _) = continue $ enqueueMsg m

    -- | Remote worker messages
    mHandler (Register wid) = continue $ do
      mlogM $ unwords ["Registering worker", wid]
      putMWI wid sock addr
      cOpts <- getCO
      sendC sock $ RegisterAck cOpts

    -- TODO: detect worker changes.
    mHandler (HeartbeatAck hbid) = continue $ mdbgM $ unwords ["Got a heartbeat ack", show hbid]

    -- | Service termination.
    mHandler Quit = halt $ terminate

    mHandler m = halt $ merrM $ boxToString $ ["Invalid message:"] %$ [show m]

    continue m = m >> return False
    halt     m = m >> return True

    enqueueMsg m = getQ >>= \(qEnq,_) -> runEffect (yield m >-> toOutput qEnq)

    trackRequest = requestIDM >>= \rid ->
      putMC addr sock (Set.singleton rid) >> putMR rid addr >> return rid


processWorkerConn :: ServiceOptions -> Socket -> SockAddr -> ServiceM ServiceWState ()
processWorkerConn sOpts@(serviceId -> wid) sock addr = do
    putWC sock addr
    workqueue sOpts $ goSWorker sOpts
    sendC sock $ Register $ serviceId sOpts
    messages sock logmHandler

  where
    wPfxM     = "[" ++ wid ++ "] "
    wlogM msg = noticeM $ wPfxM ++ msg
    werrM msg =  errorM $ wPfxM ++ msg

    logmHandler msg = do
      wlogM $ cshow msg
      mHandler msg

    -- | Work messages forwarded out of connection thread.
    mHandler m@(Block _ _ _ _) = continue $ enqueueMsg m

    -- | Connection messages processed in-band.
    mHandler (RegisterAck cOpts) = continue $ do
      wlogM $ unwords ["Registered", show cOpts]
      modifyCO_ $ mergeCompileOpts cOpts

    mHandler (Heartbeat hbid) = continue $ do
      sendC sock $ HeartbeatAck hbid

    mHandler Quit = halt $ terminate

    mHandler m = halt $ werrM $ boxToString $ ["Invalid message:"] %$ [show m]

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
      BlockDone wid pid blocksByBID      -> completeBlocks wid pid blocksByBID
      BlockAborted wid pid bids reason   -> abortBlocks wid pid bids reason
      m -> lift $ mlogerrM $ boxToString $ ["Invalid message:"] %$ [show m]

  where
    mPfxM    m = "[" ++ msid ++ " worker " ++ show workerId ++ "] " ++ m
    mlogM    m = noticeM $ mPfxM m
    mlogerrM m = errorM $ mPfxM m

    nfP        = noFeed $ input opts
    includesP  = (includes $ paths opts)

    process prog jobOpts rq rid = lift $ flip catchE (abortProgram rid rq) $ do
      mlogM $ unwords ["Processing program", rq, "(", show rid, ")"]
      (blocks, initSt) <- preprocess prog jobOpts
      assignBlocks rid rq jobOpts blocks initSt

    preprocess prog jobOpts = do
      mlogM $ "Parsing with paths " ++ show includesP
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
    js0 rid rq = JobState rid rq Set.empty Map.empty []

    assignBlocks rid rq jobOpts blocks initSt = do
      pid <- progIDM
      mlogM $ unwords ["Assigning blocks for program", show pid]
      mlogM $ unwords ["State size", show $ BC.length $ BC.pack $ show initSt]
      waCounts <- countAssignments pid
      (_, blocksByWID, nassigns, js) <- foldM assignBlock (waCounts, Map.empty, Map.empty, js0 rid rq) blocks
      mlogM $ unwords ["Assignments:", show $ Map.toList nassigns]
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

    -- | Block abort processing. This aborts the given block ids, cleaning up state, but
    --   does not affect any other in-flight blocks.
    -- TODO: pre-emptively abort all other remaining blocks, and clean up the job state.
    abortBlocks wid pid bids reason = lift $ do
      forM_ bids $ \bid -> do
        modifyMA_ $ \assigns -> Map.adjust (Set.delete bid) wid assigns
        progAborted <- modifyMJ $ \sjobs -> tryAbortJS pid bid reason sjobs $ Map.lookup pid sjobs
        maybe (return ()) (\(rid,rq,aborts) -> abortProgram rid rq $ concat aborts) progAborted

    tryCompleteJS pid bid block sjobs jsOpt =
      maybe (sjobs, Nothing) (either (completeJS pid sjobs) (incompleteJS pid sjobs) . completeJobBlock bid block) jsOpt

    tryAbortJS pid bid reason sjobs jsOpt =
      maybe (sjobs, Nothing) (either (completeJS pid sjobs) (incompleteJS pid sjobs) . abortJobBlock bid reason) jsOpt

    completeJS pid sjobs result = (Map.delete pid sjobs, Just result)
    incompleteJS pid sjobs partials = (Map.insert pid partials sjobs, Nothing)

    completeJobBlock bid block js =
      let nc = if null $ aborted js then Map.insert bid block $ completed js else Map.empty
          np = Set.delete bid $ pending js
      in if Set.null np then Left $ (jrid js, jrq js, aborted js, nc)
                        else Right $ js { pending = np, completed = nc }

    abortJobBlock bid reason js =
      let np = Set.delete bid $ pending js
      in if Set.null np then Left $ (jrid js, jrq js, aborted js)
                        else Right $ js { pending = np, aborted = aborted js ++ [reason] }

    -- | Program completion processing. This garbage collects client request state.
    completeProgram (rid, rq, aborts, sources) = do
      let prog = DC.role "__global" $ concatMap snd $ Map.toAscList sources
      nprogrpE <- liftIO $ evalTransform (sfinalStages $ smOpts) prog
      case (aborts, nprogrpE) of
        (_, Left err) -> abortProgram rid rq err
        (h:t, _)      -> abortProgram rid rq $ concat $ h:t
        ([], Right (nprog, _)) -> do
          clOpt <- getMR rid
          case clOpt of
            Nothing -> requestError rid
            Just cid -> completeRequest cid rid rq nprog

    completeRequest cid rid rq prog = do
      modifyMR_ $ \rm -> Map.delete rid rm
      sockOpt <- modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      let packedProg = BC.pack $ show prog
      mlogM $ "Packed result program size " ++ (show $ BC.length packedProg)
      maybe (return ()) (\sock -> sendC sock $ ProgramDone rq packedProg) sockOpt

    -- | Abort compilation.
    abortProgram rid rq reason = do
      clOpt <- getMR rid
      case clOpt of
        Nothing -> requestError rid
        Just cid -> abortRequest cid rid rq reason

    abortRequest cid rid rq reason = do
      modifyMR_ $ \rm -> Map.delete rid rm
      sockOpt <- modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      maybe (return ()) (\sock -> sendC sock $ ProgramAborted rq reason) sockOpt

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
      _ -> lift $ wlogerrM $ boxToString $ ["Invalid message:"] %$ [show msg]

  where
    wPfxM    m = "[" ++ wid ++ " worker " ++ show tid ++ "] " ++ m
    wlogM    m = noticeM $ wPfxM m
    wlogerrM m = errorM  $ wPfxM m

    processBlock :: ProgramID -> CompileStages -> ByteString -> [(BlockID, [ByteString])] -> SConsumer ServiceWState ()
    processBlock pid ([SDeclOpt cSpec]) (read . BC.unpack -> st) blocksByBID =
      lift $ flip catchE (abortBlock pid blocksByBID) $ do
        Just (sock, _)    <- getWC
        (cBlocksByBID, _) <- foldM (compileBlock pid cSpec) ([], st) blocksByBID
        sendC sock $ BlockDone wid pid cBlocksByBID

    processBlock _ _ _ _ = lift $ wlogerrM $ "Invalid worker compile stages"

    abortBlock pid blocksByBID reason = do
      Just (sock, _) <- getWC
      sendC sock $ BlockAborted wid pid (map fst blocksByBID) reason

    compileBlock pid cSpec (blacc, st) (bid, map (read . BC.unpack) -> block) = do
      (nblock, nst) <- debugCompileBlock pid bid (unwords [show $ length block])
                        $ liftIE $ ST.runTransformM st $ ST.runDeclOptPassesBLM cSpec Nothing block
      return (blacc ++ [(bid, map (BC.pack . show) nblock)], nst)

    debugCompileBlock pid bid str m = do
      wlogM $ unwords ["got block", show pid, show bid, str]
      result <- m
      wlogM $ unwords ["finished block", show pid, show bid]
      return result


-- | One-shot connection to submit a remote job and wait for compilation to complete.
submitJob :: ServiceOptions -> RemoteJobOptions -> Options -> IO ()
submitJob sOpts@(serviceId -> rq) rjOpts opts = simpleClient sOpts Nothing processClientConn
  where
    processClientConn :: Socket -> SockAddr -> ServiceM ServiceWState ()
    processClientConn sock addr = do
      putWC sock addr
      prog <- liftIE $ runDriverM $ k3read opts
      sendC sock $ Program rq (BC.pack $ concat $ prog) rjOpts Nothing
      noticeM $ boxToString $ ["Client waiting for compilation"]
      messages sock mHandler

    mHandler (ProgramDone rrq bytes) | rq == rrq = halt $ liftIO $ do
      noticeM $ "Client compiling request " ++ rq
      noticeM $ "Client received program of size " ++ (show $ BC.length bytes)
      CPPC.compile opts (scompileOpts $ sOpts) ($) (read $ BC.unpack bytes)

    mHandler (ProgramAborted rrq reason) | rq == rrq = halt $ liftIO $ do
      errorM $ unwords ["Failed to compile request", rq, ":", reason]

    mHandler m = halt $ errorM $ boxToString $ ["Invalid message:"] %$ [show m]

    continue m = m >> return False
    halt     m = m >> return True

command :: ServiceOptions -> CProtocol -> IO ()
command sOpts msg = simpleClient sOpts Nothing $ \sock _ -> sendC sock msg

shutdownService :: ServiceOptions -> IO ()
shutdownService sOpts = command sOpts Quit

