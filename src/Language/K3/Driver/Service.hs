{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.Service where

import Control.Arrow ( (&&&), second )
import Control.Concurrent
import Control.Concurrent.Async ( Async, asyncThreadId, cancel )
import Control.Exception ( ErrorCall(..) )
import Control.Monad
import Control.Monad.Catch ( throwM, catchIOError, finally )
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Except
import Control.Monad.Trans.State.Strict

import Data.Binary ( Binary )
import Data.Serialize ( Serialize )
import Data.ByteString.Char8 ( ByteString )
import qualified Data.Binary as SB
import qualified Data.Serialize as SC
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as LBC

import Data.Time.Format
import Data.Time.Clock
import Data.Time.Clock.POSIX ( getPOSIXTime )
import Data.Time.LocalTime

import Data.Monoid
import Data.List
import Data.List.Split
import Data.Heap ( Heap )
import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Heap as Heap
import qualified Data.Map as Map
import qualified Data.Set as Set

import Criterion.Types ( Measured(..) )
import Criterion.Measurement ( initializeTime, getTime, secs )

import GHC.Conc
import GHC.Generics ( Generic )

import System.Random
import System.ZMQ4.Monadic

import System.IO ( Handle, stdout, stderr )
import qualified System.Log.Logger         as Log
import qualified System.Log.Formatter      as LogF
import qualified System.Log.Handler        as LogH
import qualified System.Log.Handler.Simple as LogHS

import Language.K3.Core.Annotation
import Language.K3.Core.Declaration
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Parser ( parseK3 )

import Language.K3.Stages ( TransformReport(..), TransformSt )
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
               | Program        Request String RemoteJobOptions
               | ProgramDone    Request (K3 Declaration) String
               | ProgramAborted Request String
               | Block          ProgramID CompileStages TransformSt CompileBlock
               | BlockDone      WorkerID ProgramID CompileBlock TransformReport
               | BlockAborted   WorkerID ProgramID [BlockID] String
               | Quit
               deriving (Eq, Read, Show, Generic)

instance Binary    CProtocol
instance Serialize CProtocol

-- | Service thread state, encapsulating task workers and connection workers.
data ThreadState = ThreadState { sthread    :: Maybe (Async ())
                               , ttworkers  :: Set (Async ()) }
                  deriving ( Eq )

-- | Primitive service datatypes.
type Request   = String
type RequestID = Int
type SocketID  = ByteString
type WorkerID  = String
type DeclID    = Int
type BlockID   = Int
type ProgramID = Int

type CompileBlock = [(BlockID, [(DeclID, K3 Declaration)])]

-- | Compilation progress state per program.
data JobState = JobState { jrid         :: RequestID
                         , jrq          :: Request
                         , jprofile     :: JobProfile
                         , jpending     :: BlockSet
                         , jcompleted   :: BlockSourceMap
                         , jaborted     :: [String] }
                deriving (Eq, Read, Show)

-- | Profiling information per worker and block.
data JobProfile = JobProfile { jstartTime :: Double
                             , jendTimes  :: Map WorkerID Double
                             , jworkerst  :: Map WorkerID WorkerJobState
                             , jppreport  :: TransformReport
                             , jreports   :: [TransformReport] }
                   deriving (Eq, Read, Show)

-- | Cost book-keeping per worker.
--   This is a snapshot of the worker's total weight (across all jobs),
--   the per-block contributions from the current job during assignment,
--   and a block completion counter per worker.
--   We use this information to validate the cost model accuracy.
type JobBlockCosts  = Map BlockID Double
data WorkerJobState = WorkerJobState { jwwsnap    :: Double
                                     , jwassign   :: JobBlockCosts
                                     , jwcomplete :: Int }
                     deriving (Eq, Read, Show)

-- | Assignment book-keeping for a single service worker.
data WorkerAssignment = WorkerAssignment { wablocks :: BlockSet
                                         , waweight :: Double }
                        deriving (Eq, Read, Show)

type BlockSet       = Set BlockID
type BlockSourceMap = Map BlockID   [(DeclID, K3 Declaration)]
type JobsMap        = Map ProgramID JobState
type AssignmentsMap = Map WorkerID  WorkerAssignment
type WorkersMap     = Map WorkerID  SocketID

type RequestSet     = Set RequestID
type ClientMap      = Map SocketID  RequestSet
type RequestMap     = Map RequestID SocketID

-- | Common state for the compiler service.
data ServiceState a = ServiceState { sterminate    :: MVar ()
                                   , threadSt      :: ThreadState
                                   , stcompileOpts :: CompileOptions
                                   , compileSt     :: a }

-- | Compiler service master state.
data SMST = SMST { cworkers     :: WorkersMap
                 , cclients     :: ClientMap
                 , crequests    :: RequestMap
                 , bcnt         :: Int
                 , pcnt         :: Int
                 , rcnt         :: Int
                 , jobs         :: JobsMap
                 , assignments  :: AssignmentsMap }

type ServiceMState = ServiceState SMST

-- | Compiler service worker state. Note: workers are stateless.
data SWST = SWST { hbcnt :: Integer }

type ServiceWState = ServiceState SWST

-- | Mutable service state variable.
type ServiceSTVar a = MVar a
type ServiceST    a = MVar (ServiceState a)
type ServiceMSTVar  = ServiceST SMST
type ServiceWSTVar  = ServiceST SWST

-- | A K3 compiler service monad.
type ServiceM z a = ExceptT String (StateT (ServiceSTVar a) (ZMQ z))

-- | Type definitions for service master and worker monads.
type ServiceMM z = ServiceM z ServiceMState
type ServiceWM z = ServiceM z ServiceWState

type SocketAction   z = Int -> Socket z Dealer -> ZMQ z ()
type ClientHandler  t = forall z. Socket z t -> ZMQ z ()
type MessageHandler   = forall z. CProtocol -> ZMQ z ()

{- Initial states -}

sthread0 :: ThreadState
sthread0 = ThreadState Nothing Set.empty

sst0 :: MVar () -> CompileOptions -> a -> ServiceState a
sst0 trmv cOpts st = ServiceState trmv sthread0 cOpts st

sstm0 :: MVar () -> CompileOptions -> ServiceMState
sstm0 trmv cOpts = sst0 trmv cOpts $ SMST Map.empty Map.empty Map.empty 0 0 0 Map.empty Map.empty

svm0 :: CompileOptions -> IO ServiceMSTVar
svm0 cOpts = newEmptyMVar >>= \trmv0 -> newMVar $ sstm0 trmv0 cOpts

sstw0 :: MVar () -> CompileOptions -> ServiceWState
sstw0 trmv cOpts = sst0 trmv cOpts $ SWST 0

svw0 :: CompileOptions -> IO ServiceWSTVar
svw0 cOpts = newEmptyMVar >>= \trmv0 -> newMVar $ sstw0 trmv0 cOpts


{- Service monad utilities -}

runServiceM :: ServiceSTVar a -> (forall z. ServiceM z a b) -> IO (Either String b)
runServiceM st m = runZMQ $ evalStateT (runExceptT m) st

runServiceM_ :: ServiceSTVar a -> (forall z. ServiceM z a b) -> IO ()
runServiceM_ st m = runServiceM st m >>= either putStrLn (const $ return ())

runServiceZ :: ServiceSTVar a -> ServiceM z a b -> ZMQ z (Either String b)
runServiceZ st m = evalStateT (runExceptT m) st

liftI :: IO b -> ServiceM z a b
liftI = lift . liftIO

liftIE :: IO (Either String b) -> ServiceM z a b
liftIE m = ExceptT $ liftIO m

liftEitherM :: Either String b -> ServiceM z a b
liftEitherM e = liftIE $ return e

reasonM :: String -> ServiceM z a b -> ServiceM z a b
reasonM msg m = withExceptT (msg ++) m

modifyM :: (a -> (a, b)) -> ServiceM z a b
modifyM f = getV >>= \sv -> liftI (modifyMVar sv (return  . f))

modifyM_ :: (a -> a) -> ServiceM z a ()
modifyM_ f = getV >>= \sv -> liftI (modifyMVar_ sv (return  . f))

getV :: ServiceM z a (ServiceSTVar a)
getV = lift get

getSt :: ServiceM z a a
getSt = getV >>= liftIO . readMVar

{- Thread and async task management -}
getTS :: ServiceM z (ServiceState a) ThreadState
getTS = getSt >>= return . threadSt

modifyTS :: (ThreadState -> (ThreadState, b)) -> ServiceM z (ServiceState a) b
modifyTS f = modifyM $ \st -> let (nt,r) = f $ threadSt st in (st {threadSt = nt}, r)

modifyTS_ :: (ThreadState -> ThreadState) -> ServiceM z (ServiceState a) ()
modifyTS_ f = modifyM_ $ \st -> st { threadSt = f $ threadSt st }

getTSIO :: ServiceST a -> IO ThreadState
getTSIO v = readMVar v >>= return . threadSt

modifyTSIO_ :: ServiceST a -> (ThreadState -> ThreadState) -> IO ()
modifyTSIO_ v f = modifyMVar_ v $ \st -> return $ st { threadSt = f $ threadSt st }

{- Compile options accessors -}
getCO :: ServiceM z (ServiceState a) CompileOptions
getCO = getSt >>= return . stcompileOpts

putCO :: CompileOptions -> ServiceM z (ServiceState a) ()
putCO cOpts = modifyM_ $ \st -> st { stcompileOpts = cOpts }

modifyCO_ :: (CompileOptions -> CompileOptions) -> ServiceM z (ServiceState a) ()
modifyCO_ f = modifyM_ $ \st -> st { stcompileOpts = f $ stcompileOpts st }

{- Service master accessors -}
modifyMM :: (SMST -> (SMST, a)) -> ServiceMM z a
modifyMM f = modifyM $ \st -> let (ncst, r) = f $ compileSt st in (st { compileSt = ncst }, r)

modifyMM_ :: (SMST -> SMST) -> ServiceMM z ()
modifyMM_ f = modifyM_ $ \st -> st { compileSt = f $ compileSt st }

modifyMIO :: ServiceMSTVar -> (SMST -> (SMST, a)) -> IO a
modifyMIO sv f = modifyMVar sv $ \st -> let (ncst, r) = f $ compileSt st in return (st { compileSt = ncst }, r)

getM :: ServiceMM z SMST
getM = getSt >>= return . compileSt

getMIO :: ServiceMSTVar -> IO SMST
getMIO sv = liftIO (readMVar sv) >>= return . compileSt

putM :: SMST -> ServiceMM z ()
putM ncst = modifyM_ $ \st -> st { compileSt = ncst }

{- Counter accessors -}
progIDM :: ServiceMM z ProgramID
progIDM = modifyMM $ \cst -> let i = pcnt cst + 1 in (cst {pcnt = i}, i)

blockIDM :: ServiceMM z BlockID
blockIDM = modifyMM $ \cst -> let i = bcnt cst + 1 in (cst {bcnt = i}, i)

requestIDM :: ServiceMM z BlockID
requestIDM = modifyMM $ \cst -> let i = rcnt cst + 1 in (cst {rcnt = i}, i)

{- Assignments accessors -}
getMA :: ServiceMM z AssignmentsMap
getMA = getM >>= return . assignments

putMA :: AssignmentsMap -> ServiceMM z ()
putMA nassigns = modifyMM_ $ \cst -> cst { assignments = nassigns }

modifyMA :: (AssignmentsMap -> (AssignmentsMap, a)) -> ServiceMM z a
modifyMA f = modifyMM $ \cst -> let (nassigns, r) = f $ assignments cst in (cst { assignments = nassigns }, r)

modifyMA_ :: (AssignmentsMap -> AssignmentsMap) -> ServiceMM z ()
modifyMA_ f = modifyMM_ $ \cst -> cst { assignments = f $ assignments cst }

mapMA :: (WorkerID -> WorkerAssignment -> a) -> ServiceMM z (Map WorkerID a)
mapMA f = getMA >>= return . Map.mapWithKey f

foldMA :: (a -> WorkerID -> WorkerAssignment -> a) -> a -> ServiceMM z a
foldMA f z = getMA >>= return . Map.foldlWithKey f z

{- Job state accessors -}
getMJ :: ServiceMM z JobsMap
getMJ = getM >>= return . jobs

putMJ :: JobsMap -> ServiceMM z ()
putMJ njobs = modifyMM_ $ \cst -> cst { jobs = njobs }

modifyMJ :: (JobsMap -> (JobsMap, a)) -> ServiceMM z a
modifyMJ f = modifyMM $ \cst -> let (njobs, r) = f $ jobs cst in (cst { jobs = njobs }, r)

modifyMJ_ :: (JobsMap -> JobsMap) -> ServiceMM z ()
modifyMJ_ f = modifyMM_ $ \cst -> cst { jobs = f $ jobs cst }

{- Service request, client and worker accessors -}
getMC :: SocketID -> ServiceMM z (Maybe RequestSet)
getMC c = getM >>= return . Map.lookup c . cclients

putMC :: SocketID -> RequestSet -> ServiceMM z ()
putMC c r = modifyMM_ $ \cst -> cst { cclients = Map.insert c r $ cclients cst }

modifyMC :: (ClientMap -> (ClientMap, a)) -> ServiceMM z a
modifyMC f = modifyMM $ \cst -> let (ncc, r) = f $ cclients cst in (cst {cclients = ncc}, r)

modifyMC_ :: (ClientMap -> ClientMap) -> ServiceMM z ()
modifyMC_ f = modifyMM_ $ \cst -> cst {cclients = f $ cclients cst}

getMR :: RequestID -> ServiceMM z (Maybe SocketID)
getMR r = getM >>= return . Map.lookup r . crequests

putMR :: RequestID -> SocketID -> ServiceMM z ()
putMR r c = modifyMM_ $ \cst -> cst { crequests = Map.insert r c $ crequests cst }

modifyMR :: (RequestMap -> (RequestMap, a)) -> ServiceMM z a
modifyMR f = modifyMM $ \cst -> let (nrq, r) = f $ crequests cst in (cst {crequests = nrq}, r)

modifyMR_ :: (RequestMap -> RequestMap) -> ServiceMM z ()
modifyMR_ f = modifyMM_ $ \cst -> cst {crequests = f $ crequests cst}

getMW :: ServiceMM z WorkersMap
getMW = getM >>= return . cworkers

getMWIO :: ServiceMSTVar -> IO WorkersMap
getMWIO sv = getMIO sv >>= return . cworkers

getMWI :: WorkerID -> ServiceMM z (Maybe SocketID)
getMWI wid = getM >>= return . Map.lookup wid . cworkers

putMWI :: WorkerID -> SocketID -> ServiceMM z ()
putMWI wid s = modifyMM_ $ \cst -> cst { cworkers = Map.insert wid s $ cworkers cst }

{- Worker state accessors -}
modifyWM :: (SWST -> (SWST, a)) -> ServiceWM z a
modifyWM f = modifyM $ \st -> let (ncst, r) = f $ compileSt st in (st { compileSt = ncst }, r)

modifyWM_ :: (SWST -> SWST) -> ServiceWM z ()
modifyWM_ f = modifyM_ $ \st -> st { compileSt = f $ compileSt st }

modifyWIO :: ServiceWSTVar -> (SWST -> (SWST, a)) -> IO a
modifyWIO sv f = modifyMVar sv $ \st -> let (ncst, r) = f $ compileSt st in return (st { compileSt = ncst }, r)

getW :: ServiceWM z SWST
getW = getSt >>= return . compileSt

putW :: SWST -> ServiceWM z ()
putW ncst = modifyM_ $ \st -> st { compileSt = ncst }

heartbeatM :: ServiceWM z Integer
heartbeatM = modifyWM $ \cst -> let i = hbcnt cst + 1 in (cst {hbcnt = i}, i)

heartbeatIO :: ServiceWSTVar -> IO Integer
heartbeatIO sv = modifyWIO sv $ \cst -> let i = hbcnt cst + 1 in (cst {hbcnt = i}, i)


{- Misc -}
putStrLnM :: String -> ServiceM z a ()
putStrLnM = liftIO . putStrLn

-- From ZHelpers.hs
-- In General Since We use Randomness, You should Pass in
-- an StdGen, but for simplicity we just use newStdGen
setRandomIdentity :: Socket z t -> ZMQ z ()
setRandomIdentity sock = do
   ident <- liftIO genUniqueId
   setIdentity (restrict $ BC.pack ident) sock

-- From ZHelpers.hs
-- You probably want to use a ext lib to generate random unique id in production code
genUniqueId :: IO String
genUniqueId = do
    gen <- liftIO newStdGen
    let (val1, gen') = randomR (0 :: Int, 65536) gen
    let (val2, _) = randomR (0 :: Int, 65536) gen'
    return $ show val1 ++ show val2

tcpConnStr :: ServiceOptions -> String
tcpConnStr sOpts = "tcp://" ++ (serviceHost sOpts) ++ ":" ++ (show $ servicePort sOpts)

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
          Program rq _ _              -> "Program " ++ rq
          ProgramDone rq _ _          -> "ProgramDone " ++ rq
          ProgramAborted rq _         -> "ProgramAborted " ++ rq
          Block pid _ _ bids          -> unwords ["Block", show pid, intercalate "," $ map (show . fst) bids]
          BlockDone wid pid bids _    -> unwords ["BlockDone", show wid, show pid, intercalate "," $ map (show . fst) bids]
          BlockAborted wid pid bids _ -> unwords ["BlockAborted", show wid, show pid, intercalate "," $ map show bids]
          Quit                        -> "Quit"


-- | Service utilities.
initService :: ServiceOptions -> IO () -> IO ()
initService sOpts m = initializeTime >> slog (serviceLog sOpts) >> m
  where slog (Left "stdout") = streamLogging stdout $ serviceLogLevel sOpts
        slog (Left "stderr") = streamLogging stderr $ serviceLogLevel sOpts
        slog (Left s)        = error $ "Invalid service logging handle " ++ s
        slog (Right path)    = fileLogging path $ serviceLogLevel sOpts

-- | Compiler service master.
runServiceMaster :: ServiceOptions -> ServiceMasterOptions -> Options -> IO ()
runServiceMaster sOpts smOpts opts = initService sOpts $ runZMQ $ do
    let bqid = "mbackend"
    sv <- liftIO $ svm0 (scompileOpts sOpts)

    frontend <- socket Router
    bind frontend mconn
    backend <- workqueue sv nworkers bqid $ processMasterConn sOpts smOpts opts sv
    as <- async $ proxy frontend backend Nothing
    noticeM $ unwords ["Service Master", show $ asyncThreadId as, mconn]

    flip finally (stopService sv bqid) $ liftIO $ do
      modifyTSIO_ sv $ \tst -> tst { sthread = Just as }
      wait sv

  where
    mconn = tcpConnStr sOpts
    nworkers = serviceThreads sOpts

    stopService :: ServiceMSTVar -> String -> ZMQ z ()
    stopService sv qid = do
      wsock <- socket Dealer
      connect wsock $ "inproc://" ++ qid
      shutdownRemote sv wsock
      liftIO $ threadDelay $ 5 * 1000 * 1000


-- | Compiler service worker.
runServiceWorker :: ServiceOptions -> IO ()
runServiceWorker sOpts@(serviceId -> wid) = initService sOpts $ runZMQ $ do
    let bqid = "wbackend"
    sv <- liftIO $ svw0 (scompileOpts sOpts)

    frontend <- socket Dealer
    setRandomIdentity frontend
    connect frontend wconn

    backend <- workqueue sv nworkers bqid $ processWorkerConn sOpts sv registerWorker
    as <- async $ proxy frontend backend Nothing
    noticeM $ unwords ["Service Worker", wid, show $ asyncThreadId as, wconn]

    void $ async $ heartbeatLoop sv
    liftIO $ do
      modifyTSIO_ sv $ \tst -> tst { sthread = Just as }
      wait sv

  where
    wconn = tcpConnStr sOpts
    nworkers = serviceThreads sOpts

    registerWorker :: Int -> Socket z Dealer -> ZMQ z ()
    registerWorker wtid wsock
      | wtid == 1 = do
        liftIO $ threadDelay registerPeriod
        noticeM $ unwords ["Worker", wid, "registering"]
        sendC wsock $ Register wid
      | otherwise = return ()

    heartbeatLoop :: ServiceWSTVar -> ZMQ z ()
    heartbeatLoop sv = do
      hbsock <- socket Dealer
      bind hbsock wconn
      liftIO $ putStrLn "Heartbeat loop started"
      void $ forever $ do
        liftIO $ threadDelay heartbeatPeriod
        pingPongHeartbeat sv hbsock
      close hbsock

    pingPongHeartbeat :: ServiceWSTVar -> Socket z Dealer -> ZMQ z ()
    pingPongHeartbeat sv hbsock = do
      hbid <- liftIO $ heartbeatIO sv
      sendC hbsock $ Heartbeat hbid
      [evts] <- poll heartbeatPollPeriod [Sock hbsock [In] Nothing]
      if In `elem` evts then do
        ackmsg <- receive hbsock
        case SC.decode ackmsg of
          Right (HeartbeatAck i) -> noticeM $ unwords ["Got a heartbeat ack", show i]
          Right m -> errorM $ unwords ["Invalid heartbeat ack", cshow m]
          Left err -> errorM err
      else noticeM $ "No heartbeat acknowledgement received during the last epoch."

    registerPeriod = seconds 2
    heartbeatPeriod = seconds $ sHeartbeatEpoch sOpts
    heartbeatPollPeriod = fromIntegral $ sHeartbeatEpoch sOpts * 1000
    seconds x = x * 1000000


runClient :: (SocketType t) => t -> ServiceOptions -> ClientHandler t -> IO ()
runClient sockT sOpts clientF = initService sOpts $ runZMQ $ do
    client <- socket sockT
    setRandomIdentity client
    connect client $ tcpConnStr sOpts
    clientF client


workqueue :: ServiceST a -> Int -> String -> SocketAction z -> ZMQ z (Socket z Dealer)
workqueue sv n qid workerF = do
    backend <- socket Dealer
    bind backend $ "inproc://" ++ qid
    as <- forM [1..n] $ \i -> async $ worker i qid workerF
    liftIO $ modifyTSIO_ sv $ \tst -> tst { ttworkers = Set.fromList as `Set.union` ttworkers tst }
    return backend

worker :: Int -> String -> SocketAction z -> ZMQ z ()
worker i qid workerF = do
    wsock <- socket Dealer
    connect wsock $ "inproc://" ++ qid
    liftIO $ putStrLn "Worker started"
    forever $ workerF i wsock

-- | Control primitives.
wait :: ServiceST a -> IO ()
wait sv = do
  st <- readMVar sv
  readMVar $ sterminate st

terminate :: ServiceST a -> IO ()
terminate sv = do
    st <- readMVar sv
    void $ tryPutMVar (sterminate st) ()

threadstatus :: ServiceST a -> IO ()
threadstatus sv = do
    tst  <- getTSIO sv
    tids <- mapM (return . asyncThreadId) $ (maybe [] (:[]) $ sthread tst) ++ (Set.toList $ ttworkers tst)
    thsl <- mapM (\t -> threadStatus t >>= \s -> return (unwords [show t, ":", show s])) tids
    noticeM $ concat thsl

shutdown :: ServiceST a -> IO ()
shutdown sv = do
    tst <- getTSIO sv
    tid <- myThreadId
    mapM_ (\a -> unless (tid == asyncThreadId a) $ cancel a) $ Set.toList $ ttworkers $ tst

-- | Distributed control primitives.
shutdownRemote :: (SocketType t, Sender t) => ServiceMSTVar -> Socket z t -> ZMQ z ()
shutdownRemote sv master = do
    wm <- liftIO $ getMWIO sv
    forM_ (Map.elems wm) $ \wsid -> do
      noticeM $ "Shutting down service worker " ++ (show $ BC.unpack wsid)
      sendCI wsid master Quit


-- | Messaging primitives.
sendC :: (SocketType t, Sender t) => Socket z t -> CProtocol -> ZMQ z ()
sendC s m = let msg = SC.encode m in do
    noticeM $ unwords ["Message size for ", cshow m, show $ BC.length msg]
    send s [] msg

sendCI :: (SocketType t, Sender t) => SocketID -> Socket z t -> CProtocol -> ZMQ z ()
sendCI sid s m = send s [SendMore] sid >> sendC s m

sendCIs :: (SocketType t, Sender t) => Socket z t -> [(SocketID, CProtocol)] -> ZMQ z ()
sendCIs s msgs = forM_ msgs $ \(sid,m) -> sendCI sid s m

-- | Client primitives.
command :: (SocketType t, Sender t) => t -> ServiceOptions -> CProtocol -> IO ()
command t sOpts msg = runClient t sOpts $ \client -> sendC client msg

requestreply :: (SocketType t, Receiver t, Sender t) => t -> ServiceOptions -> CProtocol -> MessageHandler -> IO ()
requestreply t sOpts req replyF = runClient t sOpts $ \client -> do
  sendC client req
  rep <- receive client
  either errorM replyF $ SC.decode rep


-- | Compiler service protocol handlers.
processMasterConn :: ServiceOptions -> ServiceMasterOptions -> Options -> ServiceMSTVar -> Int -> Socket z Dealer -> ZMQ z ()
processMasterConn sOpts@(serviceId -> msid) smOpts opts sv wtid mworker = do
    sid <- receive mworker
    msg <- receive mworker
    logmHandler sid msg

  where
    mPfxM = "[" ++ msid ++ " " ++ show wtid ++ "] "

    mlogM :: (Monad m, MonadIO m) => String -> m ()
    mlogM msg = noticeM $ mPfxM ++ msg

    merrM :: (Monad m, MonadIO m) => String -> m ()
    merrM msg =  errorM $ mPfxM ++ msg

    zm :: ServiceMM z a -> ZMQ z a
    zm m = runServiceZ sv m >>= either (throwM . ErrorCall) return

    logmHandler sid (SC.decode -> msgE) = either merrM (\msg -> mlogM (cshow msg) >> mHandler sid msg) msgE

    mHandler sid (Program rq prog jobOpts) = do
      rid' <- zm $ do
        rid <- requestIDM
        putMC sid (Set.singleton rid) >> putMR rid sid >> return rid
      process prog jobOpts rq rid'

    mHandler _ (BlockDone wid pid blocksByBID report) = completeBlocks wid pid blocksByBID report
    mHandler _ (BlockAborted wid pid bids reason)     = abortBlocks wid pid bids reason

    mHandler sid (Register wid) = do
      mlogM $ unwords ["Registering worker", wid]
      cOpts <- zm $ putMWI wid sid >> getCO
      sendCI sid mworker $ RegisterAck cOpts

    -- TODO: detect worker changes.
    mHandler sid (Heartbeat hbid) = sendCI sid mworker $ HeartbeatAck hbid

    mHandler _ Quit = liftIO $ terminate sv
    mHandler _ m = merrM $ boxToString $ ["Invalid message:"] %$ [show m]

    -- | Compilation functions
    nfP        = noFeed $ input opts
    includesP  = (includes $ paths opts)

    process prog jobOpts rq rid = abortcatch rid rq $ do
      msgs <- zm $ do
        mlogM $ unwords ["Processing program", rq, "(", show rid, ")"]
        (initP, initSt, ppProfs) <- preprocess prog
        let ppRep = TransformReport (Map.singleton "Master preprocessing" ppProfs) Map.empty
        assignBlocks rid rq jobOpts initP initSt ppRep
      sendStart <- liftIO getPOSIXTime
      sendCIs mworker msgs
      sendDone <- liftIO getPOSIXTime
      mlogM $ unwords ["Send time", show $ sendDone - sendStart]

    abortcatch rid rq m = m `catchIOError` (\e -> abortProgram Nothing rid rq $ show e)

    -- | Parse, evaluate metaprogram, and apply prepare transform.
    preprocess prog = do
      mlogM $ "Parsing with paths " ++ show includesP
      (pP, pProf) <- reasonM parseError . liftMeasured $ parseK3 nfP includesP prog
      (mP, mProf) <- liftMeasured $ runDriverM $ metaprogram opts pP
      (((initP, _), initSt), iProf) <- liftMeasured $ runTransform Nothing (coStages $ scompileOpts $ sOpts) mP
      return (initP, initSt, [pProf, mProf, iProf])

    liftMeasured :: IO (Either String a) -> ServiceMM z (a, Measured)
    liftMeasured m = liftIE $ do
      (rE, p) <- ST.profile $ const m
      return $ either Left (\a -> Right (a,p)) rE

    {------------------------
      - Job assignment.
      -----------------------}

    -- | Cost-based compile block assignment.
    assignBlocks rid rq jobOpts initP initSt ppRep = do
      pid <- progIDM
      mlogM $ unwords ["Assigning blocks for program:", show pid]

      -- Get the current worker weights, and use them to partition the program.
      ((wBlocks', nassigns', pending', wjs'), aProf) <- ST.profile $ const $ do
        wWeights <- workerWeights
        when ( Heap.null wWeights ) $ assignError pid
        (nwWeights, wBlocks, jobCosts) <- partitionProgram (jobBlockSize jobOpts) wWeights initP

        -- Compute assignment map delta.
        -- Extract block ids per worker, and join with new weights per worker to compute
        -- an assignment map with updated weights and new block sets.
        let nwaBlockIds = Map.map (\cb -> Set.fromList $ map fst cb) wBlocks
        let nwaWeights  = foldl (\m (w,wid) -> Map.insert wid w m) Map.empty nwWeights
        let nassigns    = Map.intersectionWith WorkerAssignment nwaBlockIds nwaWeights

        -- Compute new job state.
        let pending = foldl (\acc cb -> acc `Set.union` (Set.fromList $ map fst cb)) Set.empty wBlocks
        let wjs     = Map.intersectionWith (\w c -> WorkerJobState w c $ Map.size c) nwaWeights jobCosts
        return (wBlocks, nassigns, pending, wjs)

      let aRep = TransformReport (Map.singleton "Master assignment" [aProf]) Map.empty
      js <- js0 rid rq pending' wjs' $ ppRep <> aRep
      modifyMJ_ $ \jbs -> Map.insert pid js jbs
      modifyMA_ $ \assigns -> Map.unionWith incrWorkerAssignments assigns nassigns'
      msgs <- mkMessages pid jobOpts initSt wBlocks'

      logAssignment pid nassigns' js
      return msgs

    -- | Compute a min-heap of worker assignment weights, returning a zero-heap if no assignments exist.
    workerWeights = do
      weightHeap <- flip foldMA Heap.empty $ \acc wid assigns -> Heap.insert (waweight assigns, wid) acc
      if Heap.null weightHeap
        then getMW >>= return . Map.foldlWithKey (\acc wid _ -> Heap.insert (0.0, wid) acc) Heap.empty
        else return weightHeap

    -- | Cost-based program partitioning.
    --   This performs assignments at a per-declaration granularity rather than per-block,
    --   using a greedy heuristic to balance work across each worker (the k-partition problem).
    --   This returns final worker weights, new compile block assignments, and the job costs per worker.
    partitionProgram blockSize wWeights (tnc -> (DRole _, ch)) =
      let (total_cost, swich) = sortByCost ch in do
        (nwWeights, newAssigns) <- greedyPartition wWeights swich
        (wcBlocks, wcosts) <- foldMapKeyM (return (Map.empty, Map.empty)) newAssigns $ chunkAssigns blockSize
        return (nwWeights, Map.map reverse wcBlocks, wcosts)

    partitionProgram _ _ _ = throwE "Top level declaration is not a role."

    -- Creates compile block chunks per worker, and sums up costs per chunk.
    chunkAssigns blockSize accM wid wbwl = do
      (wcbm,wcm) <- accM
      biwsl <- forM (chunksOf blockSize wbwl) $ \bwl -> do
                 let (chunkcb, chunkcost) = second sum $ unzip bwl
                 bid <- blockIDM
                 return ((bid, sortOn fst chunkcb), (bid, chunkcost))
      let (wcompileblock, wblockcosts) = second Map.fromList $ unzip biwsl
      return $ ( Map.insertWith (++) wid wcompileblock wcbm
               , Map.insertWith mergeBlockCosts wid wblockcosts wcm )

    -- | A simple compilation cost model
    sortByCost ch = second (sortOn fst) $ foldl foldCost (0,[]) $ zip [0..] ch
    foldCost (total, acc) (i, d@(cost -> c)) = (total + c, acc ++ [(c, (i,d))])

    cost (tag -> DGlobal _ _ (Just (treesize -> n))) = n
    cost (tag -> DTrigger _ _ (treesize -> n))       = n
    cost _                                           = 1

    -- | Greedy solution to the k-partitioning problem.
    --   We maintain a heap of weights, to easily pick the partition with minimum weight,
    --   and update the corresponding partition with the new element.
    greedyPartition wWeights swich = foldM greedy (wWeights, Map.empty) swich
      where
        greedy (heap, assignsAndCosts) (w, ich) =
          maybe partitionError (assign assignsAndCosts w ich) $ Heap.uncons heap

        assign acm (fromIntegral -> w) ich ((sz,wid), restheap) =
          return ( Heap.insert (sz + w, wid) restheap, Map.insertWith (++) wid [(ich,w)] acm )

    -- | Compile block messages construction.
    mkMessages pid (jobBlockSize &&& rcStages -> (blockSz, remoteStages)) initSt cBlocksByWID = do
      let forkFactor = blockSz * Map.size cBlocksByWID
      liftM fst $ foldMapKeyM (return ([], [0..])) cBlocksByWID $ \m wid cb -> do
        (msgacc, offgen) <- m
        (wst, restidx) <- maybe wstateError return $ workerSt forkFactor offgen
        wsockid <- getMWI wid >>= maybe (workerError wid) return
        return $ (msgacc ++ [(wsockid, Block pid remoteStages wst cb)], restidx)

      where workerSt factor offgen = do
              (widx, restidx) <- uncons offgen
              let wOffset = widx * blockSz
              wst <- ST.partitionTransformStSyms factor wOffset initSt
              return (wst, restidx)

    -- Map helpers to supply fold function as last argument.
    foldMapKeyM a m f = Map.foldlWithKey f a m

    -- | Merge block costs
    mergeBlockCosts = Map.unionWith (+)

    -- | Update worker assignments with a new weight and a delta blockset.
    incrWorkerAssignments (WorkerAssignment oldbs _) (WorkerAssignment deltabs w) =
      flip WorkerAssignment w $ oldbs `Set.union` deltabs

    -- | Update worker assignments with a completed or aborted block.
    decrWorkerAssignments bid dw (WorkerAssignment blocks weight) =
      WorkerAssignment (Set.delete bid blocks) (weight - dw)

    -- | Job state constructors.
    jprof0 workerjs pprep = liftIO getTime >>= \start -> return $ JobProfile start Map.empty workerjs pprep []
    js0 rid rq pending workerjs pprep = jprof0 workerjs pprep >>= \prof -> return $ JobState rid rq prof pending Map.empty []

    logAssignment pid nassigns js =
      let wk wid s = wid ++ ":" ++ s

          wastr (wid, WorkerAssignment b w) = (wk wid $ show $ length b, wk wid $ show w)
          wststr (wid, jwassign -> jbc)     = wk wid $ show $ foldl (+) 0.0 jbc

          (wlens, ww) = unzip $ map wastr $ Map.toList nassigns
          wcontrib    = map wststr $ Map.toList $ jworkerst $ jprofile $ js
      in
        mlogM $ boxToString $  ["Assignment for program: " ++ show pid]
                            %$ ["Worker weights:"]     %$ (indent 2 ww)
                            %$ ["Block distribution:"] %$ (indent 2 wlens)
                            %$ ["Load distribution:"]  %$ (indent 2 wcontrib)


    {------------------------------
     - Block completion handling.
     -----------------------------}

    -- | Block completion processing. This garbage collects jobs and assignment state.
    completeBlocks wid pid cblocksByBID report = do
      time <- liftIO $ getTime
      forM_ cblocksByBID $ \(bid, iblock) -> do
        psOpt <- zm $ do
          (r, bcontrib) <- modifyMJ $ \sjobs -> tryCompleteJS time wid pid bid iblock sjobs $ Map.lookup pid sjobs
          modifyMA_ $ \assigns -> Map.adjust (decrWorkerAssignments bid bcontrib) wid assigns
          return r
        zm $ modifyMJ_ $ \sjobs -> Map.adjust (appendProfileReport report) pid sjobs
        maybe (return ()) (completeProgram pid) psOpt

    -- | Block abort processing. This aborts the given block ids, cleaning up state, but
    --   does not affect any other in-flight blocks.
    -- TODO: pre-emptively abort all other remaining blocks, and clean up the job state.
    abortBlocks wid pid bids reason = do
      time <- liftIO $ getTime
      forM_ bids $ \bid -> do
        psOpt <- zm $ do
          (r, bcontrib) <- modifyMJ $ \sjobs -> tryAbortJS time wid pid bid reason sjobs $ Map.lookup pid sjobs
          modifyMA_ $ \assigns -> Map.adjust (decrWorkerAssignments bid bcontrib) wid assigns
          return r
        maybe (return ()) (\(rid,rq,aborts) -> abortProgram (Just pid) rid rq $ concat aborts) psOpt

    tryCompleteJS time wid pid bid iblock sjobs jsOpt =
      maybe (sjobs, (Nothing, 0.0)) id $ jsOpt >>= \js ->
        let jsE = completeJobBlock time wid bid iblock js in
        return $ either (completeJS pid sjobs) (incompleteJS pid sjobs) jsE

    tryAbortJS time wid pid bid reason sjobs jsOpt =
      maybe (sjobs, (Nothing, 0.0)) id $ jsOpt >>= \js ->
        let jsE = abortJobBlock time wid bid reason js in
        return $ either (completeJS pid sjobs) (incompleteJS pid sjobs) jsE

    completeJS pid sjobs (result, contrib) = (Map.delete pid sjobs, (Just result, contrib))
    incompleteJS pid sjobs (partials, contrib) = (Map.insert pid partials sjobs, (Nothing, contrib))

    completeJobBlock time wid bid iblock js =
      let ncomp             = if null $ jaborted js
                                then Map.insertWith (++) bid iblock $ jcompleted js
                                else Map.empty

          npend             = Set.delete bid $ jpending js
          (nprof, bcontrib) = updateProfile time wid bid $ jprofile js

      in if Set.null npend
            then Left $ ((jrid js, jrq js, jaborted js, nprof, ncomp), bcontrib)
            else Right $ (js { jpending = npend, jcompleted = ncomp, jprofile = nprof }, bcontrib)

    abortJobBlock time wid bid reason js =
      let npend             = Set.delete bid $ jpending js
          (nprof, bcontrib) = updateProfile time wid bid $ jprofile js
      in if Set.null npend
            then Left  ((jrid js, jrq js, jaborted js), bcontrib)
            else Right (js { jpending = npend, jaborted = jaborted js ++ [reason], jprofile = nprof }, bcontrib)

    -- | Profile maintenance.
    updateProfile time wid bid jprof@(jendTimes &&& jworkerst -> (jends, jws)) =
      let njws          = Map.adjust (\wjs -> wjs { jwcomplete = jwcomplete wjs - 1 }) wid jws
          njends        = maybe jends completew $ Map.lookup wid njws
          completew wjs = if jwcomplete wjs == 0 then Map.insert wid time jends else jends
          bcontrib      = maybe 0.0 id (Map.lookup wid njws >>= Map.lookup bid . jwassign)
      in (jprof { jendTimes = njends, jworkerst = njws }, bcontrib)

    appendProfileReport report js@(jprofile -> jprof) =
      js { jprofile = jprof { jreports = (jreports jprof) ++ [report] } }


    {------------------------------
     - Program completion handling.
     -----------------------------}

    -- | Program completion processing. This garbage collects client request state.
    completeProgram pid (rid, rq, aborts, profile, sources) = do
      let prog = DC.role "__global" $ map snd $ sortOn fst $ concatMap snd $ Map.toAscList sources
      (nprogrpE, fpProf) <- liftIO $ ST.profile $ const $ evalTransform Nothing (sfinalStages $ smOpts) prog
      case (aborts, nprogrpE) of
        (_, Left err) -> abortProgram (Just pid) rid rq err
        (h:t, _)      -> abortProgram (Just pid) rid rq $ concat $ h:t
        ([], Right (nprog, _)) -> do
            clOpt <- zm $ getMR rid
            case clOpt of
              Nothing -> zm $ requestError rid
              Just cid -> let fpRep = TransformReport (Map.singleton "Master finalization" [fpProf]) Map.empty
                          in completeRequest pid cid rid rq nprog $ generateReport profile fpRep

    completeRequest pid cid rid rq prog report = do
      mlogM $ unwords ["Completed program", show pid]
      sockOpt <- zm $ do
        modifyMJ_ $ \sjobs -> Map.delete pid sjobs
        modifyMR_ $ \rm -> Map.delete rid rm
        modifyMC  $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      maybe (return ()) (\sid -> sendCI sid mworker $ ProgramDone rq prog report) sockOpt

    -- | Abort compilation.
    abortProgram pidOpt rid rq reason = do
      mlogM $ unwords ["Aborting program", rq, maybe "" show pidOpt]
      cid <- zm $ getMR rid >>= maybe (requestError rid) return
      abortRequest pidOpt cid rid rq reason

    abortRequest pidOpt cid rid rq reason = do
      clOpt <- zm $ do
        modifyMJ_ $ \sjobs -> maybe sjobs (flip Map.delete sjobs) pidOpt
        modifyMR_ $ \rm -> Map.delete rid rm
        modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      maybe (return ()) (\sid -> sendCI sid mworker $ ProgramAborted rq reason) clOpt

    tryCompleteCL cid rid cm rOpt = maybe (cm, Nothing) (completeClient cid rid cm) rOpt
    completeClient cid rid cm rs =
      let nrs = Set.delete rid rs in
      (if null nrs then Map.delete cid cm else Map.insert cid nrs cm, Just cid)


    -- | Compilation report construction.
    generateReport profile finalreport =
      let mkspan s e = e - s
          mkwtrep  (wid, tspan) = wid ++ ": " ++ (secs $ tspan)
          mkwvstr  (wid, v)     = wid ++ ": " ++ (show v)

          -- Compile time per worker
          workertimes    = Map.map (mkspan $ jstartTime profile) $ jendTimes profile

          -- Assigned cost per worker
          workercontribs = Map.map (foldl (+) 0.0 . jwassign) $ jworkerst $ profile

          -- Per-worker fraction of total worker time.
          totaltime      = foldl (+) 0.0 workertimes
          workertratios  = Map.map (/ totaltime) workertimes

          -- Per-worker fraction of total worker cost.
          totalcontrib   = foldl (+) 0.0 workercontribs
          workercratios  = Map.map (/ totalcontrib) workercontribs

          -- Absolute time ratio and cost ratio difference.
          wtcratiodiff   = Map.intersectionWith (\t c -> 1.0 - ((abs $ t - c) / t)) workertratios workercratios

          -- Reports.
          masterreport  = prettyLines $ mconcat [jppreport profile, finalreport]
          profreport    = prettyLines $ mconcat $ jreports profile
          timereport    = map mkwtrep $ Map.toList workertimes
          wtratioreport = map mkwvstr $ Map.toList workertratios
          wcratioreport = map mkwvstr $ Map.toList workercratios
          costreport    = map mkwvstr $ Map.toList wtcratiodiff

          i x = indent $ 2*x
      in boxToString $ ["Workers"]             %$ (i 1 profreport)
                    %$ ["Sequential"]          %$ (i 1 masterreport)
                    %$ ["Compiler service"]
                    %$ (i 1 ["Time ratios"]    %$ (i 2 wtratioreport))
                    %$ (i 1 ["Cost ratios"]    %$ (i 2 wcratioreport))
                    %$ (i 1 ["Cost accuracy"]  %$ (i 2 costreport))
                    %$ ["Time"]                %$ (i 1 timereport)

    parseError = "Could not parse input: "

    wstateError      = throwE $ "Could not create a worker symbol state."
    partitionError   = throwE $ "Could not greedily pick a partition"
    assignError  pid = throwE $ unwords ["Could not assign program", show pid, "(no workers available)"]
    workerError  wid = throwE $ "No worker named " ++ show wid
    requestError rid = throwE $ "No request found: " ++ show rid


processWorkerConn :: ServiceOptions -> ServiceWSTVar -> SocketAction z -> Int -> Socket z Dealer -> ZMQ z ()
processWorkerConn sOpts@(serviceId -> wid) sv initM wtid wworker = do
    initM wtid wworker
    msg <- receive wworker
    logmHandler msg

  where
    wPfxM = "[" ++ wid ++ " " ++ show wtid ++ "] "

    wlogM :: (Monad m, MonadIO m) => String -> m ()
    wlogM msg = noticeM $ wPfxM ++ msg

    werrM :: (Monad m, MonadIO m) => String -> m ()
    werrM msg =  errorM $ wPfxM ++ msg

    zm :: ServiceWM z a -> ZMQ z a
    zm m = runServiceZ sv m >>= either (throwM . ErrorCall) return

    logmHandler (SC.decode -> msgE) = either werrM (\msg -> wlogM (cshow msg) >> mHandler msg) msgE

    -- | Worker message processing.
    mHandler (Block pid cstages st blocksByBID) = processBlock pid cstages st blocksByBID

    mHandler (RegisterAck cOpts) = zm $ do
      wlogM $ unwords ["Registered", show cOpts]
      modifyCO_ $ mergeCompileOpts cOpts

    mHandler Quit = liftIO $ terminate sv

    mHandler m = werrM $ boxToString $ ["Invalid message:"] %$ [show m]

    -- | Synchronizes relevant master and worker compiler options.
    --   These are defaults that can be overridden per compile job.
    mergeCompileOpts mcopts wcopts = wcopts { outLanguage = outLanguage $ mcopts
                                            , programName = programName $ mcopts
                                            , outputFile  = outputFile  $ mcopts
                                            , useSubTypes = useSubTypes $ mcopts
                                            , optimizationLevel = optimizationLevel $ mcopts }

    -- | Block compilation functions.
    processBlock pid ([SDeclOpt cSpec]) initSt blocksByBID =
      abortcatch pid blocksByBID $ do
        start <- liftIO getTime
        startP <- liftIO getPOSIXTime
        wlogM $ boxToString $ ["Worker blocks start"] %$ (indent 2 [show startP])
        (cBlocksByBID, finalSt) <- zm $ foldM (compileBlock pid cSpec) ([], initSt) blocksByBID
        end <- liftIO getTime
        wlogM $ boxToString $ ["Worker local time"] %$ (indent 2 [secs $ end - start])
        sendC wworker $ BlockDone wid pid cBlocksByBID $ ST.report finalSt

    processBlock _ _ _ _ = werrM $ "Invalid worker compile stages"

    abortcatch pid blocksByBID m = m `catchIOError` (\e -> abortBlock pid blocksByBID $ show e)

    abortBlock pid blocksByBID reason =
      sendC wworker $ BlockAborted wid pid (map fst blocksByBID) reason

    compileBlock pid cSpec (blacc, st) (bid, unzip -> (ids, block)) = do
      (nblock, nst) <- debugCompileBlock pid bid (unwords [show $ length block])
                        $ liftIE $ ST.runTransformM st $ ST.runDeclOptPassesBLM cSpec Nothing block
      return (blacc ++ [(bid, zip ids nblock)], nst)

    debugCompileBlock pid bid str m = do
      wlogM $ unwords ["got block", show pid, show bid, str]
      result <- m
      wlogM $ unwords ["finished block", show pid, show bid]
      return result


-- | One-shot connection to submit a remote job and wait for compilation to complete.
submitJob :: ServiceOptions -> RemoteJobOptions -> Options -> IO ()
submitJob sOpts@(serviceId -> rq) rjOpts opts = do
    start <- getTime
    progE <- runDriverM $ k3read opts
    either putStrLn (\p -> runClient Dealer sOpts $ processClientConn start $ concat p) progE

  where
    processClientConn :: Double -> String -> (forall z. Socket z Dealer -> ZMQ z ())
    processClientConn start prog client = do
      noticeM $ "Client submitting compilation job " ++ rq
      sendC client $ Program rq prog rjOpts
      msg <- receive client
      either errorM (mHandler start) $ SC.decode msg

    mHandler :: forall z. Double -> CProtocol -> ZMQ z ()
    mHandler start (ProgramDone rrq prog report) | rq == rrq = liftIO $ do
      end <- getTime
      noticeM $ unwords ["Client finalizing request", rq]
      noticeM $ clientReport (end - start) report
      CPPC.compile opts (scompileOpts $ sOpts) ($) $ prog

    mHandler _ (ProgramAborted rrq reason) | rq == rrq = liftIO $ do
      errorM $ unwords ["Failed to compile request", rq, ":", reason]

    mHandler _ m = errorM $ boxToString $ ["Invalid message:"] %$ [show m]

    clientReport time report =
      boxToString $  ["Compile report:"] %$ (indent 4 $ lines $ report)
                  %$ ["Compile time: " ++ secs time]

shutdownService :: ServiceOptions -> IO ()
shutdownService sOpts = command Dealer sOpts Quit
