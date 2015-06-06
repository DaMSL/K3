{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Language.K3.Driver.ZMQService where

import Control.Concurrent
import Control.Concurrent.Async ( Async, asyncThreadId, cancel )
import Control.Exception ( ErrorCall(..) )
import Control.Monad
import Control.Monad.Catch ( throwM, catchIOError, finally )
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Except
import Control.Monad.Trans.State.Strict

import Data.Binary ( Binary, encode, decode )
import Data.ByteString.Char8 ( ByteString )
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as LBC

import Data.Time.Format
import Data.Time.Clock
import Data.Time.LocalTime

import Data.List
import Data.List.Split
import Data.Maybe ( fromJust )
import Data.Heap ( Heap )
import Data.Map ( Map )
import Data.Set ( Set )
import qualified Data.Heap as Heap
import qualified Data.Map as Map
import qualified Data.Set as Set

import GHC.Conc
import GHC.Generics ( Generic )

import System.Random
import System.ZMQ4.Monadic

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
               | Program        Request ByteString RemoteJobOptions
               | ProgramDone    Request ByteString
               | ProgramAborted Request String
               | Block          ProgramID CompileStages ByteString [(BlockID, [ByteString])]
               | BlockDone      WorkerID ProgramID [(BlockID, [ByteString])]
               | BlockAborted   WorkerID ProgramID [BlockID] String
               | Quit
               deriving (Eq, Read, Show, Generic)

instance Binary CProtocol

-- | Service thread state, encapsulating task workers and connection workers.
data ThreadState = ThreadState { sthread    :: Maybe (Async ())
                               , ttworkers  :: Set (Async ()) }
                  deriving ( Eq )

-- | Compilation progress state.
type Request   = String
type RequestID = Int
type SocketID  = ByteString
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
                 , hbcnt        :: Integer
                 , jobs         :: JobsMap
                 , assignments  :: AssignmentsMap }

type ServiceMState = ServiceState SMST

-- | Compiler service worker state. Note: workers are stateless.
type SWST = ()

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

type WorkQHandler   z = Int -> Socket z Dealer -> ZMQ z ()
type ClientHandler  t = forall z. Socket z t -> ZMQ z ()
type MessageHandler   = forall z. CProtocol -> ZMQ z ()

{- Initial states -}

sthread0 :: ThreadState
sthread0 = ThreadState Nothing Set.empty

sst0 :: MVar () -> CompileOptions -> a -> ServiceState a
sst0 trmv cOpts st = ServiceState trmv sthread0 cOpts st

sstm0 :: MVar () -> CompileOptions -> ServiceMState
sstm0 trmv cOpts = sst0 trmv cOpts $ SMST Map.empty Map.empty Map.empty 0 0 0 0 Map.empty Map.empty

svm0 :: CompileOptions -> IO ServiceMSTVar
svm0 cOpts = newEmptyMVar >>= \trmv0 -> newMVar $ sstm0 trmv0 cOpts

sstw0 :: MVar () -> CompileOptions -> ServiceWState
sstw0 trmv cOpts = sst0 trmv cOpts ()

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

heartbeatM :: ServiceMM z Integer
heartbeatM = modifyMM $ \cst -> let i = hbcnt cst + 1 in (cst {hbcnt = i}, i)

{- Assignments accessors -}
getMA :: ServiceMM z AssignmentsMap
getMA = getM >>= return . assignments

putMA :: AssignmentsMap -> ServiceMM z ()
putMA nassigns = modifyMM_ $ \cst -> cst { assignments = nassigns }

modifyMA :: (AssignmentsMap -> (AssignmentsMap, a)) -> ServiceMM z a
modifyMA f = modifyMM $ \cst -> let (nassigns, r) = f $ assignments cst in (cst { assignments = nassigns }, r)

modifyMA_ :: (AssignmentsMap -> AssignmentsMap) -> ServiceMM z ()
modifyMA_ f = modifyMM_ $ \cst -> cst { assignments = f $ assignments cst }

mapMA :: (WorkerID -> BlockSet -> a) -> ServiceMM z (Map WorkerID a)
mapMA f = getMA >>= return . Map.mapWithKey f

foldMA :: (a -> WorkerID -> BlockSet -> a) -> a -> ServiceMM z a
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

getW :: ServiceWM z SWST
getW = getSt >>= return . compileSt

putW :: SWST -> ServiceWM z ()
putW ncst = modifyM_ $ \st -> st { compileSt = ncst }


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
          ProgramDone rq _            -> "ProgramDone " ++ rq
          ProgramAborted rq _         -> "ProgramAborted " ++ rq
          Block pid _ _ bids          -> unwords ["Block", show pid, intercalate "," $ map (show . fst) bids]
          BlockDone wid pid bids      -> unwords ["BlockDone", show wid, show pid, intercalate "," $ map (show . fst) bids]
          BlockAborted wid pid bids _ -> unwords ["BlockAborted", show wid, show pid, intercalate "," $ map show bids]
          Quit                        -> "Quit"


-- | Service utilities.
initService :: IO () -> IO ()
initService m = streamLogging stdout Log.DEBUG >> m


-- | Compiler service master.
runServiceMaster :: ServiceOptions -> ServiceMasterOptions -> Options -> IO ()
runServiceMaster sOpts smOpts opts = initService $ runZMQ $ do
    sv <- liftIO $ svm0 (scompileOpts sOpts)
    frontend <- socket Router
    bind frontend mconn
    backend <- workqueue sv nworkers "mbackend" $ processMasterConn sOpts smOpts opts sv
    as <- async $ proxy frontend backend Nothing
    noticeM $ unwords ["Service Master", show $ asyncThreadId as, mconn]
    flip finally (shutdownRemote sv frontend) $ liftIO $ do
      modifyTSIO_ sv $ \tst -> tst { sthread = Just as }
      wait sv

  where
    mconn = tcpConnStr sOpts
    nworkers = serviceThreads sOpts


-- | Compiler service worker.
runServiceWorker :: ServiceOptions -> IO ()
runServiceWorker sOpts@(serviceId -> wid) = initService $ runZMQ $ do
    sv <- liftIO $ svw0 (scompileOpts sOpts)
    frontend <- socket Dealer
    setRandomIdentity frontend
    connect frontend mconn
    backend <- workqueue sv nworkers "wbackend" $ processWorkerConn sOpts sv

    noticeM $ unwords ["Worker", wid, "registering"]
    sendC frontend $ Register wid

    as <- async $ proxy frontend backend Nothing
    noticeM $ unwords ["Service Worker", wid, show $ asyncThreadId as, mconn]
    liftIO $ do
      modifyTSIO_ sv $ \tst -> tst { sthread = Just as }
      wait sv

  where
    mconn = tcpConnStr sOpts
    nworkers = serviceThreads sOpts


runClient :: (SocketType t) => t -> ServiceOptions -> ClientHandler t -> IO ()
runClient sockT sOpts clientF = runZMQ $ do
    client <- socket sockT
    setRandomIdentity client
    connect client $ tcpConnStr sOpts
    clientF client


workqueue :: ServiceST a -> Int -> String -> WorkQHandler z -> ZMQ z (Socket z Dealer)
workqueue sv n qid workerF = do
    backend <- socket Dealer
    bind backend $ "inproc://" ++ qid
    as <- forM [1..n] $ \i -> async $ worker i qid workerF
    liftIO $ modifyTSIO_ sv $ \tst -> tst { ttworkers = Set.fromList as `Set.union` ttworkers tst }
    return backend

worker :: Int -> String -> WorkQHandler z -> ZMQ z ()
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
      noticeM $ "Shutting down worker " ++ (show $ BC.unpack wsid)
      sendCI wsid master Quit
    liftIO $ threadDelay $ 5 * 1000 * 1000


-- | Messaging primitives.
sendC :: (SocketType t, Sender t) => Socket z t -> CProtocol -> ZMQ z ()
sendC s m = send s [] $ LBC.toStrict $ encode m

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
  replyF $ decode $ LBC.fromStrict rep


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

    mdbgM :: (Monad m, MonadIO m) => String -> m ()
    mdbgM msg =  debugM $ mPfxM ++ msg

    merrM :: (Monad m, MonadIO m) => String -> m ()
    merrM msg =  errorM $ mPfxM ++ msg

    zm :: ServiceMM z a -> ZMQ z a
    zm m = runServiceZ sv m >>= either (throwM . ErrorCall) return

    logmHandler sid (decode . LBC.fromStrict -> msg) = mlogM (cshow msg) >> mHandler sid msg

    mHandler sid (Program rq prog jobOpts) = do
      rid' <- zm $ do
        rid <- requestIDM
        putMC sid (Set.singleton rid) >> putMR rid sid >> return rid
      process (BC.unpack prog) jobOpts rq rid'

    mHandler _ (BlockDone wid pid blocksByBID)    = completeBlocks wid pid blocksByBID
    mHandler _ (BlockAborted wid pid bids reason) = abortBlocks wid pid bids reason

    mHandler sid (Register wid) = do
      mlogM $ unwords ["Registering worker", wid]
      cOpts <- zm $ putMWI wid sid >> getCO
      sendCI sid mworker $ RegisterAck cOpts

    -- TODO: detect worker changes.
    mHandler _ (HeartbeatAck hbid) = mdbgM $ unwords ["Got a heartbeat ack", show hbid]

    mHandler _ Quit = liftIO $ terminate sv
    mHandler _ m = merrM $ boxToString $ ["Invalid message:"] %$ [show m]

    -- | Compilation functions
    nfP        = noFeed $ input opts
    includesP  = (includes $ paths opts)

    process prog jobOpts rq rid = abortcatch rid rq $ do
      msgs <- zm $ do
        mlogM $ unwords ["Processing program", rq, "(", show rid, ")"]
        (blocks, initSt) <- preprocess prog jobOpts
        assignBlocks rid rq jobOpts blocks initSt
      sendCIs mworker msgs

    abortcatch rid rq m = m `catchIOError` (\e -> abortProgram rid rq $ show e)

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
      mkMessages pid jobOpts initSt blocksByWID

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
        wsockid <- getMWI wid >>= maybe (workerError wid) return
        return $ msgacc ++ [(wsockid, Block pid rstg pst bbl)]

    -- | Block completion processing. This garbage collects jobs and assignment state.
    completeBlocks wid pid blocksByBID = do
      forM_ blocksByBID $ \(bid, map (read . BC.unpack) -> block) -> do
        psOpt <- zm $ do
          modifyMA_ $ \assigns -> Map.adjust (Set.delete bid) wid assigns
          modifyMJ $ \sjobs -> tryCompleteJS pid bid block sjobs $ Map.lookup pid sjobs
        maybe (return ()) completeProgram psOpt

    -- | Block abort processing. This aborts the given block ids, cleaning up state, but
    --   does not affect any other in-flight blocks.
    -- TODO: pre-emptively abort all other remaining blocks, and clean up the job state.
    abortBlocks wid pid bids reason = do
      forM_ bids $ \bid -> do
        psOpt <- zm $ do
          modifyMA_ $ \assigns -> Map.adjust (Set.delete bid) wid assigns
          modifyMJ $ \sjobs -> tryAbortJS pid bid reason sjobs $ Map.lookup pid sjobs
        maybe (return ()) (\(rid,rq,aborts) -> abortProgram rid rq $ concat aborts) psOpt

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
            clOpt <- zm $ getMR rid
            case clOpt of
              Nothing -> zm $ requestError rid
              Just cid -> completeRequest cid rid rq nprog

    completeRequest cid rid rq prog = do
      let packedProg = BC.pack $ show prog
      mlogM $ "Packed result program size " ++ (show $ BC.length packedProg)
      sockOpt <- zm $ do
        modifyMR_ $ \rm -> Map.delete rid rm
        modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      maybe (return ()) (\sid -> sendCI sid mworker $ ProgramDone rq packedProg) sockOpt

    -- | Abort compilation.
    abortProgram rid rq reason = do
      cid <- zm $ getMR rid >>= maybe (requestError rid) return
      abortRequest cid rid rq reason

    abortRequest cid rid rq reason = do
      clOpt <- zm $ do
                 modifyMR_ $ \rm -> Map.delete rid rm
                 modifyMC $ \cm -> tryCompleteCL cid rid cm $ Map.lookup cid cm
      maybe (return ()) (\sid -> sendCI sid mworker $ ProgramAborted rq reason) clOpt

    tryCompleteCL cid rid cm rOpt = maybe (cm, Nothing) (completeClient cid rid cm) rOpt
    completeClient cid rid cm rs =
      let nrs = Set.delete rid rs in
      (if null nrs then Map.delete cid cm else Map.insert cid nrs cm, Just cid)

    parseError = "Could not parse input: "

    assignError  pid = throwE $ unwords ["Could not assign program", show pid, "(no workers available)"]
    workerError  wid = throwE $ "No worker named " ++ show wid
    requestError rid = throwE $ "No request found: " ++ show rid


processWorkerConn :: ServiceOptions -> ServiceWSTVar -> Int -> Socket z Dealer -> ZMQ z ()
processWorkerConn sOpts@(serviceId -> wid) sv wtid wworker = do
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

    logmHandler (decode . LBC.fromStrict -> msg) = wlogM (cshow msg) >> mHandler msg

    -- | Worker message processing.
    mHandler (Block pid cstages st blocksByBID) = processBlock pid cstages st blocksByBID

    mHandler (RegisterAck cOpts) = zm $ do
      wlogM $ unwords ["Registered", show cOpts]
      modifyCO_ $ mergeCompileOpts cOpts

    mHandler (Heartbeat hbid) = sendC wworker $ HeartbeatAck hbid
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
    processBlock pid ([SDeclOpt cSpec]) (read . BC.unpack -> st) blocksByBID =
      abortcatch pid blocksByBID $ do
        (cBlocksByBID, _) <- zm $ foldM (compileBlock pid cSpec) ([], st) blocksByBID
        sendC wworker $ BlockDone wid pid cBlocksByBID

    processBlock _ _ _ _ = werrM $ "Invalid worker compile stages"

    abortcatch pid blocksByBID m = m `catchIOError` (\e -> abortBlock pid blocksByBID $ show e)

    abortBlock pid blocksByBID reason =
      sendC wworker $ BlockAborted wid pid (map fst blocksByBID) reason

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
submitJob sOpts@(serviceId -> rq) rjOpts opts = do
    progE <- runDriverM $ k3read opts
    either putStrLn (\p -> runClient Dealer sOpts $ processClientConn p) progE

  where
    processClientConn :: [String] -> (forall z. Socket z Dealer -> ZMQ z ())
    processClientConn prog client = do
      noticeM $ "Client submitting compilation job"
      sendC client $ Program rq (BC.pack $ concat $ prog) rjOpts
      noticeM $ "Client waiting for compilation"
      msg <- receive client
      mHandler $ decode $ LBC.fromStrict msg

    mHandler :: forall z. CProtocol -> ZMQ z ()
    mHandler (ProgramDone rrq bytes) | rq == rrq = liftIO $ do
      noticeM $ "Client compiling request " ++ rq
      noticeM $ "Client received program of size " ++ (show $ BC.length bytes)
      CPPC.compile opts (scompileOpts $ sOpts) ($) (read $ BC.unpack bytes)

    mHandler (ProgramAborted rrq reason) | rq == rrq = liftIO $ do
      errorM $ unwords ["Failed to compile request", rq, ":", reason]

    mHandler m = errorM $ boxToString $ ["Invalid message:"] %$ [show m]

shutdownService :: ServiceOptions -> IO ()
shutdownService sOpts = command Dealer sOpts Quit
