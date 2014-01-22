{-# LANGUAGE CPP #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | A message processing runtime for the K3 interpreter
-- TODO: 
-- i. parallelism and non-interleaved I/O.
-- ii. implement message framing as wrapping wire descriptors
-- iii. remove bidirectional IOHandles for type-safe one-way handles (i.e. read-only or write-only)
module Language.K3.Runtime.Engine (
    FrameDesc(..)
  , WireDesc(..)
  , MessageProcessor(..)

  , Engine(..)
  , EngineControl(..)

  , EngineT
  , EngineM
  , runEngineT
  , runEngineM
  , EngineError(..)
  , throwEngineError

  , EngineConfiguration(..)
  , EEndpoints

  , defaultConfig

  , simulationEngine
  , parSimulationEngine
  , networkEngine
  , parNetworkEngine

  , exprWD

  , nodes
  , statistics

  , runEngine
  , forkEngine
  , waitForEngine
  , terminateEngine

  , enqueue
  , dequeue
  , send

  , openBuiltin
  , openFile
  , openSocket
  , close

  , hasRead
  , doRead
  , hasWrite
  , doWrite

  , attachNotifier
  , attachNotifier_
  , detachNotifier
  , detachNotifier_

  , showMessageQueues
  , showEngine

#ifdef TEST
  , LoopStatus(..)

  , Workers(..)
  , Listeners

  , NConnection(..)
  , NEndpoint(..)

  , IOHandle(..)
  , BufferSpec(..)
  , BufferContents(..)

  , EndpointBuffer(..)
  , EndpointBindings
  , EndpointNotification(..)
  , Endpoint(..)
  , EEndpointState(..)

  , EConnectionMap(..)
  , EConnectionState(..)

  , connectionId
  , peerEndpointId

  , internalSendAddress
  , externalSendAddress

  , simulation

  , cleanupEngine

  , newEndpoint
  , closeEndpoint
  , getEndpoint

  , newConnection

  , emptySingletonBuffer
  , emptyBoundedBuffer
  , emptyUnboundedBuffer
  , singletonBuffer
  , boundedBuffer
  , unboundedBuffer
  , exclusive
  , shared

  , emptyEBuffer
  , fullEBuffer
  , sizeEBuffer
  , capacityEBuffer
  , readEBuffer
  , appendEBuffer
  , takeEBuffer
#endif

) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MSampleVar
import Control.Monad
import Control.Monad.Reader
import Control.Monad.Trans.Either

import qualified Data.ByteString.Char8 as BS
import qualified Data.HashMap.Lazy as H
import Data.List
import Data.List.Split (wordsBy)

import qualified System.IO as SIO
import System.Process
import System.Mem.Weak (Weak)

import Network.Socket (withSocketsDo)
import qualified Network.Transport     as NT
import qualified Network.Transport.TCP as NTTCP

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Runtime.Common

import Language.K3.Utils.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["EngineSteps"])


-- | The engine data type, storing all engine components.
data Engine a = Engine { config          :: EngineConfiguration
                       , internalFormat  :: WireDesc (InternalMessage a)
                       , control         :: EngineControl
                       , deployment      :: SystemEnvironment
                       , queueConfig     :: QueueConfiguration a
                       , workers         :: Workers
                       , listeners       :: Listeners
                       , endpoints       :: EEndpointState a
                       , connections     :: EConnectionState }

data EngineError = EngineError String deriving (Eq, Read, Show)

instance Pretty EngineError where
    prettyLines e = [show e]

type EngineT e r m = EitherT e (ReaderT r m)

type EngineM a = EngineT EngineError (Engine a) IO

runEngineT :: EngineT e r m a -> r -> m (Either e a)
runEngineT s r = flip runReaderT r $ runEitherT s

runEngineM :: EngineM a b -> Engine a -> IO (Either EngineError b)
runEngineM = runEngineT

throwEngineError :: EngineError -> EngineM a b
throwEngineError = Control.Monad.Trans.Either.left

{- Configuration parameters -}

data EngineConfiguration = EngineConfiguration { address           :: Address
                                               , defaultBufferSpec :: BufferSpec
                                               , connectionRetries :: Int
                                               , waitForNetwork    :: Bool }

{- Message processing -}

data EngineControl = EngineControl {
    -- | A concurrent boolean used internally by workers to determine if
    -- they should terminate.
    -- No blocking required, only safe access to a termination boolean
    -- for both workers (readers) and a console (i.e., a single writer)
    terminateV :: MVar Bool,

    -- | A concurrent counter that will reach 0 once all network listeners have finished.
    -- This is incremented and decremented as listeners are added to the engine, or as
    -- they are removed or complete.
    -- No blocking required, only safe access as part of determining whether to
    -- terminate the engine.
    networkDoneV :: MVar Int,

    -- | Mutex for external threads to wait on this engine to complete.
    -- External threads should use readMVar on this variable, and this will
    -- be signalled by the last worker thread to finish.
    waitV :: MVar (),

    -- | When a worker empties all of its queues, it waits to read from its 
    -- corresponding SV in this map. When a message destined for the
    --  worker is enqueued, the worker's SV will be populated, waking the worker.
    messageReadyMap :: MVar (H.HashMap ThreadId (MSampleVar ())),

    -- TODO: gracefully terminate after all workers have completed
    workersDoneV :: MVar Bool
}

data LoopStatus res err = Result res | Error err | MessagesDone res

-- | Each backend provides must provide a message processor which can handle the initialization of
-- the message queues and the dispatch of individual messages to the corresponding triggers.
data MessageProcessor prog msg res err = MessageProcessor {
    -- | Initialization of the execution environment.
    initialize :: prog -> EngineM msg res,

    -- | Process a single message.
    process    :: (Address, Identifier, msg) -> res -> EngineM msg res,

    -- | Query the status of the message processor.
    status     :: res -> Either err res,

    -- | Clean up the execution environment.
    finalize   :: res -> EngineM msg res,

    -- | Generate an execution report
    report     :: Either err res -> EngineM msg ()
}

{- Engine components -}
type QueueId = String

defaultQueueId :: QueueId
defaultQueueId = "Queue"

data QueueConfiguration a = PerGroup { 
    -- Map a queueId to a queue of messages
    queueIdToQueue  :: (MVar (H.HashMap QueueId [(InternalMessage a)])),
    
    -- Group queues that should share a worker.
    queueGroups     :: (MVar [[QueueId]]), 

    -- Map a workers threadId to a list of queueIds that the worker should manage
    workerIdToQueueIds :: (MVar (H.HashMap ThreadId [QueueId])),

    -- Map an incoming messages to the proper QueueId
    messageToQueueId  :: (MVar (H.HashMap (Address,Identifier) QueueId)) 
}

data Workers
  = Uniprocess    (MVar ThreadId)
  | Multithreaded ThreadPool
  | Multiprocess  ProcessPool

-- TODO
type ThreadPool  = MVar [ThreadId]       -- TODO: use parallel-io's Pool
type ProcessPool = MVar [ProcessHandle]  -- TODO: use System.process with pipes or Haskell's forkOS

-- | Listener threads run network endpoints. We track these as weak refs
--   to allow them to be garbage collected on listener socket termination.
--   The caller is responsible for using deRefWeak to determine if the thread is valid.
type Listeners   = MVar [(Identifier, Weak ThreadId)]


{- Wire Descriptions -}

data FrameDesc = Delimiter String | FixedSize Int | PrefixLength

-- | A description of a wire format, with serialization of data, deserialization into data, and
-- validation of deserialized data.
data WireDesc a = WireDesc { packWith     :: a -> IO String
                           , unpackWith   :: String -> IO (Maybe a)
                           , frame        :: FrameDesc }
-- TODO: frames as wrapped wire descriptors, with the following implementations:
-- i. byte-oriented framing (i.e. pack/unpack as bytestring)
-- ii. prefix length framing
-- iii. delimiter-based framing

-- | Internal messaging between triggers includes a sender address and
--   destination trigger identifier with each message.
type InternalMessage a = (Address, Identifier, a)


{- Endpoints and internal IO handles -}

-- | K3 endpoint datatype. Endpoints may be external (for external data sources or sinks), 
--   or internal (for peer-to-peer communication).
data Endpoint a b = Endpoint { handle      :: IOHandle a
                             , buffer      :: Maybe (EndpointBuffer a)
                             , subscribers :: EndpointBindings b }

type EEndpoints a b = MVar (H.HashMap Identifier (Endpoint a b))

data EEndpointState a = EEndpointState { internalEndpoints :: EEndpoints (InternalMessage a) a
                                       , externalEndpoints :: EEndpoints a a }

data Builtin = Stdin | Stdout | Stderr deriving (Eq, Show, Read)

data IOHandle a
  = BuiltinH (WireDesc a) SIO.Handle Builtin
  | FileH    (WireDesc a) SIO.Handle
  | SocketH  (WireDesc a) (Either NEndpoint NConnection)

data BufferSpec = BufferSpec { maxSize :: Int, batchSize :: Int }

-- | Sources buffer the next value, while sinks keep a buffer of values waiting to be flushed.
data BufferContents a
  = Single (Maybe a)
  | Multiple [a] BufferSpec

-- | Endpoint buffers, which may be used by concurrent workers (shared), or by a single worker thread (exclusive)
data EndpointBuffer a
  = Exclusive (BufferContents a)
  | Shared    (MVar (BufferContents a))

-- | Endpoint notifications (i.e. triggers attached to open/close/data)
-- | Currently, notifications can only use a constant (i.e., compile-time value) argument on dispatch
type EndpointBindings a = [(EndpointNotification, InternalMessage a)]


{- Builtin notifications -}
data EndpointNotification
    = FileData
    | FileClose
    | SocketAccept
    | SocketData
    | SocketClose
  deriving (Eq, Show, Read)


{- Network state -}

data NEndpoint   = NEndpoint { endpoint :: LLEndpoint, epTransport :: LLTransport }
data NConnection = NConnection { conn :: LLConnection, cEndpointAddress :: Address }

-- | Low-level transport layer, built on network-transport
type LLTransport  = NT.Transport
type LLEndpoint   = NT.EndPoint
type LLConnection = NT.Connection

-- | Connection maps may be initialized without binding an endpoint (e.g., if the program has no
--   communication with any other peer or network sink), and are also safely modifiable to enable
--   their construction on demand.
-- TODO: implement an actual cache, or a connection pool with Data.Pool from the resource-pool package.
data EConnectionMap = EConnectionMap { anchor :: (Address, Maybe NEndpoint)
                                     , cache  :: [(Address, NConnection)] }

-- | Two connection maps for outgoing internal (i.e., K3 peer) and external (i..e, network sink) connections
--   The first is optional capturing simulations that cannot send to peers without name resolution.
newtype EConnectionState = EConnectionState (Maybe (MVar EConnectionMap), MVar EConnectionMap)


{- Listeners -}
type ListenerProcessor a b =
  ListenerState a b -> EndpointBuffer a -> Endpoint a b -> EngineM b (EndpointBuffer a)

data ListenerState a b = ListenerState { name       :: Identifier
                                       , engineSync :: (MVar Int)
                                       , processor  :: ListenerProcessor a b }

data ListenerError a
    = SerializeError  BS.ByteString
    | InvalidBuffer
    | OverflowError   a
    | PropagatedError a


{- Naming schemes and constants -}

internalEndpointPrefix :: String
internalEndpointPrefix = "__"

connectionId :: Address -> Identifier
connectionId addr = internalEndpointPrefix ++ "_conn_" ++ show addr

peerEndpointId :: Address -> Identifier
peerEndpointId addr = internalEndpointPrefix ++ "_node_" ++ show addr

externalEndpointId :: Identifier -> Bool
externalEndpointId = not . isPrefixOf internalEndpointPrefix


{- Configurations -}
defaultConfig :: EngineConfiguration
defaultConfig = EngineConfiguration { address           = defaultAddress
                                    , defaultBufferSpec = bufferSpec
                                    , connectionRetries = 5
                                    , waitForNetwork    = False }
  where
    bufferSpec = BufferSpec { maxSize = 100, batchSize = 10 }

configureWithAddress :: Address -> EngineConfiguration
configureWithAddress addr = EngineConfiguration addr bufSpec connRetries wforNet
  where bufSpec = defaultBufferSpec defaultConfig
        connRetries = connectionRetries defaultConfig
        wforNet = waitForNetwork defaultConfig


{- Wire descriptions -}
exprWD :: WireDesc (K3 Expression)
exprWD = WireDesc (return . show) (return . Just . read) PrefixLength

internalizeWD :: WireDesc a -> WireDesc (InternalMessage a)
internalizeWD (WireDesc packF unpackF _) =
  WireDesc { packWith      = packInternal
           , unpackWith    = unpackInternal
           , frame         = PrefixLength }
  where
    packInternal (addr, n, a) = packF a >>= return . ((show addr ++ "|" ++ n ++ "|") ++)
    unpackInternal str =
        case wordsBy (== '|') str of
          [addr, n, aStr] -> unpackF aStr >>= return . maybe Nothing (Just . ((read addr) :: Address, n,))
          _               -> return Nothing

{- QueueConfiguration constructors -}

-- Single Queue, Single Worker
singleQSingleW :: [Address] -> [Identifier] -> IO (QueueConfiguration a)
singleQSingleW addrL idL = do
  -- setup a single queue hashmap
  qMap    <- newMVar (H.insert defaultQueueId [] H.empty)
  -- create a single group containing the default queue
  qGroups <- newMVar $ [[defaultQueueId]] 
  wMap    <- newEmptyMVar
  -- route messages to the default queue
  cross   <- return $ [(addr,id) | addr <- addrL, id <- idL]
  kvs     <- return $ map (,defaultQueueId) cross
  mMap    <- newMVar $ H.fromList kvs
  return $ PerGroup qMap qGroups wMap mMap

-- Per Peer Queues. Single Worker
peerQSingleW :: [Address] -> [Identifier] -> IO (QueueConfiguration a)
peerQSingleW addrL idL = do
  -- setup queue-per-peer hashmap
  allQs   <- return $ map qName addrL
  qMap    <- newMVar $ H.fromList $ map (\q -> (q, []) ) allQs
  -- place all queues in one group (for single worker)
  qGroups <- newMVar $ [allQs]
  wMap    <- newEmptyMVar
  -- route each message based on the address provided
  kvs     <- return $ [((addr,id), qName addr) | addr <- addrL, id <- idL]
  mMap    <- newMVar $ H.fromList kvs
  return  $ PerGroup qMap qGroups wMap mMap
  where qName addr = show addr

-- Per Peer Queues and Workers
peerQPeerW :: [Address] -> [Identifier] -> IO (QueueConfiguration a)
peerQPeerW addrL idL = do
  -- setup queue-per-peer hashmap
  allQs   <- return $ map qName addrL
  qMap    <- newMVar $ H.fromList $ map (\q -> (q, []) ) allQs
  -- place each queue in its own group (for its own worker)
  qGroups <- newMVar $ map (\q -> [q]) allQs
  wMap    <- newEmptyMVar
  -- route each message based on the address provided
  kvs     <- return $ [((addr,id), qName addr) | addr <- addrL, id <- idL]
  mMap    <- newMVar $ H.fromList kvs
  return  $ PerGroup qMap qGroups wMap mMap
  where qName addr = show addr

-- Per Trigger Queues and Workers
triggerQtriggerW :: [Address] -> [Identifier] -> IO (QueueConfiguration a)
triggerQtriggerW addrL idL = do
  -- setup queue-per-trigger hashmap
  allQs   <- return $ map qName [(a,n) | a <- addrL, n <- idL]
  qMap    <- newMVar $ H.fromList $ map (\q -> (q, []) ) allQs
  -- place each queue in its own group (for its own worker)
  qGroups <- newMVar $ map (\q -> [q]) allQs
  wMap    <- newEmptyMVar
  -- route each message based on the address,id provided
  kvs     <- return $ [((addr,id), qName (addr,id)) | addr <- addrL, id <- idL]
  mMap    <- newMVar $ H.fromList kvs
  return  $ PerGroup qMap qGroups wMap mMap
  where qName (addr,id) = (show addr) ++ "," ++ id

{- EngineControl Constructors -}
defaultControl :: IO EngineControl 
defaultControl = EngineControl <$> newMVar False <*> newMVar 0 <*> newEmptyMVar <*> newEmptyMVar <*> newMVar False

{- Engine constructors -}

-- | Simulation engine constructor.
--   This is initialized with an empty internal connections map
--   to ensure it cannot send internal messages.
simulationEngine :: [Identifier] -> SystemEnvironment -> WireDesc a -> IO (Engine a)
simulationEngine trigs systemEnv (internalizeWD -> internalWD)  = do
  config'         <- return $ configureWithAddress $ head $ deployedNodes systemEnv
  ctrl            <- defaultControl
  workers'        <- newEmptyMVar >>= return . Uniprocess
  listeners'      <- newMVar []
  endpoints'      <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  externalConns   <- emptyConnectionMap . externalSendAddress . address $ config'
  connState       <- return $ EConnectionState (Nothing, externalConns)
  qconfig         <- case deployedNodes systemEnv of
                       [x] -> singleQSingleW (deployedNodes systemEnv) trigs
                       _   -> peerQSingleW (deployedNodes systemEnv) trigs
  return $ Engine config' internalWD ctrl systemEnv qconfig workers' listeners' endpoints' connState

-- Parallel Simulation Engine: Run each peer's message loop in its own thread
parSimulationEngine :: [Identifier] -> SystemEnvironment -> WireDesc a -> IO (Engine a)
parSimulationEngine trigs systemEnv (internalizeWD -> internalWD)  = do
  config'         <- return $ configureWithAddress $ head $ deployedNodes systemEnv
  ctrl            <- defaultControl
  workers'        <- newEmptyMVar >>= return . Multithreaded
  listeners'      <- newMVar []
  endpoints'      <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  externalConns   <- emptyConnectionMap . externalSendAddress . address $ config'
  connState       <- return $ EConnectionState (Nothing, externalConns)
  qconfig         <- peerQPeerW (deployedNodes systemEnv) trigs
  return $ Engine config' internalWD ctrl systemEnv qconfig workers' listeners' endpoints' connState

-- | Network engine constructor.
--   This is initialized with listening endpoints for each given peer as well
--   as internal and external connection anchor endpoints for messaging.
networkEngine :: [Identifier] -> SystemEnvironment -> WireDesc a -> IO (Engine a)
networkEngine trigs systemEnv (internalizeWD -> internalWD) = do
  config'       <- return $ configureWithAddress $ head peers
  ctrl          <- defaultControl
  workers'      <- newEmptyMVar >>= return . Uniprocess
  listnrs       <- newMVar []
  endpoints'    <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  internalConns <- emptyConns internalSendAddress config' >>= return . Just
  externalConns <- emptyConns externalSendAddress config'
  connState     <- return $ EConnectionState (internalConns, externalConns)
  qconfig       <- peerQSingleW (deployedNodes systemEnv) trigs
  engine        <- return $ Engine config' internalWD ctrl systemEnv qconfig workers' listnrs endpoints' connState

  -- TODO: Verify correctness.
  void $ runEngineM startNetwork engine
  return engine

  where
    peers                      = deployedNodes systemEnv
    emptyConns addrF config'   = emptyConnectionMap . addrF . address $ config'
    startNetwork = mapM_ runPeerEndpoint peers
    runPeerEndpoint addr = openSocketInternal (peerEndpointId addr) addr "r"

parNetworkEngine :: [Identifier] -> SystemEnvironment -> WireDesc a -> IO (Engine a)
parNetworkEngine trigs systemEnv (internalizeWD -> internalWD) = do
  config'       <- return $ configureWithAddress $ head peers
  ctrl          <- defaultControl
  workers'      <- newEmptyMVar >>= return . Uniprocess
  listnrs       <- newMVar []
  endpoints'    <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  internalConns <- emptyConns internalSendAddress config' >>= return . Just
  externalConns <- emptyConns externalSendAddress config'
  connState     <- return $ EConnectionState (internalConns, externalConns)
  qconfig       <- peerQPeerW (deployedNodes systemEnv) trigs
  engine        <- return $ Engine config' internalWD ctrl systemEnv qconfig workers' listnrs endpoints' connState

  -- TODO: Verify correctness.
  void $ runEngineM startNetwork engine
  return engine

  where
    peers                      = deployedNodes systemEnv
    emptyConns addrF config'   = emptyConnectionMap . addrF . address $ config'
    startNetwork = mapM_ runPeerEndpoint peers
    runPeerEndpoint addr = openSocketInternal (peerEndpointId addr) addr "r"

{- Engine extractors -}

internalSendAddress :: Address -> Address
internalSendAddress (Address (host, port)) = Address (host, port+1)

externalSendAddress :: Address -> Address
externalSendAddress (Address (host, port)) = Address (host, port+2)

deployedNodes :: SystemEnvironment -> [Address]
deployedNodes = map fst

nodes :: Engine a -> [Address]
nodes e = deployedNodes $ deployment e

numQueuedMessages :: QueueConfiguration b -> EngineM a Int
numQueuedMessages qconfig = liftIO $ withMVar (queueIdToQueue qconfig) (return . sumListSizes)
  where sumListSizes = H.foldl' (\acc msgs -> acc + length msgs) 0

numEndpoints :: EEndpointState b -> EngineM a Int
numEndpoints es = (+) <$> (count $ externalEndpoints es) <*> (count $ internalEndpoints es)
  where count ep = liftIO $ withMVar ep (return . H.size)

statistics :: EngineM a (Int, Int)
statistics = do
    engine <- ask
    nqm    <- numQueuedMessages $ queueConfig engine
    nep    <- numEndpoints $ endpoints engine
    return (nqm, nep)

simulation :: EngineM a Bool
simulation = ask >>= return . \case
    Engine { connections = (EConnectionState (Nothing, _)) } -> True
    _ -> False

{- Message processing and engine control -}
registerNetworkListener :: (Identifier, Weak ThreadId) -> EngineM a ()
registerNetworkListener iwt = do
    engine <- ask
    liftIO $ modifyMVar_ (networkDoneV $ control engine) $ return . succ
    liftIO $ modifyMVar_ (listeners engine) $ return . (iwt:)

deregisterNetworkListener :: Identifier -> EngineM a ()
deregisterNetworkListener n = do
    engine <- ask
    liftIO $ modifyMVar_ (networkDoneV $ control engine) $ return . pred
    liftIO $ modifyMVar_ (listeners engine) $ return . filter ((n /=) . fst)

-- TODO: properly terminate workers after all queues are empty
terminate :: EngineM a Bool
terminate = do
    engine     <- ask
    terminate' <- liftIO $ readMVar (terminateV $ control engine)
    netdone    <- networkDone
    workdone   <- workersDone
    return $ workdone && netdone || not (waitForNetwork $ config engine) && terminate'

workersDone :: EngineM a Bool
workersDone = do
  engine <- ask
  done   <- liftIO . readMVar . workersDoneV $ control engine
  return done

networkDone :: EngineM a Bool
networkDone = do
    engine <- ask
    done   <- liftIO . readMVar . networkDoneV $ control engine
    return $ done == 0

-- Wait on the MSampleVar associated with the given worker
waitForMessage :: ThreadId -> EngineM a ()
waitForMessage tid = do
  engine <- ask 
  mv     <- return $ messageReadyMap $ control engine
  h      <- liftIO $ readMVar mv
  sv     <- return $ maybe wDie id (H.lookup tid h)
  liftIO $ readSV sv

-- Write to the MSampleVar associated with the given worker
notifyMessage :: ThreadId -> EngineM a ()
notifyMessage tid = do
  engine <- ask
  mv     <- return $ messageReadyMap $ control engine
  liftIO $ withMVar mv (\h -> do
    sv <- return $ maybe wDie id $ H.lookup tid h
    writeSV sv ()
    )

-- | Process a single message within the engine, given the message processor to use, and the
-- previous message result. Return the status of the loop.
processMessage :: MessageProcessor p a r e -> r -> EngineM a (LoopStatus r e)
processMessage mp pr = do
    engine <- ask
    message <- dequeue $ queueConfig engine
    maybe terminate' process' message
  where
    terminate' = return $ MessagesDone pr
    process' m = do
        nextResult <- process mp m pr
        return $ either Error Result (status mp nextResult)

runMessages :: (Pretty r, Pretty e, Show a)
            => MessageProcessor p a r e -> EngineM a (LoopStatus r e) -> EngineM a ()
runMessages mp status' = ask >>= \engine -> status' >>= \case
    Result r       -> debugStep >> runMessages mp (processMessage mp r)
    Error e        -> die "Error:" e (control engine)
    MessagesDone r -> terminate >>= \case
        True -> do
            fr <- finalize mp r
            die "Terminated:" fr (control engine)
        _    -> liftIO myThreadId >>= waitForMessage >> runMessages mp (processMessage mp r)
  where
    debugStep = do
      q <- prettyQueues
      logStep $ boxToString $ ["", "EVENT LOOP {"] ++ indent 2 q ++ ["}"]

    die msg r cntrl = do
        logStep $ boxToString $ ["", msg] ++ prettyLines r
        cleanupEngine
        void $ liftIO $ tryPutMVar (waitV cntrl) ()
        logStep $ "Finished."

    logStep s = void $ _notice_EngineSteps $ s

runEngine :: (Pretty r, Pretty e, Show a) => MessageProcessor p a r e -> p -> EngineM a ()
runEngine mp p = do
    engine <- ask
    result <- initialize mp p
    case workers engine of 
      Uniprocess worker_mv -> do
        engine  <- ask
        myId    <- liftIO myThreadId
        qGroups <- liftIO $ readMVar (queueGroups $ queueConfig engine)
        configWorkers [myId] qGroups
        runMessages mp (return . either Error Result $ status mp result)
      
      -- TODO: graceful termination after all queues are empty in simulation mode
      Multithreaded workers_mv -> do 
        engine  <- ask
        qGroups <- liftIO $ readMVar $ queueGroups $ queueConfig engine
        threads <- mapM (const $ forkWorker mp result) qGroups
        configWorkers threads qGroups
        waitForEngine
      
      Multiprocess _ -> error "Unsupported engine mode: Multiprocess" 

forkWorker :: (Pretty r, Pretty e, Show a) => MessageProcessor p a r e -> r -> EngineM a ThreadId
forkWorker mp result = ask >>= \engine -> liftIO . forkIO $ void $ runWorker engine
  where 
   runWorker engine = do
      runEngineM (runMessages mp (return . either Error Result $ status mp result)) engine

configWorkers :: [ThreadId] -> [[QueueId]] -> EngineM a ()
configWorkers threads qgroups = do
  engine  <- ask
  -- log threadIds of workers
  liftIO $ case workers engine of
    Uniprocess mv    -> tryTakeMVar mv >> putMVar mv (head threads)
    Multithreaded mv -> tryTakeMVar mv >> putMVar mv threads
    Multiprocess _   -> error "Multiprocess mode unsupported"
  -- map threadIds to queue groups
  wm_mv    <- return $ workerIdToQueueIds $ queueConfig engine
  wm_kvs  <- return $ zip threads qgroups
  liftIO $ putMVar wm_mv (H.fromList wm_kvs)
  -- map threadIds to emptySVs
  mrmv    <- return $ messageReadyMap $ control engine
  mr_kvs  <- liftIO $ mapM (\tid -> newEmptySV >>= return . ((,) tid) ) threads
  liftIO $ putMVar mrmv (H.fromList mr_kvs)

forkEngine :: (Pretty r, Pretty e, Show a) => MessageProcessor p a r e -> p -> EngineM a ThreadId
forkEngine mp p = ask >>= \engine -> liftIO . forkIO $ void $ runEngineM (runEngine mp p) engine

waitForEngine :: EngineM a ()
waitForEngine = ask >>= liftIO . readMVar . waitV . control

terminateEngine :: EngineM a ()
terminateEngine = do
    engine <- ask
    liftIO $ modifyMVar_ (terminateV $ control engine) (const $ return True)
    -- send the message ready signal to all workers
    mv <- return $ messageReadyMap $ control engine
    h <- liftIO $ readMVar mv
    liftIO $ mapM_ (\sv -> writeSV sv ()) (map snd $ H.toList h)

cleanupEngine :: EngineM a ()
cleanupEngine = do
    engine <- ask
    case connections engine of
        EConnectionState (Nothing, y) -> clearConnections y
        EConnectionState (Just x, y) -> clearConnections x >> clearConnections y

    let EEndpointState ieps eeps = endpoints engine in do
        liftIO (withMVar ieps (return . H.keys)) >>= mapM_ closeInternal
        liftIO (withMVar eeps (return . H.keys)) >>= mapM_ close

{- Network endpoint execution -}

-- TODO: handle ReceivedMulticast events
-- TODO: log errors on ErrorEvent
internalListenerProcessor :: ListenerProcessor (InternalMessage a) a
internalListenerProcessor (engineSync -> _) buf _ = do
    engine  <- ask
    buffer' <- enqueueEBuffer (queueConfig engine) buf
    return buffer'

externalListenerProcessor :: ListenerProcessor a a
externalListenerProcessor (engineSync -> _) buf ep = do
    notifySubscribers SocketData (subscribers ep)
    return buf

runNEndpoint :: ListenerState a b -> Endpoint a b -> EngineM b ()
runNEndpoint ls ep@(Endpoint h@(networkSource -> Just(wd,llep)) _ subs) = do
    event <- liftIO $ NT.receive $ endpoint llep
    case event of
        NT.ConnectionOpened _ _ _  -> notifySubscribers SocketAccept subs >> runNEndpoint ls ep
        NT.ConnectionClosed _      -> runNEndpoint ls ep
        NT.Received _   payload    -> processMsg payload
        NT.ReceivedMulticast _ _   -> runNEndpoint ls ep
        NT.EndPointClosed          -> return ()
        NT.ErrorEvent err          -> endpointError $ show err
  where
    processMsg msg = bufferMsg msg >>= \case
        (Just b, []) -> (processor ls) ls b ep >>= \buf -> runNEndpoint ls (Endpoint h (Just buf) subs)
        (_, errors)  -> endpointError $ summarize errors

    bufferMsg msg = case buffer ep of
                      Just b  -> liftIO (foldM safeAppend (b, []) msg) >>= (\(x,y) -> return (Just x, y))
                      Nothing -> return (Nothing, [InvalidBuffer])
    
    unpackMsg = unpackWith wd . BS.unpack

    safeAppend (b, errors) payload = unpackMsg payload >>= \case
        Nothing -> return (b, errors ++ [SerializeError payload])
        Just mg -> case errors of
                    [] -> flip appendEBuffer b mg >>= \case
                            (nb, Nothing) -> return (nb, [])
                            (nb, Just m)  -> return (nb, [OverflowError m])
                    l  -> return (b, l ++ [PropagatedError mg])

    summarize l = "Endpoint message errors (runNEndpoint " ++ (name ls) ++ ", " ++ show (length l) ++ " messages)"
    endpointError s = close (name ls) >> (liftIO $ putStrLn s) -- TODO: close vs closeInternal

runNEndpoint ls _ = error $ "Invalid endpoint for network source " ++ (name ls)

{- Message passing -}

-- | Queue accessor

mDie = error "Could not find the queueId for given message"
qDie = error "Could not find the queue for given queueId"
wDie = error "Could not find the queueGroup for given worker"

enqueue :: QueueConfiguration a -> Address -> Identifier -> a -> EngineM a ()
enqueue qconfig addr n arg = do
  -- lookup the queue for the given (addr,n)
  mMap   <- liftIO $ readMVar (messageToQueueId qconfig)
  qId    <- return $ maybe mDie id (H.lookup (addr,n) mMap)
  qMapMv <- return $ queueIdToQueue qconfig
  -- enqueue the message
  liftIO  $ modifyMVar_  qMapMv (enqueueTo qId)
  -- notify the proper worker that a message has arrived
  tid    <-  liftIO $ lookupWorker qconfig qId
  maybe (return ()) notifyMessage tid 
  where 
    enqueueTo qid qs = return $ H.adjust (++ [(addr,n, arg)]) qid qs
    lookupWorker qconfig qid = do
      wm_mv   <- return (workerIdToQueueIds qconfig)
      -- enqueue is called once before workers are initialized.
      -- If worker map mvar is still empty, there is no
      -- need to wake any worker.
      is_init <- isEmptyMVar wm_mv
      if is_init 
      then return Nothing
      else withMVar (workerIdToQueueIds qconfig) (\h -> return $ getWorker qid (H.toList h))
    getWorker queueId [] = error "failed to find the worker responsible for the given queue"
    getWorker queueId ((k,v):kvs) = if queueId `elem` v  then Just k else (getWorker queueId kvs)

dequeue :: QueueConfiguration a -> EngineM a (Maybe (Address, Identifier, a))
dequeue qconfig = do
  -- find the list of queues this worker must manage
  wMap <- liftIO $ readMVar (workerIdToQueueIds qconfig)
  myId <- liftIO $ myThreadId
  myQs <- return $ maybe wDie id (H.lookup myId wMap)
  -- dequeue from one of those queues
  liftIO $ modifyMVar (queueIdToQueue qconfig) $ return . mkDequeue myQs
  where 
    dequeueFromMap group qMap = let
      myList = maybe qDie id (H.lookup group qMap)
      in case myList of 
        []     -> (qMap, Nothing)
        (x:xs) -> ((H.insert group xs qMap), Just x) 
    -- multi key dequeue: given a list of keys, lookup each sequentially
    mkDequeue []     qMap = (qMap, Nothing)
    mkDequeue (k:ks) qMap = 
      case dequeueFromMap k qMap of
        (q, Nothing) -> mkDequeue ks q
        (q, Just x)  -> (q, Just x)

-- | Message passing
send :: Address -> Identifier -> a -> EngineM a ()
send addr n arg = do
    engine <- ask

    shortCircuit <- (elem addr (nodes engine) ||) <$> simulation

    if shortCircuit
        then enqueue (queueConfig engine) addr n arg
        else trySend (connectionRetries $ config engine)
  where
    trySend 0 = send' (connectionId addr) $ error $ "Failed to connect to " ++ show addr
    trySend i = send' (connectionId addr) $ trySend $ i - 1

    send' eid retryF = do
        EEndpointState ieps _ <- endpoints <$> ask
        getEndpoint eid ieps >>= \case
            Just _  -> hasWriteInternal eid >>= write eid
            Nothing -> openSocketInternal eid addr "w" >> retryF

    write eid (Just True) = doWriteInternal eid (addr, n, arg)
    write _ _             = error $ "No write available to " ++ show addr

{- Module API implementation -}

builtin :: String -> Builtin
builtin "stdin"  = Stdin
builtin "stdout" = Stdout
builtin "stderr" = Stderr
builtin s        = error $ "Invalid builtin endpoint: " ++ s

builtinHandle :: Builtin -> SIO.Handle
builtinHandle Stdin  = SIO.stdin
builtinHandle Stdout = SIO.stdout
builtinHandle Stderr = SIO.stderr

ioMode :: String -> SIO.IOMode
ioMode "r"  = SIO.ReadMode
ioMode "w"  = SIO.WriteMode
ioMode "a"  = SIO.AppendMode
ioMode "rw" = SIO.ReadWriteMode
ioMode s    = error $ "Invalid IO mode: " ++ s


-- | Builtin endpoint constructor
genericOpenBuiltin :: Identifier -> Identifier -> WireDesc a -> EEndpoints a b -> EngineM b ()
genericOpenBuiltin eid (builtin -> b) wd eps = do
    let file = BuiltinH wd (builtinHandle b) b
    let buffer' = case b of
                   Stdin -> Nothing
                   _     -> Just $ Exclusive emptySingletonBuffer
    void $ addEndpoint eid (file, buffer', []) eps


-- | File endpoint constructor.
-- TODO: validation with the given type
genericOpenFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type)
                -> String -> EEndpoints a b
                -> EngineM b ()
genericOpenFile eid path wd _ mode eps = do
    file <- liftIO $ openFileHandle path wd (ioMode mode)
    let buffer' = Just $ Exclusive emptySingletonBuffer
    void $ addEndpoint eid (file, buffer', []) eps
    case ioMode mode of
        SIO.ReadMode -> void $ genericDoRead eid eps -- Prime the file's buffer.
        _ -> return ()

-- | Socket endpoint constructor.
--   This initializes the engine's connections as necessary.
-- TODO: validation with the given type
genericOpenSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type)
                  -> String -> ListenerState a b -> EEndpoints a b
                  -> EngineM b ()
genericOpenSocket eid addr wd _ (ioMode -> mode) lst endpoints' = do
    engine <- ask
    let cfm = case mode of
            SIO.WriteMode -> getConnections eid (connections engine)
            _ -> Nothing
    socket <- openSocketHandle addr wd mode cfm

    maybe (return ()) (registerEndpoint mode) socket

  where
    registerEndpoint SIO.ReadMode ioh = do
        engine <- ask
        let t = emptyBoundedBuffer $ defaultBufferSpec $ config engine
        buf <- liftIO (shared t >>= return . Just)
        e <- addEndpoint eid (ioh, buf, []) endpoints'
        wkThreadId <- forkEndpoint e
        registerNetworkListener (eid, wkThreadId)

    registerEndpoint _ ioh = do
        engine <- ask
        let t = emptyBoundedBuffer $ defaultBufferSpec $ config engine
        buf <- liftIO (shared t >>= return . Just)
        void $ addEndpoint eid (ioh, buf, []) endpoints'

    forkEndpoint e = do
        engine <- ask
        tid <- liftIO $ forkIO $ void $ runEngineM (runNEndpoint lst e) engine
        liftIO $ mkWeakThreadId tid

genericClose :: String -> EEndpoints a b -> EngineM b ()
genericClose n endpoints' = getEndpoint n endpoints' >>= \case
    Nothing -> return ()
    Just e  -> do
        closeHandle (handle e)
        deregister (networkSource $ handle e)
        removeEndpoint n endpoints'
        notifySubscribers (notifyType $ handle e) (subscribers e)
  where
    deregister = maybe (return ()) (const $ deregisterNetworkListener n)
    notifyType (BuiltinH _ _ _) = FileClose
    notifyType (FileH _ _)      = FileClose
    notifyType (SocketH _ _)    = SocketClose

genericHasRead :: Identifier -> EEndpoints a b -> EngineM b (Maybe Bool)
genericHasRead n endpoints' = getEndpoint n endpoints' >>= \case
    Nothing -> return Nothing
    Just e  -> do
      readable      <- liftIO (maybe (return True) SIO.hIsReadable $ getSystemHandle $ handle e)
      invalidBuffer <- liftIO (maybe (return False) emptyEBuffer $ buffer e)
      return . Just $ readable && (not invalidBuffer)

genericDoRead :: Identifier -> EEndpoints a b -> EngineM b (Maybe a)
genericDoRead n endpoints' = getEndpoint n endpoints' >>= maybe (return Nothing) refresh
  where
    refresh e = do
      let h = handle e
      readInfo <- maybe (readDirect h) (readFromBuffer h) (buffer e)
      updateAndYield e readInfo

    updateAndYield e (nBufOpt, (vOpt, notifyType)) = do
      void $ addEndpoint n (handle e, nBufOpt, subscribers e) endpoints'
      notify notifyType (subscribers e)
      return vOpt

    notify Nothing _      = return ()
    notify (Just nt) subs = notifySubscribers nt subs

    readDirect h@(BuiltinH _ _ _) = readFromHandle h
    readDirect h@(FileH _ _)      = readFromHandle h
    readDirect   (SocketH _ _)    = readError "Cannot read directly from a network socket."
    
    readFromHandle h   = liftIO (readHandle h >>= return . maybe emptyRead validRead)
    readFromBuffer h b = liftIO (refreshEBuffer h b >>= \(x,(y,z)) -> return (Just x,(y,z)))
    
    emptyRead   = (Nothing, (Nothing, Nothing))
    validRead v = (Nothing, (Just v, Just FileData))
    
    readError msg = do
      void $ genericClose n endpoints'
      void $ (liftIO $ putStrLn $ msg ++ " (doRead)")
      return emptyRead

genericHasWrite :: Identifier -> EEndpoints a b -> EngineM b (Maybe Bool)
genericHasWrite n endpoints' = getEndpoint n endpoints' >>= \case
    Nothing -> return Nothing
    Just e -> do
      writeable     <- liftIO (maybe (return True) SIO.hIsWritable $ getSystemHandle $ handle e)
      invalidBuffer <- liftIO (maybe (return False) fullEBuffer $ buffer e)
      return . Just $ writeable && not invalidBuffer

genericDoWrite :: Identifier -> a -> EEndpoints a b -> EngineM b ()
genericDoWrite n arg endpoints' = getEndpoint n endpoints' >>= maybe (return ()) write
  where
    write e = do
      let h    = handle e
      let subs = subscribers e
      void $ maybe (writeToHandle h subs) (writeToBuffer h subs) $ buffer e

    writeToHandle h subs = do
      void $ liftIO (writeHandle arg h) 
      notify (notification h) subs

    writeToBuffer h subs b = liftIO (appendEBuffer arg b) >>= \case
        (nb, Nothing) -> liftIO (flushEBuffer h nb) >>= update h subs
        (_, Just _)   -> overflowError

    update h subs (nBuf, notifyType) = do
      void $ addEndpoint n (h, Just nBuf, subs) endpoints'
      notify notifyType subs

    notify Nothing _ = return ()
    notify (Just nt) subs = notifySubscribers nt subs

    notification (networkSource -> Just _) = Nothing
    notification (networkSink -> Just _)   = Just SocketData
    notification _                         = Just FileData

    overflowError =
      genericClose n endpoints'
        >> (liftIO $ putStrLn "Endpoint buffer overflow (doWrite)")


{- External endpoint methods -}

-- | Open a builtin endpoint.
--   The wire description must yield a string value for each payload processed.
openBuiltin :: Identifier -> Identifier -> WireDesc a -> EngineM a ()
openBuiltin eid bid wd = ask >>= genericOpenBuiltin eid bid wd . externalEndpoints . endpoints

openFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> String -> EngineM a ()
openFile eid path wd tOpt mode = ask >>= genericOpenFile eid path wd tOpt mode . externalEndpoints . endpoints

openSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> String -> EngineM a ()
openSocket eid addr wd tOpt mode = do
    engine <- ask
    let eep = externalEndpoints $ endpoints engine
    let ctl = control engine
    let lst = ListenerState eid (networkDoneV ctl) externalListenerProcessor
    genericOpenSocket eid addr wd tOpt mode lst eep

close :: String -> EngineM a ()
close n = ask >>= genericClose n . externalEndpoints . endpoints

hasRead :: Identifier -> EngineM a (Maybe Bool)
hasRead n = ask >>= genericHasRead n . externalEndpoints . endpoints

hasWrite :: Identifier -> EngineM a (Maybe Bool)
hasWrite n = ask >>= genericHasWrite n . externalEndpoints . endpoints

doRead :: Identifier -> EngineM a (Maybe a)
doRead n = ask >>= genericDoRead n . externalEndpoints . endpoints

doWrite :: Identifier -> a -> EngineM a ()
doWrite n arg = ask >>= genericDoWrite n arg . externalEndpoints . endpoints


{- Internal endpoint methods -}

openBuiltinInternal :: Identifier -> Identifier -> EngineM a ()
openBuiltinInternal eid bid = do
    engine <- ask
    let ife = internalFormat engine
    let iep = internalEndpoints $ endpoints engine
    genericOpenBuiltin eid bid ife iep

openFileInternal :: Identifier -> String -> String -> EngineM a ()
openFileInternal eid path mode = do
    engine <- ask
    let ife = internalFormat engine
    let iep = internalEndpoints $ endpoints engine
    genericOpenFile eid path ife Nothing mode iep

openSocketInternal :: Identifier -> Address -> String -> EngineM a ()
openSocketInternal eid addr mode = do
    engine <- ask
    let ife = internalFormat engine
    let iep = internalEndpoints $ endpoints engine
    let ctl = control engine
    let lst = ListenerState eid (networkDoneV ctl) internalListenerProcessor
    genericOpenSocket eid addr ife Nothing mode lst iep

closeInternal :: String -> EngineM a ()
closeInternal n = ask >>= genericClose n . internalEndpoints . endpoints

hasReadInternal :: Identifier -> EngineM a (Maybe Bool)
hasReadInternal n = ask >>= genericHasRead n . internalEndpoints . endpoints

hasWriteInternal :: Identifier -> EngineM a (Maybe Bool)
hasWriteInternal n = ask >>= genericHasWrite n . internalEndpoints . endpoints

doReadInternal :: Identifier -> EngineM a (Maybe (InternalMessage a))
doReadInternal n = ask >>= genericDoRead n . internalEndpoints . endpoints

doWriteInternal :: Identifier -> InternalMessage a -> EngineM a ()
doWriteInternal n arg = ask >>= genericDoWrite n arg . internalEndpoints . endpoints


{- IO Handle methods -}

getSystemHandle :: IOHandle a -> Maybe SIO.Handle
getSystemHandle h = case h of
  BuiltinH _ h' _  -> Just h'
  FileH    _ h'    -> Just h'
  SocketH  _ _    -> Nothing

networkSource :: IOHandle a -> Maybe (WireDesc a, NEndpoint)
networkSource (SocketH wd (Left ep)) = Just (wd, ep)
networkSource _ = Nothing

networkSink :: IOHandle a -> Maybe (WireDesc a, NConnection)
networkSink (SocketH wd (Right conn')) = Just (wd, conn')
networkSink _ = Nothing

-- | Open an external file, with given wire description and file path.
openFileHandle :: FilePath -> WireDesc a -> SIO.IOMode -> IO (IOHandle a)
openFileHandle p wd mode = SIO.openFile p mode >>= return . FileH wd

-- | Open an external socket, with given wire description and address.
openSocketHandle :: Address -> WireDesc a -> SIO.IOMode -> Maybe (MVar EConnectionMap)
    -> EngineM b (Maybe (IOHandle a))
openSocketHandle addr wd mode conns =
  case mode of
    SIO.ReadMode      -> incoming
    SIO.WriteMode     -> outgoing
    SIO.AppendMode    -> error "Unsupported network handle mode"
    SIO.ReadWriteMode -> error "Unsupported network handle mode"

  where incoming = newEndpoint addr >>= return . (>>= return . SocketH wd . Left)
        outgoing = case conns of
          Just c  -> getEstablishedConnection addr c >>= return . (>>= return . SocketH wd . Right)
          Nothing -> error "Invalid outgoing network connection map"

-- | Close an external.
closeHandle :: IOHandle a -> EngineM b ()
closeHandle (BuiltinH _ _ _) = return () -- Leave open to allow other standard IO.
closeHandle (FileH _ h)      = liftIO $ SIO.hClose h
closeHandle (networkSource -> Just (_,ep)) = closeEndpoint ep
closeHandle (networkSink   -> Just (_,_))  = return ()
  -- TODO: above, reference count aggregated outgoing connections for garbage collection

closeHandle _ = error "Invalid IOHandle argument for closeHandle"

-- | Read a single payload from an external.
readHandle :: IOHandle a -> IO (Maybe a)
readHandle = \case
  BuiltinH wd h Stdin -> readIOH wd h
  FileH wd h          -> readIOH wd h
  BuiltinH _ _ b      -> error $ "Unsupported: read from " ++ show b
  SocketH _ _         -> error "Unsupported: read from Socket"
  where True ? x  = const x  
        False ? _ = id
        readIOH wd h = do
          done <- SIO.hIsEOF h
          done ? return Nothing $ do
            s <- SIO.hGetLine h
            unpackWith wd s
            -- TODO: Add proper delimiter support, to avoid delimiting by newline.

-- | Write a single payload to a handle
writeHandle :: a -> IOHandle a -> IO ()
writeHandle payload = \case
  BuiltinH wd h Stdout -> writeIOH wd h
  BuiltinH wd h Stderr -> writeIOH wd h
  FileH wd h           -> writeIOH wd h
  
  SocketH wd (Right (conn -> c)) ->
    packWith wd payload >>= NT.send c . (:[]) . BS.pack >>= return . either (\_ -> ()) id
  
  BuiltinH _ _ b -> error $ "Unsupported: write to " ++ show b 
  SocketH _ _    -> error "Unsupported write operation on network handle"

  where True ? x  = const x  
        False ? _ = id
        writeIOH wd h = do
          done <- SIO.hIsClosed h
          done ? return () $ packWith wd payload >>= SIO.hPutStrLn h
            -- TODO: delimiter, as with readHandle


{- Endpoint accessors -}

newEndpoint :: Address -> EngineM a (Maybe NEndpoint)
newEndpoint (Address (host, port)) = liftIO . withSocketsDo $ do
  t <- eitherAsMaybe $ NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters
  e <- maybe (return Nothing) (eitherAsMaybe . NT.newEndPoint) t
  return $ t >>= \tr -> e >>= return . flip NEndpoint tr
  where eitherAsMaybe m = m >>= return . either (\_ -> Nothing) Just

closeEndpoint :: NEndpoint -> EngineM a ()
closeEndpoint ep = liftIO $ NT.closeEndPoint (endpoint ep) >> NT.closeTransport (epTransport ep)

emptyEndpoints :: IO (EEndpoints a b)
emptyEndpoints = newMVar (H.fromList [])

addEndpoint :: Identifier -> (IOHandle a, Maybe (EndpointBuffer a), EndpointBindings b) -> EEndpoints a b
    -> EngineM b (Endpoint a b)
addEndpoint n (h,b,s) eps = liftIO $ modifyMVar eps (rebuild Endpoint {handle=h, buffer=b, subscribers=s})
  where rebuild e m = return (H.insert n e m, e)

removeEndpoint :: Identifier -> EEndpoints a b -> EngineM b ()
removeEndpoint n eps = liftIO $ modifyMVar_ eps $ return . H.delete n

getEndpoint :: Identifier -> EEndpoints a b -> EngineM b (Maybe (Endpoint a b))
getEndpoint n eps = liftIO $ withMVar eps (return . H.lookup n)


{- Connection and connection map accessors -}

llAddress :: Address -> NT.EndPointAddress
llAddress addr = NT.EndPointAddress $ BS.pack $ show addr ++ ":0"
    -- TODO: above, check if it is safe to always use ":0" for NT.EndPointAddress

newConnection :: Address -> NEndpoint -> EngineM a (Maybe NConnection)
newConnection addr (endpoint -> ep) = liftIO $
  NT.connect ep (llAddress addr) NT.ReliableOrdered NT.defaultConnectHints
    >>= either connectionError connectionSuccess
  where connectionSuccess = return . Just . flip NConnection addr
        connectionError _ = return Nothing

emptyConnectionMap :: Address -> IO (MVar EConnectionMap)
emptyConnectionMap addr = liftIO $ newMVar $ EConnectionMap { anchor = (addr, Nothing), cache = [] }

getConnections :: Identifier -> EConnectionState -> Maybe (MVar EConnectionMap)
getConnections n (EConnectionState (icp, ecp)) =
  if externalEndpointId n then Just ecp else maybe err Just icp
  where err = error $ "Invalid internal connection to " ++ n ++ " in a simulation"

withConnectionMap :: MVar EConnectionMap -> (EConnectionMap -> IO a) -> IO a
withConnectionMap connsMV withF = withMVar connsMV withF

modifyConnectionMap :: MVar EConnectionMap -> (EConnectionMap -> IO (EConnectionMap, a)) -> EngineM b a
modifyConnectionMap connsMV modifyF = liftIO $ modifyMVar connsMV modifyF

-- TODO: Verify exception-safety of this translation to EngineM.
addConnection :: Address -> MVar EConnectionMap -> EngineM a (Maybe NConnection)
addConnection addr conns = do
    c <- liftIO $ readMVar conns
    case c of
        (anchor -> (anchorAddr, Nothing)) -> newEndpoint anchorAddr >>= \case
            Nothing -> return Nothing
            Just ep -> connect $ EConnectionMap (anchorAddr, Just ep) (cache c)
        _ -> connect c
  where
    connect :: EConnectionMap -> EngineM a (Maybe NConnection)
    connect c@(anchor -> (_, Just ep)) = do
        nc <- newConnection addr ep
        case nc of
            Nothing -> return Nothing
            Just c' -> liftIO $ swapMVar conns (add c c') >> return (Just c')

    connect _ = throwEngineError $ EngineError "Invalid connection map, no anchor endpoint found."

    add cs c = EConnectionMap (anchor cs) $ (addr, c):(cache cs)


removeConnection :: Address -> MVar EConnectionMap -> EngineM a ()
removeConnection addr conns = modifyConnectionMap conns (liftIO . remove)
  where remove cm@(anchor -> (_, Nothing)) = return (cm, ())
        remove cm = closeConnection (cache cm) >>= return . (,()) . EConnectionMap (anchor cm)
        closeConnection (matchConnections -> (matches, rest)) =
          mapM_ (NT.close . conn . snd) matches >> return rest
        matchConnections = partition ((addr ==) . fst)

getConnection :: Address -> MVar EConnectionMap -> EngineM a (Maybe NConnection)
getConnection addr conns = liftIO $ withConnectionMap conns $ return . lookup addr . cache

getEstablishedConnection :: Address -> MVar EConnectionMap -> EngineM a (Maybe NConnection)
getEstablishedConnection addr conns = getConnection addr conns >>= \case
    Nothing -> addConnection addr conns
    Just c  -> return $ Just c

clearConnections :: MVar EConnectionMap -> EngineM a ()
clearConnections cm = liftIO (withConnectionMap cm (\x -> return (snd $ anchor x, cache x))) >>= clear
  where clear (ep, conns) = clearC conns >> clearE ep
        clearC conns      = mapM_ (flip removeConnection cm . fst) conns
        clearE Nothing    = return ()
        clearE (Just ep)  = closeEndpoint ep


{- Endpoint Notifiers -}

getNotificationType :: Identifier -> Endpoint a b -> EndpointNotification
getNotificationType n (handle -> BuiltinH _ _ _) = case n of
  "data"  -> FileData
  "close" -> FileClose
  _ -> error $ "Invalid notification type " ++ n

getNotificationType n (handle -> FileH _ _) = case n of
  "data"  -> FileData
  "close" -> FileClose
  _ -> error $ "Invalid notification type " ++ n

getNotificationType n (handle -> SocketH _ _) = case n of
  "accept" -> SocketAccept
  "data"   -> SocketData
  "close"  -> SocketClose
  _ -> error $ "Invalid notification type " ++ n

getNotificationType _ _ = error "Invalid endpoint handle for notifications"

notifySubscribers :: EndpointNotification -> EndpointBindings a -> EngineM a ()
notifySubscribers nt subs = mapM_ (notify . snd) $ filter ((nt == ) . fst) subs
  where notify (addr, tid, msg) = send addr tid msg

modifySubscribers :: Identifier -> (Endpoint a b -> EndpointBindings b) -> EEndpoints a b -> EngineM b Bool
modifySubscribers eid f eps = getEndpoint eid eps >>= maybe (return False) updateSub
  where updateSub e = (addEndpoint eid (nep e $ f e) eps) >> return True
        nep e subs = (handle e, buffer e, subs)

attachNotifier :: Identifier -> Identifier -> InternalMessage a -> EngineM a Bool
attachNotifier eid nt msg = endpoints <$> ask >>= \(EEndpointState _ eeps) -> modifySubscribers eid newSubs eeps
  where newSubs e = nubBy (\(a,_) (b,_) -> a == b) $ (getNotificationType nt e, msg):(subscribers e)

attachNotifier_ :: Identifier -> Identifier -> InternalMessage a -> EngineM a ()
attachNotifier_ eid nt msg = void $ attachNotifier eid nt msg

detachNotifier :: Identifier -> Identifier -> EngineM a Bool
detachNotifier eid nt = endpoints <$> ask >>= \(EEndpointState _ eeps) -> modifySubscribers eid newSubs eeps
  where newSubs e = filter (((getNotificationType nt e) /= ) . fst) $ subscribers e

detachNotifier_ :: Identifier -> Identifier -> EngineM a ()
detachNotifier_ eid nt = void $ detachNotifier eid nt


{- Endpoint buffers -}

emptySingletonBuffer :: BufferContents a
emptySingletonBuffer = Single Nothing

singletonBuffer :: a -> BufferContents a
singletonBuffer contents = Single $ Just contents

emptyBoundedBuffer :: BufferSpec -> BufferContents a
emptyBoundedBuffer spec = Multiple [] spec

boundedBuffer :: BufferSpec -> [a] -> Maybe (BufferContents a)
boundedBuffer spec contents
  | length contents <= maxSize spec = Just $ Multiple contents spec
  | otherwise                       = Nothing

emptyUnboundedBuffer :: BufferContents a
emptyUnboundedBuffer = Multiple [] unboundedSpec
  where unboundedSpec = BufferSpec maxBound (batchSize $ defaultBufferSpec $ defaultConfig)

unboundedBuffer :: [a] -> Maybe (BufferContents a)
unboundedBuffer = boundedBuffer unboundedSpec
  where unboundedSpec = BufferSpec maxBound (batchSize $ defaultBufferSpec $ defaultConfig)

exclusive :: BufferContents a -> IO (EndpointBuffer a)
exclusive c = return $ Exclusive c

shared :: BufferContents a -> IO (EndpointBuffer a)
shared c = newMVar c >>= return . Shared

wrapEBuffer :: (BufferContents b -> a) -> EndpointBuffer b -> IO a
wrapEBuffer f = \case
  Exclusive c -> return $ f c
  Shared mvc -> readMVar mvc >>= return . f

modifyEBuffer :: (BufferContents b -> IO (BufferContents b, a)) -> EndpointBuffer b -> IO (EndpointBuffer b, a)
modifyEBuffer f = \case
  Exclusive c -> f c >>= (\(a,b) -> return (Exclusive a, b))
  Shared mvc -> modifyMVar mvc (\c -> f c) >>= return . (Shared mvc,)

modifyEBuffer_ :: (BufferContents a -> IO (BufferContents a)) -> EndpointBuffer a -> IO (EndpointBuffer a)
modifyEBuffer_ f b = modifyEBuffer (\c -> f c >>= return . (,())) b >>= return . fst

emptyEBContents :: BufferContents a -> Bool
emptyEBContents (Single x)     = maybe True (\_ -> False) x
emptyEBContents (Multiple x _) = null x

emptyEBuffer :: EndpointBuffer a -> IO Bool
emptyEBuffer = wrapEBuffer emptyEBContents

fullEBContents :: BufferContents a -> Bool
fullEBContents (Single x)        = maybe False (\_ -> True) x
fullEBContents (Multiple x spec) = length x == maxSize spec

fullEBuffer :: EndpointBuffer a -> IO Bool
fullEBuffer = wrapEBuffer fullEBContents

sizeEBContents :: BufferContents a -> Int
sizeEBContents (Single x)     = maybe 0 (\_ -> 1) x
sizeEBContents (Multiple l _) = length l

sizeEBuffer :: EndpointBuffer a -> IO Int
sizeEBuffer = wrapEBuffer sizeEBContents

capacityEBContents :: BufferContents a -> Int
capacityEBContents (Single _)        = 1
capacityEBContents (Multiple _ spec) = maxSize spec

capacityEBuffer :: EndpointBuffer a -> IO Int
capacityEBuffer = wrapEBuffer capacityEBContents

readEBContents :: BufferContents a -> Maybe a
readEBContents (Single x) = x
readEBContents (Multiple x _) = if null x then Nothing else Just $ head x

readEBuffer :: EndpointBuffer a -> IO (Maybe a)
readEBuffer = wrapEBuffer readEBContents

appendEBContents :: v -> BufferContents v -> (BufferContents v, Maybe v)
appendEBContents v c | fullEBContents c = (c, Just v)
                     | otherwise        = append c
  where append (Single Nothing)  = (Single $ Just v, Nothing)
        append (Multiple x spec) = (flip Multiple spec $ x++[v], Nothing)
        append _ = error "Internal append error"

appendEBuffer :: v -> EndpointBuffer v -> IO (EndpointBuffer v, Maybe v)
appendEBuffer v buf = modifyEBuffer (return . appendEBContents v) buf

takeEBContents :: BufferContents v -> (BufferContents v, Maybe v)
takeEBContents = \case
  Single x         -> (Single Nothing, x)
  Multiple [] s    -> (Multiple [] s, Nothing)
  Multiple (h:t) s -> (Multiple t s, Just h)

takeEBuffer :: EndpointBuffer v -> IO (EndpointBuffer v, Maybe v)
takeEBuffer = modifyEBuffer $ return . takeEBContents

flushEBContents :: IOHandle v -> BufferContents v
                -> IO (BufferContents v, Maybe EndpointNotification)
flushEBContents (networkSource -> Just _) _ = error "Invalid buffer flush for network source"
flushEBContents h c =  case batch c of
                        ([], nc) -> return (nc, Nothing)
                        (x, nc)  -> mapM_ (flip writeHandle h) x >> return (nc, notification h x)

  where batch c'@(Single Nothing)  = ([], c')
        batch (Single (Just x)) = ([x], Single Nothing)
        batch (Multiple x s) = let (a,b) = splitAt (batchSize s) x in (a, Multiple b s)

        notification _ []                      = Nothing
        notification (networkSink -> Just _) _ = Just SocketData
        notification _ _                       = Just FileData

flushEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, Maybe EndpointNotification)
flushEBuffer h = modifyEBuffer $ flushEBContents h

refreshEBContents :: IOHandle v -> BufferContents v
                  -> IO (BufferContents v, (Maybe v, Maybe EndpointNotification))

-- For now, ignore the buffer for reading from builtins.
refreshEBContents f@(BuiltinH _ _ _) c =
  readHandle f >>= (\vOpt -> return (c,(vOpt, maybe Nothing (const $ Just FileData) vOpt)))

refreshEBContents f@(FileH _ _) c = refill $ takeEBContents c
  where refill (c', vOpt) | refillPolicy c' = readHandle f >>= return . rebuild c' >>= (\(x,y) -> return (x, (vOpt, y)))
                          | otherwise       = return (c, (vOpt, Nothing))

        rebuild (Single _)     = maybe (Single Nothing, Nothing) (\c' -> (Single $ Just c', Just FileData))
        rebuild (Multiple x s) = maybe (Multiple x s, Nothing)   (\y -> (Multiple (x++[y]) s, Just FileData))

        refillPolicy = emptyEBContents

refreshEBContents (SocketH _ (Left _)) c  = let (x,y) = takeEBContents c in return (x, (y, Nothing))
refreshEBContents (SocketH _ (Right _)) _ = error "Invalid buffer refresh for network sink"

refreshEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, (Maybe v, Maybe EndpointNotification))
refreshEBuffer h = modifyEBuffer $ refreshEBContents h

enqueueEBContents :: QueueConfiguration a -> BufferContents (InternalMessage a)
                  -> EngineM a (BufferContents (InternalMessage a))
enqueueEBContents qconfig = \case
  Single Nothing             -> return emptySingletonBuffer
  Single (Just (addr, n, v)) -> enqueue qconfig addr n v >> return emptySingletonBuffer
  Multiple x spec            -> mapM_ (\(addr, n, v) -> enqueue qconfig addr n v) x >> return (emptyBoundedBuffer spec)

enqueueEBuffer :: QueueConfiguration a -> EndpointBuffer (InternalMessage a)
               -> EngineM a (EndpointBuffer (InternalMessage a))
enqueueEBuffer qconfig = modifyEBuffer'_ $ enqueueEBContents qconfig

modifyEBuffer' :: (BufferContents b -> EngineM c (BufferContents b, a)) -> EndpointBuffer b
               -> EngineM c (EndpointBuffer b, a)
modifyEBuffer' f (Exclusive c) = f c >>= \(a, b) -> return (Exclusive a, b)
modifyEBuffer' f (Shared mvc) = modifyMVE mvc f >>= return . (Shared mvc,)

modifyEBuffer'_ :: (BufferContents b -> EngineM c (BufferContents b)) -> EndpointBuffer b
                -> EngineM c (EndpointBuffer b)
modifyEBuffer'_ f b = modifyEBuffer' (\c -> f c >>= return . (,())) b >>= return . fst


{- Pretty printing helpers -}

showMessageQueues :: Show a => QueueConfiguration a -> EngineM a String
showMessageQueues qconfig = liftIO (readMVar (queueIdToQueue qconfig))  >>= return . show

showEngine :: Show a => EngineM a String
showEngine = return . unlines =<< ((++) <$> (ask >>= return . (:[]) . show) <*> prettyQueues)

prettyQueues :: Show a => EngineM a [String]
prettyQueues = ask >>= showMessageQueues . queueConfig >>= return . (["Queues: "]++) . (:[])


{- Instance definitions -}

-- TODO: put workers, endpoints
instance (Show a) => Show (Engine a) where
    show engine = case engine of
        (Engine { connections = (EConnectionState (Nothing, _)) }) ->
            "Engine (simulation):\n" ++ ("Nodes:\n" ++ show n ++ "\n")
        _ -> "Engine (network):\n" ++ ("Nodes: \n" ++ show n ++ "\n")
      where
        n = map fst $ deployment engine

instance (Show a) => Pretty (Engine a) where
    prettyLines = lines . show


{- Misc. helpers -}

modifyMVE :: MVar a -> (a -> EngineM b (a, c)) -> EngineM b c
modifyMVE v f = do
    engine <- ask
    result <- liftIO $ modifyMVar v $ \c -> runEngineM (f c) engine >>= return . \case
        Left e -> (c, Left e)
        Right (r, x) -> (r, Right (r, x))
    case result of
        Left e -> left e
        Right (_, x) -> return x
