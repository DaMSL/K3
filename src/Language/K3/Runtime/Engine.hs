{-# LANGUAGE CPP #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Runtime.Engine (
    FrameDesc(..)
  , WireDesc(..)
  , PeerBootstrap
  , SystemEnvironment
  , MessageProcessor(..)

  , Engine(..)

  , EngineT
  , EngineM
  , runEngineT
  , runEngineM
  , EngineError

  , EngineConfiguration(..)
  , EEndpoints

  , defaultAddress
  , defaultConfig
  , defaultSystem

  , simpleQueues
  , perPeerQueues
  , perTriggerQueues

  , simulationEngine
  , networkEngine

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
  , EngineControl(..)
  , LoopStatus(..)

  , MessageQueues(..)
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

import Control.Arrow hiding (left)
import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.MSampleVar
import qualified Control.Concurrent.MSem as MSem
import Control.Concurrent.MSem (MSem)
import Control.Exception (throwIO)
import Control.Monad
import Control.Monad.Reader
import Control.Monad.IO.Class
import Control.Monad.Trans.Either

import qualified Data.ByteString.Char8 as BS
import Data.Function
import Data.Functor
import qualified Data.HashMap.Lazy as H
import Data.List
import Data.List.Split (wordsBy)

import Debug.Trace

import qualified System.IO as SIO
import System.Process
import System.Mem.Weak (Weak)

import Text.Read hiding (lift)

import Network.Socket (withSocketsDo)
import qualified Network.Transport     as NT
import qualified Network.Transport.TCP as NTTCP

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Expression as EC
import qualified Language.K3.Core.Constructor.Type       as TC

import Language.K3.Pretty
import Language.K3.Utils.Logger

$(loggingFunctions)
$(customLoggingFunctions ["EngineSteps"])


data Engine a = Engine { config          :: EngineConfiguration
                       , internalFormat  :: WireDesc (InternalMessage a)
                       , control         :: EngineControl
                       , deployment      :: SystemEnvironment
                       , queues          :: MessageQueues a
                       , workers         :: Workers
                       , listeners       :: Listeners
                       , endpoints       :: EEndpointState a
                       , connections     :: EConnectionState }

data EngineError = EngineError

type EngineT e r m = EitherT e (ReaderT r m)

type EngineM a = EngineT EngineError (Engine a) IO

runEngineT :: EngineT e r m a -> r -> m (Either e a)
runEngineT s r = flip runReaderT r $ runEitherT s

runEngineM :: EngineM a b -> Engine a -> IO (Either EngineError b)
runEngineM = runEngineT

{- Configuration parameters -}

data EngineConfiguration = EngineConfiguration { address           :: Address
                                               , defaultBufferSpec :: BufferSpec
                                               , connectionRetries :: Int
                                               , waitForNetwork    :: Bool }

-- | A bootstrap environment for a peer.
type PeerBootstrap = [(Identifier, K3 Expression)]

-- | A system environment, to bootstrap a set of deployed peers.
type SystemEnvironment = [(Address, PeerBootstrap)]


{- Message processing -}

data EngineControl = EngineControl {
    -- | A concurrent boolean used internally by workers to determine if
    -- they should terminate.
    -- No blocking required, only safe access to a termination boolean
    -- for both workers (readers) and a console (i.e., a single writer)
    terminateV :: MVar Bool,

    -- | Blocking required for m workers (i.e., for empty queues during
    -- message processing), with signalling from n producers.
    -- We could use a semaphore, but a MSampleVar should be more lightweight.
    -- That is, we need not track how many messages are available, rather
    -- we need a single wake-up when any producer makes a message available.
    -- TODO: this functionality should be merged with MessageQueues.
    messageReadyV :: MSampleVar (),

    -- | A concurrent counter that will reach 0 once all network listeners have finished.
    -- This is incremented and decremented as listeners are added to the engine, or as
    -- they are removed or complete.
    -- No blocking required, only safe access as part of determining whether to
    -- terminate the engine.
    networkDoneV :: MVar Int,

    -- | Mutex for external threads to wait on this engine to complete.
    -- External threads should use readMVar on this variable, and this will
    -- be signalled by the last worker thread to finish.
    waitV :: MVar ()
}

data LoopStatus res err = Result res | Error err | MessagesDone res

-- | Each backend provides must provide a message processor which can handle the initialization of
-- the message queues and the dispatch of individual messages to the corresponding triggers.
data MessageProcessor inits prog msg res err = MessageProcessor {
    -- | Initialization of the execution environment.
    initialize :: inits -> prog -> EngineM msg res,

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

data MessageQueues a
  = Peer          (MVar (Address, [(Identifier, a)]))
  | ManyByPeer    (MVar (H.HashMap Address [(Identifier, a)]))
  | ManyByTrigger (MVar (H.HashMap (Address, Identifier) [a]))

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

-- | Internal messaging between triggers includes a sender address and
--   destination trigger identifier with each message.
type InternalMessage a = (Address, Identifier, a)

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

{- Endpoints and internal IO handles -}
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

data Endpoint a b = Endpoint { handle      :: IOHandle a
                             , buffer      :: EndpointBuffer a
                             , subscribers :: EndpointBindings b }

type EEndpoints a b = MVar (H.HashMap Identifier (Endpoint a b))

data EEndpointState a = EEndpointState { internalEndpoints :: EEndpoints (InternalMessage a) a
                                       , externalEndpoints :: EEndpoints a a }

{- Builtin notifications -}
data EndpointNotification
    = FileData
    | FileClose
    | SocketAccept
    | SocketData
    | SocketClose
  deriving (Eq, Show, Read)

{- Listeners -}
type ListenerProcessor a b =
  ListenerState a b -> EndpointBuffer a -> Endpoint a b -> EngineM b (EndpointBuffer a)

data ListenerState a b = ListenerState { name       :: Identifier
                                       , engineSync :: (MSampleVar (), MVar Int)
                                       , processor  :: ListenerProcessor a b }

data ListenerError a
    = SerializeError  BS.ByteString
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

defaultAddress :: Address
defaultAddress = Address ("127.0.0.1", 40000)

defaultConfig :: EngineConfiguration
defaultConfig = EngineConfiguration { address           = defaultAddress
                                    , defaultBufferSpec = bufferSpec
                                    , connectionRetries = 5
                                    , waitForNetwork    = False }
  where
    bufferSpec = BufferSpec { maxSize = 100, batchSize = 10 }

defaultSystem :: SystemEnvironment
defaultSystem = [(defaultAddress, [ ("me", defaultAddressExpr)
                                  , ("peers", defaultPeersExpr)
                                  , ("role", defaultRoleExpr)])]
  where defaultAddressExpr = EC.address (EC.constant $ CString "127.0.0.1") (EC.constant $ CInt 40000)
        defaultPeersExpr = EC.empty TC.address
        defaultRoleExpr = EC.constant $ CString ""

{- Configurations -}
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

{- Queue constructors -}
simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

perPeerQueues :: [Address] -> IO (MessageQueues a)
perPeerQueues peers = newMVar (H.fromList $ map (,[]) peers) >>= return . ManyByPeer

perTriggerQueues :: [Address] -> [Identifier] -> IO (MessageQueues a)
perTriggerQueues peers triggerIds =
  newMVar (H.fromList $ map (, []) $ cartesian peers triggerIds) >>= return . ManyByTrigger
  where cartesian l r = concatMap (flip zip r . repeat) l

{- Engine constructors -}

-- | Simulation engine constructor.
--   This is initialized with an empty internal connections map
--   to ensure it cannot send internal messages.
simulationEngine :: SystemEnvironment -> WireDesc a -> IO (Engine a)
simulationEngine systemEnv (internalizeWD -> internalWD) = do
  config          <- return $ configureWithAddress $ head $ deployedNodes systemEnv
  ctrl            <- EngineControl <$> newMVar False <*> newEmptySV <*> newMVar 0 <*> newEmptyMVar
  workers         <- newEmptyMVar >>= return . Uniprocess
  listeners       <- newMVar []
  q               <- case deployedNodes systemEnv of
                      [addr] -> simpleQueues addr
                      _      -> perPeerQueues $ deployedNodes systemEnv
  endpoints       <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  externalConns   <- emptyConnectionMap . externalSendAddress . address $ config
  connState       <- return $ EConnectionState (Nothing, externalConns)
  return $ Engine config internalWD ctrl systemEnv q workers listeners endpoints connState

-- | Network engine constructor.
--   This is initialized with listening endpoints for each given peer as well
--   as internal and external connection anchor endpoints for messaging.
networkEngine :: SystemEnvironment -> WireDesc a -> IO (Engine a)
networkEngine systemEnv (internalizeWD -> internalWD) = do
  config        <- return $ configureWithAddress $ head peers
  ctrl          <- EngineControl <$> newMVar False <*> newEmptySV <*> newMVar 0 <*> newEmptyMVar
  workers       <- newEmptyMVar >>= return . Uniprocess
  listnrs       <- newMVar []
  q             <- perPeerQueues peers
  endpoints     <- EEndpointState <$> emptyEndpoints <*> emptyEndpoints
  internalConns <- emptyConns internalSendAddress config >>= return . Just
  externalConns <- emptyConns externalSendAddress config
  connState     <- return $ EConnectionState (internalConns, externalConns)
  engine        <- return $ Engine config internalWD ctrl systemEnv q workers listnrs endpoints connState

  -- TODO: Verify correctness.
  void $ runEngineM startNetwork engine
  return engine

  where
    peers                      = deployedNodes systemEnv
    emptyConns addrF config    = emptyConnectionMap . addrF . address $ config
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

numQueuedMessages :: MessageQueues b -> EngineM a Int
numQueuedMessages = liftIO . \case
    Peer qmv           -> withMVar qmv $ return . count
    ManyByPeer qsmv    -> withMVar qsmv $ return . sumListSizes
    ManyByTrigger qsmv -> withMVar qsmv $ return . sumListSizes
  where
    count        = length . snd
    sumListSizes = H.foldl' (\acc msgs -> acc + length msgs) 0

numEndpoints :: EEndpointState b -> EngineM a Int
numEndpoints es = (+) <$> (count $ externalEndpoints es) <*> (count $ internalEndpoints es)
  where count ep = liftIO $ withMVar ep (return . H.size)

statistics :: EngineM a (Int, Int)
statistics = do
    engine <- ask
    nqm <- numQueuedMessages $ queues engine
    nep <- numEndpoints $ endpoints engine
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

terminate :: EngineM a Bool
terminate = do
    engine <- ask
    terminate <- liftIO $ readMVar (terminateV $ control engine)
    done <- networkDone
    return $ done || not (waitForNetwork $ config engine) && terminate

networkDone :: EngineM a Bool
networkDone = do
    engine <- ask
    done <- liftIO . readMVar . networkDoneV $ control engine
    return $ done == 0

waitForMessage :: EngineM a ()
waitForMessage = do
    engine <- ask
    liftIO $ readSV (messageReadyV $ control engine)

-- | Process a single message within the engine, given the message processor to use, and the
-- previous message result. Return the status of the loop.
processMessage :: MessageProcessor i p a r e -> r -> EngineM a (LoopStatus r e)
processMessage mp pr = do
    engine <- ask
    message <- dequeue $ queues engine
    maybe terminate' process' message
  where
    terminate' = return $ MessagesDone pr
    process' m = do
        nextResult <- process mp m pr
        return $ either Error Result (status mp nextResult)

runMessages :: (Pretty r, Pretty e, Show a) =>
    MessageProcessor i p a r e -> EngineM a (LoopStatus r e) -> EngineM a ()
runMessages mp status = ask >>= \engine -> status >>= \case
    Result r       -> debugStep >> runMessages mp (processMessage mp r)
    Error e        -> die "Error:" e (control engine)
    MessagesDone r -> terminate >>= \case
        True -> do
            fr <- finalize mp r
            die "Terminated:" fr (control engine)
        _ -> waitForMessage >> runMessages mp (processMessage mp r)
  where
    debugStep = do
      q <- prettyQueues
      logStep $ boxToString $ ["", "EVENT LOOP {"] ++ indent 2 q ++ ["}"]

    die msg r cntrl = do
        logStep $ boxToString $ ["", msg] ++ prettyLines r
        cleanupEngine
        liftIO $ tryPutMVar (waitV cntrl) ()
        logStep $ "Finished."

    logStep s = void $ _notice_EngineSteps $ s

runEngine :: (Pretty r, Pretty e, Show a) => MessageProcessor i p a r e -> i -> p -> EngineM a ()
runEngine mp is p = do
    engine <- ask
    result <- initialize mp is p
    liftIO $ case workers engine of
        Uniprocess workerMV -> tryTakeMVar workerMV >> myThreadId >>= putMVar workerMV
        Multithreaded _ -> error "Unsupported engine mode: Multithreaded"
        _ -> error "Unsupported engine mode: Multiprocess"

    runMessages mp (return . either Error Result $ status mp result)

forkEngine :: (Pretty r, Pretty e, Show a) => MessageProcessor i p a r e -> i -> p -> EngineM a ThreadId
forkEngine mp is p = ask >>= \engine -> liftIO . forkIO $ void $ runEngineM (runEngine mp is p) engine

waitForEngine :: EngineM a ()
waitForEngine = ask >>= liftIO . readMVar . waitV . control

terminateEngine :: EngineM a ()
terminateEngine = do
    engine <- ask
    liftIO $ modifyMVar_ (terminateV $ control engine) (const $ return True)
    liftIO $ writeSV (messageReadyV $ control engine) ()

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
internalListenerProcessor (engineSync -> (msgAvail, _)) buf _ = do
    engine <- ask
    buffer <- enqueueEBuffer (queues engine) buf
    liftIO $ writeSV msgAvail ()
    return buffer

externalListenerProcessor :: ListenerProcessor a a
externalListenerProcessor (engineSync -> (msgAvail, _)) buf ep = do
    notifySubscribers SocketData (subscribers ep)
    liftIO $ writeSV msgAvail ()
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
    processMsg msg = liftIO (bufferMsg msg) >>= \case
        (b, [])     -> (processor ls) ls b ep >>= \buf -> runNEndpoint ls (Endpoint h buf subs)
        (_, errors) -> endpointError $ summarize errors

    bufferMsg = foldM safeAppend (buffer ep, [])
    unpackMsg = unpackWith wd . BS.unpack

    safeAppend (b, errors) payload = unpackMsg payload >>= \case
        Nothing -> return (b, errors ++ [SerializeError payload])
        Just mg -> case errors of
            [] ->flip appendEBuffer b mg >>= \case
                (nb, Nothing) -> return (nb, [])
                (nb, Just m) -> return (nb, [OverflowError m])
            ls -> return (b, ls ++ [PropagatedError mg])

    summarize l = "Endpoint message errors (runNEndpoint " ++ (name ls) ++ ", " ++ show (length l) ++ " messages)"
    endpointError s = close (name ls) >> (liftIO $ putStrLn s) -- TODO: close vs closeInternal

runNEndpoint ls _ = error $ "Invalid endpoint for network source " ++ (name ls)

{- Message passing -}

-- | Queue accessors
enqueue :: MessageQueues a -> Address -> Identifier -> a -> EngineM a ()
enqueue (Peer qmv) addr n arg = liftIO $ modifyMVar_ qmv enqueueIfValid
  where enqueueIfValid (addr', q)
          | addr == addr' = return (addr, q++[(n,arg)])
          | otherwise = return (addr', q)  -- TODO: should this be more noisy, i.e. logged?

enqueue (ManyByPeer qsmv) addr n arg = liftIO $ modifyMVar_ qsmv enqueueToNode
  where enqueueToNode qs = return $ H.adjust (++ [(n, arg)]) addr qs

enqueue (ManyByTrigger qsmv) addr n arg = liftIO $ modifyMVar_ qsmv enqueueToTrigger
  where enqueueToTrigger qs = return $ H.adjust (++ [arg]) (addr, n) qs

-- TODO: fair peer traversal
-- TODO: efficient queue modification rather than toList / fromList
dequeue :: MessageQueues a -> EngineM a (Maybe (Address, Identifier, a))
dequeue = liftIO . \case
    Peer qmv           -> modifyMVar qmv $ return . someMessage
    ManyByPeer qsmv    -> modifyMVar qsmv $ return . messageFromMap mpDequeue
    ManyByTrigger qsmv -> modifyMVar qsmv $ return . messageFromMap mtDequeue

  where someMessage (addr,l) = case trySplit l of
          (nl, Nothing)      -> ((addr, nl), Nothing)
          (nl, Just (n,val)) -> ((addr, nl), Just (addr,n,val))

        messageFromMap f m = 
          let chosenKVOpt = snd . trySplit . H.toList . H.filter (not . null) $ m
          in maybe (m, Nothing) (\kv -> rebuild m (fst kv) $ f kv) chosenKVOpt

        rebuild m k (nv, r) = (H.adjust (const nv) k m, r)

        mpDequeue (_, [])          = ([], Nothing)
        mpDequeue (addr, (n,v):t)  = (t, Just (addr, n, v))
        mtDequeue (_, [])          = ([], Nothing)
        mtDequeue ((addr, n), v:t) = (t, Just (addr, n, v))

        trySplit l = if null l then (l, Nothing) else (tail l, Just $ head l)

-- | Message passing
send :: Address -> Identifier -> a -> EngineM a ()
send addr n arg = do
    engine <- ask

    shortCircuit <- (elem addr (nodes engine) ||) <$> simulation

    if shortCircuit
        then enqueue (queues engine) addr n arg
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
    let buffer = Exclusive emptySingletonBuffer
    void $ addEndpoint eid (file, buffer, []) eps


-- | File endpoint constructor.
-- TODO: validation with the given type
genericOpenFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> String -> EEndpoints a b
    -> EngineM b ()
genericOpenFile eid path wd _ mode eps = do
    file <- liftIO $ openFileHandle path wd (ioMode mode)
    let buffer = Exclusive emptySingletonBuffer
    void $ addEndpoint eid (file, buffer, []) eps
    case ioMode mode of
        SIO.ReadMode -> void $ genericDoRead eid eps -- Prime the file's buffer.
        _ -> return ()

-- | Socket endpoint constructor.
--   This initializes the engine's connections as necessary.
-- TODO: validation with the given type
genericOpenSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> String
    -> ListenerState a b -> EEndpoints a b -> EngineM b ()
genericOpenSocket eid addr wd _ (ioMode -> mode) lst endpoints = do
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
        buf <- liftIO $ shared t
        e <- addEndpoint eid (ioh, buf, []) endpoints
        wkThreadId <- forkEndpoint e
        registerNetworkListener (eid, wkThreadId)

    registerEndpoint _ ioh = do
        engine <- ask
        let t = emptyBoundedBuffer $ defaultBufferSpec $ config engine
        buf <- liftIO $ shared t
        void $ addEndpoint eid (ioh, buf, []) endpoints

    forkEndpoint e = do
        engine <- ask
        tid <- liftIO $ forkIO $ void $ runEngineM (runNEndpoint lst e) engine
        liftIO $ mkWeakThreadId tid

genericClose :: String -> EEndpoints a b -> EngineM b ()
genericClose n endpoints = getEndpoint n endpoints >>= \case
    Nothing -> return ()
    Just e  -> do
        closeHandle (handle e)
        deregister (networkSource $ handle e)
        removeEndpoint n endpoints
        notifySubscribers (notifyType $ handle e) (subscribers e)
  where
    deregister = maybe (return ()) (const $ deregisterNetworkListener n)
    notifyType (BuiltinH _ _ _) = FileClose
    notifyType (FileH _ _)      = FileClose
    notifyType (SocketH _ _)    = SocketClose

genericHasRead :: Identifier -> EEndpoints a b -> EngineM b (Maybe Bool)
genericHasRead n endpoints = getEndpoint n endpoints >>= \case
    Nothing -> return Nothing
    Just e  -> case handle e of
        BuiltinH _ h Stdin -> liftIO (SIO.hIsReadable h) >>= return . Just
        _ -> liftIO (emptyEBuffer (buffer e)) >>= return . Just . not

genericDoRead :: Identifier -> EEndpoints a b -> EngineM b (Maybe a)
genericDoRead n endpoints = getEndpoint n endpoints >>= maybe (return Nothing) refresh
  where
    refresh e = liftIO (refreshEBuffer (handle e) (buffer e)) >>= updateAndYield e

    updateAndYield e (nBuf, (vOpt, notifyType)) = do
      addEndpoint n (handle e, nBuf, subscribers e) endpoints
      notify notifyType (subscribers e)
      return vOpt

    notify Nothing _      = return ()
    notify (Just nt) subs = notifySubscribers nt subs

genericHasWrite :: Identifier -> EEndpoints a b -> EngineM b (Maybe Bool)
genericHasWrite n endpoints = getEndpoint n endpoints >>= \case
    Nothing -> return Nothing
    Just e -> liftIO (fullEBuffer (buffer e)) >>= return . Just . not

genericDoWrite :: Identifier -> a -> EEndpoints a b -> EngineM b ()
genericDoWrite n arg endpoints = getEndpoint n endpoints >>= maybe (return ()) write
  where
    write e = liftIO (appendEBuffer arg (buffer e)) >>= \case
        (nb, Nothing) -> liftIO (flushEBuffer (handle e) nb) >>= update e
        (_, Just _)   -> overflowError

    update e (nBuf, notifyType) = addEndpoint n (nep e nBuf) endpoints >> notify notifyType (subscribers e)

    nep e b = (handle e, b, subscribers e)

    notify Nothing _ = return ()
    notify (Just nt) subs = notifySubscribers nt subs

    overflowError = genericClose n endpoints >> (liftIO $ putStrLn "Endpoint buffer overflow (doWrite)")

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
    let lst = ListenerState eid (messageReadyV ctl, networkDoneV ctl) externalListenerProcessor
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
    let lst = ListenerState eid (messageReadyV ctl, networkDoneV ctl) internalListenerProcessor
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

networkSource :: IOHandle a -> Maybe (WireDesc a, NEndpoint)
networkSource (SocketH wd (Left ep)) = Just (wd, ep)
networkSource _ = Nothing

networkSink :: IOHandle a -> Maybe (WireDesc a, NConnection)
networkSink (SocketH wd (Right conn)) = Just (wd, conn)
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
    SIO.ReadWriteMode -> error "Unsupport network handle mode"

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

addEndpoint :: Identifier -> (IOHandle a, EndpointBuffer a, EndpointBindings b) -> EEndpoints a b
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
  where connectionSuccess   = return . Just . flip NConnection addr
        connectionError err = return Nothing

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
    c@(anchor -> (anchorAddr, Nothing)) <- liftIO $ readMVar conns

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

    connection _ = error "Invalid connection map, no anchor endpoint found."

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
                        (x, nc)  -> mapM_ (flip writeHandle h) x >> return (nc, notification x)

  where batch c@(Single Nothing)  = ([], c)
        batch (Single (Just x)) = ([x], Single Nothing)
        batch (Multiple x s) = let (a,b) = splitAt (batchSize s) x in (a, Multiple b s)

        notification [] = Nothing
        notification _  = Just FileData

flushEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, Maybe EndpointNotification)
flushEBuffer h = modifyEBuffer $ flushEBContents h

refreshEBContents :: IOHandle v -> BufferContents v
                     -> IO (BufferContents v, (Maybe v, Maybe EndpointNotification))

-- For now, ignore the buffer for reading from builtins.
refreshEBContents f@(BuiltinH _ _ _) c =
  readHandle f >>= (\vOpt -> return (c,(vOpt, maybe Nothing (const $ Just FileData) vOpt)))

refreshEBContents f@(FileH _ _) c = refill $ takeEBContents c
  where refill (c, vOpt) | refillPolicy c = readHandle f >>= return . rebuild c >>= (\(x,y) -> return (x, (vOpt, y)))
                         | otherwise      = return (c, (vOpt, Nothing))

        rebuild (Single _)     = maybe (Single Nothing, Nothing) (\c -> (Single $ Just c, Just FileData))
        rebuild (Multiple x s) = maybe (Multiple x s, Nothing)   (\y -> (Multiple (x++[y]) s, Just FileData))

        refillPolicy = emptyEBContents

refreshEBContents (SocketH _ (Left _)) c  = let (x,y) = takeEBContents c in return (x, (y, Nothing))
refreshEBContents (SocketH _ (Right _)) _ = error "Invalid buffer refresh for network sink"

refreshEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, (Maybe v, Maybe EndpointNotification))
refreshEBuffer h = modifyEBuffer $ refreshEBContents h

enqueueEBContents :: MessageQueues a -> BufferContents (InternalMessage a)
    -> EngineM a (BufferContents (InternalMessage a))
enqueueEBContents q = \case
  Single Nothing             -> return emptySingletonBuffer
  Single (Just (addr, n, v)) -> enqueue q addr n v >> return emptySingletonBuffer
  Multiple x spec            -> mapM_ (\(addr, n, v) -> enqueue q addr n v) x >> return (emptyBoundedBuffer spec)

enqueueEBuffer :: MessageQueues a -> EndpointBuffer (InternalMessage a)
    -> EngineM a (EndpointBuffer (InternalMessage a))
enqueueEBuffer q = modifyEBuffer'_ $ enqueueEBContents q

modifyEBuffer' :: (BufferContents b -> EngineM c (BufferContents b, a)) -> EndpointBuffer b
    -> EngineM c (EndpointBuffer b, a)
modifyEBuffer' f (Exclusive c) = f c >>= \(a, b) -> return (Exclusive a, b)
modifyEBuffer' f (Shared mvc) = modifyMVE mvc f >>= return . (Shared mvc,)

modifyEBuffer'_ f b = modifyEBuffer' (\c -> f c >>= return . (,())) b >>= return . fst

{- Pretty printing helpers -}

showMessageQueues :: Show a => MessageQueues a -> EngineM a String
showMessageQueues (Peer q)           = liftIO (readMVar q)  >>= return . show
showMessageQueues (ManyByPeer qs)    = liftIO (readMVar qs) >>= return . show
showMessageQueues (ManyByTrigger qs) = liftIO (readMVar qs) >>= return . show

showEngine :: Show a => EngineM a String
showEngine = return . unlines =<< ((++) <$> (ask >>= return . (:[]) . show) <*> prettyQueues)

prettyQueues :: Show a => EngineM a [String]
prettyQueues = ask >>= showMessageQueues . queues >>= return . (["Queues: "]++) . (:[])


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
        Right (r, x) -> return x
