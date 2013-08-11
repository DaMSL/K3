{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ViewPatterns #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Runtime.Engine (
  Address(..),
  WireDesc(..),

  MessageProcessor(..),

  Engine(..),
  EEndpoints(..),

  MessageQueues(..),
  Workers(..),

  simpleQueues,
  perPeerQueues,
  perTriggerQueues,

  simulationEngine,
  networkEngine,

  exprWD,

  runEngine,
  forkEngine,
  waitForEngine,
  terminateEngine,

  enqueue,
  dequeue,
  send,

  openFile,
  openSocket,
  close,
  
  hasRead,
  doRead,
  hasWrite,
  doWrite,

  attachNotifier,
  attachNotifier_,
  detachNotifier,
  detachNotifier_,

  putMessageQueues,
  putEngine

) where

import Control.Arrow
import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Concurrent.MSampleVar
import qualified Control.Concurrent.MSem as MSem
import Control.Concurrent.MSem (MSem)
import Control.Exception (throwIO)
import Control.Monad
import Control.Monad.IO.Class

import qualified Data.ByteString.Char8 as BS
import Data.Functor
import qualified Data.HashMap.Lazy as H
import Data.List

import qualified System.IO as SIO
import System.Process
import System.Mem.Weak (Weak)

import Network.Socket (withSocketsDo)
import qualified Network.Transport as NT
import qualified Network.Transport.TCP as NTTCP

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Parser

-- | Address implementation
type Address = (String, Int)


data Engine a = Engine { config          :: EngineConfiguration
                       , internalFormat  :: WireDesc a 
                       , control         :: EngineControl
                       , nodes           :: [Address]
                       , queues          :: MessageQueues a
                       , workers         :: Workers
                       , listeners       :: Listeners
                       , endpoints       :: EEndpoints a
                       , connections     :: EConnectionState }


{- Configuration parameters -}

data EngineConfiguration = EngineConfiguration { address           :: Address 
                                               , defaultBufferSpec :: BufferSpec 
                                               , connectionRetries :: Int
                                               , waitForNetwork    :: Bool }

{- Message processing -}

data EngineControl = EngineControl { terminateV    :: MVar Bool
                                        -- A concurrent boolean used internally by workers to determine if
                                        -- they should terminate.
                                        -- No blocking required, only safe access to a termination boolean
                                        -- for both workers (readers) and a console (i.e., a single writer)

                                   , messageReadyV :: MSampleVar ()
                                        -- Blocking required for m workers (i.e., for empty queues during
                                        -- message processing), with signalling from n producers.
                                        -- We could use a semaphore, but a MSampleVar should be more lightweight.
                                        -- That is, we need not track how many messages are available, rather
                                        -- we need a single wake-up when any producer makes a message available.
                                        -- TODO: this functionality should be merged with MessageQueues.

                                   , networkDoneV  :: MVar Int
                                        -- A concurrent counter that will reach 0 once all network listeners have finished.
                                        -- This is incremented and decremented as listeners are added to the engine, or as
                                        -- they are removed or complete.
                                        -- No blocking required, only safe access as part of determining whether to
                                        -- terminate the engine.

                                   , waitV         :: MVar ()
                                        -- Mutex for external threads to wait on this engine to complete.
                                        -- External threads should use readMVar on this variable, and this will
                                        -- be signalled by the last worker thread to finish.
                                   }

data LoopStatus res err = Result res | Error err | MessagesDone res

data MessageProcessor prog msg res err =
     MessageProcessor { initialize :: prog -> Engine msg -> IO res
                      , process    :: (Address, Identifier, msg) -> res -> IO res
                      , status     :: res -> Either err res
                      , finalize   :: res -> IO res }

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

-- | A description of a wire format, with serialization of data, deserialization into data, and
-- validation of deserialized data.
data WireDesc a = WireDesc { packWith     :: a -> String
                           , unpackWith   :: String -> a
                           , validateWith :: a -> Bool }


{- Network state -}

data NEndpoint          = NEndpoint { endpoint :: LLEndpoint, epTransport :: LLTransport }
data NConnection        = NConnection { conn :: LLConnection, cEndpointAddress :: Address }

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


{- Endpoints -}

data IOHandle a
  = FileH   (WireDesc a) SIO.Handle 
  | SocketH (WireDesc a) (Either NEndpoint NConnection)

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
type EndpointBindings msg = [(EndpointNotification, (Address, Identifier, msg))]

data Endpoint msg = Endpoint { handle      :: IOHandle msg
                             , buffer      :: EndpointBuffer msg
                             , subscribers :: EndpointBindings msg } 

type EEndpoints msg = MVar (H.HashMap Identifier (Endpoint msg))

{- Builtin notifications -}
data EndpointNotification
    = FileData
    | FileClose
    | SocketAccept
    | SocketData
    | SocketClose
  deriving (Eq, Show, Read)


{- Naming schemes and constants -}
connectionId :: Address -> Identifier
connectionId addr@(host,port) = "__" ++ host ++ show port

defaultAddress :: Address
defaultAddress = ("127.0.0.1", 40000)

defaultConfig :: EngineConfiguration
defaultConfig = EngineConfiguration { address           = defaultAddress
                                    , defaultBufferSpec = bufferSpec
                                    , connectionRetries = 5 
                                    , waitForNetwork    = False }
  where
    bufferSpec = BufferSpec { maxSize = 100, batchSize = 10 }

{- Wire descriptions -}
exprWD :: WireDesc (K3 Expression)
exprWD = WireDesc show read (const True)

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
simulationEngine :: [Address] -> WireDesc a -> IO (Engine a)
simulationEngine peers internalWD = do
  ctrl          <- EngineControl <$> newMVar False <*> newEmptySV <*> newMVar 0 <*> newEmptyMVar
  workers       <- newEmptyMVar >>= return . Uniprocess
  listeners     <- newMVar []
  (q, eps)      <- case peers of
                    [addr] -> (,) <$> simpleQueues addr <*> emptyEndpoints
                    _      -> (,) <$> perPeerQueues peers <*> emptyEndpoints
  externalConns <- emptyConnectionMap . externalSendAddress . address $ defaultConfig
  connState     <- return $ EConnectionState (Nothing, externalConns)
  return $ Engine defaultConfig internalWD ctrl peers q workers listeners eps connState


-- | Network engine constructor.
--   This is initialized with listening endpoints for each given peer as well 
--   as internal and external connection anchor endpoints for messaging.
networkEngine :: [Address] -> WireDesc a -> IO (Engine a)
networkEngine peers internalWD = do
  ctrl          <- EngineControl <$> newMVar False <*> newEmptySV <*> newMVar 0 <*> newEmptyMVar
  workers       <- newEmptyMVar >>= return . Uniprocess
  listnrs       <- newMVar []
  q             <- perPeerQueues peers 
  eps           <- emptyEndpoints
  internalConns <- defaultConnectionMap internalSendAddress >>= return . Just
  externalConns <- defaultConnectionMap externalSendAddress
  connState     <- return $ EConnectionState (internalConns, externalConns)
  engine        <- return $ Engine defaultConfig internalWD ctrl peers q workers listnrs eps connState
  return engine

  where
    defaultConnectionMap addrF = emptyConnectionMap . addrF . address $ defaultConfig
    startNetwork eg            = mapM_ (runPeerEndpoint eg) peers
    runPeerEndpoint eg addr    = openSocket ("__node_" ++ show addr) addr internalWD Nothing "r" eg


{- Engine extractors -}

internalSendAddress :: Address -> Address
internalSendAddress baseAddr@(host, port) = (host, port+1)

externalSendAddress :: Address -> Address
externalSendAddress baseAddr@(host, port) = (host, port+2)

-- | Engine classification
simulation :: Engine a -> Bool
simulation (Engine {connections = (EConnectionState (Nothing, _))}) = True
simulation _ = False


{- Message processing and engine control -}
registerNetworkListener :: (Identifier, Weak ThreadId) -> Engine a -> IO ()
registerNetworkListener x (control &&& listeners -> (ctrl, lstnrs)) = do
  modifyMVar_ (networkDoneV ctrl) $ return . (1+)
  modifyMVar_ lstnrs $ return . (x:)

deregisterNetworkListener :: Identifier -> Engine a -> IO ()
deregisterNetworkListener n (control &&& listeners -> (ctrl, lstnrs)) = do
  modifyMVar_ (networkDoneV ctrl) $ return . flip (-) 1
  modifyMVar_ lstnrs $ return . filter ((n /= ) . fst)

terminate :: Engine a -> IO Bool
terminate e =  (&&) <$> readMVar (terminateV $ control e)
                    <*> if waitForNetwork $ config e then networkDone e
                                                     else return True

networkDone :: Engine a -> IO Bool
networkDone e = readMVar (networkDoneV $ control e) >>= return . (0 ==)

waitForMessage :: Engine a -> IO ()
waitForMessage e = readSV (messageReadyV $ control e)


processMessage :: MessageProcessor p a r e -> Engine a -> r -> IO (LoopStatus r e)
processMessage msgPrcsr e prevResult = (dequeue . queues) e >>= maybe term proc
  where term = return $ MessagesDone prevResult
        proc msg = (process msgPrcsr msg prevResult) >>= return . either Error Result . status msgPrcsr


runMessages :: (Show r, Show e) => MessageProcessor p a r e -> Engine a -> IO (LoopStatus r e) -> IO ()
runMessages msgPrcsr e status = status >>= \case 
  Result r       -> rcr r
  Error e        -> finish "Error:\n" e
  MessagesDone r -> terminate e >>= \case
                      True -> finalize msgPrcsr r >>= finish "Terminated:\n"
                      _    -> waitForMessage e >> rcr r
  
  where rcr          = runMessages msgPrcsr e . processMessage msgPrcsr e
        cleanup      = cleanC (connections e) >> cleanE (endpoints e)        
        finish msg r = cleanup >> putMVar (waitV $ control e) () >> (putStrLn $ msg ++ show r)
        
        cleanC (EConnectionState (Nothing, x)) = clearConnections x
        cleanC (EConnectionState (Just x, y))  = clearConnections x >> clearConnections y
        cleanE eps                             = withMVar eps (mapM_ (flip close e) . H.keys)


runEngine :: (Show r, Show e) => MessageProcessor prog a r e -> Engine a -> prog -> IO ()
runEngine msgPrcsr e prog = (initialize msgPrcsr prog e)
                              >>= (\res -> initializeWorker e >> return res)
                              >>= runMessages msgPrcsr e . return . initStatus
  where initStatus = either Error Result . status msgPrcsr

        initializeWorker (workers -> Uniprocess workerMV) = tryTakeMVar workerMV >> myThreadId >>= putMVar workerMV
        initializeWorker (workers -> Multithreaded _)     = error $ "Unsupported engine mode: Multithreaded"
        initializeWorker (workers -> Multiprocess _)      = error $ "Unsupported engine mode: Multiprocess"


forkEngine :: (Show r, Show e) => MessageProcessor prog a r e -> Engine a -> prog -> IO ThreadId
forkEngine msgPrcsr e prog = forkIO $ runEngine msgPrcsr e prog

waitForEngine :: Engine a -> IO ()
waitForEngine = readMVar . waitV . control 

terminateEngine :: Engine a -> IO ()
terminateEngine e = modifyMVar_ (terminateV $ control e) (\_ -> return True)


{- Network endpoint execution -}

-- TODO: handle ReceivedMulticast events
-- TODO: log errors on ErrorEvent
runNEndpoint :: Identifier -> (MSampleVar (), MVar Int) -> Engine a -> Endpoint a -> IO ()
runNEndpoint n (msgAvail, netCntr) eg e@(Endpoint {handle = h@(networkSource -> Just (wd, _, ep)), subscribers = subs}) = do
  event <- NT.receive ep
  case event of
    NT.ConnectionOpened cid rel addr   -> notify SocketAccept >> rcrE
    NT.ConnectionClosed cid            -> rcrE
    NT.Received cid payload            -> processMsg payload
    NT.ReceivedMulticast maddr payload -> rcrE                     
    NT.EndPointClosed                  -> return ()
    NT.ErrorEvent err                  -> close n eg >> (putStrLn $ show err)
  where
    rcr       = runNEndpoint n (msgAvail, netCntr) eg
    rcrE      = rcr e
    rcrNE buf = rcr $ nep buf
    nep  buf  = Endpoint {handle = h, buffer = buf, subscribers = subs}

    processMsg msg = bufferMsg msg >>= (\buf -> writeSV msgAvail () >> notify SocketData >> rcrNE buf)
    bufferMsg      = foldM (\b msg -> flip appendEBuffer b $ unpackMsg msg) (buffer e)
    unpackMsg      = unpackWith wd . BS.unpack
    notify evt     = notifySubscribers evt subs eg

runNEndpoint n _ _ _ = error $ "Invalid endpoint for network source " ++ n


{- Message passing -}

-- | Queue accessors
enqueue :: MessageQueues a -> Address -> Identifier -> a -> IO ()
enqueue (Peer qmv) addr n arg = modifyMVar_ qmv enqueueIfValid
  where enqueueIfValid (addr', q)
          | addr == addr' = return (addr, q++[(n,arg)])
          | otherwise = return (addr', q)  -- TODO: should this be more noisy, i.e. logged?

enqueue (ManyByPeer qsmv) addr n arg = modifyMVar_ qsmv enqueueToNode
  where enqueueToNode qs = return $ H.adjust (++[(n,arg)]) addr qs

enqueue (ManyByTrigger qsmv) addr n arg = modifyMVar_ qsmv enqueueToTrigger
  where enqueueToTrigger qs = return $ H.adjust (++[arg]) (addr, n) qs

-- TODO: fair peer traversal
-- TODO: efficient queue modification rather than toList / fromList
dequeue :: MessageQueues a -> IO (Maybe (Address, Identifier, a))
dequeue = \case
    (Peer qmv)           -> modifyMVar qmv $ return . someMessage
    (ManyByPeer qsmv)    -> modifyMVar qsmv $ return . someMapMessage mpMessage
    (ManyByTrigger qsmv) -> modifyMVar qsmv $ return . someMapMessage mtMessage

  where someMessage (addr,l) = case trySplit l of
          (nl, Nothing)      -> ((addr, nl), Nothing)
          (nl, Just (n,val)) -> ((addr, nl), Just (addr,n,val))

        someMapMessage f = messageFromMap (uncurry $ rebuildMap f)
        messageFromMap f = f . trySplit . H.toList . H.filter (not . null)
        rebuildMap f l kvOpt = (H.fromList l, kvOpt >>= f)

        mpMessage (addr, idvs) = tryHead idvs >>= return . (\(n,val) -> (addr,n,val))
        mtMessage ((addr, n), vs) = tryHead vs >>= return . (\v -> (addr,n,v))

        tryHead l  = if null l then Nothing else Just $ head l
        trySplit l = if null l then (l, Nothing) else (tail l, Just $ head l)


-- | Message passing
send :: Engine a ->  Address -> Identifier -> a -> IO ()
send e@(endpoints -> eps) addr n arg 
  | shortCircuit = enqueue (queues e) addr n arg
  | otherwise    = trySend (connectionRetries $ config e)
  where
    shortCircuit = simulation e || elem addr (nodes e)
    endpointId   = getEndpoint n eps >>= \case 
                    Nothing -> return $ connectionId addr
                    Just _ -> return n

    trySend 0       = endpointId >>= (send' $ error $ "Failed to connect to " ++ show addr)
    trySend retries = endpointId >>= (send' $ trySend $ retries - 1)

    send' retryF eid = getEndpoint eid eps >>= \case 
      Just c  -> hasWrite eid e >>= write eid
      Nothing -> openSocket eid addr (internalFormat e) Nothing "w" e >> retryF

    write eid (Just True) = doWrite eid arg e
    write _ _             = error $ "No write available to " ++ show addr 



{- Module API implementation -}

ioMode :: String -> SIO.IOMode
ioMode "r"  = SIO.ReadMode
ioMode "w"  = SIO.WriteMode
ioMode "a"  = SIO.AppendMode
ioMode "rw" = SIO.ReadWriteMode


openFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> String -> Engine a -> IO ()
openFile eid path wd tOpt mode (endpoints -> eps) = do
    file <- openFileHandle path wd (ioMode mode)
    void $ addEndpoint eid (file, (Exclusive $ Single Nothing), []) eps


-- | Socket constructor. 
--   This initializes the engine's connections as necessary.
openSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> String -> Engine a -> IO ()
openSocket eid addr wd tOpt (ioMode -> mode) eg@(control &&& endpoints -> (ctrl, eps)) =
  do
    socket <- openSocketHandle addr wd mode $ connectionsForMode mode
    maybe (return ()) (registerEndpoint mode) socket
  
  where connectionsForMode SIO.WriteMode = getConnections eid $ connections eg
        connectionsForMode _             = Nothing

        registerEndpoint SIO.ReadMode ioh = do
          mvar       <- buffer
          e          <- addEndpoint eid (ioh, Shared mvar, []) eps
          wkThreadId <- forkEndpoint e
          registerNetworkListener (eid, wkThreadId) eg

        registerEndpoint _ ioh = do
          mvar <- buffer
          void $ addEndpoint eid (ioh, Shared mvar, []) eps

        buffer         = newMVar (Multiple [] $ defaultBufferSpec $ config eg)
        forkEndpoint e = (forkIO $ runNEndpoint eid peerControl eg e) >>= mkWeakThreadId
        peerControl    = (messageReadyV ctrl, networkDoneV ctrl)


close :: String -> Engine a -> IO ()
close n eg@(endpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return ()
  Just e  -> closeHandle (handle e) 
              >> deregister (networkSource $ handle e)
              >> removeEndpoint n eps 
              >> notifySubscribers (notifyType $ handle e) (subscribers e) eg
  
  where deregister = maybe (return ()) (\_ -> deregisterNetworkListener n eg)
        notifyType (FileH _ _) = FileClose
        notifyType (SocketH _ _) = SocketClose


hasRead :: Identifier -> Engine a -> IO (Maybe Bool)
hasRead n (endpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> emptyEBuffer (buffer e) >>= return . Just . not


doRead :: Identifier -> Engine a -> IO (Maybe a)
doRead n eg@(endpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> refresh e
  
  where refresh e = refreshEBuffer (handle e) (buffer e) >>= updateAndYield e
        
        updateAndYield e (nBuf, (vOpt, notifyType)) =
          addEndpoint n (nep e nBuf) eps >> notify notifyType (subscribers e) >> return vOpt
        
        nep e b = (handle e, b, subscribers e)

        notify Nothing subs   = return ()
        notify (Just nt) subs = notifySubscribers nt subs eg 


hasWrite :: Identifier -> Engine a -> IO (Maybe Bool)
hasWrite n (endpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> fullEBuffer (buffer e) >>= return . Just . not


doWrite :: Identifier -> a -> Engine a -> IO ()
doWrite n arg eg@(endpoints -> eps) = getEndpoint n eps  >>= \case
  Nothing -> return ()
  Just e  -> write e

  where write e = appendEBuffer arg (buffer e) >>= flushEBuffer (handle e) >>= update e
        update e (nBuf, notifyType) =
          addEndpoint n (nep e nBuf) eps >> notify notifyType (subscribers e)

        nep e b = (handle e, b, subscribers e)

        notify Nothing subs   = return ()
        notify (Just nt) subs = notifySubscribers nt subs eg


{- IO Handle methods -}

networkSource :: IOHandle a -> Maybe (WireDesc a, LLTransport, LLEndpoint)
networkSource (SocketH wd (Left ep)) = Just (wd, epTransport ep, endpoint ep)
networkSource _ = Nothing

networkSink :: IOHandle a -> Maybe (WireDesc a, NConnection)
networkSink (SocketH wd (Right conn)) = Just (wd, conn)
networkSink _ = Nothing
 
-- | Open an external file, with given wire description and file path.
openFileHandle :: FilePath -> WireDesc a -> SIO.IOMode -> IO (IOHandle a)
openFileHandle p wd mode = SIO.openFile p mode >>= return . (FileH wd)

-- | Open an external socket, with given wire description and address.
openSocketHandle :: Address -> WireDesc a -> SIO.IOMode -> Maybe (MVar EConnectionMap) -> IO (Maybe (IOHandle a))
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
closeHandle :: IOHandle a -> IO ()
closeHandle (FileH _ h) = SIO.hClose h
closeHandle (networkSource -> Just (_,t,e)) = NT.closeEndPoint e >> NT.closeTransport t
closeHandle (networkSink   -> Just (_,_)) = return () 
  -- TODO: above, reference count aggregated outgoing connections for garbage collection
closeHandle _ = error "Invalid IOHandle argument for closeHandle"

-- | Read a single payload from an external.
readHandle :: IOHandle a -> IO (Maybe a)
readHandle (FileH wd h) = do
    done <- SIO.hIsEOF h
    if done then return Nothing
    else do
      -- TODO: Add proper delimiter support, to avoid delimiting by newline.
      payload <- unpackWith wd <$> SIO.hGetLine h
      if validateWith wd payload then return $ Just payload
      else return Nothing

readHandle (SocketH wd np) = error "Unsupported: read from Socket"

-- | Write a single payload to a handle
writeHandle :: a -> IOHandle a -> IO ()
writeHandle payload (FileH wd h) = do
    done <- SIO.hIsEOF h
    if done then return ()
    else SIO.hPutStrLn h $ packWith wd payload -- TODO: delimiter, as with readHandle

writeHandle payload (SocketH wd (Right (conn -> c))) =
  NT.send c [BS.pack $ packWith wd payload] >>= return . either (\_ -> ()) id

writeHandle payload (SocketH _ _) = error "Unsupported write operation on network handle"


{- Endpoint accessors -}

newEndpoint :: Address -> IO (Maybe NEndpoint)
newEndpoint addr@(host, port) = withSocketsDo $ do
  t <- eitherAsMaybe $ NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters
  e <- maybe (return Nothing) (eitherAsMaybe . NT.newEndPoint) t
  return $ t >>= \tr -> e >>= return . flip NEndpoint tr
  where eitherAsMaybe m = m >>= return . either (\_ -> Nothing) Just

emptyEndpoints :: IO (EEndpoints a)
emptyEndpoints = newMVar (H.fromList [])

addEndpoint :: Identifier -> (IOHandle a, EndpointBuffer a, EndpointBindings a) -> EEndpoints a -> IO (Endpoint a)
addEndpoint n (h,b,s) eps = modifyMVar eps (rebuild Endpoint {handle=h, buffer=b, subscribers=s})
  where rebuild e m = return (H.insert n e m, e)

removeEndpoint :: Identifier -> EEndpoints a -> IO ()
removeEndpoint n eps = modifyMVar_ eps $ return . H.delete n

getEndpoint :: Identifier -> EEndpoints a -> IO (Maybe (Endpoint a))
getEndpoint n eps = withMVar eps (return . H.lookup n)


{- Connection and connection map accessors -}

newConnection :: Address -> NEndpoint -> IO (Maybe NConnection)
newConnection addr (endpoint -> ep) =
    NT.connect ep (epAddr addr) NT.ReliableOrdered NT.defaultConnectHints
      >>= return . either (\_ -> Nothing) (Just . flip NConnection addr)
  
  where epAddr (host,port) = NT.EndPointAddress $ BS.pack $ host ++ (show port) ++ ":0"
    -- TODO: above, check if it is safe to always use ":0" for NT.EndPointAddress

emptyConnectionMap :: Address -> IO (MVar EConnectionMap)
emptyConnectionMap addr = newMVar $ EConnectionMap { anchor = (addr, Nothing), cache = [] }

getConnections :: Identifier -> EConnectionState -> Maybe (MVar EConnectionMap)
getConnections n (EConnectionState (icp, ecp)) =
  if isPrefixOf "__" n then maybe err Just icp else Just ecp
  where err = error $ "Invalid internal connection to " ++ n ++ " in a simulation"

withConnectionMap :: MVar EConnectionMap -> (EConnectionMap -> IO a) -> IO a
withConnectionMap connsMV withF = withMVar connsMV withF

modifyConnectionMap :: MVar EConnectionMap -> (EConnectionMap -> IO (EConnectionMap, a)) -> IO a
modifyConnectionMap connsMV modifyF = modifyMVar connsMV modifyF

addConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
addConnection addr conns = modifyConnectionMap conns establishConnection
  where establishConnection c@(anchor -> (anchorAddr, Nothing)) =
          newEndpoint anchorAddr >>= \case
            Nothing -> return (c, Nothing)
            Just ep -> connect $ EConnectionMap (anchorAddr, Just ep) (cache c)
        establishConnection c = connect c

        connect conns@(anchor -> (anchorAddr, Just ep)) =
          newConnection addr ep >>= return . maybe (conns, Nothing) (add conns)

        add conns c = (EConnectionMap (anchor conns) $ (addr, c):(cache conns), Just c)

removeConnection :: Address -> MVar EConnectionMap -> IO ()
removeConnection addr conns = modifyConnectionMap conns remove
  where remove cm@(anchor -> (_, Nothing)) = return (cm, ())
        remove cm = closeConnection (cache cm) >>= return . (,()) . EConnectionMap (anchor cm)
        closeConnection (matchConnections -> (matches, rest)) =
          mapM_ (NT.close . conn . snd) matches >> return rest
        matchConnections = partition ((addr ==) . fst)

getConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
getConnection addr conns = withConnectionMap conns $ return . lookup addr . cache

getEstablishedConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
getEstablishedConnection addr conns =
  getConnection addr conns >>= \case
    Nothing -> addConnection addr conns
    Just c  -> return $ Just c

clearConnections :: MVar EConnectionMap -> IO ()
clearConnections cm = modifyConnectionMap cm clear
  where clear cm@(EConnectionMap (a, ep) conns) =
          clearC conns >> clearE ep >> return (EConnectionMap (a, Nothing) [], ())
        
        clearC conns     = mapM_ (flip removeConnection cm . fst) conns
        clearE Nothing   = return ()
        clearE (Just ep) = NT.closeEndPoint $ endpoint ep


{- Endpoint Notifiers -} 

getNotificationType :: Identifier -> Endpoint a -> EndpointNotification
getNotificationType n (handle -> FileH _ _) = case n of
  "data"  -> FileData
  "close" -> FileClose
  _ -> error $ "invalid notification type " ++ n

getNotificationType n (handle -> SocketH _ _) = case n of
  "accept" -> SocketAccept
  "data"   -> SocketData
  "close"  -> SocketClose
  _ -> error $ "invalid notification type " ++ n

notifySubscribers :: EndpointNotification -> EndpointBindings a -> Engine a -> IO ()
notifySubscribers nt subs eg = mapM_ (notify . snd) $ filter ((nt == ) . fst) subs
  where notify (addr, tid, msg) = send eg addr tid msg

modifySubscribers :: Identifier -> (Endpoint a -> EndpointBindings a) -> EEndpoints a -> IO Bool
modifySubscribers eid f eps = getEndpoint eid eps >>= maybe (return False) updateSub
  where updateSub e = (addEndpoint eid (nep e $ f e) eps) >> return True  
        nep e subs = (handle e, buffer e, subs)

attachNotifier :: Identifier -> Identifier -> (Address, Identifier, a) -> EEndpoints a -> IO Bool
attachNotifier eid nt msg eps = modifySubscribers eid newSubs eps
  where newSubs e = nubBy (\(a,_) (b,_) -> a == b) $ (getNotificationType nt e, msg):(subscribers e)

attachNotifier_ :: Identifier -> Identifier -> (Address, Identifier, a) -> EEndpoints a -> IO ()
attachNotifier_ eid nt msg eps = void $ attachNotifier eid nt msg eps 

detachNotifier :: Identifier -> Identifier -> EEndpoints a -> IO Bool
detachNotifier eid nt eps = modifySubscribers eid newSubs eps 
  where newSubs e = filter (((getNotificationType nt e) /= ) . fst) $ subscribers e

detachNotifier_ :: Identifier -> Identifier -> EEndpoints a -> IO ()
detachNotifier_ eid nt eps = void $ detachNotifier eid nt eps 


{- Endpoint buffers -}

wrapEBuffer :: (BufferContents b -> a) -> EndpointBuffer b -> IO a
wrapEBuffer f = \case
  Exclusive c -> return $ f c
  Shared mvc -> readMVar mvc >>= return . f

modifyEBuffer :: (BufferContents b -> IO (BufferContents b, a)) -> EndpointBuffer b -> IO (EndpointBuffer b, a)
modifyEBuffer f = \case
  Exclusive c -> f c >>= (\(a,b) -> return (Exclusive a, b))
  Shared mvc -> modifyMVar mvc (\c -> f c) >>= return . (Shared mvc,)

emptyEBContents :: BufferContents a -> Bool
emptyEBContents (Single x)   = maybe True (\_ -> False) x
emptyEBContents (Multiple x _) = null x

emptyEBuffer :: EndpointBuffer a -> IO Bool
emptyEBuffer = wrapEBuffer emptyEBContents

fullEBContents :: BufferContents a -> Bool
fullEBContents (Single x) = maybe False (\_ -> True) x
fullEBContents (Multiple x spec) = length x == maxSize spec

fullEBuffer :: EndpointBuffer a -> IO Bool
fullEBuffer = wrapEBuffer fullEBContents

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
        append _ = error "Cannot append to full buffer"

appendEBuffer :: v -> EndpointBuffer v -> IO (EndpointBuffer v)
appendEBuffer v buf = modifyEBuffer (return . appendEBContents v) buf >>= return . fst

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

refreshEBContents f@(FileH _ _) c = refill $ takeEBContents c 
  
  where refill (c, vOpt) | refillPolicy c = readHandle f >>= return . rebuild c >>= (\(x,y) -> return (x, (vOpt, y)))
                         | otherwise      = return (c, (vOpt, Nothing))

        rebuild (Single _)     = maybe (Single Nothing, Nothing) (\c -> (Single $ Just c, Just FileData))
        rebuild (Multiple x s) = maybe (Multiple x s, Nothing)   (\y -> (Multiple (x++[y]) s, Just FileData))

        refillPolicy = emptyEBContents


refreshEBContents (SocketH _ (Left _)) c  = let (x,y) = takeEBContents c in return (x, (y, Nothing))
refreshEBContents (SocketH _ (Right _)) c = error "Invalid buffer refresh for network sink"

refreshEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, (Maybe v, Maybe EndpointNotification))
refreshEBuffer h = modifyEBuffer $ refreshEBContents h


{- Pretty printing helpers -}

putMessageQueues :: Show a => MessageQueues a -> IO ()
putMessageQueues (Peer q)           = readMVar q >>= putStrLn . show
putMessageQueues (ManyByPeer qs)    = readMVar qs >>= putStrLn . show
putMessageQueues (ManyByTrigger qs) = readMVar qs >>= putStrLn . show

putEngine :: (Show a) => Engine a -> IO ()
putEngine e@(Engine {queues = q})= putStrLn (show e) >> putMessageQueues q


{- Instance implementations -}

-- TODO: put workers, endpoints
instance (Show a) => Show (Engine a) where
  show e@(Engine {nodes = n}) | simulation e = "Engine (simulation):\n" ++ ("Nodes:\n" ++ show n ++ "\n")
                              | otherwise    = "Engine (network):\n" ++ ("Nodes:\n" ++ show n ++ "\n")
