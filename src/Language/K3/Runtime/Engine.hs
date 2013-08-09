{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ViewPatterns #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Runtime.Engine (
  Address(..),

  Engine(..),
  MessageQueues(..),
  Workers(..),

  WireDesc(..),
  
  EConnectionState(..),
  ETransport(..),
  EEndpoints(..),

  MessageProcessor(..),

  enqueue,
  dequeue,
  send,
  transport,

  simpleQueues,
  simpleEngine,

  exprWD,

  runEngine,

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
  putTransport,
  putEngine

) where

import Control.Arrow
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

import Network.Socket (withSocketsDo)
import qualified Network.Transport as NT
import qualified Network.Transport.TCP as NTTCP

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Parser

-- | Address implementation
type Address = (String, Int)


data Engine a = Engine { config          :: EngineConfiguration
                       , nodes           :: [Address]
                       , queues          :: MessageQueues a
                       , workers         :: Workers
                       , endpoints       :: EEndpoints a
                       , connections     :: EConnectionState
                       , internalFormat  :: WireDesc a }

{- Configuration parameters -}

data EngineConfiguration = EngineConfiguration { address           :: Address 
                                               , defaultBufferSpec :: BufferSpec 
                                               , connectionRetries :: Int }

{- Message processing -}

data LoopStatus res err = Result res | Error err | MessagesDone res

data MessageProcessor prog msg res err =
     MessageProcessor { initialize :: prog -> Engine msg -> IO res
                      , process    :: (Address, Identifier, msg) -> res -> IO res
                      , status     :: res -> Either err res }

{- Engine components -}

data MessageQueues a
  = Peer          (MVar (Address, [(Identifier, a)]))
  | ManyByPeer    (MVar (H.HashMap Address [(Identifier, a)]))
  | ManyByTrigger (MVar (H.HashMap (Address, Identifier) [a]))

data Workers
  = Uniprocess
  | MultiThreaded ThreadPool
  | MultiProcess  ProcessPool

-- TODO
type ThreadPool  = [Int]
type ProcessPool = [Int]

{- Network transports -}
data NEndpoint          = NEndpoint { endpoint :: LLEndpoint, epTransport :: LLTransport }
data NConnection        = NConnection { conn :: LLConnection, cEndpointAddress :: Address }

-- | Low-level transport layer, built on network-transport
type LLTransport  = NT.Transport
type LLEndpoint   = NT.EndPoint
type LLConnection = NT.Connection

-- | Connection pools may be initialized without binding an endpoint (e.g., if the program has no 
--   communication with any other peer or network sink), and are also safely modifiable to enable
--   their construction on demand.
data EConnectionMap = EConnectionMap { anchor :: (Address, Maybe NEndpoint)
                                     , cache  :: [(Address, NConnection)] }

-- | Two pools for outgoing internal (i.e., K3 peer) and external (i..e, network sink) connections
--   The first is optional capturing simulations that cannot send to peers without name resolution.
newtype EConnectionState = EConnectionState (Maybe (MVar EConnectionMap), MVar EConnectionMap)


-- | An interpreter transport that is used to implement message passing.
data ETransport a
  = TRSim (MessageQueues a) 
  | TRNet { trConfig      :: EngineConfiguration
          , trFormat      :: WireDesc a
          , trEndpoints   :: EEndpoints a
          , trConnections :: EConnectionState } 

-- | A description of a wire format, with serialization of data, deserialization into data, and
-- validation of deserialized data.
data WireDesc a = WireDesc { packWith     :: a -> String
                           , unpackWith   :: String -> a
                           , validateWith :: a -> Bool }

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


{- Module public API -}

openFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> String -> Engine a -> IO ()
openFile n path wd tOpt mode (transport -> tr) = openFileInternal n path wd tOpt mode tr

openSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> String -> Engine a -> IO ()
openSocket n addr wd tOpt mode (transport -> tr) = openSocketInternal n addr wd tOpt mode tr

close :: String -> Engine a -> IO ()
close n (transport -> tr) = closeInternal n tr

hasRead :: Identifier -> Engine a -> IO (Maybe Bool)
hasRead n (transport -> tr) = hasReadInternal n tr

doRead :: Identifier -> Engine a -> IO (Maybe a)
doRead n (transport -> tr) = doReadInternal n tr

hasWrite :: Identifier -> Engine a -> IO (Maybe Bool)
hasWrite n (transport -> tr) = hasWriteInternal n tr

doWrite :: Identifier -> a -> Engine a -> IO ()
doWrite n arg (transport -> tr) = doWriteInternal n arg tr


{- Helpers -}

connectionId :: Address -> Identifier
connectionId addr@(host,port) = "__" ++ host ++ show port

{- Engine defaults -}
defaultAddress :: Address
defaultAddress = ("127.0.0.1", 40000)

defaultConfig :: EngineConfiguration
defaultConfig = EngineConfiguration { address           = defaultAddress
                                    , defaultBufferSpec = bufferSpec
                                    , connectionRetries = 5 }
  where
    bufferSpec = BufferSpec { maxSize = 100, batchSize = 10 }

{- Engine constructors -}

simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

simpleEngine :: [Address] -> WireDesc a -> IO (Engine a)
simpleEngine [addr] internalWd = do
  q <- simpleQueues addr
  eps <- newMVar (H.fromList [])
  externalPool <- newMVar $ emptyConnectionMap $ address defaultConfig
  return $ Engine defaultConfig [addr] q
            Uniprocess eps (EConnectionState (Nothing, externalPool)) internalWd

simpleEngine peers internalWd = do
  q <- newMVar (H.fromList $ map (,[]) peers)
  eps <- newMVar (H.fromList [])
  externalPool <- newMVar $ emptyConnectionMap $ address defaultConfig
  return $ Engine defaultConfig peers (ManyByPeer q)
            Uniprocess eps (EConnectionState (Nothing, externalPool)) internalWd

-- TODO: network engine constructor. This should initialize endpoints
-- for all given address, for incoming trigger invocations.
-- networkEngine :: [Address] -> IO (Engine a)


{- Engine extractors -}

internalSendAddress :: Engine a -> Address
internalSendAddress (address . config -> (host,port)) = (host, port+1)

externalSendAddress :: Engine a -> Address
externalSendAddress (address . config -> (host,port)) = (host, port+2)

transport :: Engine a -> ETransport a
transport e@(Engine {queues = q, endpoints = ep, connections = c
                    , internalFormat = f, config = cfg})
  | simulation e = TRSim q
  | otherwise    = TRNet cfg f ep c


-- | Engine classification
simulation :: Engine a -> Bool
simulation (Engine {connections = (EConnectionState (Nothing, _))}) = True
simulation _ = False

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
send :: ETransport a ->  Address -> Identifier -> a -> IO ()
send (TRSim q) addr n arg = enqueue q addr n arg

send tr@(TRNet cfg wd eps _) addr n arg = trySend (connectionRetries cfg)
  where 
    endpointId = getEndpoint n eps >>= \case 
                    Nothing -> return $ connectionId addr
                    Just _ -> return n

    trySend 0       = endpointId >>= (send' $ error $ "Failed to connect to " ++ show addr)
    trySend retries = endpointId >>= (send' $ trySend $ retries - 1)

    send' retryF eid = getEndpoint eid eps >>= \case 
      Just c  -> hasWriteInternal eid tr >>= write eid
      Nothing -> openSocketInternal eid addr wd Nothing "w" tr >> retryF

    write eid (Just True) = doWriteInternal eid arg tr
    write _ _             = error $ "No write available to " ++ show addr 

{- Wire descriptions -}

exprWD :: WireDesc (K3 Expression)
exprWD = WireDesc show read (const True)


{- Message processing -}

processMessage :: MessageProcessor p a r e -> Engine a -> r -> IO (LoopStatus r e)
processMessage msgPrcsr e prevResult = (dequeue . queues) e >>= maybe term proc
  where term = return $ MessagesDone prevResult
        proc msg = (process msgPrcsr msg prevResult) >>= return . either Error Result . status msgPrcsr

runMessages :: (Show r, Show e) => MessageProcessor p a r e -> Engine a -> IO (LoopStatus r e) -> IO ()
runMessages msgPrcsr e status = status >>= \case 
  Result r       -> runMessages msgPrcsr e $ processMessage msgPrcsr e r
  Error e        -> putStrLn $ "Error:\n" ++ show e
  MessagesDone r -> putStrLn $ "Terminated:\n" ++ show r

runEngine :: (Show r, Show e) => MessageProcessor prog a r e -> Engine a -> prog -> IO ()
runEngine msgPrcsr e prog = (initialize msgPrcsr prog e)
                              >>= runNetwork
                              >>= runMessages msgPrcsr e . return . initStatus
  where
    -- TODO: termination variables?
    runNetwork res = initNetwork e >> (startNEndpoints e) >>= return . (res,)
    initNetwork e@(Engine {nodes = peers}) | simulation e = return ()
                                           | otherwise    = mapM_ initPeerEndpoint peers

    startNEndpoints engine = initControl (endpoints engine) >>= runNEndpoints engine
    
    initControl eps = newEmptySV >>= (\v -> numSockets eps >>= MSem.new . (1+) >>= return . (v,))
    
    runNEndpoints engine ctrl =
      withMVar (endpoints engine)
        (sequence . H.foldlWithKey' (startEndpoint ctrl $ transport engine) []) >>= return . (ctrl,)

    initPeerEndpoint addr = openSocket ("__node_" ++ show addr) addr (internalFormat e) Nothing "r" e

    startEndpoint ctrl tr acc n e@(Endpoint {handle = (networkSource -> Just _)}) =
      acc ++ [(forkIO $ runNEndpoint n ctrl tr e) >>= return . (n,) . Just]
    startEndpoint _ _ acc n _ = acc ++ [return (n, Nothing)]

    numSockets eps = withMVar eps (return . H.foldl' incrSocket 0)
    incrSocket acc (Endpoint {handle = SocketH _ (Left _)}) = acc-1
    incrSocket acc _ = acc

    initStatus (res, (ctrl, threads)) = either Error Result $ status msgPrcsr $ res


{- Network endpoint execution -}

-- TODO: handle ReceivedMulticast events
-- TODO: log errors on ErrorEvent
runNEndpoint :: Identifier -> (MSampleVar (), MSem Int) -> ETransport a -> Endpoint a -> IO ()
runNEndpoint n (msgAvail, sem) tr e@(Endpoint {handle = h@(networkSource -> Just (wd, _, ep)), subscribers = subs}) = do
  event <- NT.receive ep
  case event of
    NT.ConnectionOpened cid rel addr                 -> notify SocketAccept >> rcr
    NT.ConnectionClosed cid                          -> rcr
    NT.Received cid payload                          -> processMsg payload
    NT.ReceivedMulticast maddr payload               -> rcr                     
    NT.EndPointClosed                                -> MSem.signal sem
    NT.ErrorEvent (NT.TransportError errCode errMsg) -> MSem.signal sem
  where
    rcr           = runNEndpoint n (msgAvail, sem) tr e
    rcr' buf      = runNEndpoint n (msgAvail, sem) tr $ nep buf
    nep  buf      = Endpoint {handle = h, buffer = buf, subscribers = subs}
    
    processMsg msg = bufferMsg msg >>= (\buf -> writeSV msgAvail () >> notify SocketData >> rcr' buf)
    bufferMsg      = foldM (\b msg -> flip appendEBuffer b $ unpackMsg msg) (buffer e)
    unpackMsg      = unpackWith wd . BS.unpack
    notify evt     = notifySubscribers evt subs tr

runNEndpoint n _ _ _ = error $ "Invalid endpoint for network source " ++ n

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
openSocketHandle addr wd mode pool = 
  case mode of 
    SIO.ReadMode      -> incoming
    SIO.WriteMode     -> outgoing
    SIO.AppendMode    -> error "Unsupported network handle mode"
    SIO.ReadWriteMode -> error "Unsupport network handle mode"
  
  where incoming = newEndpoint addr >>= return . (>>= return . SocketH wd . Left)
        outgoing = case pool of
          Just p  -> getEstablishedConnection addr p >>= return . (>>= return . SocketH wd . Right)
          Nothing -> error "Invalid outgoing network connection pool"

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


{- Module API implementation -}

ioMode :: String -> SIO.IOMode
ioMode "r"  = SIO.ReadMode
ioMode "w"  = SIO.WriteMode
ioMode "a"  = SIO.AppendMode
ioMode "rw" = SIO.ReadWriteMode


openFileInternal :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> String -> ETransport a -> IO ()
openFileInternal cid path wd tOpt mode (trEndpoints -> eps) = do
    file <- openFileHandle path wd (ioMode mode)
    addEndpoint cid (file, (Exclusive $ Single Nothing), []) eps

-- | Socket constructor. 
--   This initializes the engine's connection pool as necessary.
openSocketInternal :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> String -> ETransport a -> IO ()
openSocketInternal cid addr wd tOpt
                   (ioMode -> mode)
                   tr@(trConfig &&& trEndpoints &&& trConnections -> (cfg, (eps, conns))) =
  do
    socket <- openSocketHandle addr wd mode $ connections mode
    mvar <- newMVar (Multiple [] $ defaultBufferSpec cfg)
    maybe (return ()) (\x -> addEndpoint cid (x, Shared mvar, []) eps) socket
  
  where connections SIO.WriteMode = getConnections cid conns
        connections _             = Nothing

closeInternal :: String -> ETransport a -> IO ()
closeInternal n tr@(trEndpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return ()
  Just e  -> closeHandle (handle e) 
            >> removeEndpoint n eps 
            >> notifySubscribers (notifyType $ handle e) (subscribers e) tr
  
  where notifyType (FileH _ _) = FileClose
        notifyType (SocketH _ _) = SocketClose

hasReadInternal :: Identifier -> ETransport a -> IO (Maybe Bool)
hasReadInternal n (trEndpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> emptyEBuffer (buffer e) >>= return . Just . not

doReadInternal :: Identifier -> ETransport a -> IO (Maybe a)
doReadInternal n tr@(trEndpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> refresh e
  
  where refresh e = refreshEBuffer (handle e) (buffer e) >>= updateAndYield e
        
        updateAndYield e (nBuf, (vOpt, notifyType)) =
          addEndpoint n (nep e nBuf) eps >> notify notifyType (subscribers e) >> return vOpt
        
        nep e b = (handle e, b, subscribers e)

        notify Nothing subs   = return ()
        notify (Just nt) subs = notifySubscribers nt subs tr 

hasWriteInternal :: Identifier -> ETransport a -> IO (Maybe Bool)
hasWriteInternal n (trEndpoints -> eps) = getEndpoint n eps >>= \case
  Nothing -> return Nothing
  Just e  -> fullEBuffer (buffer e) >>= return . Just . not

doWriteInternal :: Identifier -> a -> ETransport a -> IO ()
doWriteInternal n arg tr@(trEndpoints -> eps) = getEndpoint n eps  >>= \case
  Nothing -> return ()
  Just e  -> write e

  where write e = appendEBuffer arg (buffer e) >>= flushEBuffer (handle e) >>= update e
        update e (nBuf, notifyType) =
          addEndpoint n (nep e nBuf) eps >> notify notifyType (subscribers e)

        nep e b = (handle e, b, subscribers e)

        notify Nothing subs   = return ()
        notify (Just nt) subs = notifySubscribers nt subs tr



{- Connection and connection pool accessors -}

newEndpoint :: Address -> IO (Maybe NEndpoint)
newEndpoint addr@(host, port) = withSocketsDo $ do
  t <- eitherAsMaybe $ NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters
  e <- maybe (return Nothing) (eitherAsMaybe . NT.newEndPoint) t
  return $ t >>= \tr -> e >>= return . flip NEndpoint tr
  where eitherAsMaybe m = m >>= return . either (\_ -> Nothing) Just

newConnection :: Address -> NEndpoint -> IO (Maybe NConnection)
newConnection addr (endpoint -> ep) =
    NT.connect ep (epAddr addr) NT.ReliableOrdered NT.defaultConnectHints
      >>= return . either (\_ -> Nothing) (Just . flip NConnection addr)
  
  where epAddr (host,port) = NT.EndPointAddress $ BS.pack $ host ++ (show port) ++ ":0"
    -- TODO: above, check if it is safe to always use ":0" for NT.EndPointAddress

emptyConnectionMap :: Address -> EConnectionMap
emptyConnectionMap addr = EConnectionMap { anchor = (addr, Nothing), cache = [] }

getConnections :: Identifier -> EConnectionState -> Maybe (MVar EConnectionMap)
getConnections n (EConnectionState (icp, ecp)) =
  if isPrefixOf "__" n then maybe err Just icp else Just ecp
  where err = error $ "Invalid internal connection to " ++ n ++ " in a simulation"

withPool :: MVar EConnectionMap -> (EConnectionMap -> IO a) -> IO a
withPool poolMv withF = withMVar poolMv withF

modifyPool :: MVar EConnectionMap -> (EConnectionMap -> IO (EConnectionMap, a)) -> IO a
modifyPool poolMv modifyF = modifyMVar poolMv modifyF

getConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
getConnection addr pool = withPool pool $ return . lookup addr . cache

addConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
addConnection addr pool = modifyPool pool establishConnection
  where establishConnection p@(anchor -> (anchorAddr, Nothing)) =
          newEndpoint anchorAddr >>= \case
            Nothing -> return (p, Nothing)
            Just ep -> connect $ EConnectionMap (anchorAddr, Just ep) (cache p)
        establishConnection p = connect p

        connect p@(anchor -> (anchorAddr, Just ep)) =
          newConnection addr ep >>= return . maybe (p, Nothing) (add p)

        add p c = (EConnectionMap (anchor p) $ (addr, c):(cache p), Just c)

removeConnection :: Address -> MVar EConnectionMap -> IO ()
removeConnection addr pool = modifyPool pool remove
  where remove p@(anchor -> (_, Nothing)) = return (p, ())
        remove p = closeConnection (cache p) >>= return . (,()) . EConnectionMap (anchor p)
        closeConnection (matchConnections -> (matches, rest)) =
          mapM_ ((\c -> NT.close $ conn c) . snd) matches >> return rest
        matchConnections = partition ((addr ==) . fst)

getEstablishedConnection :: Address -> MVar EConnectionMap -> IO (Maybe NConnection)
getEstablishedConnection addr pool =
  getConnection addr pool >>= \case
    Nothing -> addConnection addr pool
    Just c  -> return $ Just c


{- Endpoint accessors -}

addEndpoint :: Identifier -> (IOHandle a, EndpointBuffer a, EndpointBindings a) -> EEndpoints a -> IO ()
addEndpoint n (h,b,s) eps = modifyMVar_ eps (return . H.insert n Endpoint {handle=h, buffer=b, subscribers=s})

removeEndpoint :: Identifier -> EEndpoints a -> IO ()
removeEndpoint n eps = modifyMVar_ eps $ return . H.delete n

getEndpoint :: Identifier -> EEndpoints a -> IO (Maybe (Endpoint a))
getEndpoint n eps = withMVar eps (return . H.lookup n)


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

notifySubscribers :: EndpointNotification -> EndpointBindings a -> ETransport a -> IO ()
notifySubscribers nt subs tr = mapM_ (notify . snd) $ filter ((nt == ) . fst) subs
  where notify (addr, tid, msg) = send tr addr tid msg

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

putTransport :: Show a => ETransport a -> IO ()
putTransport = \case
  (TRSim q)             -> putStrLn "Messages:" >> putMessageQueues q
  (TRNet cfg wd eps conns) -> putStrLn "Network" -- TODO endpoints, connection state

putEngine :: (Show a) => Engine a -> IO ()
putEngine e@(Engine {queues = q})= putStrLn (show e) >> putMessageQueues q


{- Instance implementations -}

-- TODO: put workers, endpoints
instance (Show a) => Show (Engine a) where
  show e@(Engine {nodes = n}) | simulation e = "Engine (simulation):\n" ++ ("Nodes:\n" ++ show n ++ "\n")
                              | otherwise    = "Engine (network):\n" ++ ("Nodes:\n" ++ show n ++ "\n")
