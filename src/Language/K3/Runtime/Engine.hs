{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DoAndIfThenElse #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Runtime.Engine (
  Address(..),

  Engine(..),
  MessageQueues(..),
  Workers(..),

  NTransports(..),
  NInTransport(..),
  NOutTransport(..),

  NConnectionPool(..),
  NEndpoint(..),
  NConnection(..),

  WireDesc(..),
  
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
  
  hasNext,
  next,

  attachNotifier,
  attachNotifier_,
  detachNotifier,
  detachNotifier_,

  putMessageQueues,
  putTransport,
  putEngine

) where

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

import qualified Network.Transport as NT
import qualified Network.Transport.TCP as NTTCP

import Language.K3.Core.Annotation
import Language.K3.Core.Expression
import Language.K3.Core.Type

import Language.K3.Parser

-- | Address implementation
type Address = (String, Int)


data Engine a b
  = Simulation { nodes     :: [Address]
               , queues    :: MessageQueues a
               , workers   :: Workers
               , endpoints :: EEndpoints a b }

  | Network    { nodes      :: [Address]
               , queues     :: MessageQueues a
               , workers    :: Workers
               , endpoints  :: EEndpoints a b
               , transports :: NTransports }

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

newtype NTransports   = NTransports ([(Address, NInTransport)], NOutTransport)
newtype NInTransport  = NInTransport NEndpoint
newtype NOutTransport = NOutTransport NConnectionPool

type NConnectionPool  = [(NEndpoint, Maybe NConnection)]
newtype NEndpoint     = NEndpoint (LLTransport, LLEndpoint)
newtype NConnection   = NConnection (Address, LLConnection)

-- | Low-level transport layer, built on network-transport
type LLTransport  = NT.Transport
type LLEndpoint   = NT.EndPoint
type LLConnection = NT.Connection

-- | An interpreter transport that is used to implement message passing.
data ETransport a = TRSim (MessageQueues a) | TRNet NOutTransport

-- | A description of a wire format, with serialization of data, deserialization into data, and
-- validation of deserialized data.
data WireDesc a = WireDesc { packWith     :: a -> String
                           , unpackWith   :: String -> a
                           , validateWith :: a -> Bool }

{- Endpoints -}

data IOHandle a
  = FileH   (WireDesc a) SIO.Handle 
  | SocketH (WireDesc a) NEndpoint

-- | Sources buffer the next value, while sinks keep a buffer of values waiting to be flushed.
data EndpointBufferContents a 
  = Single (Maybe a)
  | Multiple [a]

-- | Endpoint buffers, which may be used by concurrent workers (shared), or by a single worker thread (exclusive)
data EndpointBuffer a
  = Exclusive (EndpointBufferContents a)
  | Shared    (MVar (EndpointBufferContents a))

-- | Endpoint bindings (i.e. triggers attached to open/close/data)
type EndpointBindings sub = [(Identifier, sub)]

data Endpoint msg sub = Endpoint { handle      :: IOHandle msg
                                 , buffer      :: EndpointBuffer msg
                                 , subscribers :: EndpointBindings sub } 

type EEndpoints msg sub = MVar (H.HashMap Identifier (Endpoint msg sub))


{- Message processing -}

data LoopStatus res err = Result res | Error err | MessagesDone res

data MessageProcessor prog msg sub res err =
     MessageProcessor { initialize :: prog -> Engine msg sub -> IO res
                      , process    :: (Address, Identifier, msg) -> res -> IO res
                      , status     :: res -> Either err res }


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
        rebuildMap f l kvOpt = (H.fromList l, maybe Nothing f kvOpt)

        mpMessage (addr, idvs) = maybe Nothing (\(n,val) -> Just (addr,n,val)) $ tryHead idvs
        mtMessage ((addr, n), vs) = maybe Nothing (\v -> Just (addr,n,v)) $ tryHead vs

        tryHead l  = if null l then Nothing else Just $ head l
        trySplit l = if null l then (l, Nothing) else (tail l, Just $ head l)


-- | Message passing
send :: ETransport a ->  Address -> Identifier -> a -> IO ()
send (TRSim q) addr n arg = enqueue q addr n arg
send (TRNet otr) addr n arg = undefined -- TODO: establish connection in pool as necessary.

transport :: Engine a b -> ETransport a
transport (Simulation _ q _ _) = TRSim q
transport (Network _ _ _ _ (NTransports (_, otr))) = TRNet otr

{- Engine constructors -}

simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

simpleEngine :: [Address] -> IO (Engine a b)
simpleEngine [addr] = do
  q <- simpleQueues addr
  eps <- newMVar (H.fromList [])
  return $ Simulation [addr] q Uniprocess eps

simpleEngine peers = do
  q <- newMVar (H.fromList $ map (,[]) peers)
  eps <- newMVar (H.fromList [])
  return $ Simulation peers (ManyByPeer q) Uniprocess eps

-- TODO: network engine constructor. This should initialize endpoints
-- for all given address, for incoming trigger invocations.
-- networkEngine :: [Address] -> IO (Engine a)



{- Wire descriptions -}

exprWD :: WireDesc (K3 Expression)
exprWD = WireDesc show read (const True)


{- Message processing -}

processMessage :: MessageProcessor p a b r e -> Engine a b -> r -> IO (LoopStatus r e)
processMessage msgPrcsr e prevResult = (dequeue . queues) e >>= maybe term proc
  where term = return $ MessagesDone prevResult
        proc msg = (process msgPrcsr msg prevResult) >>= return . either Error Result . status msgPrcsr

runMessages :: (Show r, Show e) => MessageProcessor p a b r e -> Engine a b -> IO (LoopStatus r e) -> IO ()
runMessages msgPrcsr e status = status >>= \case 
  Result r       -> runMessages msgPrcsr e $ processMessage msgPrcsr e r
  Error e        -> putStrLn $ "Error:\n" ++ show e
  MessagesDone r -> putStrLn $ "Terminated:\n" ++ show r

runEngine :: (Show r, Show e) => WireDesc a -> MessageProcessor prog a b r e -> Engine a b -> prog -> IO ()
runEngine peerWd msgPrcsr (Network _ _ _ _ _) prog = undefined
runEngine peerWd msgPrcsr e prog = (initialize msgPrcsr prog e)
                                    >>= runNetwork
                                    >>= runMessages msgPrcsr e . return . initStatus
  where
    -- TODO: termination variables?
    runNetwork res = initNetwork >> (startNEndpoints $ endpoints e) >>= return . (res,)
    initNetwork = case e of
      Simulation _ _ _ _ -> return ()
      Network peers _ _ _ _ -> mapM_ initPeerEndpoint peers

    startNEndpoints eps = initControl eps >>= runNEndpoints eps
    initControl eps = newEmptySV >>= (\v -> numSockets eps >>= MSem.new . (1+) >>= return . (v,))
    runNEndpoints eps ctrl = withMVar eps (sequence . H.foldlWithKey' (startEndpoint ctrl) []) >>= return . (ctrl,)

    initPeerEndpoint addr = openSocket ("__node_" ++ show addr) addr peerWd Nothing $ endpoints e

    startEndpoint _ acc n (Endpoint {handle = FileH _ _}) = acc ++ [return (n, Nothing)]
    startEndpoint ctrl acc n e = acc ++ [(forkIO $ runNEndpoint n ctrl e) >>= return . (n,) . Just]

    numSockets eps = withMVar eps (return . H.foldl' incrSocket 0)
    incrSocket acc (Endpoint {handle = SocketH _ _}) = acc-1
    incrSocket acc _ = acc

    initStatus (res, (ctrl, threads)) = either Error Result $ status msgPrcsr $ res


{- Network endpoint execution -}

-- TODO: dispatch bindings
-- TODO: handle ReceivedMulticast events
-- TODO: log errors on ErrorEvent
runNEndpoint :: Identifier -> (MSampleVar (), MSem Int) -> Endpoint a b -> IO ()
runNEndpoint n (msgAvail, sem) e@(Endpoint {handle = h@(SocketH wd (NEndpoint (tr, ep)))}) = do
  event <- NT.receive ep
  case event of
    NT.ConnectionOpened cid rel addr                 -> rcr
    NT.ConnectionClosed cid                          -> rcr
    NT.Received cid payload                          -> bufferPayload payload >>= (\buf -> writeSV msgAvail () >> rcr' buf)
    NT.ReceivedMulticast maddr payload               -> rcr                     
    NT.EndPointClosed                                -> MSem.signal sem
    NT.ErrorEvent (NT.TransportError errCode errMsg) -> MSem.signal sem
  where
    rcr           = runNEndpoint n (msgAvail, sem) e
    rcr' buf      = runNEndpoint n (msgAvail, sem) $ nep buf
    nep  buf      = Endpoint {handle = h, buffer = buf, subscribers = subscribers e}
    
    bufferPayload = foldM (\b msg -> flip appendEBuffer b $ unpackMsg msg) (buffer e)
    unpackMsg     = unpackWith wd . BS.unpack


{- IO Handle methods -}

-- | Open an external file, with given wire description and file path.
openFileHandle :: FilePath -> WireDesc a -> IO (IOHandle a)
openFileHandle p wd = SIO.openFile p SIO.ReadMode >>= return . (FileH wd)

-- | Open an external socket, with given wire description and address.
openSocketHandle :: Address -> WireDesc a -> IO (IOHandle a)
openSocketHandle (host, port) wd = do
    t <- NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters >>= either throwIO return
    e <- NT.newEndPoint t >>= either throwIO return
    return $ SocketH wd (NEndpoint (t, e))

-- | Close an external.
closeHandle :: IOHandle a -> IO ()
closeHandle (FileH _ h) = SIO.hClose h
closeHandle (SocketH _ (NEndpoint (t, e))) = NT.closeEndPoint e >> NT.closeTransport t

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


{- Endpoint accessors -}

addEndpoint :: Identifier -> (IOHandle a, EndpointBuffer a, EndpointBindings b) -> EEndpoints a b -> IO ()
addEndpoint n (h,b,s) eps = modifyMVar_ eps (return . H.insert n Endpoint {handle=h, buffer=b, subscribers=s})

removeEndpoint :: Identifier -> EEndpoints a b -> IO ()
removeEndpoint n eps = modifyMVar_ eps $ return . H.delete n

getEndpoint :: Identifier -> EEndpoints a b -> IO (Maybe (Endpoint a b))
getEndpoint n eps = withMVar eps (return . H.lookup n)


{- High-level endpoint accessors -}

openFile :: Identifier -> String -> WireDesc a -> Maybe (K3 Type) -> EEndpoints a b -> IO ()
openFile cid path wd tOpt eps = do
    file <- openFileHandle path wd
    addEndpoint cid (file, (Exclusive $ Single Nothing), []) eps

openSocket :: Identifier -> Address -> WireDesc a -> Maybe (K3 Type) -> EEndpoints a b -> IO ()
openSocket cid addr wd tOpt eps = do
    socket <- openSocketHandle addr wd
    mvar <- newMVar (Multiple [])
    addEndpoint cid (socket, Shared mvar, []) eps

close :: String -> EEndpoints a b -> IO ()
close n eps = getEndpoint n eps >>= \case
  Nothing -> return ()
  Just e -> closeHandle (handle e) >> removeEndpoint n eps

hasNext :: Identifier -> EEndpoints a b -> IO (Maybe Bool)
hasNext n eps = getEndpoint n eps >>= \case
  Just e -> emptyEBuffer (buffer e) >>= return . Just . not
  Nothing -> return Nothing

next :: Identifier -> EEndpoints a b -> IO (Maybe a)
next n eps = getEndpoint n eps >>= \case
  Just e -> refresh e
  Nothing -> return Nothing
  where refresh e = refreshEBuffer (handle e) (buffer e) >>= updateAndYield e
        updateAndYield e (nBuf, vOpt) = addEndpoint n (nep e nBuf) eps >> return vOpt
        nep e b = (handle e, b, subscribers e)


{- Endpoint Notifiers -} 

modifySubscribers :: Identifier -> (EndpointBindings b -> EndpointBindings b) -> EEndpoints a b -> IO Bool
modifySubscribers eid f eps = getEndpoint eid eps >>= maybe (return False) updateSub
  where updateSub e = (addEndpoint eid (nep e $ f $ subscribers e) eps) >> return True  
        nep e subs = (handle e, buffer e, subs)

attachNotifier :: Identifier -> Identifier -> b -> EEndpoints a b -> IO Bool
attachNotifier eid nid sub eps = modifySubscribers eid newSubs eps
  where newSubs subs = nubBy (\(a,_) (b,_) -> a == b) $ (nid,sub):subs

attachNotifier_ :: Identifier -> Identifier -> b -> EEndpoints a b -> IO ()
attachNotifier_ eid nid sub eps = void $ attachNotifier eid nid sub eps 

detachNotifier :: Identifier -> Identifier -> EEndpoints a b -> IO Bool
detachNotifier eid nid eps = modifySubscribers eid newSubs eps
  where newSubs subs = filter ((nid /= ) . fst) subs

detachNotifier_ :: Identifier -> Identifier -> EEndpoints a b -> IO ()
detachNotifier_ eid nid eps = void $ detachNotifier eid nid eps 



{- Endpoint buffers -}

wrapEBuffer :: (EndpointBufferContents b -> a) -> EndpointBuffer b -> IO a
wrapEBuffer f = \case
  Exclusive c -> return $ f c
  Shared mvc -> readMVar mvc >>= return . f

modifyEBuffer :: (EndpointBufferContents b -> IO (EndpointBufferContents b, a)) -> EndpointBuffer b -> IO (EndpointBuffer b, a)
modifyEBuffer f = \case
  Exclusive c -> f c >>= (\(a,b) -> return (Exclusive a, b))
  Shared mvc -> modifyMVar mvc (\c -> f c) >>= return . (Shared mvc,)

emptyEBContents :: EndpointBufferContents a -> Bool
emptyEBContents (Single x)   = maybe True (\_ -> False) x
emptyEBContents (Multiple x) = null x

emptyEBuffer :: EndpointBuffer a -> IO Bool
emptyEBuffer = wrapEBuffer emptyEBContents

readEBContents :: EndpointBufferContents a -> Maybe a
readEBContents (Single x) = x
readEBContents (Multiple x) = if null x then Nothing else Just $ head x

readEBuffer :: EndpointBuffer a -> IO (Maybe a)
readEBuffer = wrapEBuffer readEBContents

appendEBContents :: v -> EndpointBufferContents v -> IO (EndpointBufferContents v, Maybe v)
appendEBContents v (Single x) = return (Single $ Just v, x)
appendEBContents v (Multiple x) = return $ (Multiple $ x++[v], Nothing)

appendEBuffer :: v -> EndpointBuffer v -> IO (EndpointBuffer v)
appendEBuffer v buf = modifyEBuffer (appendEBContents v) buf >>= return . fst

takeEBContents :: EndpointBufferContents v -> IO (EndpointBufferContents v, Maybe v)
takeEBContents = \case
  Single x       -> return (Single Nothing, x)
  Multiple []    -> return (Multiple [], Nothing)
  Multiple (h:t) -> return (Multiple t, Just h)

takeEBuffer :: EndpointBuffer v -> IO (EndpointBuffer v, Maybe v)
takeEBuffer = modifyEBuffer $ takeEBContents

refreshEBContents :: IOHandle v -> EndpointBufferContents v -> IO (EndpointBufferContents v, Maybe v)
refreshEBContents f@(FileH _ _) c = takeEBContents c >>= refill
  where refill (c, vOpt) | refillPolicy c = rebuild f c >>= return . (, vOpt)
                         | otherwise = return (c, vOpt)

        rebuild h (Single _)   = readHandle h >>= maybe mkESingle mkSingle
        rebuild h (Multiple x) = readHandle h >>= maybe (mkMulti x) (\y -> mkMulti $ x++[y])

        refillPolicy = emptyEBContents

        mkSingle  = return . Single . Just
        mkESingle = return $ Single Nothing
        mkMulti x = return $ Multiple x

refreshEBContents (SocketH _ _) c = takeEBContents c

refreshEBuffer :: IOHandle v -> EndpointBuffer v -> IO (EndpointBuffer v, Maybe v)
refreshEBuffer h = modifyEBuffer $ refreshEBContents h


{- Pretty printing helpers -}

putMessageQueues :: Show a => MessageQueues a -> IO ()
putMessageQueues (Peer q)           = readMVar q >>= putStrLn . show
putMessageQueues (ManyByPeer qs)    = readMVar qs >>= putStrLn . show
putMessageQueues (ManyByTrigger qs) = readMVar qs >>= putStrLn . show

putTransport :: Show a => ETransport a -> IO ()
putTransport = \case
  (TRSim q)   -> putStrLn "Messages:" >> putMessageQueues q
  (TRNet otr) -> undefined -- TODO

putEngine :: (Show a, Show b) => Engine a b -> IO ()
putEngine = \case
  e@(Simulation _ q _ _) -> putStrLn (show e) >> putMessageQueues q
  e@(Network _ q _ _ _)  -> putStrLn (show e) >> putMessageQueues q


{- Instance implementations -}

-- TODO: put workers, endpoints
instance (Show a, Show b) => Show (Engine a b) where
  show (Simulation n q _ _) = "Simulation:\n" ++ ("Nodes:\n" ++ show n ++ "\n")
  show (Network n q _ _ _)  = "Network:\n" ++ ("Nodes:\n" ++ show n ++ "\n")
