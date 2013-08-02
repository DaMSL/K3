{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Runtime.Engine (
  Address(..),

  Engine(..),
  MessageQueues(..),
  Workers(..),

  NTransports(..),
  NInTransport(..),
  NOutTransport(..),

  EEndPoint(..),
  ITransport(..),

  NConnectionPool(..),
  NEndPoint(..),
  NConnection(..),

  WireDesc(..),

  enqueue,
  dequeue,
  send,
  transport,

  simpleQueues,
  simpleTransport,
  simpleEngine,

  putMessageQueues,
  putTransport,

  openFileEP,
  closeEP,
  openSocketEP,

  readEP,

  EndPointBuffer(..),
  EndPointBufferContents(..),
  EndPointBindings(..),
  IEndPoints(..),

  appendEBuffer,
  takeEBContents,
  emptyEBContents,
  modifyEBuffer,
  emptyEBuffer,
  refreshEBuffer,
  refreshEBContents,

  exprWD

) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.IO.Class

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


data Engine a
  = Simulation { nodes :: [Address], queues :: (MessageQueues a), workers :: Workers }
  | Network    { nodes :: [Address], queues :: (MessageQueues a), workers :: Workers, transports :: NTransports }

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
newtype NInTransport  = NInTransport NEndPoint
newtype NOutTransport = NOutTransport NConnectionPool

type NConnectionPool  = [(NEndPoint, Maybe NConnection)]
newtype NEndPoint     = NEndPoint (LLTransport, LLEndPoint)
newtype NConnection   = NConnection (Address, LLConnection)

-- | Low-level transport layer, built on network-transport
type LLTransport  = NT.Transport
type LLEndPoint   = NT.EndPoint
type LLConnection = NT.Connection

-- | An external source/sink.
data EEndPoint a
  = FileEP (WireDesc a) SIO.Handle
  | SocketEP (WireDesc a) NEndPoint

-- | A description of a wire format, with serialization of data, deserialization into data, and
-- validation of deserialized data.
data WireDesc a = WireDesc { packWith :: a -> String, unpackWith :: String -> a, validateWith :: a -> Bool }

-- | An interpreter transport that is used to implement message passing.
data ITransport a = TRSim (MessageQueues a) | TRNet NOutTransport

-- | EndPoint bindings (i.e. triggers attached to open/close/data)
type EndPointBindings v = [(Identifier, v)]

-- | Named sources and sinks.
type IEndPoints a = [(Identifier, (EEndPoint a, EndPointBuffer a, EndPointBindings a))]

-- | Sources buffer the next value, while sinks keep a buffer of values waiting to be flushed.
data EndPointBufferContents a
  = Single   (Maybe a)
  | Multiple [a]

-- | EndPoint buffers, which may be used by concurrent workers (shared), or by a single worker thread (exclusive)
data EndPointBuffer a
  = Exclusive (EndPointBufferContents a)
  | Shared    (MVar (EndPointBufferContents a))


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
send :: ITransport a ->  Address -> Identifier -> a -> IO ()
send (TRSim q) addr n arg = enqueue q addr n arg
send (TRNet otr) addr n arg = undefined -- TODO: establish connection in pool as necessary.

transport :: Engine a -> ITransport a
transport (Simulation _ q _) = TRSim q
transport (Network _ _ _ (NTransports (_, otr))) = TRNet otr

{- Engine constructors -}

simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

simpleTransport :: Address -> IO (ITransport a)
simpleTransport addr = simpleQueues addr >>= return . TRSim

simpleEngine :: [Address] -> IO (Engine a)
simpleEngine [addr] = simpleQueues addr >>= return . (\q -> Simulation [addr] q Uniprocess)
simpleEngine peers  = newMVar (H.fromList $ map (,[]) peers) >>= return . (\q -> Simulation peers q Uniprocess) . ManyByPeer

-- TODO: network engine constructor. This should initialize EndPoints
-- for all given address, for incoming trigger invocations.
-- networkEngine :: [Address] -> IO (Engine a)


{- Endpoint constructors -}

-- | Open an external file, with given wire description and file path.
openFileEP :: WireDesc a -> FilePath -> IO (EEndPoint a)
openFileEP wd p = SIO.openFile p SIO.ReadMode >>= return . FileEP wd

-- | Open an external socket, with given wire description and address.
openSocketEP :: WireDesc a -> Address -> IO (EEndPoint a)
openSocketEP wd (host, port) = do
    t <- NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters >>= either throwIO return
    e <- NT.newEndPoint t >>= either throwIO return
    return $ SocketEP wd (NEndPoint (t, e))

-- | Close an external.
closeEP :: (EEndPoint a) -> IO ()
closeEP (FileEP _ h) = SIO.hClose h
closeEP (SocketEP _ (NEndPoint (t, e))) = NT.closeEndPoint e >> NT.closeTransport t

-- | Read a single payload from an external.
readEP :: EEndPoint a -> IO (Maybe a)
readEP (FileEP wd h) = do
    done <- SIO.hIsEOF h
    if done then return Nothing
        else do

            -- TODO: Add proper delimiter support, to avoid delimiting by newline.
            payload <- unpackWith wd <$> SIO.hGetLine h
            if validateWith wd payload then return (Just payload)
                else return Nothing

readEP (SocketEP wd np) = error "Unsupported: readEEndPoint from Socket"


{- EndPoint buffers -}

wrapEBuffer :: (EndPointBufferContents b -> a) -> EndPointBuffer b -> IO a
wrapEBuffer f = \case
  Exclusive c -> return $ f c
  Shared mvc -> readMVar mvc >>= return . f

modifyEBuffer :: (EndPointBufferContents b -> IO (EndPointBufferContents b, a)) -> EndPointBuffer b -> IO (EndPointBuffer b, a)
modifyEBuffer f = \case
  Exclusive c -> f c >>= (\(a,b) -> return (Exclusive a, b))
  Shared mvc -> modifyMVar mvc (\c -> f c) >>= return . (Shared mvc,)

emptyEBContents :: EndPointBufferContents a -> Bool
emptyEBContents (Single x)   = maybe True (\_ -> False) x
emptyEBContents (Multiple x) = null x

emptyEBuffer :: EndPointBuffer a -> IO Bool
emptyEBuffer = wrapEBuffer emptyEBContents

readEBContents :: EndPointBufferContents a -> Maybe a
readEBContents (Single x) = x
readEBContents (Multiple x) = if null x then Nothing else Just $ head x

readEBuffer :: EndPointBuffer a -> IO (Maybe a)
readEBuffer = wrapEBuffer readEBContents

appendEBContents :: v -> EndPointBufferContents v -> IO (EndPointBufferContents v, Maybe v)
appendEBContents v (Single x) = return (Single $ Just v, x)
appendEBContents v (Multiple x) = return $ (Multiple $ x++[v], Nothing)

appendEBuffer :: v -> EndPointBuffer v -> IO (EndPointBuffer v)
appendEBuffer v buf = modifyEBuffer (appendEBContents v) buf >>= return . fst

takeEBContents :: EndPointBufferContents v -> IO (EndPointBufferContents v, Maybe v)
takeEBContents = \case
  Single x       -> return (Single Nothing, x)
  Multiple []    -> return (Multiple [], Nothing)
  Multiple (h:t) -> return (Multiple t, Just h)

takeEBuffer :: EndPointBuffer v -> IO (EndPointBuffer v, Maybe v)
takeEBuffer = modifyEBuffer $ takeEBContents

refreshEBContents :: EEndPoint v -> EndPointBufferContents v -> IO (EndPointBufferContents v, Maybe v)
refreshEBContents f@(FileEP _ _) c = takeEBContents c >>= refill
  where refill (c, vOpt) | refillPolicy c = rebuild f c >>= return . (, vOpt)
                         | otherwise = return (c, vOpt)

        rebuild ep (Single _) = readEP ep >>= maybe mkESingle mkSingle
        rebuild ep (Multiple x) = readEP ep >>= maybe (mkMulti x) (\y -> mkMulti $ x++[y])

        refillPolicy = emptyEBContents

        mkSingle = return . Single . Just
        mkESingle = return $ Single Nothing
        mkMulti x = return $ Multiple x

refreshEBContents (SocketEP _ _) c = takeEBContents c

refreshEBuffer :: EEndPoint v -> EndPointBuffer v -> IO (EndPointBuffer v, Maybe v)
refreshEBuffer ep = modifyEBuffer $ refreshEBContents ep

{- Wire descriptions -}

exprWD :: WireDesc (K3 Expression)
exprWD = WireDesc show read (const True)


{- Pretty printing helpers -}

putMessageQueues :: Show a => MessageQueues a -> IO ()
putMessageQueues (Peer q)           = readMVar q >>= putStrLn . show
putMessageQueues (ManyByPeer qs)    = readMVar qs >>= putStrLn . show
putMessageQueues (ManyByTrigger qs) = readMVar qs >>= putStrLn . show

putTransport :: Show a => ITransport a -> IO ()
putTransport = \case
  (TRSim q)   -> putStrLn "Messages:" >> putMessageQueues q
  (TRNet otr) -> undefined -- TODO
