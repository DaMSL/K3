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

  ValueFormat(..),

  IEndpoint(..),
  ITransport(..),

  NConnectionPool(..),
  NEndpoint(..),
  NConnection(..),

  enqueue,
  dequeue,
  send,

  nodes,
  queues,
  workers,
  transport,

  simpleQueues,
  simpleTransport,
  simpleEngine,

  putMessageQueues,
  putTransport,

  openFile,
  closeFile,
  openSocket,
  closeSocket,

  readFormattedString,
  readEndpoint

) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad.IO.Class

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
  = Simulation [Address] (MessageQueues a) Workers
  | Network    [Address] (MessageQueues a) Workers NTransports

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


{- Interpreter I/O components -}
data ValueFormat = CSV | Text | Binary

-- | K3 endpoint (source/sink) implementations
data IEndpoint
  = File SIO.Handle ValueFormat (Maybe (K3 Type))
  | Socket NEndpoint ValueFormat (Maybe (K3 Type))

-- | An interpreter transport that is used to implement message passing.
data ITransport a = TRSim (MessageQueues a) | TRNet NOutTransport



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


{- Accessors -}

nodes :: Engine a -> [Address]
nodes (Simulation n _ _) = n
nodes (Network n _ _ _) = n

queues :: Engine a -> MessageQueues a
queues (Simulation _ q _) = q
queues (Network _ q _ _) = q

workers :: Engine a -> Workers
workers (Simulation _ _ w) = w
workers (Network _ _ w _) = w

transport :: Engine a -> ITransport a
transport (Simulation _ q _) = TRSim q
transport (Network _ _ _ (NTransports (_,otr))) = TRNet otr

{- Constructors -}

simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

simpleTransport :: Address -> IO (ITransport a)
simpleTransport addr = simpleQueues addr >>= return . TRSim

simpleEngine :: [Address] -> IO (Engine a)
simpleEngine [addr] = simpleQueues addr >>= return . (\q -> Simulation [addr] q Uniprocess)
simpleEngine peers  = newMVar (H.fromList $ map (,[]) peers) >>= return . (\q -> Simulation peers q Uniprocess) . ManyByPeer

-- TODO: network engine constructor. This should initialize Endpoints
-- for all given address, for incoming trigger invocations.
-- networkEngine :: [Address] -> IO (Engine a)

{- Pretty printing helpers -}

putMessageQueues :: Show a => MessageQueues a -> IO ()
putMessageQueues (Peer q)           = readMVar q >>= putStrLn . show
putMessageQueues (ManyByPeer qs)    = readMVar qs >>= putStrLn . show
putMessageQueues (ManyByTrigger qs) = readMVar qs >>= putStrLn . show

putTransport :: Show a => ITransport a -> IO ()
putTransport = \case
  (TRSim q)   -> putStrLn "Messages:" >> putMessageQueues q
  (TRNet otr) -> undefined -- TODO


{- Interpreter I/O helpers -}
format :: String -> ValueFormat
format "csv" = CSV
format "txt" = Text
format "bin" = Binary
format _ = undefined

openFile :: Identifier -> String -> String -> Maybe (K3 Type) -> IO IEndpoint
openFile cid path fmt tOpt = SIO.openFile path SIO.ReadMode >>= (\h -> return $ File h (format fmt) tOpt)

closeFile :: Identifier -> IEndpoint -> IO ()
closeFile n (File h _ _) = SIO.hClose h
closeFile _ _ = undefined


openSocket :: Identifier -> Address -> String -> Maybe (K3 Type) -> IO IEndpoint
openSocket cid (host, port) fmt tOpt =
  NTTCP.createTransport host (show port) NTTCP.defaultTCPParameters >>= either throwIO mkEndpoint
  where mkEndpoint tr = NT.newEndPoint tr >>=
          either throwTransportError (\e -> return $ Socket (NEndpoint (tr,e)) (format fmt) tOpt)
        throwTransportError t = throwIO (ErrorCall $ show t)

closeSocket :: Identifier -> IEndpoint -> IO ()
closeSocket n (Socket (NEndpoint (t,e)) _ _) = NT.closeEndPoint e >> NT.closeTransport t
closeSocket _ _ = undefined


-- TODO: validate against expected type if available.
readFormattedString :: ValueFormat -> Maybe (K3 Type) -> String -> Maybe (K3 Expression)
readFormattedString fmt tOpt s = case fmt of
    CSV    -> parseExpressionCSV s
    Text   -> parseExpression s
    Binary -> Nothing -- TODO

readEndpoint :: IEndpoint -> IO (Maybe (K3 Expression))
readEndpoint (File h fmt tOpt) = SIO.hIsEOF h >>= readNext
  where readNext done = if done then return Nothing
                        else SIO.hGetLine h >>= return . readFormattedString fmt tOpt

readEndpoint _ = undefined
