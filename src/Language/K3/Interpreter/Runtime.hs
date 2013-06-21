{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

-- | A message processing runtime for the K3 interpreter
module Language.K3.Interpreter.Runtime (
  Address(..),

  Engine(..),
  MessageQueues(..),
  Workers(..),
  
  Transports(..),
  InTransport(..),
  OutTransport(..),

  ITransport(..),
  
  ConnectionPool(..),
  Endpoint(..),
  Connection(..),

  enqueue,
  dequeue,
  send,

  queues,
  workers,
  transport,

  simpleQueues,
  simpleTransport,
  simpleEngine,

  putMessageQueues,
  putTransport

) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class

import qualified Data.HashMap.Lazy as H 
import Data.List

import qualified Network.Transport as NT

import Language.K3.Core.Type (Identifier)

-- | Address implementation
type Address = (String, Int)


data Engine a
  = Simulation (MessageQueues a) Workers
  | Network    (MessageQueues a) Workers Transports

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

{- Transports -}

newtype Transports   = Transports ([(Address, InTransport)], OutTransport)

newtype InTransport  = InTransport Endpoint
newtype OutTransport = OutTransport ConnectionPool

-- | A Transport that is carried around in the interpreter's state.
data ITransport a    = TRSim (MessageQueues a) | TRNet OutTransport

type ConnectionPool  = [(Endpoint, Maybe Connection)]
newtype Endpoint     = Endpoint (LLTransport, LLEndpoint)
newtype Connection   = Connection (Address, LLConnection)


-- | Low-level transport layer, built on network-transport
type LLTransport  = NT.Transport
type LLEndpoint   = NT.EndPoint
type LLConnection = NT.Connection


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

queues :: Engine a -> MessageQueues a
queues (Simulation q _) = q
queues (Network q _ _) = q

workers :: Engine a -> Workers
workers (Simulation _ w) = w
workers (Network _ w _) = w

transport :: Engine a -> ITransport a
transport (Simulation q _)      = TRSim q
transport (Network _ _ (Transports (_,otr))) = TRNet otr

{- Constructors -}

simpleQueues :: Address -> IO (MessageQueues a)
simpleQueues addr = newMVar (addr, []) >>= return . Peer

simpleTransport :: Address -> IO (ITransport a)
simpleTransport addr = simpleQueues addr >>= return . TRSim

simpleEngine :: [Address] -> IO (Engine a)
simpleEngine [addr] = simpleQueues addr >>= return . flip Simulation Uniprocess
simpleEngine peers  = newMVar (H.fromList $ map (,[]) peers) >>= return . flip Simulation Uniprocess . ManyByPeer

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
