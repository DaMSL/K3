{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DoAndIfThenElse #-}

module Language.K3.Runtime.Test (tests) where

import Control.Monad

import qualified Data.ByteString.Char8 as BS
import Data.List
import Data.Maybe

import qualified Network.Transport as NT

import System.Directory

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Language.K3.Core.Annotation
import Language.K3.Core.Common
import Language.K3.Core.Declaration
import Language.K3.Core.Expression
import Language.K3.Core.Type

import qualified Language.K3.Core.Constructor.Type        as TC
import qualified Language.K3.Core.Constructor.Expression  as EC
import qualified Language.K3.Core.Constructor.Declaration as DC

import Language.K3.Runtime.Engine
import Language.K3.Interpreter
import Language.K3.Pretty

constructBuffer :: [Test]
constructBuffer = [
      testCase "Empty exclusive singleton" $ empty $ exclusive emptySingletonBuffer    
    , testCase "Empty exclusive bounded"   $ empty $ exclusive ebBuf
    , testCase "Empty exclusive unbounded" $ empty $ exclusive emptyUnboundedBuffer
    , testCase "Empty shared singleton"    $ empty $ shared emptySingletonBuffer
    , testCase "Empty shared bounded"      $ empty $ shared ebBuf
    , testCase "Empty shared unbounded"    $ empty $ shared emptyUnboundedBuffer
    , testCase "Full exclusive singleton"  $ full $ exclusive (singletonBuffer 1) 
    , testCase "Full shared singleton"     $ full $ shared (singletonBuffer 1)
    , testCase "Full exclusive bounded"    $ optional (full . exclusive) $ bBuf [1,2,3]
    , testCase "Full shared bounded"       $ optional (full . shared) $ bBuf [1,2,3]
  ]
  where 
        failed   = assertFailure "Constructor failed"
        empty b  = b >>= emptyEBuffer >>= flip unless failed
        full b   = b >>= fullEBuffer >>= flip unless failed
        ebBuf    = emptyBoundedBuffer $ defaultBufferSpec defaultConfig 
        bBuf l   = boundedBuffer (boundedBufferSpec $ length l) l
        optional = maybe failed
        boundedBufferSpec n = BufferSpec n $ min n 10

readBuffer :: [Test]
readBuffer = [
      testCase "Read empty"          $ readEmpty $ exclusive emptySingletonBuffer 
    , testCase "Read singleton"      $ readValid 1 $ exclusive (singletonBuffer 1) 
    , testCase "Read unbounded head" $ optional (readValid 1 . exclusive) $ unboundedBuffer [1,2,3]
  ]
  where success = return ()
        failed  = assertFailure "Read buffer failed"
        readEmpty b     = b >>= readEBuffer >>= maybe success (\_ -> failed)
        readValid v b   = b >>= readEBuffer >>= maybe failed (flip unless failed . (v ==))
        optional        = maybe failed

appendBuffer :: [Test]
appendBuffer = [
      testCase "Append empty" $ appendValid 1 $ exclusive emptySingletonBuffer
    , testCase "Append any"   $ optional (appendValid 1 . exclusive) $ unboundedBuffer [1,2,3]
    , testCase "Append limit" $ optional (appendValid 1 . exclusive) $ bBuf [1,2,3] 4
    , testCase "Append full"  $ optional (appendFailed 1 . exclusive) $ bBuf [1,2,3] 3
  ]
  where failed = assertFailure "Append buffer failed"
        appendValid v b         = doAppend v b >>= testAppendOverflow isNothing
        appendFailed v b        = doAppend v b >>= testAppendOverflow (maybe False (v ==))
        doAppend v b            = b >>= appendEBuffer v
        testAppend f g (nb, rv) = unless (f nb && g rv) failed
        testAppendOverflow f    = testAppend (\_ -> True) f
        testAppendBuffer f      = testAppend f (\_ -> True)
        
        optional            = maybe failed
        bBuf l sz           = boundedBuffer (boundedBufferSpec sz) l
        boundedBufferSpec n = BufferSpec n $ min n 10

takeBuffer :: [Test]
takeBuffer = [
      testCase "Take empty/1" $ takeFailed $ exclusive $ emptySingletonBuffer
    , testCase "Take empty/2" $ takeFailed $ exclusive $ ebBuf
    , testCase "Take empty/3" $ takeFailed $ exclusive $ emptyUnboundedBuffer
    , testCase "Take any"     $ optional (takeValid 1 . exclusive) $ unboundedBuffer [1,2,3]
    , testCase "Take limit"   $ optional (takeValid 1 . exclusive) $ unboundedBuffer [1]
    , testCase "Take full"    $ optional (takeValid 1 . exclusive) $ bBuf [1,2,3] 3
  ]
  where failed = assertFailure "Take buffer failed"
        takeValid v b         = doTake b >>= testTakeResult (maybe False (v ==))
        takeFailed b          = doTake b >>= testTakeResult isNothing
        doTake b              = b >>= takeEBuffer
        testTake f g (nb, rv) = unless (f nb && g rv) failed
        testTakeResult f      = testTake (\_ -> True) f
        testTakeBuffer f      = testTake f (\_ -> True)

        optional            = maybe failed
        ebBuf               = emptyBoundedBuffer $ defaultBufferSpec defaultConfig 
        bBuf l sz           = boundedBuffer (boundedBufferSpec sz) l
        boundedBufferSpec n = BufferSpec n $ min n 10

flushBuffer :: [Test]
flushBuffer = []

refreshBuffer :: [Test]
refreshBuffer = []

bufferTests :: [Test]
bufferTests = [
      testGroup "Construction" constructBuffer
    , testGroup "Read"         readBuffer
    , testGroup "Append"       appendBuffer
    , testGroup "Take"         takeBuffer
    , testGroup "Flush"        flushBuffer
    , testGroup "Refresh"      refreshBuffer
  ]

transportTests :: [Test]
transportTests = [
      testCase "NEndpoint"   $ withEndpoint "receiver" recvr (\_ -> success)
    , testCase "NConnection" $ withEndpointPair (\x y -> newConnection (fst x) (snd y) >>= failNothing)
    
    , testCase "Send data" $ 
        let testData = ["hello", "this", "is", "K3"]
            expected = (:[]) . concatMap id
              -- Above, NT uses Network.Socket.ByteString.sendMany for its sends.
              -- This has the effect of concatenating data segments.
            
            sendData ep (conn -> c) = NT.send c (map BS.pack testData)
            
            recvData (endpoint -> ep) =
              testRecvSeq ep [ (matchConnectionOpen, Nothing, "connect")
                             , (matchReceive (expected testData), Just showReceive, "data/1") ]

        in withEndpointPair (\x y -> 
              newConnection (fst x) (snd y) >>= optional (\c -> 
                sendData (snd x) c >>= either (\_ -> failed) (\_ -> recvData $ snd x)))

    -- TODO
    , testCase "Value write description" $ return ()
    , testCase "Expression wire description" $ return ()
  ]
  where success     = return ()
        failed      = assertFailure "Transport test failed"
        failedS s   = assertFailure $ "Transport test failed: " ++ s
        failNothing = flip unless failed . isJust

        parens s    = "(" ++ s ++ ")"

        optional            = maybe failed
        optionalSnd f (x,y) = optional (f x) y

        recvr = newEndpoint defaultAddress >>= return . (defaultAddress,)
        sendr = let addr = internalSendAddress defaultAddress in newEndpoint addr >>= return . (addr,)

        withEndpoint n ep f = do
          (addr, opt) <- ep
          if not $ isJust opt then failedS $ n ++ " " ++ (parens $ show addr)
          else f (addr, fromJust opt) >> closeEndpoint (fromJust opt)

        withEndpointPair f = withEndpoint "receiver" recvr (\r -> withEndpoint "sender " sendr (f r))

        matchConnectionOpen (NT.ConnectionOpened _ _ _) = True
        matchConnectionOpen _ = False

        matchReceive expected (NT.Received _ msgs) = (map BS.unpack msgs == expected)
        matchReceive _ _ = False

        showReceive (NT.Received _ msgs) = putStrLn $ "Data:\n" ++ (intercalate "\n" $ map BS.unpack msgs)
        showReceive _ = return ()

        testRecvSeq ep evtSeq = 
          let testMsg (testF, showF, s) evt = 
                evt >>= (\e -> maybe (return ()) ($ e) showF >> unless (testF e) (failedS s))
          in mapM_ (uncurry testMsg) $ zip evtSeq (repeat $ NT.receive ep)


connectionTests :: [Test]
connectionTests = []

endpointTests :: [Test]
endpointTests = []

handleTests :: [Test]
handleTests = [
      testCase "Open readable file" $ 
        let (n, path) = ("testSource", "data/expr-i.txt") in
        withSimulation (\eg -> openFile n path syntaxValueWD Nothing "r" eg >> failExternalEndpoint n eg)

    , testCase "Open writeable file" $ 
        let (n, path) = ("testSink", "data/out.txt") in
        withFile n path "w" (const $ failPath path) (const $ return ()) 

    , testCase "Write to file" $
        let (n, path) = ("testSink", "data/out.txt") in
        withFile n path "w" (const $ failPath path) (doWrite n $ VInt 1)

    , testCase "Read from same file" $
        let (n, path) = ("testSource", "data/out.txt") in
        withFile n path "r" failRead (doRead n)

    , testCase "Read simple syntax values from file" $
        readAndShowValue "testSource" "data/expr-i.txt"

    , testCase "Read complex syntax values from file" $
        readAndShowValue "testSource" "data/expr-ii.txt"    

    -- TODO
    , testCase "Open readable socket"  $ return ()
    , testCase "Open writeable socket" $ return ()

    , testCase "Read available"   $ return ()
    , testCase "Write available"  $ return ()

    , testCase "Read from socket" $ return ()
    , testCase "Write to socket"  $ return ()
  ]
  where failed = assertFailure "Handle test failed"
        
        failInternalEndpoint n (endpoints -> EEndpointState ieps _) = getEndpoint n ieps >>= flip unless failed . isJust
        failExternalEndpoint n (endpoints -> EEndpointState _ eeps) = getEndpoint n eeps >>= flip unless failed . isJust

        failPath p = doesFileExist p >>= flip unless failed
        failRead   = flip when failed . isNothing
        
        withSimulation f = simulationEngine defaultSystem syntaxValueWD >>= f

        withFile n path mode test f = 
          withSimulation (\eg -> 
            openFile n path syntaxValueWD Nothing mode eg 
              >> f eg >>= (\v -> close n eg >> return v)) >>= test

        readAndShowValue n path =
          withFile n path "r" failRead (\eg -> do
            v <- doRead n eg
            print v
            return v)

notificationTests :: [Test]
notificationTests = [
      testCase "File data notification"  $ return ()
    , testCase "File close notification" $ return ()

    , testCase "Socket accept notification" $ return ()
    , testCase "Socket data notification"   $ return ()
    , testCase "Socket close notification"  $ return ()
  ]
  where failed = assertFailure "Notification test failed"

engineTests :: [Test]
engineTests = [
      testCase "Simulation engine constructor" $ 
        simulationEngine defaultSystem exprWD >>= flip unless failed . simulation

    -- TODO
    , testCase "Network engine constructor" $ return ()

    , testCase "Queue construction/1" $ return ()
    , testCase "Queue construction/2" $ return ()
    , testCase "Queue construction/3" $ return ()

    , testCase "Message enqueuing" $ return ()
    , testCase "Message dequeuing" $ return ()

    , testCase "Send message" $ return ()
  ]
  where failed = assertFailure "Engine test failed"

tests :: [Test]
tests = [
    testGroup "Buffers"       bufferTests,
    testGroup "Transport"     transportTests,
    testGroup "Connections"   connectionTests,
    testGroup "Endpoints"     endpointTests,
    testGroup "Handles"       handleTests,
    testGroup "Notifications" notificationTests,
    testGroup "Engine"        engineTests
  ]