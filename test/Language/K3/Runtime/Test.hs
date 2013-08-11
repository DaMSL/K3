module Language.K3.Runtime.Test (tests) where

import Control.Monad

import Data.Maybe

import Test.HUnit hiding (Test)
import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit

import Language.K3.Runtime.Engine

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
        failed  = assertFailure "Constructor failed"
        empty b = b >>= emptyEBuffer >>= flip unless failed
        full b  = b >>= fullEBuffer >>= flip unless failed
        ebBuf   = emptyBoundedBuffer $ defaultBufferSpec defaultConfig 
        bBuf l  = boundedBuffer (boundedBufferSpec $ length l) l
        optional f bOpt = maybe failed f bOpt
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
        optional f bOpt = maybe failed f bOpt

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
        
        optional f bOpt     = maybe failed f bOpt
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

        optional f bOpt     = maybe failed f bOpt
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
transportTests = []

connectionTests :: [Test]
connectionTests = []

endpointTests :: [Test]
endpointTests = []

handleTests :: [Test]
handleTests = []

engineTests :: [Test]
engineTests = []

tests :: [Test]
tests = [
    testGroup "Buffers"     bufferTests,
    testGroup "Transport"   transportTests,
    testGroup "Connections" connectionTests,
    testGroup "Endpoints"   endpointTests,
    testGroup "Handles"     handleTests,
    testGroup "Engine"      engineTests
  ]