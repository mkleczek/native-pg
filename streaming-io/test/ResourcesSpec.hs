{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent.Resource.Acquisition qualified as Acquire
import Control.Exception (try)
import Control.Monad (forM_, void)
import Control.Concurrent (forkFinally, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import System.Timeout (timeout)
import System.IO.Error (ioeGetErrorString)
import Control.Concurrent.STM (retry)
import Data.Tuple (Solo (MkSolo))
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Array (peekArray, pokeArray)
import Foreign.Storable (peek, poke)
import Foreign.Ptr (plusPtr)
import System.Mem (performGC)
import Data.Either (isLeft)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveInt)
import Network.Streaming.Buffer
import Network.Streaming.Resources qualified as Resources
import Network.Streaming.Resources
import Skeletest

spec :: Spec
spec = do
 describe "resource settings" $ do
  xfail "Count-based pool allocation follows in the implementation change" $ it "allocates the smallest positive counts as ordinary, reserve and setup blocks" $
    withTestResources (ResourceSettings (positiveInt @1) (positiveInt @1) (positiveInt @1) (positiveInt @1)) $ \resources ->
      withRequestBuffers resources $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferCapacity (loanBuffer primary) `shouldBe` 1
        bufferCapacity (loanBuffer extra) `shouldBe` 1
        withSetupBuffers @(Solo Buffer) resources $ \(MkSolo setup) ->
          bufferCapacity setup `shouldBe` 1
        releaseBuffer extra
        releaseBuffer primary
  xfail "Count-based pool allocation follows in the implementation change" $ it "keeps reserve independent even when it exceeds ordinary capacity" $
    withTestResources (ResourceSettings (positiveInt @8) (positiveInt @1) (positiveInt @3) (positiveInt @1)) $ \resources ->
      withRequestBuffers resources $ \access -> do
        timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
        releaseBuffer extra
        releaseBuffer primary
  xfail "Count-based pool allocation follows in the implementation change" $ it "allocates exactly the requested ordinary and setup counts at the requested size" $
    withTestResources (ResourceSettings (positiveInt @7) (positiveInt @2) (positiveInt @1) (positiveInt @3)) $ \resources ->
      withRequestBuffers resources $ \first -> withRequestBuffers resources $ \second -> do
        timeout 20_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Nothing
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        extra <- borrowBuffer first
        map (bufferCapacity . loanBuffer) [primary, otherPrimary, extra] `shouldBe` [7, 7, 7]
        timeout 20_000 (void (borrowBuffer second)) `shouldReturn` Nothing
        withSetupBuffers @(Buffer, Buffer, Buffer) resources $ \(a, b, c) -> do
          map bufferCapacity [a, b, c] `shouldBe` [7, 7, 7]
          timeout 20_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Nothing
        releaseBuffer extra
        releaseBuffer otherPrimary
        releaseBuffer primary
  xfail "Count-based pool allocation follows in the implementation change" $ it "rejects a body count sum exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation (ResourceSettings (positiveInt @1) largest (positiveInt @1) (positiveInt @1))
  xfail "Count-based pool allocation follows in the implementation change" $ it "rejects a body byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation (ResourceSettings largest (positiveInt @1) (positiveInt @1) (positiveInt @1))
  xfail "Count-based pool allocation follows in the implementation change" $ it "rejects a setup byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation (ResourceSettings (positiveInt @2) (positiveInt @1) (positiveInt @1) largest)

 describe "caller-owned acquisition context" $ do
  it "keeps the context usable after an unsupported allocation fails" $
    getAcquisition >>= \waiting -> do
      largest <- requirePositive (toInteger (maxBound :: Int))
      result <- try @IOError (withResources waiting (ResourceSettings (positiveInt @1) largest (positiveInt @1) (positiveInt @1)) (const (pure ())))
      isLeft result `shouldBe` True
      assertAcquisitionUsable waiting

 describe "buffer initialization" $ do
  it "allocates initialized storage with the requested capacity" $ do
    buffer <- newBuffer (positiveInt @8)
    bufferCapacity buffer `shouldBe` 8
    withForeignPtr (bufferMemory buffer) $ \pointer ->
      peekArray 8 pointer `shouldReturn` replicate 8 0
  it "accepts checked runtime capacities" $ do
    size <- requirePositive 7
    buffer <- newBuffer size
    bufferCapacity buffer `shouldBe` 7
    withForeignPtr (bufferMemory buffer) $ \pointer ->
      peekArray 7 pointer `shouldReturn` replicate 7 0

 describe "contiguous buffers" $ do
  it "returns adjacent initialized slices whose writes remain disjoint" $ do
    buffers <- newBuffers (positiveInt @21) (positiveInt @7)
    map bufferCapacity buffers `shouldBe` [7, 7, 7]
    forM_ buffers $ \buffer -> withForeignPtr (bufferMemory buffer) $ \pointer ->
      peekArray 7 pointer `shouldReturn` replicate 7 0
    case buffers of
      first : _ -> withForeignPtr (bufferMemory first) $ \start ->
        forM_ (zip [0 ..] buffers) $ \(index, buffer) ->
          withForeignPtr (bufferMemory buffer) $ \pointer ->
            pointer `shouldBe` start `plusPtr` (index * 7)
      [] -> failTest "expected three buffers"
    forM_ (zip [11, 22, 33] buffers) $ \(value, buffer) ->
      withForeignPtr (bufferMemory buffer) $ \pointer -> pokeArray pointer (replicate 7 value)
    forM_ (zip [11, 22, 33] buffers) $ \(value, buffer) ->
      withForeignPtr (bufferMemory buffer) $ \pointer ->
        peekArray 7 pointer `shouldReturn` replicate 7 value
  it "keeps the backing block alive when only a later slice remains" $ do
    survivor <- do
      buffers <- newBuffers (positiveInt @8192) (positiveInt @4096)
      case buffers of
        [_, second] -> do
          withForeignPtr (bufferMemory second) $ \pointer -> pokeArray pointer (replicate 4096 73)
          pure second
        _ -> failTest "expected two buffers"
    performGC
    withForeignPtr (bufferMemory survivor) $ \pointer ->
      peekArray 4096 pointer `shouldReturn` replicate 4096 73
  it "leaves trailing budget bytes outside the exposed blocks" $ do
    buffers <- newBuffers (positiveInt @23) (positiveInt @7)
    map bufferCapacity buffers `shouldBe` [7, 7, 7]
  it "returns no slices when the block size exceeds the budget" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    forM_ [positiveInt @8, largest] $ \size ->
      length <$> newBuffers (positiveInt @7) size `shouldReturn` 0

 describe "storage" $ do
  xfail "Request capability scaffold: scoped loans follow in the next change" $ it "initializes blocks once and reuses their addresses and contents" $
    withTestResources smallSettings $ \resources -> do
      first <- withRequestBuffers resources $ \access -> do
        loan <- borrowBuffer access
        let buffer = loanBuffer loan
        bufferCapacity buffer `shouldBe` 8
        withForeignPtr (bufferMemory buffer) $ \pointer -> do
          peek pointer `shouldReturn` 0
          poke pointer 73
        releaseBuffer loan
        pure (bufferMemory buffer)
      withRequestBuffers resources $ \access -> do
        loan <- borrowBuffer access
        bufferMemory (loanBuffer loan) `shouldBe` first
        withForeignPtr (bufferMemory (loanBuffer loan)) $ \pointer -> peek pointer `shouldReturn` 73
        releaseBuffer loan

 describe "whole-block setup grants" $ do
  it "leaves remaining blocks usable when a larger grant must wait" $
    withTestResources smallSettings {setupMemoryBytes = positiveInt @24} $ \resources ->
      withSetupBuffers @(Solo Buffer) resources $ \(MkSolo first) -> do
        timeout 20_000 (withSetupBuffers @(Buffer, Buffer, Buffer) resources (const (pure ()))) `shouldReturn` Nothing
        withSetupBuffers @(Buffer, Buffer) resources $ \(second, third) -> do
          bufferMemory first `shouldNotBe` bufferMemory second
          bufferMemory first `shouldNotBe` bufferMemory third
          bufferMemory second `shouldNotBe` bufferMemory third
  it "returns five-block tuples without copying buffer payloads" $
    withTestResources smallSettings {setupMemoryBytes = positiveInt @40} $ \resources -> do
      original <- withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) resources $ \(a, b, c, d, e) -> do
        withForeignPtr (bufferMemory c) $ \pointer -> poke pointer 91
        pure (map bufferMemory [a, b, c, d, e])
      withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) resources $ \(a, b, c, d, e) -> do
        map bufferMemory [a, b, c, d, e] `shouldBe` original
        withForeignPtr (bufferMemory c) $ \pointer -> peek pointer `shouldReturn` 91
  it "rejects grants that exceed setup capacity" $
    withTestResources smallSettings $ \resources -> do
      result <- try @IOError (withSetupBuffers @(Buffer, Buffer) resources (const (pure ())))
      isLeft result `shouldBe` True

 describe "setup scope ownership" $ do
  xfail "Retained inventories follow in the implementation change" $ it "keeps retained inventories reusable after an allocator helper returns" $ do
    resources <- withTestResources smallSettings pure
    performGC
    forM_ [1 .. 3 :: Int] $ \_ -> do
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Just ()
      timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()
  xfail "Retained inventories follow in the implementation change" $ it "keeps retained inventories usable after their allocating helper throws" $ do
    retained <- newEmptyMVar
    result <- try @IOError $ withTestResources smallSettings $ \resources -> do
      putMVar retained resources
      ioError (userError "helper")
    isLeft result `shouldBe` True
    resources <- takeMVar retained
    performGC
    timeout 1_000_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Just ()
    timeout 1_000_000 (withRequestBuffers resources (const (pure ()))) `shouldReturn` Just ()
  it "returns buffers after callback exceptions" $
    withTestResources smallSettings $ \resources -> do
      result <- try @IOError (withSetupBuffers @(Solo Buffer) resources $ const $ ioError (userError "callback"))
      isLeft result `shouldBe` True
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Just ()
  it "returns buffers after asynchronous cancellation" $
    withTestResources smallSettings $ \resources -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withSetupBuffers @(Solo Buffer) resources $ const $ putMVar started () >> takeMVar hold)
        (const $ putMVar done ())
      timeout 1_000_000 (takeMVar started) `shouldReturn` Just ()
      killThread worker
      timeout 1_000_000 (takeMVar done) `shouldReturn` Just ()
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Just ()

 describe "setup capacity" $ do
  xfail "Request capability scaffold: scoped loans follow in the next change" $ it "admits setup while all stream blocks are busy" $
    withTestResources smallSettings $ \resources -> withRequestBuffers resources $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      withSetupBuffers @(Solo Buffer) resources $ \(MkSolo setup) -> do
        bufferMemory setup `shouldNotBe` bufferMemory (loanBuffer primary)
        bufferMemory setup `shouldNotBe` bufferMemory (loanBuffer extra)
        timeout 20_000 (withSetupBuffers @(Solo Buffer) resources (const (pure ()))) `shouldReturn` Nothing
      releaseBuffer extra
      releaseBuffer primary
  xfail "Request capability scaffold: scoped loans follow in the next change" $ it "reuses setup storage without taking a stream block" $
    withTestResources smallSettings $ \resources -> do
      setupMemory <- withSetupBuffers @(Solo Buffer) resources $ \(MkSolo buffer) -> pure (bufferMemory buffer)
      withRequestBuffers resources $ \access -> do
        stream <- borrowBuffer access
        withSetupBuffers @(Solo Buffer) resources $ \(MkSolo buffer) -> do
          bufferMemory buffer `shouldBe` setupMemory
          bufferMemory buffer `shouldNotBe` bufferMemory (loanBuffer stream)
        releaseBuffer stream

rejectsAllocation :: ResourceSettings -> IO ()
rejectsAllocation settings = do
  result <- try @IOError (withTestResources settings (const (pure ())))
  case result of
    Left failure -> ioeGetErrorString failure `shouldBe` "buffer allocation exceeds Int capacity"
    Right () -> failTest "expected unsupported allocation to fail before allocating"

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt

withTestResources :: ResourceSettings -> (Resources.Resources -> IO a) -> IO a
withTestResources settings action = getAcquisition >>= \waiting ->
  Resources.withResources waiting settings action

smallSettings :: ResourceSettings
smallSettings = ResourceSettings (positiveInt @16) (positiveInt @8) (positiveInt @8) (positiveInt @1)

assertAcquisitionUsable :: Acquire.Context -> IO ()
assertAcquisitionUsable waiting =
  Acquire.acquire waiting Acquire.NoDeadline
    retry
    (\waiter -> void (Acquire.tryGrant waiter (pure ())))
    (const (pure ())) (const (pure ())) `shouldReturn` Just ()
