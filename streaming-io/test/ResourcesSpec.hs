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
import Network.Streaming.BufferPool
import Skeletest

spec :: Spec
spec = do
 describe "resource settings" $ do
  it "allocates the smallest positive counts as ordinary, reserve and setup blocks" $
    withTestPool minimumSettings $ \pool ->
      withRequestBuffers pool $ \access -> do
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferCapacity (loanBuffer primary) `shouldBe` 1
        bufferCapacity (loanBuffer extra) `shouldBe` 1
        withSetupBuffers @(Solo Buffer) pool $ \(MkSolo setup) ->
          bufferCapacity setup `shouldBe` 1
        releaseBuffer extra
        releaseBuffer primary
  it "keeps reserve independent even when it exceeds ordinary capacity" $
    withTestPool smallSettings {reserveBufferCount = positiveInt @3} $ \pool ->
      withRequestBuffers pool $ \access -> do
        timeout 20_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Nothing
        primary <- borrowBuffer access
        extra <- borrowBuffer access
        bufferMemory (loanBuffer primary) `shouldNotBe` bufferMemory (loanBuffer extra)
        releaseBuffer extra
        releaseBuffer primary
  it "allocates exactly the requested ordinary and setup counts at the requested size" $
    withTestPool smallSettings {bufferSize = positiveInt @7, ordinaryBufferCount = positiveInt @2, setupBufferCount = positiveInt @3} $ \pool ->
      withRequestBuffers pool $ \first -> withRequestBuffers pool $ \second -> do
        timeout 20_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Nothing
        primary <- borrowBuffer first
        otherPrimary <- borrowBuffer second
        extra <- borrowBuffer first
        map (bufferCapacity . loanBuffer) [primary, otherPrimary, extra] `shouldBe` [7, 7, 7]
        timeout 20_000 (void (borrowBuffer second)) `shouldReturn` Nothing
        withSetupBuffers @(Buffer, Buffer, Buffer) pool $ \(a, b, c) -> do
          map bufferCapacity [a, b, c] `shouldBe` [7, 7, 7]
          timeout 20_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Nothing
        releaseBuffer extra
        releaseBuffer otherPrimary
        releaseBuffer primary
  it "rejects a body count sum exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {ordinaryBufferCount = largest}
  it "rejects a body byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {bufferSize = largest}
  it "rejects a setup byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {bufferSize = positiveInt @2, setupBufferCount = largest}

 describe "caller-owned acquisition context" $ do
  it "keeps sibling inventory and the context usable after allocation" $
    getAcquisition >>= \waiting -> do
      sibling <- newBufferPool waiting smallSettings
      _ <- newBufferPool waiting smallSettings
      timeout 200_000 (withRequestBuffers sibling (const (pure ()))) `shouldReturn` Just ()
      assertAcquisitionUsable waiting
  it "keeps the context usable after an unsupported allocation fails" $
    getAcquisition >>= \waiting -> do
      largest <- requirePositive (toInteger (maxBound :: Int))
      result <- try @IOError (newBufferPool waiting minimumSettings {ordinaryBufferCount = largest})
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
    (count, buffers) <- newBuffers (positiveInt @21) (positiveInt @7)
    count `shouldBe` 3
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
      (count, buffers) <- newBuffers (positiveInt @8192) (positiveInt @4096)
      count `shouldBe` 2
      case buffers of
        [_, second] -> do
          withForeignPtr (bufferMemory second) $ \pointer -> pokeArray pointer (replicate 4096 73)
          pure second
        _ -> failTest "expected two buffers"
    performGC
    withForeignPtr (bufferMemory survivor) $ \pointer ->
      peekArray 4096 pointer `shouldReturn` replicate 4096 73
  it "leaves trailing budget bytes outside the exposed blocks" $ do
    (count, buffers) <- newBuffers (positiveInt @23) (positiveInt @7)
    count `shouldBe` 3
    map bufferCapacity buffers `shouldBe` [7, 7, 7]
  it "returns no slices when the block size exceeds the budget" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    forM_ [positiveInt @8, largest] $ \size -> do
      (count, buffers) <- newBuffers (positiveInt @7) size
      count `shouldBe` 0
      length buffers `shouldBe` 0

 describe "storage" $ do
  it "initializes blocks once and reuses their addresses and contents" $
    withTestPool smallSettings $ \pool -> do
      first <- withRequestBuffers pool $ \access -> do
        loan <- borrowBuffer access
        let buffer = loanBuffer loan
        bufferCapacity buffer `shouldBe` 8
        withForeignPtr (bufferMemory buffer) $ \pointer -> do
          peek pointer `shouldReturn` 0
          poke pointer 73
        releaseBuffer loan
        pure (bufferMemory buffer)
      withRequestBuffers pool $ \access -> do
        loan <- borrowBuffer access
        bufferMemory (loanBuffer loan) `shouldBe` first
        withForeignPtr (bufferMemory (loanBuffer loan)) $ \pointer -> peek pointer `shouldReturn` 73
        releaseBuffer loan

 describe "whole-block setup grants" $ do
  it "leaves remaining blocks usable when a larger grant must wait" $
    withTestPool smallSettings {setupBufferCount = positiveInt @3} $ \pool ->
      withSetupBuffers @(Solo Buffer) pool $ \(MkSolo first) -> do
        timeout 20_000 (withSetupBuffers @(Buffer, Buffer, Buffer) pool (const (pure ()))) `shouldReturn` Nothing
        withSetupBuffers @(Buffer, Buffer) pool $ \(second, third) -> do
          bufferMemory first `shouldNotBe` bufferMemory second
          bufferMemory first `shouldNotBe` bufferMemory third
          bufferMemory second `shouldNotBe` bufferMemory third
  it "returns five-block tuples without copying buffer payloads" $
    withTestPool smallSettings {setupBufferCount = positiveInt @5} $ \pool -> do
      original <- withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) pool $ \(a, b, c, d, e) -> do
        withForeignPtr (bufferMemory c) $ \pointer -> poke pointer 91
        pure (map bufferMemory [a, b, c, d, e])
      withSetupBuffers @(Buffer, Buffer, Buffer, Buffer, Buffer) pool $ \(a, b, c, d, e) -> do
        map bufferMemory [a, b, c, d, e] `shouldBe` original
        withForeignPtr (bufferMemory c) $ \pointer -> peek pointer `shouldReturn` 91
  it "rejects grants that exceed setup capacity" $
    withTestPool smallSettings $ \pool -> do
      result <- try @IOError (withSetupBuffers @(Buffer, Buffer) pool (const (pure ())))
      isLeft result `shouldBe` True

 describe "setup scope ownership" $ do
  it "keeps retained inventories reusable after an allocator helper returns" $ do
    pool <- withTestPool smallSettings pure
    performGC
    forM_ [1 .. 3 :: Int] $ \_ -> do
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Just ()
      timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()
  it "keeps retained inventories usable after their allocating helper throws" $ do
    retained <- newEmptyMVar
    result <- try @IOError $ withTestPool smallSettings $ \pool -> do
      putMVar retained pool
      ioError (userError "helper")
    isLeft result `shouldBe` True
    pool <- takeMVar retained
    performGC
    timeout 1_000_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Just ()
    timeout 1_000_000 (withRequestBuffers pool (const (pure ()))) `shouldReturn` Just ()
  it "returns buffers after callback exceptions" $
    withTestPool smallSettings $ \pool -> do
      result <- try @IOError (withSetupBuffers @(Solo Buffer) pool $ const $ ioError (userError "callback"))
      isLeft result `shouldBe` True
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Just ()
  it "returns buffers after asynchronous cancellation" $
    withTestPool smallSettings $ \pool -> do
      started <- newEmptyMVar
      hold <- newEmptyMVar
      done <- newEmptyMVar
      worker <- forkFinally
        (withSetupBuffers @(Solo Buffer) pool $ const $ putMVar started () >> takeMVar hold)
        (const $ putMVar done ())
      timeout 1_000_000 (takeMVar started) `shouldReturn` Just ()
      killThread worker
      timeout 1_000_000 (takeMVar done) `shouldReturn` Just ()
      timeout 1_000_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Just ()

 describe "setup capacity" $ do
  it "admits setup while all stream blocks are busy" $
    withTestPool smallSettings $ \pool -> withRequestBuffers pool $ \access -> do
      primary <- borrowBuffer access
      extra <- borrowBuffer access
      withSetupBuffers @(Solo Buffer) pool $ \(MkSolo setup) -> do
        bufferMemory setup `shouldNotBe` bufferMemory (loanBuffer primary)
        bufferMemory setup `shouldNotBe` bufferMemory (loanBuffer extra)
        timeout 20_000 (withSetupBuffers @(Solo Buffer) pool (const (pure ()))) `shouldReturn` Nothing
      releaseBuffer extra
      releaseBuffer primary
  it "reuses setup storage without taking a stream block" $
    withTestPool smallSettings $ \pool -> do
      setupMemory <- withSetupBuffers @(Solo Buffer) pool $ \(MkSolo buffer) -> pure (bufferMemory buffer)
      withRequestBuffers pool $ \access -> do
        stream <- borrowBuffer access
        withSetupBuffers @(Solo Buffer) pool $ \(MkSolo buffer) -> do
          bufferMemory buffer `shouldBe` setupMemory
          bufferMemory buffer `shouldNotBe` bufferMemory (loanBuffer stream)
        releaseBuffer stream

rejectsAllocation :: PoolSettings -> IO ()
rejectsAllocation settings = do
  result <- try @IOError (withTestPool settings (const (pure ())))
  case result of
    Left failure -> ioeGetErrorString failure `shouldBe` "buffer allocation exceeds Int capacity"
    Right () -> failTest "expected unsupported allocation to fail before allocating"

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt

withTestPool :: PoolSettings -> (BufferPool -> IO a) -> IO a
withTestPool settings action = getAcquisition >>= \waiting ->
  newBufferPool waiting settings >>= action

smallSettings :: PoolSettings
smallSettings = minimumSettings {bufferSize = positiveInt @8}

minimumSettings :: PoolSettings
minimumSettings = PoolSettings
  { bufferSize = positiveInt @1
  , ordinaryBufferCount = positiveInt @1
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @1
  }

assertAcquisitionUsable :: Acquire.Context -> IO ()
assertAcquisitionUsable waiting =
  Acquire.acquire waiting Acquire.NoDeadline
    retry
    (\waiter -> void (Acquire.tryGrant waiter (pure ())))
    (const (pure ())) (const (pure ())) `shouldReturn` Just ()
