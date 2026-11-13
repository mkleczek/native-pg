{-# LANGUAGE DataKinds #-}
module ResourcesSpec (spec) where

import Control.Exception (try)
import Control.Monad (forM_)
import System.IO.Error (ioeGetErrorString)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Array (peekArray, pokeArray)
import Foreign.Ptr (plusPtr)
import System.Mem (performGC)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt, positiveInt)
import Network.Streaming.Buffer
import Network.Streaming.BufferPool
import Skeletest

spec :: Spec
spec = do
 describe "resource settings" $ do
  it "rejects a body count sum exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {ordinaryBufferCount = largest}
  it "rejects a body byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {bufferSize = largest}
  it "rejects a setup byte product exceeding Int before allocation" $ do
    largest <- requirePositive (toInteger (maxBound :: Int))
    rejectsAllocation minimumSettings {bufferSize = positiveInt @2, setupBufferCount = largest}

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
    forM_ buffers $ (`withForeignPtr` (\pointer ->
      peekArray 7 pointer `shouldReturn` replicate 7 0)) . bufferMemory
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


rejectsAllocation :: BufferPoolSettings -> IO ()
rejectsAllocation settings = do
  result <- try @IOError (newBufferPool settings)
  case result of
    Left failure -> ioeGetErrorString failure `shouldBe` "buffer allocation exceeds Int capacity"
    Right _ -> failTest "expected unsupported allocation to fail before allocating"

requirePositive :: Integer -> IO PositiveInt
requirePositive = maybe (failTest "expected a positive Int") pure . mkPositiveInt

minimumSettings :: BufferPoolSettings
minimumSettings = BufferPoolSettings
  { bufferSize = positiveInt @1
  , ordinaryBufferCount = positiveInt @1
  , reserveBufferCount = positiveInt @1
  , setupBufferCount = positiveInt @1
  }
