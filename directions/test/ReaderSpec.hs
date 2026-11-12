module ReaderSpec (spec) where

import Control.Arrow (ArrowChoice, returnA, (>>>), (>>^), (^>>))
import Control.Exception (IOException, evaluate, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Either (isLeft)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Network.Directions.Classes
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Memory
import Network.Directions.Reader
import Network.Directions.Resumable
import Network.Directions.Rows
import Network.Directions.Threads
import Skeletest

spec :: Spec
spec = describe "the reader" $ do
  describe "rows of five fields" $ do
    it "add up to the checksum that the wire was built with, with every arrow, whatever a receive gives" $
      forM_ [maxBound, 1460, 97, 7, 1] $ \limit ->
        forM_ [16384, 64] $ \capacity ->
          runAll (pure . foldRows checksum) 0 rowsOf1000 limit capacity `shouldReturn` replicate 3 (Right (rowsChecksum 1000))

    it "are parsed into the fields they were built from, in order" $
      forM_ [maxBound, 1460] $ \limit -> do
        parsed <- runAll (pure . foldRows (flip (:))) [] rowsOf1000 limit 16384
        parsed `shouldBe` replicate 3 (Right (reverse (map rowFields [0 .. 999])))

  describe "a field larger than the buffer" $
    it "streams in pieces no larger than the buffer, which reassemble to the field" $
      forM_ [maxBound, 1460, 100] $ \limit -> do
        results <- runAll collected () (streamWire field) limit 4096
        forM_ results $ \result -> do
          pieces <- either (failTest . show) pure result
          BS.concat pieces `shouldBe` field
          all ((<= 4096) . BS.length) pieces `shouldBe` True
          (length pieces > 24) `shouldBe` True

  describe "bytes" $
    it "fail for more than the buffer holds, and give as many as it holds, however a receive gives" $
      forM_ [maxBound, 5] $ \limit -> do
        over <- runAll (\reader -> pure (const 17 ^>> bytes reader)) () rowsOf1000 limit 16
        all isLeft over `shouldBe` True
        length over `shouldBe` 3
        exactly <- runAll (\reader -> pure (const 16 ^>> bytes reader)) () rowsOf1000 limit 16
        exactly `shouldBe` replicate 3 (Right (BS.take 16 rowsOf1000))

  describe "resumable parsers" $ do
    it "add up the rows to the same checksum, with every arrow, whatever a receive gives" $
      forM_ [maxBound, 1460, 97, 7, 1] $ \limit ->
        forM_ [16384, 64] $ \capacity ->
          resumed (\memory buffer -> pure (parse memory buffer (returnA >>^ const ()) rowsResumable >>^ fst)) rowsOf1000 limit capacity
            `shouldReturn` replicate 3 (rowsChecksum 1000)

    it "yield a field larger than the buffer as pieces that reassemble to it" $
      forM_ [maxBound, 1460, 100] $ \limit -> do
        results <- resumed resumedPieces (streamWire field) limit 4096
        forM_ results $ \pieces -> do
          BS.concat pieces `shouldBe` field
          all ((<= 4096) . BS.length) pieces `shouldBe` True
          (length pieces > 24) `shouldBe` True

-- | A program over a reader of a memory link, for every arrow.
type Program a b = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => Reader Memory -> IO (arr a b)

-- | A program that is made in IO, once the reader is there, for every arrow.
type Made a b = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => IO (arr a b)

-- | Runs the program on the wire with each arrow, a reader of the capacity,
-- and a link that gives at most the limit at once: what each gave, or how it
-- failed.
runAll :: forall a b. Program a b -> a -> ByteString -> Int -> Int -> IO [Either IOException b]
runAll program input wire limit capacity =
  sequence
    [ over (\made -> made >>= (`runThreads` input)),
      over (\made -> made >>= \arrow -> fst <$> runCompiled arrow input),
      over (\made -> fst <$> runDelimitedWith made input)
    ]
  where
    over :: (Made a b -> IO b) -> IO (Either IOException b)
    over run = try $ do
      memory <- newMemory wire limit
      reader <- newReader memory capacity
      run (program reader)

-- | The pieces of the stream's fields, copied, in order.
collected :: Program () [ByteString]
collected reader = do
  kept <- newIORef []
  pure (streamMessages reader (keeping kept) >>> now (\() -> reverse <$> readIORef kept))

keeping :: (Effect arr) => IORef [ByteString] -> arr ByteString ()
keeping kept = now (\piece -> evaluate (BS.copy piece) >>= \owned -> modifyIORef' kept (owned :))

-- | A resumable parser over a memory link, driven by an arrow.
type Resumed a = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => Memory -> Buffer -> IO (arr ByteString a)

-- | A resumable parser that is made in IO, for every arrow.
type MadeFrom a = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => IO (arr ByteString a)

-- | Runs the parser on the wire with each arrow: what each gave.
resumed :: forall a. Resumed a -> ByteString -> Int -> Int -> IO [a]
resumed program wire limit capacity =
  sequence
    [ over (\made -> made >>= (`runThreads` BS.empty)),
      over (\made -> made >>= \arrow -> fst <$> runCompiled arrow BS.empty),
      over (\made -> fst <$> runDelimitedWith made BS.empty)
    ]
  where
    over :: (MadeFrom a -> IO a) -> IO a
    over run = do
      memory <- newMemory wire limit
      buffer <- newBuffer capacity
      run (program memory buffer)

-- | The pieces that the resumable parser of the stream yields, copied.
resumedPieces :: Resumed [ByteString]
resumedPieces memory buffer = do
  kept <- newIORef []
  pure (parse memory buffer (keeping kept) streamResumable >>> now (\_ -> reverse <$> readIORef kept))

rowsOf1000 :: ByteString
rowsOf1000 = rowsWire 1000

field :: ByteString
field = BS.pack (take 100000 (cycle [97 .. 122]))
