{-# LANGUAGE BangPatterns #-}

-- | Parses the same bytes with native-pg's 'Input' monad and with arrows over
-- a 'Reader' of the directions prototype, under each way to run an arrow, and
-- says what each did: how fast, and how much it allocated (needs @+RTS -T@).
-- The bytes are in memory and the link never waits, so only the parsing is
-- measured. Two workloads: a million DataRow messages of five fields, and one
-- DataRow of one field that is a whole response body, streamed to a consumer
-- that counts it; each with a link that gives what the room takes, and one
-- that gives 1,460 bytes at most.
--
-- Usage: @parsers-bench [rounds [rows [MiB]]]@. Every variant runs once in each
-- round, in turn; the end of the output has the range over the rounds.
module Main (main) where

import Control.Arrow (Arrow, returnA, (>>^))
import Control.Exception (evaluate)
import Control.Monad (forM, forM_, replicateM, replicateM_, unless, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Word (Word64)
import Database.PostgreSQL.Internal.Protocol.Codec.Input
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import qualified Network.Directions.Classes as Directions
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Memory
import Network.Directions.Reader
import Network.Directions.Resumable
import Network.Directions.Rows
import Network.Directions.Threads
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.PositiveInt (mkPositiveInt)
import System.Environment (getArgs)
import System.Exit (die)
import System.Mem (performMajorGC)
import Text.Printf (printf)

-- | The size of the buffer that every variant parses in.
capacity :: Int
capacity = 16384

-- | One way to parse: what it gives for each workload, which is the rows'
-- checksum, and the count of the bytes of the field.
data Variant = Variant
  { variantName :: String,
    variantRows :: Memory -> IO Int,
    variantStream :: Memory -> IO Int
  }

variants :: [Variant]
variants =
  [ Variant "A input" (`inputRun` inputRows) (`inputRun` inputStream),
    Variant
      "B threads"
      (\memory -> withReader memory (\reader -> runThreads (foldRows checksum reader) 0))
      (streamed (\counter reader -> runThreads (streamMessages reader (counting counter)) ())),
    Variant
      "B compiled"
      (\memory -> withReader memory (\reader -> fst <$> runCompiled (foldRows checksum reader) 0))
      (streamed (\counter reader -> fst <$> runCompiled (streamMessages reader (counting counter)) ())),
    Variant
      "B delimited"
      (\memory -> withReader memory (\reader -> fst <$> runDelimited (foldRows checksum reader) 0))
      (streamed (\counter reader -> fst <$> runDelimited (streamMessages reader (counting counter)) ())),
    Variant
      "C compiled"
      (\memory -> resumable (\buffer -> fst . fst <$> runCompiled (parse memory buffer ignored rowsResumable) BS.empty))
      (streamedResumable (\memory buffer counter -> void (runCompiled (parse memory buffer (counting counter) streamResumable) BS.empty))),
    Variant
      "C delimited"
      (\memory -> resumable (\buffer -> fst . fst <$> runDelimited (parse memory buffer ignored rowsResumable) BS.empty))
      (streamedResumable (\memory buffer counter -> void (runDelimited (parse memory buffer (counting counter) streamResumable) BS.empty)))
  ]

withReader :: Memory -> (Reader Memory -> IO a) -> IO a
withReader memory use = newReader memory capacity >>= use

-- | Runs the program that streams the field to a counter, and gives the count.
streamed :: (Counter -> Reader Memory -> IO ()) -> Memory -> IO Int
streamed run memory = do
  counter <- newCounter
  withReader memory (run counter)
  counted counter

-- | Runs the program of a resumable parser, which is given a buffer.
resumable :: (Directions.Buffer -> IO a) -> IO a
resumable run = Directions.newBuffer capacity >>= run

-- | The same for the program that streams the field to a counter.
streamedResumable :: (Memory -> Directions.Buffer -> Counter -> IO ()) -> Memory -> IO Int
streamedResumable run memory = do
  counter <- newCounter
  resumable (\buffer -> run memory buffer counter)
  counted counter

-- | The consumer of a parser that yields nothing.
ignored :: (Arrow arr) => arr ByteString ()
ignored = returnA >>^ const ()

-- | Runs the parser of today's input over the link, in a buffer of the size.
inputRun :: Memory -> Input a -> IO a
inputRun memory parser = do
  storage <- maybe (ioError (userError "the capacity is not positive")) newBuffer (mkPositiveInt (toInteger capacity))
  fst <$> runInput (InputSource (memoryReceive memory) capacity) (reusableInputBuffer storage) parser

-- | The rows' checksum, as the arrows' 'foldRows' gives it.
inputRows :: Input Int
inputRows = loop 0
  where
    loop !total =
      readInputWord8 >>= \tag -> case tag of
        68 -> readInputWord32 *> row >>= \fields -> loop (checksum total fields)
        67 -> readInputWord32 >>= \size -> total <$ readInputExactly (fromIntegral size - 4)
        _ -> failInput ("a message with the tag " <> show tag)
    row = readInputWord16 >>= \count -> replicateM (fromIntegral count) (readInputWord32 >>= readInputExactly . fromIntegral)

-- | The bytes of the fields, counted in the chunks that native-pg's own reads
-- of a field give.
inputStream :: Input Int
inputStream = loop 0
  where
    loop !total =
      readInputWord8 >>= \tag -> case tag of
        68 -> readInputWord32 *> readInputWord16 >>= \count -> fields (fromIntegral count) total >>= loop
        67 -> readInputWord32 >>= \size -> total <$ readInputExactly (fromIntegral size - 4)
        _ -> failInput ("a message with the tag " <> show tag)
    fields :: Int -> Int -> Input Int
    fields 0 total = pure total
    fields count total = readInputWord32 >>= \size -> chunks (fromIntegral size) total >>= fields (count - 1)
    chunks :: Int -> Int -> Input Int
    chunks 0 total = pure total
    chunks left total = readInputBorrowedSome left >>= \chunk -> chunks (left - BS.length chunk) (total + BS.length chunk)

-- | The two workloads, and what a variant has to give for each.
data Workload = Rows | Stream
  deriving (Eq, Show)

-- | The seconds a run took and the bytes it allocated.
data Sample = Sample !Double !Word64

sampleSeconds :: Sample -> Double
sampleSeconds (Sample seconds _) = seconds

-- | How long the passes of an action took, and how much they allocated.
sample :: Int -> IO () -> IO Sample
sample passes pass = do
  performMajorGC
  before <- allocated_bytes <$> getRTSStats
  started <- getMonotonicTime
  replicateM_ passes pass
  ended <- getMonotonicTime
  after <- allocated_bytes <$> getRTSStats
  pure (Sample (ended - started) (after - before))

-- | How many times a workload runs in a sample, for samples of a second or more.
passesOf :: Workload -> Int
passesOf Rows = 10
passesOf Stream = 500

main :: IO ()
main = do
  arguments <- getArgs
  enabled <- getRTSStatsEnabled
  unless enabled $ die "run with +RTS -T"
  let number index fallback = maybe fallback read (lookup index (zip [0 :: Int ..] arguments))
      rounds = number 0 5 :: Int
      rowCount = number 1 1000000 :: Int
      mebibytes = number 2 128 :: Int
      limits = [("unlimited", maxBound), ("1460", 1460)] :: [(String, Int)]
  rowsBytes <- evaluate (rowsWire rowCount)
  streamedWire <- evaluate (streamWire (BS.replicate (mebibytes * 1048576) 97))
  let wire Rows = rowsBytes
      wire Stream = streamedWire
      expected Rows = rowsChecksum rowCount
      expected Stream = mebibytes * 1048576
      run Rows = variantRows
      run Stream = variantStream
  printf "rows %d (%d bytes), field %d MiB, buffer %d, rounds %d\n" rowCount (BS.length rowsBytes) mebibytes capacity rounds
  agreed <- forM variants $ \variant -> do
    results <- forM limits $ \(_, limit) -> do
      memory <- newMemory (rowsWire 1000) limit
      variantRows variant memory
    pure (variantName variant, results)
  printf "checksum of 1000 rows, by variant and limit: %s (expected %d)\n" (show agreed) (rowsChecksum 1000)
  when (any (any (/= rowsChecksum 1000) . snd) agreed) $ die "the checksums of the small wire differ"
  samples <- fmap concat . forM [1 .. rounds] $ \roundNumber -> do
    printf "== round %d\n" roundNumber
    fmap concat . forM [Rows, Stream] $ \workload -> fmap concat . forM limits $ \(limitName, limit) -> forM variants $ \variant -> do
      let pass = do
            memory <- newMemory (wire workload) limit
            result <- run workload variant memory
            when (result /= expected workload) $ die (variantName variant <> ": " <> show workload <> " gave " <> show result <> ", not " <> show (expected workload))
      measured <- sample (passesOf workload) pass
      report workload rowCount mebibytes (BS.length (wire workload)) limitName (variantName variant) measured
      pure ((workload, limitName, variantName variant), measured)
  printf "\n== ranges over %d rounds, one pass of each workload\n" rounds
  forM_ [Rows, Stream] $ \workload -> forM_ limits $ \(limitName, _) -> forM_ variants $ \variant -> do
    let key = (workload, limitName, variantName variant)
        measured = [one | (found, one) <- samples, found == key]
    summary workload rowCount mebibytes (BS.length (wire workload)) limitName (variantName variant) measured

-- | What one pass did: for the rows, per row, and for the stream, per MiB.
rates :: Workload -> Int -> Int -> Int -> Sample -> (Double, Double, Double)
rates workload rowCount mebibytes wireBytes (Sample seconds allocated) = case workload of
  Rows -> (fromIntegral rowCount / perPass, fromIntegral wireBytes / 1048576 / perPass, fromIntegral allocated / passes / fromIntegral rowCount)
  Stream -> (0, fromIntegral mebibytes / perPass, fromIntegral allocated / passes / fromIntegral mebibytes)
  where
    passes = fromIntegral (passesOf workload) :: Double
    perPass = seconds / passes

report :: Workload -> Int -> Int -> Int -> String -> String -> Sample -> IO ()
report workload rowCount mebibytes wireBytes limitName name measured = do
  let (perSecond, mibPerSecond, allocation) = rates workload rowCount mebibytes wireBytes measured
  printf "%-7s %-10s %-12s %8.3f s %12.0f rows/s %9.1f MiB/s %12.1f B/%s\n" (show workload) limitName name (sampleSeconds measured) perSecond mibPerSecond allocation (unit workload)

summary :: Workload -> Int -> Int -> Int -> String -> String -> [Sample] -> IO ()
summary workload rowCount mebibytes wireBytes limitName name measured = do
  let all' = rates workload rowCount mebibytes wireBytes <$> measured
      range pick = (minimum (pick <$> all'), maximum (pick <$> all'))
      (rowsLow, rowsHigh) = range (\(a, _, _) -> a)
      (mibLow, mibHigh) = range (\(_, b, _) -> b)
      (allocLow, allocHigh) = range (\(_, _, c) -> c)
  case workload of
    Rows -> printf "%-7s %-10s %-12s %9.0f - %9.0f rows/s %8.1f - %8.1f MiB/s %8.1f - %8.1f B/row\n" (show workload) limitName name rowsLow rowsHigh mibLow mibHigh allocLow allocHigh
    Stream -> printf "%-7s %-10s %-12s %8.1f - %8.1f MiB/s %12.0f - %12.0f B/MiB\n" (show workload) limitName name mibLow mibHigh allocLow allocHigh

unit :: Workload -> String
unit Rows = "row"
unit Stream = "MiB"
