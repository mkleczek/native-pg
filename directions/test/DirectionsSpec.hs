{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}

module DirectionsSpec (spec) where

import Control.Arrow (ArrowChoice, (>>>), (>>^), (^>>))
import Control.Exception (bracket)
import Control.Monad (forM_, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List (group)
import Data.Word (Word64)
import Network.Directions.Memory
import Network.Directions.Proxy
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes hiding (within)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Scheduler (Turn (..), microseconds)
import Network.Streaming.Loop
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Reader (replicateA)
import Network.Streaming.Simulated
import Network.Streaming.Threads
import Network.Streaming.With (With (..), withOnly)
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "directions" $ do
  describe "the proxy, between a client and an echo server" $ do
    it "brings the request back, with threads" $
      within (snd <$> roundTrip plain request threaded) `shouldReturn` Just request

    it "brings the request back, with every part on one thread" $
      within (snd <$> roundTrip plain request looped) `shouldReturn` Just request

    it "brings the request back, compiled, with every part on one thread" $
      within (snd <$> roundTrip plain request compiled) `shouldReturn` Just request

    it "brings the request back, on delimited continuations, with every part on one thread" $
      within (snd <$> roundTrip plain request delimited) `shouldReturn` Just request

    it "brings the request back in a simulation, whatever order the seed gives" $
      forM_ [1 .. 20] $ \seed ->
        roundTrip plain request (simulate seed) `shouldReturn` (Completed (), request)

    it "brings the request back over sockets, with threads and on one thread" $ do
      within (overSockets threaded) `shouldReturn` Just request
      within (overSockets looped) `shouldReturn` Just request
      within (overSockets compiled) `shouldReturn` Just request
      within (overSockets delimited) `shouldReturn` Just request

    it "changes the client's bytes on their way" $ do
      let through = Setup exchange (proxyThrough (BS.map toUpper))
      roundTrip through request (simulate 7) `shouldReturn` (Completed (), BS.map toUpper request)
      within (snd <$> roundTrip through request looped) `shouldReturn` Just (BS.map toUpper request)

  describe "the simulation" $ do
    it "runs the same way for the same seed, and differently for another" $ do
      first <- simulatedTrace . fst <$> roundTrip plain request (\built -> built >>= \program -> runSimulation 1 limit program ())
      again <- simulatedTrace . fst <$> roundTrip plain request (\built -> built >>= \program -> runSimulation 1 limit program ())
      other <- simulatedTrace . fst <$> roundTrip plain request (\built -> built >>= \program -> runSimulation 2 limit program ())
      again `shouldBe` first
      (other == first) `shouldBe` False

    it "finds that a client that sends all before it reads waits for the server, which waits for it" $ do
      (outcome, kept) <- roundTrip oneDirection request (simulate 1)
      kept `shouldBe` BS.empty
      case outcome of
        Deadlocked parts -> length parts `shouldBe` 4
        other -> failTest ("the run ended as " <> show other)

    -- A pump holds what one receive brought, and four lanes of 1000 bytes and
    -- three pumps hold about 7000 bytes.
    it "lets the same client finish while its request fits in the buffers on the way" $
      roundTrip oneDirection (BS.take 5000 request) (simulate 1) `shouldReturn` (Completed (), BS.take 5000 request)

  describe "tickets" $ do
    it "let one direction go on with what the other decided" $ do
      within (transact (== batch) threaded) `shouldReturn` Just ((), BS8.pack "COMMIT\n")
      within (transact (const False) threaded) `shouldReturn` Just ((), BS8.pack "ROLLBACK\n")
      within (transact (== batch) looped) `shouldReturn` Just ((), BS8.pack "COMMIT\n")
      within (transact (const False) looped) `shouldReturn` Just ((), BS8.pack "ROLLBACK\n")
      within (transact (== batch) compiled) `shouldReturn` Just ((), BS8.pack "COMMIT\n")
      within (transact (const False) compiled) `shouldReturn` Just ((), BS8.pack "ROLLBACK\n")
      within (transact (== batch) delimited) `shouldReturn` Just ((), BS8.pack "COMMIT\n")
      within (transact (const False) delimited) `shouldReturn` Just ((), BS8.pack "ROLLBACK\n")
      forM_ [1 .. 20] $ \seed ->
        transact (== batch) (simulate seed) `shouldReturn` (Completed (), BS8.pack "COMMIT\n")

  describe "giving way, with every part on one thread" $ do
    it "happens at a link's receives once the part has run for its turn, and not at actions that never wait" $ do
      -- A turn far longer than the whole run: the parts take theirs one after
      -- the other, and don't give way at each receive.
      byReceives <- twoParts (microseconds 1000000) receiving
      forM_ byReceives $ \(turns, report) -> do
        sum turns `shouldBe` 2000
        (length turns <= 10) `shouldBe` True
        reportSteps report `shouldBe` 2000
      -- A turn of nothing: every receive is a point where it gives way.
      everyReceive <- twoParts (Turn 0) receiving
      forM_ everyReceive $ \(turns, report) -> do
        turns `shouldBe` replicate 2000 1
        reportSteps report `shouldBe` 2000
      byEffects <- twoParts (Turn 0) effects
      forM_ byEffects $ \(turns, report) -> do
        turns `shouldBe` [1000, 1000]
        reportSteps report `shouldBe` 0

    it "happens about once a turn, for a part that computes between its points to give way at" $ do
      -- Each part does 40 chunks of a millisecond, a turn is 10: about 4 turns
      -- of a part in a row, not 1 and not 40.
      byChunks <- twoParts (microseconds 10000) computing
      forM_ byChunks $ \(turns, _) -> do
        sum turns `shouldBe` 80
        (length turns >= 5) `shouldBe` True
        (length turns <= 40) `shouldBe` True

-- | A part that notes which part it was, after each of its 1000 steps, and is given its link.
type Part = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => IORef [Int] -> Int -> Buffer -> arr Memory ()

-- | Each step is a receive from a link that never waits.
receiving :: Part
receiving order ident buffer =
  withOnly (const 1000 ^>> replicateA (const (roomOf buffer) ^>> With receive >>> now (\_ -> modifyIORef' order (ident :))) >>^ const ())

-- | Each step is an action that never waits.
effects :: Part
effects order ident _ = const 1000 ^>> replicateA (now (\() -> modifyIORef' order (ident :))) >>^ const ()

-- | Each step is a point to give way at, and then work for a millisecond that
-- doesn't give way.
computing :: Part
computing order ident _ = const 40 ^>> replicateA (giveWay >>> now (\() -> spin 1000000 *> modifyIORef' order (ident :))) >>^ const ()
  where
    spin nanoseconds = monotonicNanoseconds >>= \started -> let wait = monotonicNanoseconds >>= \at -> when (at - started < nanoseconds) wait in wait

-- | Runs two of the parts at once, compiled and on delimited continuations,
-- with turns of that length: how many steps each turn of a part had, in order,
-- and the run's report.
twoParts :: Turn -> Part -> IO [([Int], Report)]
twoParts turn part = sequence [measured (compiledFor turn), measured (delimitedFor turn)]
  where
    measured :: (Built -> IO ((), Report)) -> IO ([Int], Report)
    measured run = do
      order <- newIORef []
      (_, report) <-
        run $ do
          one <- newMemory (BS.replicate 1000 0) 1
          two <- newMemory (BS.replicate 1000 0) 1
          buffer <- newBuffer (positiveInt @16)
          pure (together [const one ^>> part order 1 buffer, const two ^>> part order 2 buffer])
      (\notes -> (map length (group (reverse notes)), report)) <$> readIORef order

-- | Compiled and on delimited continuations, with turns of that length.
compiledFor, delimitedFor :: Turn -> Built -> IO ((), Report)
compiledFor turn built = built >>= \program -> runCompiledTurn turn program ()
delimitedFor turn built = runDelimitedWithTurn turn built ()

batch :: ByteString
batch = BS8.pack "BATCH\n"

-- | A transaction against an echo server over simulated links, deciding to
-- commit by the batch's answer: what the run gave, and what came back after
-- the batch.
transact :: (ByteString -> Bool) -> (Built -> IO a) -> IO (a, ByteString)
transact commits run = do
  (client, server) <- connect 1000
  forClient <- newBuffer (positiveInt @4096)
  forServer <- newBuffer (positiveInt @4096)
  kept <- newIORef []
  result <- run (pure (together [const client ^>> transaction forClient batch commits kept, const server ^>> echo forServer]))
  (,) result . BS.concat . reverse <$> readIORef kept

-- | How the client and the proxy are made.
data Setup = Setup
  { setupClient :: forall arr end. (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> ByteString -> IORef [ByteString] -> arr end (),
    setupProxy :: forall arr end. (ArrowChoice arr, Concurrent arr, Link arr end) => Buffer -> Buffer -> arr (end, end) ()
  }

plain, oneDirection :: Setup
plain = Setup exchange proxy
oneDirection = Setup sendThenReceive proxy

request :: ByteString
request = BS.pack (take 100000 (cycle [97 .. 122]))

-- | The whole round trip as one program: the client, the proxy, and an echo
-- server, over the four ends.
world :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Setup -> ByteString -> IORef [ByteString] -> (end, end, end, end) -> IO (arr () ())
world setup bytes kept (client, front, back, server) = do
  forClient <- newBuffer (positiveInt @16384)
  requests <- newBuffer (positiveInt @16384)
  answers <- newBuffer (positiveInt @16384)
  forServer <- newBuffer (positiveInt @16384)
  pure $
    together
      [ const client ^>> setupClient setup forClient bytes kept,
        const (front, back) ^>> setupProxy setup requests answers,
        const server ^>> echo forServer
      ]

-- | A program made in IO, which each arrow runs: what the tests run.
type Built = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr (Local arr), GiveWay arr, Block arr) => IO (arr () ())

-- | The round trip over simulated links that hold 1000 bytes each way: what
-- the run gave, and what the client kept.
roundTrip :: Setup -> ByteString -> (Built -> IO a) -> IO (a, ByteString)
roundTrip setup bytes run = do
  (client, front) <- connect 1000
  (back, server) <- connect 1000
  kept <- newIORef []
  result <- run (world setup bytes kept (client, front, back, server))
  (,) result . BS.concat . reverse <$> readIORef kept

-- | The same over socket pairs: what the client kept.
overSockets :: (Built -> IO ()) -> IO ByteString
overSockets run =
  withPair $ \client front -> withPair $ \back server -> do
    kept <- newIORef []
    run (world plain request kept (client, front, back, server))
    BS.concat . reverse <$> readIORef kept

-- | A pair of connected sockets, closed after the action.
withPair :: (Socket -> Socket -> IO a) -> IO a
withPair =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
    . uncurry

threaded :: Built -> IO ()
threaded built = built >>= \program -> runThreads program ()

looped :: Built -> IO ()
looped built = built >>= \program -> fst <$> runLoop program ()

compiled :: Built -> IO ()
compiled built = built >>= \program -> fst <$> runCompiled program ()

delimited :: Built -> IO ()
delimited built = fst <$> runDelimitedWith built ()

simulate :: Word64 -> Built -> IO (Outcome ())
simulate seed built = built >>= \program -> simulatedOutcome <$> runSimulation seed limit program ()

limit :: Int
limit = 10000000

within :: IO a -> IO (Maybe a)
within = timeout 20000000

toUpper :: (Ord a, Num a) => a -> a
toUpper byte
  | byte >= 97 && byte <= 122 = byte - 32
  | otherwise = byte
