module DirectionsSpec (spec) where

import Control.Arrow (ArrowChoice, (>>>), (>>^), (^>>))
import Control.Exception (ErrorCall (..), bracket, throwIO, try)
import Control.Monad (forM_, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List (group)
import Data.Word (Word64)
import Network.Directions.Classes hiding (within)
import qualified Network.Directions.Classes as Classes (within)
import Network.Directions.Combinators (sleep)
import Network.Directions.Compiled
import Network.Directions.Delimited
import Network.Directions.Loop
import Network.Directions.Memory
import Network.Directions.Proxy
import Network.Directions.Reader (replicateA)
import Network.Directions.Scheduler (Turn (..), microseconds)
import Network.Directions.Simulated
import Network.Directions.Threads
import Network.Socket (Socket)
import qualified Network.Socket as Socket
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

    it "show, in a simulation, a part that waits for a ticket nothing gives a value" $ do
      (simulatedOutcome <$> runSimulation 1 limit (newTicket >>> await :: Loop () ()) ()) `shouldReturn` Deadlocked [0]

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

    it "goes on first with the parts that I/O woke, before those that yielded" $ do
      notes <- sequence [wokenFirst (compiledFor (Turn 0)), wokenFirst (delimitedFor (Turn 0))]
      forM_ notes $ \order -> do
        let afterSent = drop 1 (dropWhile (/= "sent") order)
            between = takeWhile (/= "woke") afterSent
        -- The part that waited for the byte is woken at the start of the round
        -- after the byte was sent, before the part that sent it, which yielded
        -- in that round, and before any other part's second turn.
        ("woke" `elem` afterSent) `shouldBe` True
        ("writer" `elem` between) `shouldBe` False
        (length (filter (== "yielder 1") between) <= 1) `shouldBe` True
        (length (filter (== "yielder 2") between) <= 1) `shouldBe` True

    it "goes on first with a part that a cancellation took off its wait, before those that yielded" $ do
      notes <- sequence [cancelledFirst (compiledScoped (Turn 0)), cancelledFirst (delimitedScoped (Turn 0))]
      forM_ notes $ \order -> do
        let afterWon = drop 1 (dropWhile (/= "won") order)
            between = takeWhile (/= "released") afterWon
        -- The loser's release runs in the round after the winner ended, before
        -- any part that yielded in that round has another turn.
        ("released" `elem` afterWon) `shouldBe` True
        (length (filter (== "yielder 1") between) <= 1) `shouldBe` True
        (length (filter (== "yielder 2") between) <= 1) `shouldBe` True

    it "goes on first with a part that a timer woke, before those that yielded" $ do
      notes <- sequence [timedFirst (compiledScoped (Turn 0)), timedFirst (delimitedScoped (Turn 0))]
      forM_ notes $ \order -> forM_ [("asleep", "woke"), ("waiting", "expired")] $ \(before, woken) -> do
        let afterwards = drop 1 (dropWhile (/= before) order)
            between = takeWhile (/= woken) afterwards
        -- The sleeper and the part whose deadline passed go on in the round after the one they waited in, before
        -- any part that yielded in it has another turn.
        (woken `elem` afterwards) `shouldBe` True
        (length (filter (== "yielder 1") between) <= 1) `shouldBe` True
        (length (filter (== "yielder 2") between) <= 1) `shouldBe` True

  describe "threads" $
    it "stop every part when one fails, and rethrow its failure" $ do
      (_, end) <- connect 1000
      buffer <- newBuffer 1024
      kept <- newIORef []
      let failing = now (\() -> throwIO (ErrorCall "the part failed"))
      within (try (runThreads (together [receiveAll buffer end kept, failing]) ())) `shouldReturn` Just (Left (ErrorCall "the part failed"))

-- | A part that notes which part it was, after each of its 1000 steps.
type Part = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr) => IORef [Int] -> Int -> Memory -> Buffer -> arr () ()

-- | Each step is a receive from a link that never waits.
receiving :: Part
receiving order ident link buffer =
  const 1000 ^>> replicateA (const (roomOf buffer) ^>> receive link >>> now (\_ -> modifyIORef' order (ident :))) >>^ const ()

-- | Each step is an action that never waits.
effects :: Part
effects order ident _ _ = const 1000 ^>> replicateA (now (\() -> modifyIORef' order (ident :))) >>^ const ()

-- | Each step is a point to give way at, and then work for a millisecond that
-- doesn't give way.
computing :: Part
computing order ident _ _ = const 40 ^>> replicateA (giveWay >>> now (\() -> spin 1000000 *> modifyIORef' order (ident :))) >>^ const ()
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
          buffer <- newBuffer 16
          pure (together [part order 1 one buffer, part order 2 two buffer])
      (\notes -> (map length (group (reverse notes)), report)) <$> readIORef order

-- | The programs of the same kind that scope their resources, race, and keep time.
type Cancelling = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr, Scope arr, Race arr, Time arr, Within arr) => IO (arr () ())

compiledScoped, delimitedScoped :: Turn -> Cancelling -> IO ((), Report)
compiledScoped turn built = built >>= \program -> runCompiledTurn turn program ()
delimitedScoped turn built = runDelimitedWithTurn turn built ()

-- | Two parts race: the loser waits for a byte that never comes in a scope
-- that notes its release, the winner gives way five times and wins, and two
-- more parts only give way: the order in which they noted themselves.
cancelledFirst :: (Cancelling -> IO ((), Report)) -> IO [String]
cancelledFirst run = withPair $ \waiting _ -> do
  notes <- newIORef []
  let note name = modifyIORef' notes (name :)
      ticks name count = const count ^>> replicateA (giveWay >>> now (\() -> note name)) >>^ const ()
  _ <- run $ do
    buffer <- newBuffer 16
    let loser = scope (now (\() -> pure ())) (now (\_ -> note "released")) (const (roomOf buffer) ^>> receive waiting >>> now (\_ -> pure ()))
        winner = ticks "winner" 5 >>> now (\() -> note "won")
    pure $ together [race loser winner >>> now (\_ -> pure ()), ticks "yielder 1" 30, ticks "yielder 2" 30]
  reverse <$> readIORef notes

-- | One part sleeps and another waits for a byte that never comes with a deadline, both for a nanosecond, so
-- that their timers are due in the next round, once the parts that only give way are at it: the order in which
-- they noted themselves.
timedFirst :: (Cancelling -> IO ((), Report)) -> IO [String]
timedFirst run = withPair $ \waiting _ -> do
  notes <- newIORef []
  let note name = modifyIORef' notes (name :)
      ticks name count = const count ^>> replicateA (giveWay >>> now (\() -> note name)) >>^ const ()
  _ <- run $ do
    buffer <- newBuffer 16
    let sleeper = ticks "sleeper" 5 >>> now (\() -> note "asleep") >>> sleep (nanos 1) >>> now (\() -> note "woke")
        expiring = ticks "waiter" 5 >>> now (\() -> note "waiting") >>> Classes.within (nanos 1) (const (roomOf buffer) ^>> receive waiting) >>> now (\_ -> note "expired")
    pure $ together [sleeper, expiring, ticks "yielder 1" 30, ticks "yielder 2" 30]
  reverse <$> readIORef notes

-- | Compiled and on delimited continuations, with turns of that length.
compiledFor, delimitedFor :: Turn -> Built -> IO ((), Report)
compiledFor turn built = built >>= \program -> runCompiledTurn turn program ()
delimitedFor turn built = runDelimitedWithTurn turn built ()

-- | A part waits for a byte on a socket, and another sends it after five turns
-- while two more only give way: the order in which they noted themselves.
wokenFirst :: (Built -> IO ((), Report)) -> IO [String]
wokenFirst run = withPair $ \waiting sending -> do
  notes <- newIORef []
  let note name = modifyIORef' notes (name :)
      ticks name count = const count ^>> replicateA (giveWay >>> now (\() -> note name)) >>^ const ()
  _ <- run $ do
    buffer <- newBuffer 16
    pure $
      together
        [ const (roomOf buffer) ^>> receive waiting >>> now (\_ -> note "woke"),
          ticks "writer" 5 >>> (\() -> [BS.singleton 1]) ^>> sendAll sending >>> now (\() -> note "sent") >>> ticks "writer" 10,
          ticks "yielder 1" 30,
          ticks "yielder 2" 30
        ]
  reverse <$> readIORef notes

batch :: ByteString
batch = BS8.pack "BATCH\n"

-- | A transaction against an echo server over simulated links, deciding to
-- commit by the batch's answer: what the run gave, and what came back after
-- the batch.
transact :: (ByteString -> Bool) -> (Built -> IO a) -> IO (a, ByteString)
transact commits run = do
  (client, server) <- connect 1000
  forClient <- newBuffer 4096
  forServer <- newBuffer 4096
  kept <- newIORef []
  result <- run (pure (together [transaction forClient client batch commits kept, echo forServer server]))
  (,) result . BS.concat . reverse <$> readIORef kept

-- | How the client and the proxy are made.
data Setup = Setup
  { setupClient :: forall arr end. (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () (),
    setupProxy :: forall arr end. (ArrowChoice arr, Concurrent arr, Link arr end) => Buffer -> Buffer -> end -> end -> arr () ()
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
  forClient <- newBuffer 16384
  requests <- newBuffer 16384
  answers <- newBuffer 16384
  forServer <- newBuffer 16384
  pure $
    together
      [ setupClient setup forClient client bytes kept,
        setupProxy setup requests answers front back,
        echo forServer server
      ]

-- | A program made in IO, which each arrow runs: what the tests run.
type Built = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr, GiveWay arr, Block arr) => IO (arr () ())

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
