{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}

module RuntimeSpec (spec) where

import Control.Arrow (ArrowChoice, (>>>), (^>>))
import Control.Exception (ErrorCall (..), bracket, throw, throwIO, try)
import Control.Monad (forM_, void)
import qualified Data.ByteString as BS
import Data.IORef (modifyIORef', newIORef, readIORef)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.Classes hiding (within)
import qualified Network.Streaming.Classes as Classes (within)
import Network.Streaming.Combinators (done, localTicket, sleep)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Scheduler (Turn (..))
import Network.Streaming.Loop
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Reader (replicateA)
import Network.Streaming.Simulated
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "the runtime" $ do
  describe "tickets" $ do
    it "show, in a simulation, a part that waits for a ticket nothing gives a value" $ do
      (simulatedOutcome <$> runSimulation 1 limit (localTicket >>> fst ^>> await :: Loop () ()) ()) `shouldReturn` Deadlocked [0]

    it "make the giver evaluate the value, so a value that is an error call fails the giver even when no part waits for the ticket yet" $ do
      failsAtTheGive (runThreads givingErrorLocal ())
      failsAtTheGive (runLoop givingErrorLocal ())
      failsAtTheGive (runCompiled givingErrorLocal ())
      failsAtTheGive (runDelimited givingErrorLocal ())
      failsAtTheGive (runDelimited (givingError @'Anywhere) ())
      failsAtTheGive (runCompiled (givingError @'Anywhere) ())
      failsAtTheGive (runSimulation 1 limit givingErrorLocal ())

  describe "giving way, with every part on one thread" $ do
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
      buffer <- newBuffer (positiveInt @1024)
      let failing = now (const (throwIO (ErrorCall "the part failed")))
          receiving = const (end, roomOf buffer) ^>> receive >>> now (const (pure ()))
      within (try (runThreads (together [receiving, failing]) ())) `shouldReturn` Just (Left (ErrorCall "the part failed"))

-- | The programs of the same kind that scope their resources, race, and keep time.
type Cancelling = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, GiveWay arr, Block arr, Scope arr, Race arr, Time arr, Within arr, Annotate arr, Instrumented (ModeOf arr)) => IO (arr () ())

compiledScoped, delimitedScoped :: Turn -> Cancelling -> IO ((), Report)
compiledScoped turn built = built >>= \program -> runCompiledTurn turn program ()
delimitedScoped turn built = runDelimitedWithTurn turn built ()

-- | A part that gives way this many times, noting its name each time with the function it is given.
ticks :: (ArrowChoice arr, GiveWay arr, Effect arr, Annotate arr) => (String -> IO ()) -> String -> Int -> arr () ()
ticks note name count = const count ^>> replicateA (giveWay >>> now (const (note name))) >>> done

-- | Two parts race: the loser waits for a byte that never comes in a scope
-- that notes its release, the winner gives way five times and wins, and two
-- more parts only give way: the order in which they noted themselves.
cancelledFirst :: (Cancelling -> IO ((), Report)) -> IO [String]
cancelledFirst run = withPair $ \waiting _ -> do
  notes <- newIORef []
  let note name = modifyIORef' notes (name :)
  _ <- run $ do
    buffer <- newBuffer (positiveInt @16)
    let loser = scope (now (const (pure ()))) (now (const (note "released"))) (const (waiting, roomOf buffer) ^>> receive >>> now (const (pure ())))
        winner = ticks note "winner" 5 >>> now (const (note "won"))
    pure $ together [race loser winner >>> now (const (pure ())), ticks note "yielder 1" 30, ticks note "yielder 2" 30]
  reverse <$> readIORef notes

-- | One part sleeps and another waits for a byte that never comes with a deadline, both for a nanosecond, so
-- that their timers are due in the next round, once the parts that only give way are at it: the order in which
-- they noted themselves.
timedFirst :: (Cancelling -> IO ((), Report)) -> IO [String]
timedFirst run = withPair $ \waiting _ -> do
  notes <- newIORef []
  let note name = modifyIORef' notes (name :)
  _ <- run $ do
    buffer <- newBuffer (positiveInt @16)
    let sleeper = ticks note "sleeper" 5 >>> now (const (note "asleep")) >>> sleep (nanos 1) >>> now (const (note "woke"))
        expiring = ticks note "waiter" 5 >>> now (const (note "waiting")) >>> Classes.within (nanos 1) (const (waiting, roomOf buffer) ^>> receive) >>> now (const (note "expired"))
    pure $ together [sleeper, expiring, ticks note "yielder 1" 30, ticks note "yielder 2" 30]
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
  _ <- run $ do
    buffer <- newBuffer (positiveInt @16)
    pure $
      together
        [ const (waiting, roomOf buffer) ^>> receive >>> now (const (note "woke")),
          ticks note "writer" 5 >>> const (sending, [BS.singleton 1]) ^>> sendAll >>> now (const (note "sent")) >>> ticks note "writer" 10,
          ticks note "yielder 1" 30,
          ticks note "yielder 2" 30
        ]
  reverse <$> readIORef notes

-- | The error call that the value a giver gives is.
valueError :: ErrorCall
valueError = ErrorCall "the value given was an error call"

-- | A ticket that no part waits for, whose giver gives a value that is an error call.
givingError :: forall r arr. (ArrowChoice arr, Exchange arr r) => arr () Bool
givingError = ticket @arr @r >>> (,throw valueError :: Int) . snd ^>> give

givingErrorLocal :: forall arr. (ArrowChoice arr, Exchange arr (Local arr)) => arr () Bool
givingErrorLocal = givingError @(Local arr)

-- | The run fails with the error call, which the giver evaluates when it gives.
failsAtTheGive :: IO a -> IO ()
failsAtTheGive run = try (void run) `shouldReturn` (Left valueError :: Either ErrorCall ())

-- | A program made in IO, which each arrow runs: what the tests run.
type Built = forall arr. (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr (Local arr), GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => IO (arr () ())

-- | A pair of connected sockets, closed after the action.
withPair :: (Socket -> Socket -> IO a) -> IO a
withPair =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
    . uncurry

limit :: Int
limit = 10000000

within :: IO a -> IO (Maybe a)
within = timeout 20000000
