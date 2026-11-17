{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

-- | The calls that a part's waits and receives make on the system, which "Network.Streaming.Counters" counts: the
-- same programs for every arrow that has what they need, over socket pairs. A thread of the test is the other end of
-- each exchange, and answers in step, so that the counts of a run are the same on every run.
module SyscallsSpec (spec) where

import Control.Arrow (ArrowChoice, first, (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO, getNumCapabilities, rtsSupportsBoundThreads, setNumCapabilities, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (finally)
import Control.Monad (forM_, replicateM_, void, when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, expect, keeping, localTicket, sleep, tap, timesWith, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Counters
import Network.Streaming.Delimited
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Internal.Scheduler (Turn, defaultTurn, microseconds)
import Network.Streaming.Mode (Instrumented, Mode (..), ModeOf)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Time arr, Within arr, Annotate arr, Exchange arr (Local arr), Instrumented (ModeOf arr))

type Program a = forall arr. (Rich arr) => arr () a

-- | A way to run a program: the turn of the runners that have one, and whether the runner has a poller of its own, which is
-- what the counts of polls are about.
data Runner = Runner String Bool (forall a. Turn -> Program a -> IO a)

-- | The runners in 'Debug', which count what they run, and in 'Release', which count nothing.
runners, releaseRunners :: [Runner]
runners =
  [ Runner "threads" False (\_ program -> runThreadsM @'Debug program ()),
    Runner "delimited" True (\turn program -> fst <$> runDelimitedTurnM @'Debug turn program ()),
    Runner "compiled" True (\turn program -> fst <$> runCompiledTurnM @'Debug turn program ())
  ]
releaseRunners =
  [ Runner "threads" False (\_ program -> runThreads program ()),
    Runner "delimited" True (\turn program -> fst <$> runDelimitedTurn turn program ()),
    Runner "compiled" True (\turn program -> fst <$> runCompiledTurn turn program ())
  ]

spec :: Spec
spec = describe "system calls" $ do
  forM_ runners $ \(Runner name _ run) -> describe name $ do
    it "are counted as a wait and an arming for each wait on a descriptor, and a send for each echo" $ do
      counts <- counting (echoing 20 run)
      -- A receive that finds the byte there does not wait, so the waits are at most the echoes; each wait arms.
      (countOf DescriptorWaits counts > 0 && countOf DescriptorWaits counts <= 20) `shouldBe` True
      countOf Armings counts `shouldBe` countOf DescriptorWaits counts
      countOf Sends counts `shouldBe` 20
      countOf Accepts counts `shouldBe` 0

  forM_ releaseRunners $ \(Runner name _ run) -> describe name $
    it "counts nothing in Release, where the exchange that Debug counts leaves every count as it was" $ do
      counts <- counting (echoing 20 run)
      toList counts `shouldBe` [(counter, 0) | counter <- [minBound .. maxBound]]

  forM_ runners $ \(Runner name _ run) -> describe name $ do
    it "receives once without finding anything, and then waits before each receive, when each one follows a short one" $ do
      counts <- counting (echoing 20 run)
      countOf ReceivesEmpty counts `shouldBe` 1
      countOf ReceivesSkipped counts `shouldBe` 19
      countOf Receives counts `shouldBe` 21
      countOf DescriptorWaits counts `shouldBe` 20

    it "receives first after a read that filled the buffer, and waits first after a short one that a send followed" $ do
      (mine, theirs) <- newPair
      SocketBytes.sendAll theirs (BS.replicate 40 7)
      driverEnded <- newEmptyMVar
      _ <- forkIO (lateByte' theirs `finally` putMVar driverEnded ())
      counts <- counting (run defaultTurn (const mine ^>> received >>> received >>> received >>> sending >>> received >>> done))
      takeMVar driverEnded
      mapM_ Socket.close [mine, theirs]
      -- Two reads of 16 bytes, one of the 8 that were left, a send, and then the byte that came later, which the fourth receive waits for.
      countOf Receives counts `shouldBe` 4
      countOf ReceivesEmpty counts `shouldBe` 0
      countOf ReceivesSkipped counts `shouldBe` 1
      countOf DescriptorWaits counts `shouldBe` 1

    it "receives first after a short read that no send followed, since the rest of what the other end sends may be on its way" $ do
      pair@(mine, theirs) <- newPair
      driverEnded <- newEmptyMVar
      _ <- forkIO (handoverDriver pair `finally` putMVar driverEnded ())
      counts <- counting (run defaultTurn (const mine ^>> received >>> pausing >>> pausing >>> pausing >>> received >>> done))
      takeMVar driverEnded
      mapM_ Socket.close [mine, theirs]
      countOf Receives counts `shouldBe` 3
      countOf ReceivesEmpty counts `shouldBe` 1
      countOf ReceivesSkipped counts `shouldBe` 0
      countOf DescriptorWaits counts `shouldBe` 1

    it "finds what arrived between a short read and a send and the next receive, by waiting for it" $ do
      pair@(mine, theirs) <- newPair
      driverEnded <- newEmptyMVar
      _ <- forkIO (handoverDriver pair `finally` putMVar driverEnded ())
      counts <- counting (run defaultTurn (const mine ^>> received >>> sending >>> pausing >>> pausing >>> pausing >>> received >>> done))
      takeMVar driverEnded
      mapM_ Socket.close [mine, theirs]
      countOf Receives counts `shouldBe` 3
      countOf ReceivesEmpty counts `shouldBe` 1
      countOf ReceivesSkipped counts `shouldBe` 1
      countOf DescriptorWaits counts `shouldBe` 2

    it "receives first after a wait for input, which found the input that the receive then takes" $ do
      pair@(mine, theirs) <- newPair
      driverEnded <- newEmptyMVar
      _ <- forkIO (handoverDriver pair `finally` putMVar driverEnded ())
      counts <- counting (run defaultTurn (const mine ^>> received >>> awaiting >>> received >>> done))
      takeMVar driverEnded
      mapM_ Socket.close [mine, theirs]
      countOf Receives counts `shouldBe` 3
      countOf ReceivesSkipped counts `shouldBe` 0
      countOf DescriptorWaits counts `shouldBe` 2

  forM_ runners $ \(Runner name polls run) -> when polls $ describe name $ do
    sharedThread "a wait that the test's own threads share the OS thread with is cut short, and polls again and again" $ it "makes no poll that doesn't wait, and one that waits for each wake-up, while no other part can go on" $ do
      counts <- counting (echoing 20 run)
      countOf PollsImmediate counts `shouldBe` 0
      countOf PollsBlocking counts `shouldBe` countOf DescriptorWaits counts

    it "polls at most once for a turn while other parts can go on, and finds a descriptor that became ready within a few turns" $ do
      (mine, theirs) <- newPair
      stamps@(Stamps sent woke finished) <- Stamps <$> newIORef 0 <*> newIORef 0 <*> newIORef 0
      driverEnded <- newEmptyMVar
      _ <- forkIO (lateByte stamps theirs `finally` putMVar driverEnded ())
      counts <- counting (run (microseconds 100) (busyAndWaiting stamps rounds (mine, theirs)))
      takeMVar driverEnded
      mapM_ Socket.close [mine, theirs]
      (sentAt, wokeAt, finishedAt) <- (,,) <$> readIORef sent <*> readIORef woke <*> readIORef finished
      -- The part that waited was woken while the others still ran, within a few turns of the byte, which a run that
      -- looked only when it had nothing else to do would not do.
      (wokeAt < finishedAt) `shouldBe` True
      (wokeAt - sentAt < 15000000) `shouldBe` True
      -- A poll at every round is a poll for each of the many rounds of the ping-pong.
      (countOf PollsImmediate counts < rounds `div` 10) `shouldBe` True

    it "polls once the armings that waits cancelled meanwhile have piled up, and not at every round" $ do
      (mine, theirs) <- newPair
      counts <- counting (run (microseconds 1000000) (cancelledWaits 1000 20000 mine))
      mapM_ Socket.close [mine, theirs]
      countOf Armings counts `shouldBe` 1000
      (countOf PollsImmediate counts >= 10) `shouldBe` True
      (countOf PollsImmediate counts <= 100) `shouldBe` True

  describe "delimited" $
    it "counts a capture of the stack for each yield and each fork of a group, and none for a part that ends" $ do
      counts <- counting (runDelimitedTurnM @'Debug (microseconds 0) (timesWith 10 giveWay >>> together [done, done]) ())
      countOf Captures counts `shouldBe` 11

  describe "on two cores" $ do
    it "count an arming of a descriptor that another core armed last as a move" $ do
      ensureCores
      shelf <- newIORef Nothing
      pair@(mine, theirs) <- newPair
      driverEnded <- newEmptyMVar
      _ <- forkIO (handoverDriver pair `finally` putMVar driverEnded ())
      before <- snapshot
      results <- bounded (runCoresM @'Debug 2 (pure shelf) (movedTo pair))
      after <- snapshot
      takeMVar driverEnded
      Socket.close mine
      Socket.close theirs
      results `shouldBe` Just [(), ()]
      countOf DescriptorMoves (after `since` before) `shouldBe` 1

-- | Skips the case on the non-threaded run time.
sharedThread :: String -> Spec -> Spec
sharedThread reason = if rtsSupportsBoundThreads then id else skip reason

-- | A test that waits for ever fails instead: the scheduler stops where it waits for a descriptor.
bounded :: IO a -> IO (Maybe a)
bounded = timeout 10000000

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

-- | What the action counted: the counts after it, less those before.
counting :: IO a -> IO Counts
counting action = do
  before <- snapshot
  _ <- action
  after <- snapshot
  pure (after `since` before)

-- * Sockets

-- | Two connected sockets: the part that a test runs has the first, and a thread of the test the second.
type Pair = (Socket, Socket)

newPair :: IO Pair
newPair = do
  pair@(mine, theirs) <- Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol
  fresh mine *> fresh theirs
  pure pair

-- | Receives a byte on the socket: how many bytes.
receiveOne :: (Rich arr) => arr Socket Int
receiveOne = now (\sock -> (\buffer -> (sock, roomOf buffer)) <$> newBuffer (positiveInt @16)) >>> receive

-- | Receives into a buffer of 16 bytes, and goes on with the socket.
received :: (Rich arr) => arr Socket Socket
received = tap (receiveOne >>> done)

-- | Waits for 30 milliseconds, and goes on with the socket.
pausing :: (Rich arr) => arr Socket Socket
pausing = keeping (sleep (millis 30)) >>^ snd

-- | Sends a byte on the socket, and goes on with it.
sending :: (Rich arr) => arr Socket Socket
sending = tap ((,[BS.singleton 7]) ^>> sendAll)

-- | Waits until the socket has input, and goes on with it.
awaiting :: (Rich arr) => arr Socket Socket
awaiting = tap awaitInput

-- | Receives a byte on the socket and sends it back, this many times.
echoes :: (Rich arr) => Int -> arr Socket ()
echoes count = timesWith count (received >>> sending) >>> done

-- | The part receives and sends back this many bytes, one at a time, with a thread that sends a byte and waits for the
-- echo: the thread waits a little before the first, so that the part waits first.
echoing :: Int -> (forall a. Turn -> Program a -> IO a) -> IO ()
echoing count run = do
  (mine, theirs) <- newPair
  driverEnded <- newEmptyMVar
  _ <- forkIO (driver theirs count `finally` putMVar driverEnded ())
  _ <- run defaultTurn (const mine ^>> echoes count)
  takeMVar driverEnded
  Socket.close mine
  Socket.close theirs

-- | The other end of the exchange: a byte, and its echo, this many times.
driver :: Socket -> Int -> IO ()
driver sock count = do
  threadDelay 100000
  replicateM_ count (SocketBytes.sendAll sock (BS.singleton 7) *> void (SocketBytes.recv sock 16))

-- | The first core receives a byte, and leaves the socket for the second, which receives the next.
movedTo :: (Rich arr) => Pair -> IORef (Maybe Socket) -> Int -> arr () ()
movedTo (mine, _) shelf core
  | core == 0 = const mine ^>> received >>> now (atomicWriteIORef shelf . Just)
  | otherwise = untilJust (readIORef shelf) >>> receiveOne >>> done

-- | Sends a byte, and another a little later.
handoverDriver :: Pair -> IO ()
handoverDriver (_, theirs) = do
  threadDelay 100000
  SocketBytes.sendAll theirs (BS.singleton 7)
  threadDelay 50000
  SocketBytes.sendAll theirs (BS.singleton 7)

-- * Many rounds with something to wait for

-- | How many round trips the ping-pong makes: far more than the polls a run may make.
rounds :: Int
rounds = 100000

-- | The ends of the ping-pong's two sides: a request that carries the promise of its answer, and an answer that carries the promise
-- of the next request.
newtype Ping r = Ping {answer :: Promise r (Pong r)}

newtype Pong r = Pong {following :: Promise r (Ping r)}

-- | Two parts of one core that give each other a ticket and wait for the answer, this many times: each round is short, and a part
-- that waits for a ticket never yields. The server will await the first request, which the client may make before it does.
pingPong :: (Rich arr) => Int -> arr () ()
pingPong count = localTicket >>> first willAwait >>> together [snd ^>> client, fst ^>> server]
  where
    client = timesWith count (expect Ping >>> await >>^ following) >>> done
    server = timesWith count (await >>> answer ^>> expect Pong) >>> done

-- | The monotonic time when the thread sent its byte, when the part received it, and when the other parts ended.
data Stamps = Stamps (IORef Word64) (IORef Word64) (IORef Word64)

-- | A byte, sent a little after the run began.
lateByte :: Stamps -> Socket -> IO ()
lateByte (Stamps sent _ _) sock = do
  threadDelay 5000
  monotonicNanoseconds >>= writeIORef sent
  SocketBytes.sendAll sock (BS.singleton 7)

-- | A part that waits for a byte on the socket, and then for another, while a ping-pong runs, after which the other end
-- sends the second byte. The part is waiting on the descriptor throughout the ping-pong.
busyAndWaiting :: (Rich arr) => Stamps -> Int -> Pair -> arr () ()
busyAndWaiting (Stamps _ woke finished) count (mine, theirs) = together [busy, waiting]
  where
    busy = pingPong count >>> now (const (monotonicNanoseconds >>= writeIORef finished)) >>> const (theirs, [BS.singleton 7]) ^>> sendAll
    waiting = const mine ^>> receiveOne >>> now (const (monotonicNanoseconds >>= writeIORef woke)) >>> const mine ^>> receiveOne >>> done

-- | Waits for a byte on the socket with a deadline that has passed, this many times, while a ping-pong runs.
cancelledWaits :: (Rich arr) => Int -> Int -> Socket -> arr () ()
cancelledWaits waits count sock = together [pingPong count, const sock ^>> timesWith waits (within (nanos 1) receiveOne >>^ const sock) >>> done]

-- | One byte, a little after the run began.
lateByte' :: Socket -> IO ()
lateByte' sock = threadDelay 100000 *> SocketBytes.sendAll sock (BS.singleton 7)
