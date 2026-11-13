{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The calls that a part's waits and receives make on the system, which "Network.Streaming.Counters" counts: the
-- same programs for every arrow that has what they need, over socket pairs. A thread of the test is the other end of
-- each exchange, and answers in step, so that the counts of a run are the same on every run.
module SyscallsSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO, getNumCapabilities, setNumCapabilities, threadDelay)
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
import Network.Streaming.Combinators (call, expect, timesWith, untilJust)
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
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Time arr, Within arr, Exchange arr (Local arr), Instrumented (ModeOf arr))

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

  forM_ runners $ \(Runner name polls run) -> when polls $ describe name $ do
    it "makes no poll that doesn't wait, and one that waits for each wake-up, while no other part can go on" $ do
      counts <- counting (echoing 20 run)
      countOf PollsImmediate counts `shouldBe` 0
      countOf PollsBlocking counts `shouldBe` countOf DescriptorWaits counts

    it "polls at most once for a turn while other parts can go on, and finds a descriptor that became ready within a few turns" $ do
      (mine, theirs) <- newPair
      stamps@(Stamps sent woke finished) <- Stamps <$> newIORef 0 <*> newIORef 0 <*> newIORef 0
      done <- newEmptyMVar
      _ <- forkIO (lateByte stamps theirs `finally` putMVar done ())
      counts <- counting (run (microseconds 100) (busyAndWaiting stamps rounds (mine, theirs)))
      takeMVar done
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

  describe "on two cores" $ do
    it "count an arming of a descriptor that another core armed last as a move" $ do
      ensureCores
      shelf <- newIORef Nothing
      pair@(mine, theirs) <- newPair
      done <- newEmptyMVar
      _ <- forkIO (handoverDriver pair `finally` putMVar done ())
      before <- snapshot
      results <- bounded (runCoresM @'Debug 2 (pure shelf) (movedTo pair))
      after <- snapshot
      takeMVar done
      Socket.close mine
      Socket.close theirs
      results `shouldBe` Just [(), ()]
      countOf DescriptorMoves (after `since` before) `shouldBe` 1

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

-- | Receives a byte on the socket and sends it back, this many times.
echoes :: (Rich arr) => Int -> arr Socket ()
echoes count = timesWith count (received >>> echo) >>> arr (const ())
  where
    received = (\sock -> (sock, sock)) ^>> first receiveOne >>^ snd
    echo = (\sock -> ((sock, [BS.singleton 7]), sock)) ^>> first sendAll >>^ snd

-- | The part receives and sends back this many bytes, one at a time, with a thread that sends a byte and waits for the
-- echo: the thread waits a little before the first, so that the part waits first.
echoing :: Int -> (forall a. Turn -> Program a -> IO a) -> IO ()
echoing count run = do
  (mine, theirs) <- newPair
  done <- newEmptyMVar
  _ <- forkIO (driver theirs count `finally` putMVar done ())
  _ <- run defaultTurn (arr (const mine) >>> echoes count)
  takeMVar done
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
  | core == 0 = arr (const mine) >>> (\sock -> (sock, sock)) ^>> first receiveOne >>> now (\(_, sock) -> atomicWriteIORef shelf (Just sock))
  | otherwise = untilJust (readIORef shelf) >>> receiveOne >>> arr (const ())

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

-- | The ends of the ping-pong's two sides: a request that carries the giver of its answer, and an answer that carries the giver
-- of the next request.
newtype Ping r = Ping (Giver r (Pong r))

newtype Pong r = Pong (Giver r (Ping r))

-- | Two parts of one core that give each other a ticket and wait for the answer, this many times: each round is short, and a part
-- that waits for a ticket never yields.
pingPong :: forall arr. (Rich arr) => Int -> arr () ()
pingPong count = ticket @arr @(Local arr) >>> together [arr snd >>> client, arr fst >>> server]
  where
    client = timesWith count (call Ping >>^ \(Pong following) -> following) >>^ const ()
    server = timesWith count (await >>> (\(Ping answer) -> answer) ^>> expect Pong) >>^ const ()

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
    busy = pingPong count >>> now (\() -> monotonicNanoseconds >>= writeIORef finished) >>> arr (const (theirs, [BS.singleton 7])) >>> sendAll
    waiting = arr (const mine) >>> receiveOne >>> now (\_ -> monotonicNanoseconds >>= writeIORef woke) >>> arr (const mine) >>> receiveOne >>> arr (const ())

-- | Waits for a byte on the socket with a deadline that has passed, this many times, while a ping-pong runs.
cancelledWaits :: (Rich arr) => Int -> Int -> Socket -> arr () ()
cancelledWaits waits count sock = together [pingPong count, arr (const sock) >>> timesWith waits (within (nanos 1) receiveOne >>^ const sock) >>^ const ()]
