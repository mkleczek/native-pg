{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

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
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.Classes
import Network.Streaming.Combinators (timesWith, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Counters
import Network.Streaming.Delimited
import Network.Streaming.Internal.Descriptors (fresh)
import Network.Streaming.Internal.Scheduler (Turn, defaultTurn)
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
