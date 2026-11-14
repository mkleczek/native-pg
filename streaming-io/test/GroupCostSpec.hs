{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

-- | What the parts of a group cost, which is a few KiB of live heap each, however many there are, and the time to start
-- thousands of them, which is a fraction of a second: a server starts a part for each connection that it may have, and
-- they all wait for the first.
--
-- The parts wait for tickets that nothing fulfils; one more part waits until all of them do, takes the live heap and the
-- time, and fulfils the tickets, so that the run ends. The heap of 64 parts is taken from that of 4,096, which leaves what
-- the program, the run and the arrow cost out of it. The suite runs with @-T@ ("-with-rtsopts" in its cabal stanza, as the
-- cursors' does) for the heap to be counted.
module GroupCostSpec (spec) where

import Control.Arrow (ArrowChoice, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, try)
import Control.Monad (forM_, replicateM)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (uncons)
import Data.Word (Word64)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats)
import Network.Streaming.Classes (Block, Concurrent (..), Effect (..), Exchange (..), GiveWay, Promise, Local, Time, millis, monotonicNanoseconds)
import Network.Streaming.Combinators (consume, done, keeping, localTicket, sleep)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Mem (performMajorGC)
import System.Timeout (timeout)

-- | What the cases' programs need of an arrow.
type Program = forall arr. (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Time arr, Concurrent arr, Exchange arr (Local arr)) => arr () ()

-- | An arrow, and how it runs a program.
data Runner = Runner String (Program -> IO ())

runners :: [Runner]
runners =
  [Runner "threads" onThreads, Runner "delimited" onDelimited, Runner "compiled" onCompiled, Runner "loop" onLoop]

-- | Each arrow runs a program for the unit: a function of its own, since a program is for any arrow.
onThreads, onDelimited, onCompiled, onLoop :: Program -> IO ()
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimited program ()
onCompiled program = fst <$> runCompiled program ()
onLoop program = fst <$> runLoop program ()

-- | What the part that watches took, when all the others waited: the live heap, in bytes, and the instant.
data Probe = Probe !Word64 !Word64

spec :: Spec
spec = describe "the parts of a group" $ forM_ runners $ \(Runner name run) -> describe name $ do
  it "cost a few KiB each of live heap" $ do
    few <- liveWith run 64
    many <- liveWith run 4096
    let perPart = (fromIntegral many - fromIntegral few) / 4032 :: Double
    if perPart < 8192
      then pure ()
      else failTest ("a part costs " <> show (round perPart :: Int) <> " bytes of live heap, " <> show (many `div` 1024) <> " KiB for 4,096 and " <> show (few `div` 1024) <> " KiB for 64")

  it "start in under a second when there are thousands" $ do
    took <- replicateM 3 (startingWith run 4096)
    let least = minimum took
    if least < 1000000000
      then pure ()
      else failTest ("starting 4,096 parts took " <> show (least `div` 1000000) <> " ms")

-- | The program: this many parts that wait for a ticket nothing fulfils, and the one that looks at them all waiting.
idleGroup :: forall arr. (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Time arr, Concurrent arr, Exchange arr (Local arr)) => IORef (Maybe Probe) -> Int -> arr () ()
idleGroup out count = now (const (newIORef [])) >>> together (watcher : replicate count waiter)
  where
    -- A part adds the promise of its ticket to the registry, which parts on several threads add to, and waits for the ticket.
    waiter :: arr (Registry arr) ()
    waiter = keeping localTicket >>> now (\((future, promise), registry) -> future <$ atomicModifyIORef' registry (\promises -> (promise : promises, ()))) >>> await
    -- The watcher looks every millisecond whether all have registered, and then takes the heap and the time, and fulfils
    -- the tickets.
    watcher :: arr (Registry arr) ()
    watcher = looking
    looking :: arr (Registry arr) ()
    looking = now (\registry -> (\promises -> if length promises >= count then Right promises else Left registry) <$> readIORef registry) >>> ((patient >>> looking) ||| (probing >>> fulfilling))
    patient :: arr (Registry arr) (Registry arr)
    patient = keeping (sleep (millis 1)) >>^ snd
    probing :: arr [Promise (Local arr) ()] [Promise (Local arr) ()]
    probing = now (\promises -> performMajorGC *> (Probe . gcdetails_live_bytes . gc <$> getRTSStats) <*> monotonicNanoseconds >>= \probe -> promises <$ writeIORef out (Just probe))
    fulfilling :: arr [Promise (Local arr) ()] ()
    fulfilling = maybe (Left ()) Right . uncons ^>> (returnA ||| (consume ((, ()) ^>> fulfil >>> done) >>> fulfilling))

-- | What the parts of a group register: the promises of their tickets.
type Registry arr = IORef [Promise (Local arr) ()]

-- | The live heap, in bytes, when this many parts wait.
liveWith :: (Program -> IO ()) -> Int -> IO Word64
liveWith run count = fst <$> probed run count

-- | The time from the start of the run to the moment that all the parts wait, in nanoseconds.
startingWith :: (Program -> IO ()) -> Int -> IO Word64
startingWith run count = snd <$> probed run count

-- | A run of the program, on a thread of its own that the waiting side gives up on after a minute.
probed :: (Program -> IO ()) -> Int -> IO (Word64, Word64)
probed run count = do
  out <- newIORef Nothing
  result <- newEmptyMVar
  started <- monotonicNanoseconds
  _ <- forkIO (try (run (idleGroup out count)) >>= putMVar result . either (\failure -> Left (show (failure :: SomeException))) Right)
  timeout 60000000 (takeMVar result)
    >>= maybe (failTest "the run did not end in a minute") (either (failTest . ("the run failed: " <>)) pure)
  readIORef out >>= maybe (failTest "the parts were never all waiting") (\(Probe live instant) -> pure (live, instant - started))
