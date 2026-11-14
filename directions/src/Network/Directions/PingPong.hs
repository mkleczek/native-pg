{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}

-- | Two parts that send each other a value and wait for the answer, over
-- tickets that are made as they go, written once for any arrow that has them
-- and for any reach: the program that the tickets of "Network.Streaming.Classes" are measured
-- with, on one core, across two, and in two threads.
module Network.Directions.PingPong
  ( Ping,
    Pong,
    client,
    server,
    pingPong,
    pingPongAt,
    Handoff,
    newHandoff,
    leave,
    find,
    Wake,
    newWake,
    wakeLatencies,
    sleeper,
    waker,
  )
where

import Control.Arrow (Arrow, ArrowChoice, (>>>), (>>^), (^>>))
import Control.Concurrent (threadDelay)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef)
import Data.Word (Word64)
import Network.Streaming.Classes hiding (Stream (..))
import Network.Streaming.Combinators hiding (box)

-- | A request: the giver of the answer.
newtype Ping r = Ping (Giver r (Pong r))

-- | The answer: the giver of the next request.
newtype Pong r = Pong (Giver r (Ping r))

-- | The client of a ping-pong: for each round trip it makes the ticket of the answer, gives the request that
-- carries its giver, and waits for the answer, which carries the giver of the next request.
client :: (ArrowChoice arr, Annotate arr, Exchange arr r) => Int -> arr (Giver r (Ping r)) ()
client rounds = timesWith rounds (expect Ping >>> await >>^ \(Pong next) -> next) >>> done

-- | The server: it waits for the request at the end it was given, makes the ticket of the next, and answers with its
-- giver.
server :: (ArrowChoice arr, Annotate arr, Exchange arr r) => Int -> arr (Waiting arr r (Ping r)) ()
server rounds = timesWith rounds (await >>> (\(Ping answer) -> answer) ^>> expect Pong) >>> done

-- | A client and a server, at once, that make this many round trips, over tickets of this reach. They get the two
-- ends of the first ticket. Each round trip makes two tickets, gives two and waits for two, and a part waits twice,
-- as parts on one core do when each has to wait for the other. With the reach of the arrow's own core, the
-- parts are on one core; with 'Anywhere' they could be on two, and this runs them on one.
pingPongAt :: forall r arr. (ArrowChoice arr, Annotate arr, Concurrent arr, Exchange arr r) => Int -> arr () ()
pingPongAt rounds = ticket @arr @r >>> together [snd ^>> client rounds, fst ^>> server rounds]

-- | 'pingPongAt' with the reach that costs the least: that of the arrow's own core.
pingPong :: forall arr. (ArrowChoice arr, Annotate arr, Concurrent arr, Exchange arr (Local arr)) => Int -> arr () ()
pingPong = pingPongAt @(Local arr)

-- | Where a part leaves the giver of a ticket for a part of another core to find: a plain reference, which is all
-- that two cores have in common to start with. Its reach is 'Anywhere', the only one a giver can have to cross cores.
newtype Handoff a = Handoff (IORef (Maybe (Giver 'Anywhere a)))

newHandoff :: IO (Handoff a)
newHandoff = Handoff <$> newIORef Nothing

-- | Makes a ticket, leaves its giver in the handoff, and keeps the end to wait at.
leave :: (Arrow arr, Effect arr, Exchange arr 'Anywhere) => Handoff a -> arr () (Waiting arr 'Anywhere a)
leave (Handoff box) = ticket >>> now (\(end, giver) -> end <$ atomicWriteIORef box (Just giver))

-- | Waits, giving way, until a giver is left in the handoff, and gives it.
find :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr) => Handoff a -> arr () (Giver 'Anywhere a)
find (Handoff box) = untilJust (readIORef box)

-- | What a sleeper and a waker have in common: where the sleeper shows the giver of its ticket, and the
-- latencies, in nanoseconds, that it noted.
data Wake = Wake (Handoff Word64) (IORef [Word64])

newWake :: IO Wake
newWake = Wake <$> newHandoff <*> newIORef []

-- | The latencies, in the order they were noted, latest first.
wakeLatencies :: Wake -> IO [Word64]
wakeLatencies (Wake _ noted) = readIORef noted

-- | Makes a ticket, shows its giver, and waits for it, this many times; the waker gives the time it did, and the
-- sleeper notes how long it took to go on. When the sleeper's core has nothing else to run, its scheduler waits
-- in its poller.
sleeper :: (ArrowChoice arr, Effect arr, Annotate arr, Exchange arr 'Anywhere) => Wake -> Int -> arr () ()
sleeper (Wake handoff noted) count = timesWith count (leave handoff >>> await >>> now note)
  where
    note given = monotonicNanoseconds >>= \arrived -> modifyIORef' noted ((arrived - given) :)

-- | Waits for the sleeper's giver, then this many microseconds for its scheduler to wait, and gives the time:
-- this many times.
waker :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr, Exchange arr 'Anywhere) => Wake -> Int -> Int -> arr () ()
waker (Wake (Handoff box) _) microseconds count =
  timesWith count (untilJust (atomicModifyIORef' box (Nothing,)) >>> now delayed >>> give >>> done)
  where
    delayed giver = threadDelay microseconds *> monotonicNanoseconds >>= \given -> pure (giver, given)
