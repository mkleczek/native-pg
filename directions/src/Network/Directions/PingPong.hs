-- | Two parts that send each other a value and wait for the answer, over
-- tickets that are made as they go, written once for any arrow that has them:
-- the program that the tickets of "Network.Directions.Classes" are measured
-- with, on one core, across two, and in two threads.
module Network.Directions.PingPong
  ( pingPong,
    Handoff,
    newHandoff,
    remoteClient,
    remoteServer,
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
import GHC.Clock (getMonotonicTimeNSec)
import Network.Directions.Classes
import Network.Directions.Combinators

-- | A request: the ticket that the answer is given at.
newtype Request arr = Request (Ticket arr (Answer arr))

-- | The answer: the ticket of the next request.
newtype Answer arr = Answer (Ticket arr (Request arr))

-- | A client and a server, at once, that make this many round trips. The
-- client makes a ticket for the answer, gives the server the request with it,
-- and waits for the answer; the server waits for the request, makes the ticket
-- of the next, and answers with it. Each round trip makes two tickets, gives
-- two and waits for two, and a part waits twice, as parts on one core do when
-- each has to wait for the other.
pingPong :: (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr) => Int -> arr () ()
pingPong rounds = newTicket >>> concurrently client server >>^ const ()
  where
    client = timesWith rounds (callLocal Request >>^ \(Answer next) -> next)
    server = timesWith rounds (await >>> (\(Request answer) -> answer) ^>> expectLocal Answer)

-- | Where the server leaves the giver of its first request for the client to
-- find: a plain reference, which is all that two cores have in common to
-- start with.
newtype Handoff = Handoff (IORef (Maybe (Giver Ping)))

newHandoff :: IO Handoff
newHandoff = Handoff <$> newIORef Nothing

-- | The request: the giver of the answer.
newtype Ping = Ping (Giver Pong)

-- | The answer: the giver of the next request.
newtype Pong = Pong (Giver Ping)

-- | Makes an end to wait at, leaves its giver in the reference for a part of
-- another core to find, and keeps the end.
leave :: (Arrow arr, Effect arr, Remote arr) => IORef (Maybe (Giver a)) -> arr () (Awaiting arr a)
leave box = newRemote >>> now (\(end, giver) -> end <$ atomicWriteIORef box (Just giver))

-- | The server of a ping-pong across cores, which runs on the core of the end
-- it waits at: it leaves the giver of the end in the handoff, and for each
-- request makes the end of the next, and answers with its giver.
remoteServer :: (ArrowChoice arr, Effect arr, Remote arr) => Handoff -> Int -> arr () ()
remoteServer (Handoff box) rounds =
  leave box >>> timesWith rounds (awaitRemote >>> (\(Ping answer) -> answer) ^>> expect Pong) >>^ const ()

-- | The client of a ping-pong across cores: it waits for the server's giver to
-- be left in the handoff, and for each round trip makes the end for the answer,
-- gives the request, and waits for the answer.
remoteClient :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Handoff -> Int -> arr () ()
remoteClient (Handoff box) rounds =
  untilJust (readIORef box) >>> timesWith rounds (call Ping >>^ \(Pong next) -> next) >>^ const ()

-- | What a sleeper and a waker have in common: where the sleeper shows the giver of its ticket, and the
-- latencies, in nanoseconds, that it noted.
data Wake = Wake (IORef (Maybe (Giver Word64))) (IORef [Word64])

newWake :: IO Wake
newWake = Wake <$> newIORef Nothing <*> newIORef []

-- | The latencies, in the order they were noted, latest first.
wakeLatencies :: Wake -> IO [Word64]
wakeLatencies (Wake _ noted) = readIORef noted

-- | Makes a ticket, shows its giver, and waits for it, this many times; the waker gives the time it did, and the
-- sleeper notes how long it took to go on. When the sleeper's core has nothing else to run, its scheduler waits
-- in its poller.
sleeper :: (ArrowChoice arr, Effect arr, Remote arr) => Wake -> Int -> arr () ()
sleeper (Wake box noted) count = timesWith count (leave box >>> awaitRemote >>> now note)
  where
    note given = getMonotonicTimeNSec >>= \arrived -> modifyIORef' noted ((arrived - given) :)

-- | Waits for the sleeper's giver, then this many microseconds for its scheduler to wait, and gives the time:
-- this many times.
waker :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Wake -> Int -> Int -> arr () ()
waker (Wake box _) microseconds count = timesWith count (untilJust (atomicModifyIORef' box (Nothing,)) >>> now delayed >>> give >>^ const ())
  where
    delayed giver = threadDelay microseconds *> getMonotonicTimeNSec >>= \given -> pure (giver, given)
