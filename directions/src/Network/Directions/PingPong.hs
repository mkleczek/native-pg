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

import Control.Arrow (ArrowChoice, arr, first, returnA, second, (***), (>>>), (^>>), (|||))
import Control.Concurrent (threadDelay)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Network.Directions.Classes
import Network.Directions.Proxy (times)

-- | A request: the ticket of the request that follows it, and the ticket that
-- the answer is given at.
data Request arr = Request (Ticket arr (Request arr)) (Ticket arr ())

-- | A client and a server, at once, that make this many round trips. The
-- client makes a ticket for the next request and one for the answer, gives the
-- server the request with both, and waits for the answer; the server waits for
-- the request, and answers it. Each round trip makes two tickets, gives two
-- and waits for two, and a part waits twice, as parts on one core do when each
-- has to wait for the other.
pingPong :: (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr) => Int -> arr () ()
pingPong rounds = newTicket >>> (rounds,) ^>> concurrently client server >>> arr (const ())
  where
    client = (\(n, q) -> if n == 0 then Left () else Right (n, q)) ^>> (returnA ||| (turn >>> client))
      where
        turn =
          (\(n, q) -> ((n, q), ((), ())))
            ^>> second (newTicket *** newTicket)
            >>> (\((n, q), (q', r)) -> ((q, Request q' r), (n - 1, q', r)))
            ^>> first fulfil
            >>> (\((), (n, q', r)) -> (r, (n, q')))
            ^>> first await
            >>> arr snd
    server = (\(n, q) -> if n == 0 then Left () else Right (n, q)) ^>> (returnA ||| (turn >>> server))
      where
        turn =
          (\(n, q) -> (q, n - 1))
            ^>> first await
            >>> (\(Request q' r, n) -> ((r, ()), (n, q')))
            ^>> first fulfil
            >>> arr snd

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

-- | The server of a ping-pong across cores, which runs on the core of the end
-- it waits at: it leaves the giver of the end in the handoff, and for each
-- request makes the end of the next, and answers with its giver.
remoteServer :: (ArrowChoice arr, Effect arr, Remote arr) => Handoff -> Int -> arr () ()
remoteServer (Handoff box) rounds =
  newRemote
    >>> now (\(awaiting, giver) -> awaiting <$ writeIORef box (Just giver))
    >>> (rounds,)
    ^>> loop
  where
    loop = (\(n, aw) -> if n == 0 then Left () else Right (n, aw)) ^>> (returnA ||| (turn >>> loop))
    turn =
      (\(n, aw) -> (aw, n - 1))
        ^>> first awaitRemote
        >>> (\(Ping answer, n) -> ((), (answer, n)))
        ^>> first newRemote
        >>> (\((aw', gv'), (answer, n)) -> ((answer, Pong gv'), (n, aw')))
        ^>> first give
        >>> arr snd

-- | The client of a ping-pong across cores: it waits for the server's giver to
-- be left in the handoff, and for each round trip makes the end for the answer,
-- gives the request, and waits for the answer.
remoteClient :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Handoff -> Int -> arr () ()
remoteClient (Handoff box) rounds = found >>> (rounds,) ^>> loop
  where
    found = now (\() -> maybe (Left ()) Right <$> readIORef box) >>> ((giveWay >>> found) ||| returnA)
    loop = (\(n, gv) -> if n == 0 then Left () else Right (n, gv)) ^>> (returnA ||| (turn >>> loop))
    turn =
      (\(n, gv) -> ((), (n - 1, gv)))
        ^>> first newRemote
        >>> (\((awaiting, giver), (n, gv)) -> ((gv, Ping giver), (n, awaiting)))
        ^>> first give
        >>> (\(_, (n, awaiting)) -> (awaiting, n))
        ^>> first awaitRemote
        >>> arr (\(Pong gv, n) -> (n, gv))

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
sleeper (Wake box noted) count = times count (newRemote >>> now (\(awaiting, giver) -> awaiting <$ atomicWriteIORef box (Just giver)) >>> awaitRemote >>> now note)
  where
    note given = getMonotonicTimeNSec >>= \arrived -> modifyIORef' noted ((arrived - given) :)

-- | Waits for the sleeper's giver, then this many microseconds for its scheduler to wait, and gives the time:
-- this many times.
waker :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Wake -> Int -> Int -> arr () ()
waker (Wake box _) microseconds count = times count (taken >>> now delayed >>> give >>> arr (const ()))
  where
    taken = now (\() -> maybe (Left ()) Right <$> atomicModifyIORef' box (Nothing,)) >>> ((giveWay >>> taken) ||| returnA)
    delayed giver = threadDelay microseconds *> getMonotonicTimeNSec >>= \given -> pure (giver, given)
