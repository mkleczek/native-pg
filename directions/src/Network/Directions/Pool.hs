{-# LANGUAGE DataKinds #-}

-- | A pool of connections as library code, on the primitives of the classes
-- alone: one atomically updated reference to the connections that are free
-- and the 'Giver's of those that wait for one, and an arrow's tickets of reach
-- 'Anywhere for the waiting: the pool is shared by the parts of every core, so
-- the givers it keeps can't belong to one. It is written once, for any arrow that
-- has them: parts on the cores of "Network.Directions.Delimited" share it, and so
-- do the threads of "Network.Directions.Threads".
module Network.Directions.Pool
  ( Pool,
    newPool,
    checkout,
    checkoutWithin,
    reserve,
    release,
    free,
  )
where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (^>>), (|||))
import Data.IORef (IORef, newIORef, readIORef)
import Data.Sequence (Seq, ViewL (..), viewl, (|>))
import qualified Data.Sequence as Seq
import Network.Directions.Atomic (update)
import Network.Directions.Classes

-- | The connections that are free, and the givers of those that wait for one,
-- in the order they came. While there are waiters, none is free.
data State conn = State [conn] (Seq (Giver 'Anywhere conn))

newtype Pool conn = Pool (IORef (State conn))

newPool :: [conn] -> IO (Pool conn)
newPool conns = Pool <$> newIORef (State conns Seq.empty)

-- | The connections that are free now.
free :: Pool conn -> IO [conn]
free (Pool ref) = (\(State conns _) -> conns) <$> readIORef ref

-- | A connection, once one is free: the first part to ask has the first that
-- is released. A part that is cancelled while it waits gives up its place, and
-- a connection that was given to it meanwhile goes back to the pool, so that
-- none is lost.
checkout :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool conn -> arr () conn
checkout pool = reserve pool >>> (scope returnA giveUp (arr fst >>> await) ||| returnA)
  where
    giveUp = (\(awaiting, exit) -> case exit of Done -> Left (); _ -> Right awaiting) ^>> (returnA ||| (abandon >>> (maybe (Left ()) Right ^>> (returnA ||| release pool))))

-- | A connection, if one is free within the time, as 'checkout' waits for it: 'Nothing' if it isn't, once the
-- place in the queue is given up.
checkoutWithin :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Within arr) => Duration -> Pool conn -> arr () (Maybe conn)
checkoutWithin limit pool = within limit (checkout pool)

-- | A connection if one is free, or else the end to wait at for the one that
-- is released to this part: it queues the part's giver. The waiter can
-- 'abandon' it instead, which gives back a connection that came meanwhile.
-- Asking is a point to give way at, as a link's receive is: a part that does
-- nothing else than check connections out and release them doesn't keep the
-- others of its core from running.
reserve :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Pool conn -> arr () (Either (Waiting arr 'Anywhere conn) conn)
reserve (Pool ref) = giveWay >>> now (\() -> update ref takeFree) >>> (queue ||| arr Right)
  where
    takeFree (State conns waiters) = case conns of
      conn : others -> (State others waiters, Right conn)
      [] -> (State [] waiters, Left ())
    -- Only now is there a ticket to make; a connection released between the
    -- look and the queueing is there for this part to take.
    queue = ticket >>> now (\(awaiting, giver) -> update ref (enqueue giver awaiting))
    enqueue giver awaiting (State conns waiters) = case conns of
      conn : others -> (State others waiters, Right conn)
      [] -> (State [] (waiters |> giver), Left awaiting)

-- | Gives the connection to the first part that waits for one. A waiter that
-- has gone doesn't take it, and the next is tried; with none left, the
-- connection is free.
release :: (ArrowChoice arr, Effect arr, Exchange arr 'Anywhere) => Pool conn -> arr conn ()
release (Pool ref) = loop
  where
    loop = now (update ref . hand) >>> (returnA ||| handOver)
    hand conn (State conns waiters) = case viewl waiters of
      giver :< others -> (State conns others, Right (giver, conn))
      EmptyL -> (State (conn : conns) waiters, Left ())
    -- Gives it, and goes on with it if the waiter wasn't there.
    handOver =
      (\(giver, conn) -> ((giver, conn), conn))
        ^>> first give
        >>> (\(taken, conn) -> if taken then Left () else Right conn)
        ^>> (returnA ||| loop)
