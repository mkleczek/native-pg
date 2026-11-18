{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | A network server with one acceptor and a group of connection workers on each core. Cores with room ask for
-- connections. The acceptor takes a bounded batch from the listener and divides it among those cores by their
-- current load. When all cores are full, new connections wait in the listen backlog.
module Network.Streaming.Server
  ( ServerCore,
    newServerCores,
    onServerCore,
    serveConnections,
    acceptCap,
    Listen (..),
    tcp,
    inherited,
    borrowed,
  )
where

import Control.Arrow ((>>>), (>>^), (^>>), (|||))
import Control.Monad (forM_, unless, void, when)
import qualified Data.IntMap.Strict as IntMap
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import GHC.Records (HasField)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Classes
import Network.Streaming.Combinators
  ( pattern Again,
    pattern Finish,
    alongside,
    answering,
    asking,
    consume,
    decide,
    done,
    foreverA,
    iterateWith,
    step,
    tap,
    whether,
    withTicket,
  )
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (Group, group, spawn))
import Network.Streaming.Internal.Accept (Batch (..), acceptor, listening)
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Internal.Listen (Listen (..), borrowed, inherited, tcp)
import Network.Streaming.Internal.Split (shares)
import Network.Streaming.Named (newRecord)
import Network.Streaming.Pool (Pool, checkoutIn, newPool, releaseNow, takeNow)
import Network.Streaming.Wiring (flow, onExit, releasedBy, (-<), (<:))

-- | One core's view of the shared connection dispatcher.
data ServerCore = ServerCore !Int !Dispatcher

data Dispatcher = Dispatcher
  { waitingCores :: !(Pool Waiting),
    workerCounts :: !(IntMap.IntMap (IORef Count))
  }

-- | A core waiting for its next batch of connections.
data Waiting = Waiting !Int !(Promise 'Anywhere Batch)

-- | The number of live or reserved workers and the promise of the core waiting for a slot, if any.
data Count = Count !Int !(Maybe (Promise 'Anywhere ()))

-- | Allocates the dispatcher shared by the given number of cores and returns the view for core zero.
-- The dispatcher has at least one core.
newServerCores :: Int -> IO ServerCore
newServerCores cores =
  ServerCore 0
    <$> ( Dispatcher
            <$> newPool []
            <*> (IntMap.fromList <$> traverse (\core -> (core,) <$> newIORef (Count 0 Nothing)) [0 .. max 1 cores - 1])
        )

-- | Selects the view for a core numbered from zero, within the range allocated by 'newServerCores'.
onServerCore :: ServerCore -> Int -> ServerCore
onServerCore (ServerCore _ dispatcher) core = ServerCore core dispatcher

-- | Runs the server on one core. Run it on each participating core with its view of the same dispatcher
-- and its own application context. Core zero also opens and runs the listener. Each core uses the same
-- connection limit; a limit of zero or below leaves all connections in the backlog.
--
-- The worker borrows the accepted socket for the lifetime of the arrow. The server closes it and frees
-- its slot when the worker returns, fails, or is cancelled. A failed worker affects only its connection.
-- Cancelling the server closes unassigned sockets and waits for its workers to finish unwinding.
serveConnections :: (Runtime arr) => Int -> Listen arr -> arr (Socket, context) () -> arr (ServerCore, context) ()
serveConnections !limit listen worker =
  named (Label "serve connections" "Runs the connection workers on each core, with the acceptor on the first core.") $
    decide "first core?" (whether (\(ServerCore core _, _) -> core == 0))
      >>> ( "yes" |- together [fst ^>> fastLane (acceptance limit listen), workers]
              ||| "no" |- workers
          )
  where
    workers = connectionWorkers limit worker

-- * The acceptor

-- | Opens the listener, and then, round after round for ever, takes the cores that wait for connections, accepts as many as
-- they have room for, but no more than 'acceptCap', and gives each core its share. The listener is closed when the acceptor ends, however it ends.
acceptance :: (Runtime arr) => Int -> Listen arr -> arr ServerCore ()
acceptance limit Listen {openListener, closeListener} =
  named (Label "acceptance" "Opens the listener, and for ever takes the cores that wait for connections, accepts as many as they have room for, and gives each core its share.") $
    newRecord ["cores"]
      ^>> flow
        ( "listener" <: openListener                  -< []
            `releasedBy` closeListener                -< "listener" >>>
          step "make listener non-blocking" listening -< "listener" >>>
          foreverA (tap (acceptRound limit))          -< ("listener", "cores")
        ) >>>
      done

-- | A round of the acceptor: it waits until some core waits for connections and takes all that wait, accepts as many connections
-- as they have room for, in one step that takes none until one comes and no more than 'acceptCap', shares them out between the
-- cores, and hands each core its batch.
acceptRound :: (Runtime arr) => Int -> arr (Socket, ServerCore) ()
acceptRound limit =
  named (Label "round" "Takes the cores that wait for connections, accepts as many connections as they have room for, and hands each core its share.") $
    uncurry (newRecord ["listener", "cores"])
      ^>> flow
        ( "waiting" <: untilWanted        -< "cores" >>>
          "capacity" <: adding            -< ("waiting", "cores") >>>
          "batch" <: accept               -< ("capacity", "listener") >>>
          ["batches", "idle"] <: sharing' -< ("waiting", "batch", "cores") >>>
          putting                         -< ("idle", "cores") >>>
          handing                         -< "batches"
        ) >>>
      done
  where
    adding = step "add up the room of the cores, up to the cap" (capacityOf limit)
    accept = named (Label "accept" "Accepts the connections that wait, as many as the capacity, and blocks while none is waiting.") acceptor
    sharing' = step "share the sockets out between the cores" (sharing limit)
    putting = step "put back the cores that have no share" putBack
    handing = answering "connection" (step "hand over the batches" handOver)

-- | Waits until some core waits for connections, and takes all the cores that wait, the one that waited longest last.
untilWanted :: (Runtime arr) => arr ServerCore [Waiting]
untilWanted =
  named (Label "until wanted" "Waits until some core waits for connections, and takes all the cores that wait.") $
    (\(ServerCore _ dispatcher) -> waitingCores dispatcher)
      ^>> alongside checkoutIn
      >>> step "take the other waiting cores" (\(waiting, pool) -> (waiting :) <$> takeWaiting pool [])
  where
    takeWaiting pool taken = takeNow pool 1 >>= maybe (pure (reverse taken)) (\waiting -> takeWaiting pool (waiting : taken))

-- | How many connections the cores that wait have room for: their share of the connection cap, 'connectionsPerCore', less the
-- connection workers that they have, but no more than 'acceptCap', the most that one accept step takes.
capacityOf :: Int -> ([Waiting], ServerCore) -> IO Int
capacityOf limit (waiting, ServerCore _ dispatcher) = do
  counts <- readCounts dispatcher
  pure $! min acceptCap (sum [limit - liveOf counts core | Waiting core _ <- waiting])

-- | The most connections that one accept step takes, so that the step is short for the other parts of its core.
acceptCap :: Int
acceptCap = 32

-- | The most sockets in a batch that the core's own loop spawns workers for, where a larger batch is given to a starter, which costs more
-- than the give-ways of a few spawns.
smallBatch :: Int
smallBatch = 4

-- | The batch of each core that is given sockets, and the cores that are given none, from how many connection workers each core has now.
sharing :: Int -> ([Waiting], (Batch, ServerCore)) -> IO ([(Waiting, Batch)], [Waiting])
sharing limit (waiting, (Batch accepted sockets, ServerCore _ dispatcher)) = do
  counts <- readCounts dispatcher
  let loads = [(live, room) | Waiting core _ <- waiting, let !live = liveOf counts core, let !room = limit - live]
  pure $! parcel [] [] waiting (shares accepted loads) sockets
  where
    parcel batches spare cores !given remaining = case (cores, given) of
      (core : others, count : counts)
        | count <= 0 -> parcel batches (core : spare) others counts remaining
        | otherwise -> case splitAt count remaining of (mine, rest) -> parcel ((core, Batch count mine) : batches) spare others counts rest
      _ -> (reverse batches, reverse spare)

-- | Puts the cores that were given no sockets back in the pool of the cores that wait, as they were.
putBack :: ([Waiting], ServerCore) -> IO ()
putBack (idle, ServerCore _ dispatcher) =
  unless (null idle) (releaseNow (waitingCores dispatcher) idle)

-- | Gives each core its batch, in one action that never waits, so that no socket is lost to a cancellation between two cores. A batch
-- whose core has stopped waiting is closed, which is so when the server is shutting down.
handOver :: [(Waiting, Batch)] -> IO ()
handOver batches =
  forM_ batches $ \(Waiting _ promise, batch) ->
    fulfilNow promise batch >>= \taken -> unless taken (closeBatch batch)

-- | Closes the sockets of the batch.
closeBatch :: Batch -> IO ()
closeBatch (Batch _ sockets) = closeAll sockets

-- | Closes the sockets.
closeAll :: [Socket] -> IO ()
closeAll = mapM_ Socket.close

-- | How many connection workers each core has, by the core's number, each count read here and not left as a thunk of the read.
readCounts :: Dispatcher -> IO (IntMap.IntMap Int)
readCounts dispatcher = traverse (\ref -> readIORef ref >>= \(Count live _) -> pure live) (workerCounts dispatcher)

-- | How many connection workers the core has, by the counts of the cores.
liveOf :: IntMap.IntMap Int -> Int -> Int
liveOf counts core = IntMap.findWithDefault 0 core counts

-- * The connection workers

-- | The count of the core's connection workers.
countOf :: Dispatcher -> Int -> IORef Count
countOf dispatcher core = workerCounts dispatcher IntMap.! core

-- | The connection workers of the core: while the core has room, it waits for a batch of connections, and has a connection worker
-- spawned for each, for ever. When it is cancelled it waits for its connection workers to end. The loop is in the fast lane, so that its
-- waits, for room and for a batch, are inside it and the core is back in the pool of the cores that wait after at most one part of the
-- normal lane, and the starters and the connection workers are not in it.
connectionWorkers :: (Runtime arr) => Int -> arr (Socket, context) () -> arr (ServerCore, context) ()
connectionWorkers limit worker =
  named (Label "connection workers" "Waits for the core to have room and then for a batch of connections, and has a connection worker spawned for each, for ever.") $
    group AwaitAll (fastLane (foreverA (tap (spawning limit worker)))) >>> done

-- | A round of the core: it waits until it has room, waits for a batch of connections, counts the connection workers that the batch
-- will make, and has them spawned: by the round itself for a small batch, and by a starter for a larger one.
spawning ::
  (Runtime arr) =>
  Int ->
  arr (Socket, context) () ->
  arr (Group arr s AwaitAll, (ServerCore, context)) ()
spawning limit worker =
  named (Label "spawning" "Waits for room and for a batch of connections, counts the batch, and has a connection worker spawned for each socket.") $
    (\(!workers, (!cores, context)) -> newRecord ["group", "cores", "context"] workers cores context)
      ^>> flow
        ( untilRoom limit     -< "cores" >>>
          "batch" <: pull    -< "cores" >>>
          counting           -< ("batch", "cores") >>>
          started            -< ["group", "batch", "cores", "context"]
        ) >>>
      done
  where
    counting = step "count the connection workers" counted
    -- One value for both branches: two copies of the connection worker's code keep the compiler from specialising it to the arrow.
    starter = starting worker
    started =
      decide "small batch?" bySize >>>
        ("yes" |- starter ||| "no" |- startInBackground)
    bySize r = case r.batch of
      Batch size _ -> if size <= smallBatch then Left r else Right (r.group, r)
    startInBackground = named starterLabel (spawn starter)
    starterLabel =
      Label
        "hand the batch to a starter"
        "Spawns a starter to give each socket a worker. The core can wait for the next batch immediately."
    counted (Batch size _, ServerCore core dispatcher) =
      update (countOf dispatcher core) (\(Count live waiter) -> (Count (live + size) waiter, ()))

-- | Spawns a connection worker for each socket of the batch, one at a time, giving way before each, so that the other parts of the core
-- run between them when the batch is large. The sockets that have no connection worker when this ends, which it does as it is cancelled,
-- are closed and taken off the count.
starting ::
  forall arr s r context.
  (Runtime arr, HasField "group" r (Group arr s AwaitAll), HasField "batch" r Batch, HasField "cores" r ServerCore, HasField "context" r context) =>
  arr (Socket, context) () ->
  arr r ()
starting worker =
  named (Label "start workers" "Spawns a connection worker for each socket of the batch, giving way before each, and closes the sockets that were not given one if it is cancelled.") $
    flow
      ( "pendingBatch" <: keepBatch    -< "batch"
          `releasedBy` closeUnassigned -< ("pendingBatch", "cores") >>>
        spawnEach                      -< ["pendingBatch", "cores", "group", "context"]
      ) >>>
    done
  where
    keepBatch = step "keep the batch" (\batch -> newIORef $! batch)
    closeUnassigned = step "close the sockets that have no connection worker" unstarted
    spawnEach = iterateWith (giveWay >>> step "take the next socket" takeNext -| "socket left?") (consume (spawn (connectionWorker worker)))
    takeNext input =
      atomicModifyIORef' input.pendingBatch takeSocket >>= \case
        Nothing -> pure (Finish ())
        Just socket -> let !workers = input.group in pure (Again ((workers, (input.cores, (socket, input.context))), input))
    takeSocket batch = case batch of
      Batch size (socket : others) -> (Batch (size - 1) others, Just socket)
      Batch _ [] -> (batch, Nothing)
    unstarted (pendingBatch, cores) = do
      Batch size left <- readIORef pendingBatch
      unless (size == 0) (closeAll left *> leaving size cores)

-- | Waits until the core has room for another connection worker: fewer than its share of the cap. If it has none, the
-- part leaves the promise of a ticket in the count of the core, which the end of one of its connection workers fulfils,
-- and waits for it.
untilRoom :: (Runtime arr) => Int -> arr ServerCore ()
untilRoom limit =
  named (Label "until room" "Waits while the core has as many connection workers as its share of the cap, until one of them ends.") $
    iterateWith
      (step "count the connection workers" hasRoom -| "room?")
      ("full" |- tap (withTicket (step "register as waiting" registering) done >>> done))
  where
    hasRoom cores@(ServerCore core dispatcher) = decided <$> readIORef (countOf dispatcher core)
      where
        decided (Count live _) = if live < limit then Finish () else Again cores
    -- A core that has room by now has the promise fulfilled at once, as the end of a connection worker would.
    registering (promise, ServerCore core dispatcher) = do
      room <- update (countOf dispatcher core) $ \count@(Count live _) ->
        if live < limit then (count, True) else (Count live (Just promise), False)
      when room (void (fulfilNow promise ()))

-- | Puts the core in the pool of the cores that wait for connections, with the promise of a batch of sockets, and waits for the
-- acceptor to give one. A batch that is given when the wait was cancelled is closed.
pull :: (Runtime arr) => arr ServerCore Batch
pull =
  named (Label "pull" "Puts the core in the pool of the cores that wait for connections, and waits for the acceptor to give it a batch.") $
    asking "connection" (withTicket (step "wait in the pool of cores" waiting) (fst ^>> step "close the sockets" closeBatch)) >>^ snd
  where
    waiting (promise, ServerCore core dispatcher) = releaseNow (waitingCores dispatcher) [Waiting core promise]

-- | Owns the accepted socket and the worker slot until the application worker ends.
connectionWorker :: (Runtime arr) => arr (Socket, context) () -> arr (ServerCore, (Socket, context)) ()
connectionWorker worker =
  named workerLabel $
    uncurry (newRecord ["cores", "connection"])
      ^>> flow
        ( onExit (freeSlot  -< "cores") >>>
          onExit (closePeer -< "connection") >>>
          worker           -< "connection"
        ) >>>
    done
  where
    workerLabel = Label "connection worker" "Serves a connection, closes its socket and frees its worker slot on exit."
    freeSlot = step "free worker slot" (leaving 1)
    closePeer = fst ^>> close

-- | Takes this many connection workers off the count of the core, and tells the part that waits for room, if one does.
leaving :: Int -> ServerCore -> IO ()
leaving workers (ServerCore core dispatcher) = do
  waiter <- update (countOf dispatcher core) (\(Count live waiting) -> (Count (live - workers) Nothing, waiting))
  forM_ waiter (`fulfilNow` ())
