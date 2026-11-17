{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

-- | The server as a network of parts whose connection workers are spawned as the connections are accepted. The first
-- core has the acceptor, which opens the listener when the server starts and closes it when it ends, and each core has
-- a group of connection workers. A core that has room for another connection, which is to have fewer connection workers
-- than its share of the cap, puts itself in the pool of the cores that wait, with the promise of a batch of sockets, and
-- waits. In each round the acceptor waits until some core waits, takes all the cores that wait, accepts as many
-- connections as they have room for, but no more than 'acceptCap', in one step that never waits, and shares them out
-- between the cores by how many connection workers each has then, so that they have as many as one another afterwards, as
-- far as their room allows. It gives each core its batch in one hand-over, and puts back the cores that have none:
-- connections are shared out by what is true when they are accepted, and nothing is made for a connection before it is
-- there. The core counts its batch and spawns a connection worker for each socket, giving way between them, itself if the
-- batch is small, and by a starter, a part of its own, if it is not, so that the core waits for the next batch at once and the
-- acceptor's next round doesn't wait for the starts. A connection worker serves its connection, request after request, until
-- it ends, and tells the core that it has room again. While every core is full the acceptor accepts nothing, so that past the cap on connections they wait
-- in the listen backlog. A connection that waits for its next request for longer than the idle timeout is closed, so that the
-- connections that wait in the backlog are served though the ones that have a connection worker are idle: the connection
-- has a watcher beside the part that serves its requests, which sleeps until the connection can have waited that long,
-- and ends it if it has, unless a request is in the connection's socket by then: closing with the request unread would reset the
-- connection, and the client would lose it.
--
-- A request is read into the primary buffer that it was admitted with. The bytes that follow it in the buffer, which
-- belong to the next request on the connection, are copied out before the buffer is used for anything else, and
-- the next request begins with them: pipelining costs a copy of the bytes that came early, and nothing otherwise.
module Network.Fulmen.Internal.Serve
  ( acceptCap,
    serve,
  )
where

import Control.Arrow (arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (SomeException, catch, fromException)
import Control.Monad (forM_, unless, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import qualified Data.IntMap.Strict as IntMap
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, newIORef, readIORef, writeIORef)
import Data.Maybe (isJust)
import Data.Word (Word8)
import qualified Foreign.C.Error as Errno
import Foreign.C.Types (CInt (..), CSize (..))
import Foreign.ForeignPtr (plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (poke)
import GHC.Records (HasField)
import Network.Fulmen.Internal
import Network.Fulmen.Internal.Accept (acceptor, listening)
import Network.Fulmen.Internal.Body (BodyRefused (..), BodyState (..), ChunkState (..), ConnectionLost (..), Decoding (..), Marks (..), drain)
import Network.Fulmen.Internal.Bytes (equalsLower)
import Network.Fulmen.Internal.Date (beginDraining)
import Network.Fulmen.Internal.Head (Framing (..), Parsed (..), Scan (..), Spill (..), joined, notYet, parseHead, scanHead, startScan)
import Network.Fulmen.Internal.Response (Exchange (..), Phase (..), newExchange, refusal)
import Network.Fulmen.Internal.Split (shares)
import Network.Fulmen.Listen (Listen (..))
import Network.Fulmen.Request (Method (..), Request (..), Version (..))
import Network.Socket (SockAddr (..), Socket, withFdSocket)
import qualified Network.Socket as Socket
import qualified Network.Socket.Address as SocketAddress
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators
  ( pattern Again,
    pattern Finish,
    Step,
    alongside,
    answering,
    asking,
    consume,
    decide,
    done,
    foreverA,
    iterateA,
    iterateWith,
    keeping,
    step,
    tap,
    whether,
    withTicket,
  )
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (Group, group, spawn))
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Named (Inj, casesBy, inj, newRecord, type (=:), (:=), (?>))
import Network.Streaming.Pool (checkoutIn, releaseNow, takeNow)
import Network.Streaming.Wiring (choice, flow, onExit, releasedBy, result, (-<), (<:))
import System.Posix.Types (CSsize (..))

-- | The server: for each core, its connection workers, and for the first, the acceptor, which opens the listener. The whole acceptance is in
-- the fast lane, so that its waits, for a core that waits for connections and for the listener to be readable, are inside it, and it comes
-- back after at most one part of the normal lane.
serve :: (Runtime arr) => Settings -> Listen arr -> arr (Call, Request) () -> arr Cores ()
serve !settings listen handler =
  named (Label "serve" "Runs the connection workers of each core, and the acceptor besides on the first core.") $
    decide "first core?" (whether isFirst)
      >>> ("yes" |- shutting [fastLane (acceptance settings listen), workers] ||| "no" |- shutting [workers])
  where
    workers = connectionWorkers settings handler
    isFirst (Cores core _) = core == 0
    -- A server that has an event to shut down on runs its parts in a race with the wait for the event. When it fires,
    -- the server is marked as shutting down and the parts are cancelled where they wait: the acceptor, whose listener
    -- closes, and the group of connection workers of each core, whose part that waits for a connection and whose
    -- connection workers that wait for a request are cancelled. A request in flight is behind a grace (see
    -- 'handleRequest'), so it finishes, or is cancelled where it waits when the drain's deadline has passed, and the race
    -- ends when no part is left.
    shutting parts = case shutdown settings of
      Nothing -> together parts
      Just event -> race (beginning event) (together parts) >>> done

-- | Waits for the event that starts the shutdown, and marks the server as shutting down: a response from then on says that its
-- connection ends.
beginning :: (Runtime arr) => Event -> arr Cores ()
beginning event =
  named (Label "shutdown" "Waits for the event that starts the shutdown, and marks the server as shutting down.") $
    tap (const event ^>> block)
      >>> step "begin the shutdown" (\(Cores _ shared) -> beginDraining (sharedPulse shared))

-- * The acceptor

-- | Opens the listener, and then, round after round for ever, takes the cores that wait for connections, accepts as many as
-- they have room for, but no more than 'acceptCap', and gives each core its share. The listener is closed when the acceptor ends, however it ends.
acceptance :: (Runtime arr) => Settings -> Listen arr -> arr Cores ()
acceptance settings Listen {openListener, closeListener} =
  named (Label "acceptance" "Opens the listener, and for ever takes the cores that wait for connections, accepts as many as they have room for, and gives each core its share.") $
    newRecord ["cores"]
      ^>> flow
        ( "listener" <: openListener                  -< []
            `releasedBy` closeListener                -< "listener" >>>
          step "make listener non-blocking" listening -< "listener" >>>
          foreverA (tap (acceptRound settings))       -< ("listener", "cores")
        ) >>>
      done

-- | A round of the acceptor: it waits until some core waits for connections and takes all that wait, accepts as many connections
-- as they have room for, in one step that takes none until one comes and no more than 'acceptCap', shares them out between the
-- cores, and hands each core its batch.
acceptRound :: (Runtime arr) => Settings -> arr (Socket, Cores) ()
acceptRound settings =
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
    adding = step "add up the room of the cores, up to the cap" (capacityOf settings)
    accept = named (Label "accept" "Accepts the connections that wait, as many as the capacity, and blocks while none is waiting.") acceptor
    sharing' = step "share the sockets out between the cores" (sharing settings)
    putting = step "put back the cores that have no share" putBack
    handing = answering "connection" (step "hand over the batches" handOver)

-- | Waits until some core waits for connections, and takes all the cores that wait, the one that waited longest last.
untilWanted :: (Runtime arr) => arr Cores [Waiting]
untilWanted =
  named (Label "until wanted" "Waits until some core waits for connections, and takes all the cores that wait.") $
    (\(Cores _ shared) -> sharedWaiting shared)
      ^>> alongside checkoutIn
      >>> step "take the other waiting cores" (\(waiting, pool) -> (waiting :) <$> takeWaiting pool [])
  where
    takeWaiting pool taken = takeNow pool 1 >>= maybe (pure (reverse taken)) (\waiting -> takeWaiting pool (waiting : taken))

-- | How many connections the cores that wait have room for: their share of the connection cap, 'connectionsPerCore', less the
-- connection workers that they have, but no more than 'acceptCap', the most that one accept step takes.
capacityOf :: Settings -> ([Waiting], Cores) -> IO Int
capacityOf settings (waiting, Cores _ shared) = do
  counts <- readCounts shared
  pure $! min acceptCap (sum [connectionsPerCore settings - liveOf counts core | Waiting core _ <- waiting])

-- | The most connections that one accept step takes, so that the step is short for the other parts of its core.
acceptCap :: Int
acceptCap = 32

-- | The most sockets in a batch that the core's own loop spawns workers for, where a larger batch is given to a starter, which costs more
-- than the give-ways of a few spawns.
smallBatch :: Int
smallBatch = 4

-- | The batch of each core that is given sockets, and the cores that are given none, from how many connection workers each core has now.
sharing :: Settings -> ([Waiting], (Batch, Cores)) -> IO ([(Waiting, Batch)], [Waiting])
sharing settings (waiting, (Batch accepted sockets, Cores _ shared)) = do
  counts <- readCounts shared
  let loads = [(live, room) | Waiting core _ <- waiting, let !live = liveOf counts core, let !room = connectionsPerCore settings - live]
  pure $! parcel [] [] waiting (shares accepted loads) sockets
  where
    parcel batches spare cores !given remaining = case (cores, given) of
      (core : others, count : counts)
        | count <= 0 -> parcel batches (core : spare) others counts remaining
        | otherwise -> case splitAt count remaining of (mine, rest) -> parcel ((core, Batch count mine) : batches) spare others counts rest
      _ -> (reverse batches, reverse spare)

-- | Puts the cores that were given no sockets back in the pool of the cores that wait, as they were.
putBack :: ([Waiting], Cores) -> IO ()
putBack (idle, Cores _ shared) =
  unless (null idle) (releaseNow (sharedWaiting shared) idle)

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
readCounts :: Shared -> IO (IntMap.IntMap Int)
readCounts shared = traverse (\ref -> readIORef ref >>= \(Count live _) -> pure live) (sharedCounts shared)

-- | How many connection workers the core has, by the counts of the cores.
liveOf :: IntMap.IntMap Int -> Int -> Int
liveOf counts core = IntMap.findWithDefault 0 core counts

-- * The connection workers

-- | The count of the core's connection workers.
countOf :: Shared -> Int -> IORef Count
countOf shared core = sharedCounts shared IntMap.! core

-- | The connection workers of the core: while the core has room, it waits for a batch of connections, and has a connection worker
-- spawned for each, for ever. When it is cancelled it waits for its connection workers to end. The loop is in the fast lane, so that its
-- waits, for room and for a batch, are inside it and the core is back in the pool of the cores that wait after at most one part of the
-- normal lane, and the starters and the connection workers are not in it.
connectionWorkers :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
connectionWorkers settings handler =
  named (Label "connection workers" "Waits for the core to have room and then for a batch of connections, and has a connection worker spawned for each, for ever.") $
    group AwaitAll (fastLane (foreverA (tap (spawning settings handler)))) >>> done

-- | A round of the core: it waits until it has room, waits for a batch of connections, counts the connection workers that the batch
-- will make, and has them spawned: by the round itself for a small batch, and by a starter for a larger one.
spawning ::
  (Runtime arr) =>
  Settings ->
  arr (Call, Request) () ->
  arr (Group arr s AwaitAll, Cores) ()
spawning settings handler =
  named (Label "spawning" "Waits for room and for a batch of connections, counts the batch, and has a connection worker spawned for each socket.") $
    (\(!workers, !cores) -> newRecord ["group", "cores"] workers cores)
      ^>> flow
        ( untilRoom settings -< "cores" >>>
          "batch" <: pull    -< "cores" >>>
          counting           -< ("batch", "cores") >>>
          started            -< ["group", "batch", "cores"]
        ) >>>
      done
  where
    counting = step "count the connection workers" counted
    -- One value for both branches: two copies of the connection worker's code keep the compiler from specialising it to the arrow.
    starter = starting settings handler
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
    counted (Batch size _, Cores core shared) =
      update (countOf shared core) (\(Count live waiter) -> (Count (live + size) waiter, ()))

-- | Spawns a connection worker for each socket of the batch, one at a time, giving way before each, so that the other parts of the core
-- run between them when the batch is large. The sockets that have no connection worker when this ends, which it does as it is cancelled,
-- are closed and taken off the count.
starting ::
  forall arr s r.
  (Runtime arr, HasField "group" r (Group arr s AwaitAll), HasField "batch" r Batch, HasField "cores" r Cores) =>
  Settings ->
  arr (Call, Request) () ->
  arr r ()
starting settings handler =
  named (Label "start workers" "Spawns a connection worker for each socket of the batch, giving way before each, and closes the sockets that were not given one if it is cancelled.") $
    flow
      ( "pendingBatch" <: keepBatch    -< "batch"
          `releasedBy` closeUnassigned -< ("pendingBatch", "cores") >>>
        spawnEach                      -< ("pendingBatch", "cores", "group")
      ) >>>
    done
  where
    keepBatch = step "keep the batch" (\batch -> newIORef $! batch)
    closeUnassigned = step "close the sockets that have no connection worker" unstarted
    spawnEach = iterateWith (giveWay >>> step "take the next socket" takeNext -| "socket left?") (consume (spawn (connectionWorker settings handler)))
    takeNext input@(pendingBatch, (Cores core shared, groupHandle)) =
      atomicModifyIORef' pendingBatch takeSocket >>= \case
        Nothing -> pure (Finish ())
        Just socket -> let !workers = groupHandle in pure (Again ((workers, newRecord ["socket", "shared", "core"] socket shared core), input))
    takeSocket batch = case batch of
      Batch size (socket : others) -> (Batch (size - 1) others, Just socket)
      Batch _ [] -> (batch, Nothing)
    unstarted (pendingBatch, Cores core shared) = do
      Batch size left <- readIORef pendingBatch
      unless (size == 0) (closeAll left *> leaving size shared core)

-- | Waits until the core has room for another connection worker: fewer than its share of the cap. If it has none, the
-- part leaves the promise of a ticket in the count of the core, which the end of one of its connection workers fulfils,
-- and waits for it.
untilRoom :: (Runtime arr) => Settings -> arr Cores ()
untilRoom settings =
  named (Label "until room" "Waits while the core has as many connection workers as its share of the cap, until one of them ends.") $
    iterateWith
      (step "count the connection workers" hasRoom -| "room?")
      ("full" |- tap (withTicket (step "register as waiting" registering) done >>> done))
  where
    hasRoom cores@(Cores core shared) = decided <$> readIORef (countOf shared core)
      where
        decided (Count live _) = if live < connectionsPerCore settings then Finish () else Again cores
    -- A core that has room by now has the promise fulfilled at once, as the end of a connection worker would.
    registering (promise, Cores core shared) = do
      room <- update (countOf shared core) $ \count@(Count live _) ->
        if live < connectionsPerCore settings then (count, True) else (Count live (Just promise), False)
      when room (void (fulfilNow promise ()))

-- | Puts the core in the pool of the cores that wait for connections, with the promise of a batch of sockets, and waits for the
-- acceptor to give one. A batch that is given when the wait was cancelled is closed.
pull :: (Runtime arr) => arr Cores Batch
pull =
  named (Label "pull" "Puts the core in the pool of the cores that wait for connections, and waits for the acceptor to give it a batch.") $
    asking "connection" (withTicket (step "wait in the pool of cores" waiting) (fst ^>> step "close the sockets" closeBatch)) >>^ snd
  where
    waiting (promise, Cores core shared) = releaseNow (sharedWaiting shared) [Waiting core promise]

-- | A connection worker: it serves one connection, and when it ends, however it ends, its core has room for another.
connectionWorker ::
  (Runtime arr, HasField "shared" r Shared, HasField "core" r Int, HasField "socket" r Socket) =>
  Settings ->
  arr (Call, Request) () ->
  arr r ()
connectionWorker settings handler =
  named workerLabel $
    flow
      ( onExit (freeSlot            -< ("shared", "core")) >>>
        connection settings handler -< ["shared", "socket"]
      ) >>>
    done
  where
    workerLabel = Label "connection worker" "Serves one connection. Frees a worker slot on its core when the connection ends."
    freeSlot = step "free worker slot" (\(shared, core) -> leaving 1 shared core)

-- | Takes this many connection workers off the count of the core, and tells the part that waits for room, if one does.
leaving :: Int -> Shared -> Int -> IO ()
leaving workers shared core = do
  waiter <- update (countOf shared core) (\(Count live waiting) -> (Count (live - workers) Nothing, waiting))
  forM_ waiter (`fulfilNow` ())

-- | What a connection is: its socket, the bytes of the next request that came early, which a request copies out of
-- its buffer, what the cores share, and the address that it came from.
data Conn = Conn
  { connSocket :: !Socket,
    connCarry :: !(IORef ByteString),
    connShared :: !Shared,
    connPeer :: !SockAddr,
    connMark :: !(IORef Mark)
  }

-- | Whether a connection waits for its next request, and since when, which its two parts agree on when there is an idle timeout. The part that
-- serves the requests marks the connection as waiting before it waits, and takes the mark when input comes, and the watcher takes it
-- when the connection has waited for the timeout. Each takes it by a compare-and-swap, so the one that comes second finds that it lost,
-- and a request in flight is never the watcher's: it can only take a connection that waits.
data Mark
  = -- | A request is in hand, or in flight.
    Busy
  | -- | The connection has waited for a request since this instant.
    Idle !Instant
  | -- | The watcher took the connection, whose wait is over.
    TimedOut

-- | The connection until it ends, which closes its socket however it ends. What goes wrong on a connection is that
-- connection's: its connection worker ends, and the core goes on.
connection ::
  (Runtime arr, HasField "shared" r Shared, HasField "socket" r Socket) =>
  Settings ->
  arr (Call, Request) () ->
  arr r ()
connection settings handler =
  named connectionLabel $
    flow
      ( onExit (close                  -< "socket") >>>
        "connection" <: initConnection -< ("socket", "shared") >>>
        tryA runConnection             -< "connection"
      ) >>>
    done
  where
    connectionLabel = Label "serve connection" "Serves requests until the connection ends. Closes the socket on exit."
    initConnection = step "initialize connection" $ \(socket, shared) -> do
      carry <- newIORef B.empty
      peer <- peerOf socket
      mark <- newIORef Busy
      pure $! Conn socket carry shared peer mark
    -- With an idle timeout, the connection has a watcher besides the part that serves its requests, which ends the connection once it has
    -- waited for its next request for the timeout. The watcher's wait is a timer for the connection, which is armed again when it fires, and
    -- not a deadline for each wait for input: a request costs a mark of the connection, where a deadline costs a scope and a timer.
    -- A connection that the watcher has taken is not closed with a request in its socket: closing with input unread resets the connection, and
    -- the client loses the request, which came as the connection timed out. The request is served, and the connection watched again. A
    -- connection that ends gives way as it does, since the lint can't tell that the one that ends is not the one that goes round the loop.
    runConnection = case idleTimeout settings of
      Nothing -> requests settings handler
      Just limit ->
        "connection"
          |- iterateA
            ( alongside (race (watchIdle limit) (requests settings handler) >>> done)
                >>> step "check for a late request" arrivedLate -| "request in the socket?"
                >>> ( "no" |- (giveWay >>^ const (Finish ())) -| "connection ends"
                        ||| "yes" |- (alongside (tap waitForInput >>> admitRequest settings handler) >>^ nextRequest)
                    )
            )

-- | The address that the socket's peer has, or a Unix address with no name if the peer has gone already, as a client of
-- a Unix socket has no other.
peerOf :: Socket -> IO SockAddr
-- The polymorphic entry point lets GHC specialize the peer lookup to SockAddr.
peerOf socket = SocketAddress.getPeerName socket `catch` \(_ :: SomeException) -> pure (SockAddrUnix "")

-- | Waits for input and serves each request until the connection ends.
requests :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn ()
requests settings handler =
  named (Label "serve requests" "Waits for input and serves each request until the connection ends.") $
    "connection" |- iterateA ((alongside serveNext >>^ nextRequest) -| "keep the connection open?")
  where
    -- An idle connection waits holding no buffer: a request is admitted once there is input. With an idle timeout, a connection that the
    -- watcher has ended by then ends, and its connection worker ends with it: it gives way as it ends, since the lint can't tell
    -- that the connections that have bytes in hand are not the ones that end, and so sees a way round the loop that waits nowhere.
    serveNext = case idleTimeout settings of
      Nothing -> tap waitForInput >>> serveOne
      Just _ ->
        alongside waitForInputOrTimeout
          >>> decide "input before the idle timeout?" (\(hasInput, conn) -> if hasInput then Right conn else Left ())
          >>> ("no" |- (giveWay >>^ const False) ||| "yes" |- serveOne)
    serveOne = admitRequest settings handler

-- | Keeps the connection for the next request if this request allows it.
nextRequest :: (Bool, Conn) -> Step Conn ()
nextRequest (keepAlive, conn) = if keepAlive then Again conn else Finish ()

-- | Gets buffers for one request and returns them when the request ends.
admitRequest :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn Bool
admitRequest settings handler =
  alongside (arr (sharedBuffers . connShared) -| "bufferPool")
    >>> withRequestBuffers (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
waitForInput :: (Runtime arr) => arr Conn ()
waitForInput =
  named (Label "wait for input" "Uses carry-over bytes if present. Otherwise waits for input or end of stream.") $
    "connection"
      |- ( step "check carry-over" ready -| "any carry-over bytes?"
             >>> ("no" |- awaitInput ||| "yes" |- returnA)
         )

-- | The same, with the connection marked as waiting while it waits and the mark taken when its input comes, and says whether the connection
-- has input: it has none if the watcher took the mark first, and the connection ends. A connection that has bytes in hand waits for
-- nothing, and is not marked.
waitForInputOrTimeout :: (Runtime arr) => arr Conn Bool
waitForInputOrTimeout =
  named waitLabel $
    "connection"
      |- ( step "check carry-over" checkCarry -| "any carry-over bytes?"
             >>> ("no" |- (markIdle >>> tap (connSocket ^>> awaitInput) >>> claimInput) ||| "yes" |- arr (const True))
         )
      -| "hasInput"
  where
    waitLabel =
      Label
        "wait with idle timeout"
        "Uses carry-over bytes if present. Otherwise marks the connection idle and checks if input beat the timeout."
    markIdle = keeping clock >>> tap (step "mark connection idle" (\(at, conn) -> atomicWriteIORef (connMark conn) $! Idle at)) >>^ snd
    claimInput = step "claim connection for input" (\conn -> update (connMark conn) (\case Idle _ -> (Busy, True); other -> (other, False)))

-- | The connection, unless it has bytes of a request already.
checkCarry :: Conn -> IO (Either Conn ())
checkCarry conn@Conn {connCarry} = (\bytes -> if B.null bytes then Left conn else Right ()) <$> readIORef connCarry

-- | The watcher of a connection: it ends when the connection has waited for its next request for the idle timeout and the watcher has
-- taken it from the part that serves the requests. When it looks, it waits for as long as it takes the connection to have waited for the
-- timeout: from when it began to wait, if it does, and from now if it is busy, which has not begun to wait before now.
watchIdle :: (Runtime arr) => Duration -> arr Conn ()
watchIdle limit =
  named watchLabel $
    "connection"
      |- iterateWith (keeping clock >>> step "check idle time" (look limit) -| "idle timeout?") ("not yet" |- consume (Until ^>> block))
  where
    watchLabel =
      Label "watch idle connection" "Checks how long the connection has been idle. Ends the connection when its idle time runs out."

-- | What the watcher finds when it looks: it has taken the connection, or when to look again.
look :: Duration -> (Instant, Conn) -> IO (Step (Instant, Conn) ())
look limit (now', conn@Conn {connMark}) =
  readIORef connMark >>= \case
    Busy -> pure (Again (plus now' limit, conn))
    TimedOut -> pure (Finish ())
    Idle since
      | now' >= plus since limit ->
          -- The connection has waited for the timeout: unless the part that serves the requests took the mark first, in which case the
          -- watcher looks again, at once.
          update connMark (\case Idle at | at == since -> (TimedOut, Finish ()); other -> (other, Again (now', conn)))
      | otherwise -> pure (Again (plus since limit, conn))

-- | The connection, if the watcher has taken it while its socket has input, which is then left to be served: the mark goes back to a request
-- in hand, as the part that serves has ended. It is not looked at otherwise.
arrivedLate :: ((), Conn) -> IO (Either () Conn)
arrivedLate ((), conn@Conn {connSocket, connMark}) =
  readIORef connMark >>= \case
    TimedOut -> inputWaiting connSocket >>= \input -> if input then Right conn <$ atomicWriteIORef connMark Busy else pure (Left ())
    _ -> pure (Left ())

-- | Whether the socket has input, which may be the end of the stream or a failure that a receive reports: it is looked at without taking any of
-- it, and without waiting.
inputWaiting :: Socket -> IO Bool
inputWaiting sock =
  withFdSocket sock $ \fd -> allocaBytes 1 $ \pointer ->
    let attempt =
          c_recv fd pointer 1 msgPeek >>= \case
            -1 -> Errno.getErrno >>= \errno -> if errno == Errno.eINTR then attempt else pure (errno /= Errno.eAGAIN && errno /= Errno.eWOULDBLOCK)
            _ -> pure True
     in attempt

foreign import ccall unsafe "recv"
  c_recv :: CInt -> Ptr Word8 -> CSize -> CInt -> IO CSsize

foreign import capi "sys/socket.h value MSG_PEEK"
  msgPeek :: CInt

-- | The socket to wait for input on, unless the connection has bytes of a request already.
ready :: Conn -> IO (Either Socket ())
ready Conn {connSocket, connCarry} =
  (\carried -> if B.null carried then Left connSocket else Right ()) <$> readIORef connCarry

-- * A request

-- | The bytes in hand, where the head has been looked at, and whether the stream has ended.
data Progress = Progress
  { progressEnd :: !Int,
    progressScan :: !Scan,
    progressEnded :: !Bool
  }

-- | The request head and the offset of the body in the buffer.
type RequestHead = ("requestHead" := Parsed, "bodyOffset" := Int)

-- | Reading the head ends with end of stream, a refusal status, or a request head.
type HeadEnds s = (Inj "end of stream" () s, Inj "refusal" Int s, Inj "head" RequestHead s)

-- | Ends the connection after this request.
quit :: (Runtime arr) => arr a Bool
quit = arr (const False) -| "keepAlive"

-- | Reads the request head and decides how to respond. Returns whether the connection can stay open.
request :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Loans, Conn) Bool
request settings handler =
  named requestLabel $
    uncurry (newRecord ["loans", "connection"]) ^>> result "keepAlive"
      ( "headLoan" <: borrow               -< "loans" >>>
        "scanState" <: copyCarry           -< ("headLoan", "connection") >>>
        "headOutcome" <: readHead settings -< ("connection", "headLoan", "scanState") >>>
        "keepAlive" <: choice "what did the head scan find?" "headOutcome"
          ( "end of stream" ?> quit                               -< []
            ||| "refusal" =: "status" ?> refuse                   -< ("status", "connection")
            ||| "head" =: ["requestHead", "bodyOffset"] ?> handle -< [ "requestHead", "bodyOffset", "connection",
                                                                       "loans", "headLoan", "scanState"
                                                                     ]
          )
      )
  where
    requestLabel = Label "serve request" "Reads the request head and decides how to respond. Returns whether the connection can stay open."
    copyCarry = step "copy carry-over" carryOver
    handle = handleRequest settings handler
    refuse =
      step
        "render refusal"
        ( \(status, Conn {connSocket, connShared}) ->
            (,) connSocket <$> refusal (sharedPulse connShared) status
        )
        >>> sendAll
        >>> quit
    -- Copy any bytes that arrived with the previous request before reading from the socket.
    carryOver :: (Loan, Conn) -> IO (IORef Progress)
    carryOver (loan, Conn {connCarry}) = do
      bytes <- readIORef connCarry
      unless (B.null bytes) $ do
        writeIORef connCarry B.empty
        BU.unsafeUseAsCStringLen bytes $ \(source, size) ->
          withForeignPtr (bufferMemory (loanBuffer loan)) (\target -> copyBytes target (castPtr source) size)
      newIORef Progress {progressEnd = B.length bytes, progressScan = startScan, progressEnded = False}

-- | The connection, its head loan, and the scan state that each receive updates.
type HeadInput = (Conn, (Loan, IORef Progress))

-- | Receives until the head is there, or is refused, or the stream has ended.
readHead :: (Runtime arr, HeadEnds s) => Settings -> arr HeadInput s
readHead settings =
  named headLabel $
    iterateWith (step "scan head" (advance settings) -| "head complete?") ("needs more" |- more)
  where
    headLabel = Label "read request head" "Receives and scans bytes until it finds a head, a refusal, or end of stream."
    more = first receive >>> step "update scan state" updateProgress
    updateProgress (count, input@(_, (_, scanState))) = do
      progress@Progress {progressEnd} <- readIORef scanState
      writeIORef scanState $
        if count <= 0
          then progress {progressEnded = True}
          else progress {progressEnd = progressEnd + count, progressEnded = False}
      pure input

-- | Looks at the bytes that arrived: what the head came to, or the socket and the room to receive more into.
advance :: (HeadEnds s) => Settings -> HeadInput -> IO (Step ((Socket, Room), HeadInput) s)
advance settings input@(Conn {connSocket, connPeer}, (!loan, scanState)) = do
  progress@Progress {progressEnd, progressScan, progressEnded} <- readIORef scanState
  let buffer = loanBuffer loan
      bytes = view buffer progressEnd
  case scanHead (lineLimit settings) (headLimit settings) (bufferCapacity buffer) bytes progressScan of
    Refused status -> pure (Finish (inj "refusal" status))
    Complete start stop spill -> do
      -- The head is copied and parsed while the bytes are there, and the request owns them.
      let !owned = joined spill (B.copy (B.take (stop - max 0 start) (B.drop (max 0 start) bytes)))
      pure . Finish $ case parseHead (bodyLimit settings) connPeer owned of
        Left status -> inj "refusal" status
        Right parsed -> inj "head" (newRecord ["requestHead", "bodyOffset"] parsed stop)
    Full lineStart requestStart spill -> do
      -- The buffer is full and the head goes on: its bytes are copied out, but for a CR that ends them, which stays at
      -- the start of the buffer so that it is with the LF that follows it, and the buffer is received into again.
      let keep = if BU.unsafeIndex bytes (progressEnd - 1) == 13 then 1 else 0
          shift = progressEnd - keep
          from = max 0 (min shift (if requestStart == notYet then lineStart else requestStart))
          !piece = B.copy (B.take (shift - from) (B.drop from bytes))
          !spill' = case spill of
            NoSpill -> Spill (B.length piece) [piece]
            Spill size pieces -> Spill (size + B.length piece) (piece : pieces)
      when (keep == 1) $ withForeignPtr (bufferMemory buffer) (\target -> poke target (13 :: Word8))
      writeIORef scanState progress {progressEnd = keep, progressScan = Scanning 0 (lineStart - shift) (if requestStart == notYet then notYet else requestStart - shift) spill'}
      pure (Again ((connSocket, Room buffer keep (bufferCapacity buffer - keep)), input))
    scanning
      | progressEnded -> pure (Finish (inj "end of stream" ()))
      | otherwise ->
          Again ((connSocket, Room buffer progressEnd (bufferCapacity buffer - progressEnd)), input)
            <$ writeIORef scanState progress {progressScan = scanning}

-- | Builds the call, runs the handler, and decides whether to keep the connection open.
handleRequest ::
  ( Runtime arr,
    HasField "connection" r Conn,
    HasField "loans" r Loans,
    HasField "headLoan" r Loan,
    HasField "scanState" r (IORef Progress),
    HasField "requestHead" r Parsed,
    HasField "bodyOffset" r Int
  ) =>
  Settings ->
  arr (Call, Request) () ->
  arr r Bool
handleRequest settings handler =
  named handlerLabel $
    result "keepAlive"
      ( ["call", "request", "loanToReturn"] <: prepareCall -< [ "connection", "loans", "headLoan",
                                                                "scanState", "requestHead", "bodyOffset"
                                                              ] >>>
        returnHeadLoan                                     -< "loanToReturn" >>>
        "handlerOutcome" <: runHandler                     -< ("call", "request") >>>
        "keepAlive" <: finishRequest                       -< ["handlerOutcome", "call"]
      )
  where
    handlerLabel = Label "handle request" "Builds the call, runs the handler, and decides whether to keep the connection open."
    prepareCall = step "prepare call" (prepare settings)
    returnHeadLoan =
      decide "return the head loan?" (maybe (Right ()) Left)
        >>> ("yes" |- release ||| "no" |- returnA)
    runHandler = named runHandlerLabel (protectHandler (tryA handler))
    runHandlerLabel = Label "run handler" "Catches handler failures. Gives the handler time to finish during shutdown."
    -- The handler can finish before shutdown cancels it. A drain deadline lets cancellation reach it sooner.
    protectHandler :: (Runtime arr) => arr a b -> arr a b
    protectHandler = if isJust (shutdown settings) then maybe shield grace (drainTimeout settings) else id

-- | The call: the body of the request, and the response. What follows a request that has no body goes to the next
-- request, and its primary loan is given back before the handler runs, to be used for what the handler reads.
prepare ::
  ( HasField "connection" r Conn,
    HasField "loans" r Loans,
    HasField "headLoan" r Loan,
    HasField "scanState" r (IORef Progress),
    HasField "requestHead" r Parsed,
    HasField "bodyOffset" r Int
  ) =>
  Settings ->
  r ->
  IO (Call, (Request, Maybe Loan))
prepare settings r = do
  let !Conn {..} = r.connection
      !loans = r.loans
      !loan = r.headLoan
      !(Parsed request' framing keep) = r.requestHead
      !bodyOffset = r.bodyOffset
  Progress {progressEnd = end} <- readIORef r.scanState
  let buffer = loanBuffer loan
      Method name = method request'
  exchange <- newExchange connSocket loans (name == "HEAD") (version request' == Http11) (sharedPulse connShared) keep
  let callFor body loanToReturn = (Call loans body exchange, (request', loanToReturn))
  case framing of
    Length size -> do
      marks <- newIORef Marks {markStart = bodyOffset, markEnd = end, markRemaining = size}
      requestBody <- awaitContinue request' (end == bodyOffset) (Sized (BodyState connSocket buffer loan connCarry marks))
      pure (callFor requestBody Nothing)
    Chunked -> do
      marks <- newIORef Marks {markStart = bodyOffset, markEnd = end, markRemaining = 0}
      decoding <- newIORef (AtSize 0)
      let state = BodyState connSocket buffer loan connCarry marks
      requestBody <- awaitContinue request' (end == bodyOffset) (ChunkedBody (ChunkState state decoding (lineLimit settings) (bodyLimit settings) (headLimit settings)))
      pure (callFor requestBody Nothing)
    NoContent -> do
      -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
      when (end > bodyOffset) $ writeIORef connCarry $! B.copy (BSI.BS (bufferMemory buffer `plusForeignPtr` bodyOffset) (end - bodyOffset))
      pure (callFor NoBody (Just loan))

-- | The body, or the body that is told to go on when it is first read, if the request expects @100-continue@ in HTTP/1.1 and
-- none of the body came with the head, so that the client waits (RFC 9110 10.1.1; HTTP/1.0 ignores the expectation).
awaitContinue :: Request -> Bool -> Body -> IO Body
awaitContinue request' nothingYet requestBody
  | nothingYet && version request' == Http11 && any expecting (headers request') = (`Awaited` requestBody) <$> newIORef True
  | otherwise = pure requestBody
  where
    expecting (name, value) = equalsLower "expect" name && equalsLower "100-continue" value

-- | What the handler left: a connection that the client has gone from, or a response that is not there or not
-- complete, ends the connection, with a 500 if the handler had not answered; and one that is complete is kept if the
-- body of the request was read, or could be, and the response didn't end the connection.
finishRequest :: (Runtime arr, HasField "handlerOutcome" r (Either SomeException ()), HasField "call" r Call) => arr r Bool
finishRequest =
  named finishLabel $
    decide "how did the handler end?" byOutcome
      >>> ("client disconnect" ?> quit ||| "failure" =: "status" ?> handleFailure ||| "success" ?> checkResponse)
  where
    finishLabel =
      Label
        "finish request"
        "Sends an error if the handler leaves no response. Drains any remaining body before reusing the connection."
    -- Inlined, so that GHC fuses the classification with the branches and builds no sum.
    byOutcome r = casesBy "handlerOutcome" classify r
    {-# INLINE byOutcome #-}
    classify = \case
      Left failure
        | Just ConnectionLost <- fromException failure -> inj "client disconnect" ()
        | Just (BodyRefused status) <- fromException failure -> inj "failure" status
        | otherwise -> inj "failure" (500 :: Int)
      Right () -> inj "success" ()
    handleFailure =
      step
        "check for a response"
        (\r -> case r.call of Call _ _ exchange -> (,) r.status <$> unanswered exchange)
        >>> internalError
    checkResponse =
      step
        "read response phase"
        ( \r -> case r.call of
            call@(Call _ _ exchange) ->
              readIORef (exPhase exchange) >>= \case
                Ended -> pure (Right call)
                Unanswered -> pure (Left (Just exchange))
                _ -> pure (Left Nothing)
        )
        -| "response complete?"
        >>> ("no" |- ((500,) ^>> internalError) ||| "yes" |- drainBody)
    -- The body that the handler didn't read is read now and dropped, if it is not too long.
    drainBody =
      (\(Call _ requestBody exchange) -> (requestBody, exchange))
        ^>> first drain
        >>> step "check keep alive" (\(canReuse, exchange) -> (canReuse &&) <$> readIORef (exKeep exchange))
          -| "keepAlive"
    unanswered exchange = readIORef (exPhase exchange) >>= \case Unanswered -> pure (Just exchange); _ -> pure Nothing
    -- Answers with the status (500, or what refused the request) if nothing was answered, and ends the connection.
    internalError =
      decide "unanswered?" (\(status, unanswered') -> maybe (Right ()) (Left . (,) status) unanswered')
        >>> ( "yes"
                |- ( step
                       "render the refusal"
                       (\(status, exchange) -> (,) (exSocket exchange) <$> refusal (exPulse exchange) status)
                       >>> sendAll
                   )
                ||| "no" |- returnA
            )
        >>> quit
