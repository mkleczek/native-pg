{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

-- | The server as a network of parts whose connection workers are spawned as the connections are accepted. The first
-- core has the acceptor, which opens the listener when the server starts and closes it when it ends, and each core has
-- a group of connection workers. A core that has room for another connection, which is to have fewer connection workers
-- than its share of the cap, puts itself in the pool of the cores that wait, with the promise of a socket, and waits.
-- The acceptor waits until some core waits, accepts a connection, and delivers its socket to the waiting core that has
-- the fewest connection workers then, the one that has waited longest if several have as few: the connections are
-- shared out between the cores by what is true when they are accepted, and nothing is made for a connection before it
-- is there. The core spawns a connection worker for the socket, which serves the connection, request after request,
-- until it ends, and tells the core that it has room again. While every core is full the acceptor accepts nothing, so
-- that past the cap on connections they wait in the listen backlog. A connection that waits for its next request for
-- longer than the idle timeout is closed, so that the connections that wait in the backlog are served though the ones
-- that have a connection worker are idle: the connection has a watcher beside the part that serves its requests, which
-- sleeps until the connection can have waited that long, and ends it if it has.
--
-- A request is read into the primary buffer that it was admitted with. The bytes that follow it in the buffer, which
-- belong to the next request on the connection, are copied out before the buffer is used for anything else, and
-- the next request begins with them: pipelining costs a copy of the bytes that came early, and nothing otherwise.
module Network.Fulmen.Internal.Serve
  ( serve,
  )
where

import Control.Arrow (arr, first, returnA, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (SomeException, fromException, try)
import Control.Monad (forM_, unless, void, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import qualified Data.IntMap.Strict as IntMap
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef, writeIORef)
import Data.Maybe (isJust)
import Data.Word (Word8)
import Foreign.ForeignPtr (plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Foreign.Storable (poke)
import GHC.Records (HasField)
import Network.Fulmen.Internal
import Network.Fulmen.Internal.Accept (acceptor, listening)
import Network.Fulmen.Internal.Body (BodyRefused (..), BodyState (..), ChunkState (..), ConnectionLost (..), Decoding (..), Marks (..), drain)
import Network.Fulmen.Internal.Bytes (equalsLower)
import Network.Fulmen.Internal.Date (beginDraining)
import Network.Fulmen.Internal.Head (Framing (..), Parsed (..), Scan (..), Spill (..), joined, notYet, parseHead, scanHead, startScan)
import Network.Fulmen.Internal.Response (Exchange (..), Phase (..), newExchange, refusal)
import Network.Fulmen.Listen (Listen (..))
import Network.Fulmen.Request (Method (..), Request (..), Version (..))
import Network.Socket (SockAddr (..), Socket, getPeerName)
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
    typed,
    whether,
    withTicket,
  )
import Network.Streaming.Concurrent (AwaitAll (..), Concurrent (group, spawn))
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Named (Inj, adding, cases, casesBy, field, inj, is, newRecord, (:=), (?>))
import Network.Streaming.Pool (checkoutIn, releaseNow, takeNowWith)

-- | The server: for each core, its connection workers, and for the first, the acceptor, which opens the listener.
serve :: (Runtime arr) => Settings -> Listen arr -> arr (Call, Request) () -> arr Cores ()
serve !settings listen handler =
  named (Label "serve" "Runs the connection workers of each core, and the acceptor besides on the first core.") $
    decide "first core?" (whether isFirst)
      >>> ("yes" |- shutting [acceptance listen, workers] ||| "no" |- shutting [workers])
  where
    workers = connectionWorkers settings handler
    isFirst (Cores core _) = core == 0
    -- A server that has an event to shut down on runs its parts in a race with the wait for the event. When it fires,
    -- the server is marked as shutting down and the parts are cancelled where they wait: the acceptor, whose listener
    -- closes, and the group of connection workers of each core, whose part that waits for a connection and whose
    -- connection workers that wait for a request are cancelled. A request in flight is behind a grace (see
    -- 'answered'), so it finishes, or is cancelled where it waits when the drain's deadline has passed, and the race
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

-- | Opens the listener, and then waits until some core waits for a connection, accepts one, and delivers its socket to
-- the waiting core that has the fewest connection workers, for ever. The listener is closed when the acceptor ends,
-- however it ends.
acceptance :: (Runtime arr) => Listen arr -> arr Cores ()
acceptance Listen {openListener, closeListener} =
  named (Label "acceptance" "Opens the listener, waits until a core waits for a connection, accepts one, and delivers it to the waiting core that has the fewest connection workers.") $
    scope (done >>> openListener -| "listener") (fst ^>> closeListener) $
      tap (step "make listener non-blocking" (listening . fst)) >>> foreverA (tap place)
  where
    place =
      tap (snd ^>> untilWanted)
        >>> (is "socket" (fst ^>> accept) &&& is "cores" (arr snd))
        >>> deliver
    accept =
      named (Label "accept" "Accepts the next connection of the listening socket, and blocks while none is waiting.") acceptor

-- | Waits until some core waits for a connection, and takes none: the core that the pool gives is put back.
untilWanted :: (Runtime arr) => arr Cores ()
untilWanted =
  named (Label "until wanted" "Waits until some core waits for a connection, and takes none: the core that the pool gives is put back.") $
    (\(Cores _ shared) -> sharedWaiting shared)
      ^>> alongside checkoutIn
      >>> step "put the core back" (\(waiting, pool) -> releaseNow pool [waiting])

-- | Gives the socket to the waiting core that has the fewest connection workers now, the next if that core has stopped
-- waiting, and closes the socket if no core waits, which is so when the server is shutting down.
deliver :: (Runtime arr, HasField "socket" r Socket, HasField "cores" r Cores) => arr r ()
deliver =
  named (Label "deliver" "Gives the socket to the waiting core that has the fewest connection workers, the next if that one has stopped waiting, and closes it if none waits.") loop
  where
    loop =
      alongside (step "take the least busy waiting core" takeLeastBusy)
        >>> decide "core waiting?" (\(taken, r) -> maybe (Left r.socket) (Right . (,r)) taken)
        >>> ("no" |- close ||| "yes" |- (delivering -| "taken?" >>> ("stopped waiting" |- loop ||| "taken" |- returnA)))
    delivering =
      (\(waiting, r) -> ((waitingPromise waiting, r.socket), r))
        ^>> first (answering "connection" fulfil)
        >>^ \(taken, r) -> if taken then Right () else Left r
    takeLeastBusy r = case r.cores of
      Cores _ shared -> readCounts shared >>= \counts -> takeNowWith (leastBusy counts) (sharedWaiting shared)

-- | How many connection workers each core has, by the core's number.
readCounts :: Shared -> IO (IntMap.IntMap Int)
readCounts shared = traverse (fmap (\(Count live _) -> live) . readIORef) (sharedCounts shared)

-- | The position of the core that has the fewest connection workers, in the order that the pool gives the waiting cores
-- in: the one that waited least comes first, so the one that has waited longest of those that tie is the last of them.
leastBusy :: IntMap.IntMap Int -> [Waiting] -> Maybe Int
leastBusy counts waiting = snd <$> foldl' pick Nothing (zip [0 :: Int ..] waiting)
  where
    pick best (position, Waiting core _) = case best of
      Just (fewest, _) | live > fewest -> best
      _ -> Just (live, position)
      where
        live = IntMap.findWithDefault 0 core counts

-- * The connection workers

-- | The count of the core's connection workers.
countOf :: Shared -> Int -> IORef Count
countOf shared core = sharedCounts shared IntMap.! core

-- | The connection workers of the core: while the core has room, it waits for the next connection, and spawns a
-- connection worker for it, for ever. When it is cancelled it waits for its connection workers to end.
connectionWorkers :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
connectionWorkers settings handler =
  named (Label "connection workers" "Waits for the core to have room and then for a connection, and spawns a connection worker for it, for ever.") $
    group
      AwaitAll
      ( foreverA
          ( tap (snd ^>> untilRoom settings)
              >>> tap (second pulled >>> spawn (connectionWorker settings handler))
          )
      )
      >>> done
  where
    pulled = pull >>> tap (step "count the connection worker" counted)
    counted r = update (countOf r.shared r.core) (\(Count live waiter) -> (Count (live + 1) waiter, ()))

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

-- | Puts the core in the pool of the cores that wait for a connection, with the promise of the socket, and waits for
-- the acceptor to deliver one: the socket, with what the connection worker is given. A socket that is delivered when
-- the wait was cancelled is closed.
pull :: (Runtime arr) => arr Cores ("socket" := Socket, ("shared" := Shared, "core" := Int))
pull =
  named (Label "pull" "Puts the core in the pool of the cores that wait for a connection, and waits for the acceptor to deliver one.") $
    alongside (asking "connection" (withTicket (step "wait in the pool of cores" waiting) (fst ^>> close)))
      >>^ \(((), socket), Cores core shared) -> newRecord ["socket", "shared", "core"] socket shared core
  where
    waiting (promise, Cores core shared) = releaseNow (sharedWaiting shared) [Waiting core promise]

-- | A connection worker: it serves one connection, and when it ends, however it ends, its core has room for another.
connectionWorker ::
  (Runtime arr, HasField "shared" r Shared, HasField "core" r Int, HasField "socket" r Socket) =>
  Settings ->
  arr (Call, Request) () ->
  arr r ()
connectionWorker settings handler =
  named (Label "connection worker" "Serves one connection, and when it ends, however it ends, tells its core that it has room for another.") $
    scope returnA (fst ^>> step "tell the core that it has room" leaving) (snd ^>> connection settings handler)
  where
    leaving r = do
      waiter <- update (countOf r.shared r.core) (\(Count live waiting) -> (Count (live - 1) Nothing, waiting))
      forM_ waiter (\promise -> void (fulfilNow promise ()))

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
  named (Label "connection" "Serves the connection until it ends, and closes its socket however it ends.") $
    scope (field "socket") (fst ^>> close) (snd ^>> serving)
  where
    serving =
      step "make connection state" (\r -> Conn r.socket <$> newIORef B.empty <*> pure r.shared <*> peerOf r.socket <*> newIORef Busy)
        -| "Conn"
        >>> tryA watched
        >>> done
    -- With an idle timeout, the connection has a watcher besides the part that serves its requests, which ends the connection once it has
    -- waited for its next request for the timeout. The watcher's wait is a timer for the connection, which is armed again when it fires, and
    -- not a deadline for each wait for input: a request costs a mark of the connection, where a deadline costs a scope and a timer.
    watched = case idleTimeout settings of
      Nothing -> requests settings handler
      Just limit -> race (watching limit) (requests settings handler) >>> done

-- | The address that the socket's peer has, or a Unix address with no name if the peer has gone already, as a client of
-- a Unix socket has no other.
peerOf :: Socket -> IO SockAddr
peerOf socket = either (\(_ :: SomeException) -> SockAddrUnix "") id <$> try (getPeerName socket)

-- | Request after request, while the connection is kept.
requests :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn ()
requests settings handler =
  named (Label "requests" "Serves request after request, while the connection is kept.") $
    typed @Conn @() $
      iterateA ((alongside one >>^ whileKept) -| "connection kept?")
  where
    whileKept (keep, conn) = if keep then Again conn else Finish ()
    -- An idle connection waits holding no buffer: a request is admitted once there is input. With an idle timeout, a connection that the
    -- watcher has ended by then ends, and its connection worker ends with it: it gives way as it ends, since the lint can't tell
    -- that the connections that have bytes in hand are not the ones that end, and so sees a way round the loop that waits nowhere.
    one = case idleTimeout settings of
      Nothing -> tap idle >>> admitted
      Just _ ->
        alongside idleWatched
          >>> decide "input before the idle timeout?" (\(arrived, conn) -> if arrived then Right conn else Left ())
          >>> ("no" |- (giveWay >>^ const False) -| "keep alive" ||| "yes" |- admitted)
    admitted =
      alongside (arr (sharedBuffers . connShared) -| "buffer pool")
        >>> withRequestBuffers (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
idle :: (Runtime arr) => arr Conn ()
idle =
  named (Label "idle" "Waits until the connection has input, which may be its end, unless it has bytes in hand.") $
    typed @Conn @() $
      step "check carry-over" ready -| "bytes carried over?"
        >>> ("no" |- awaitInput ||| "yes" |- returnA)

-- | The same, with the connection marked as waiting while it waits and the mark taken when its input comes, and says whether the connection
-- has input: it has none if the watcher took the mark first, and the connection ends. A connection that has bytes in hand waits for
-- nothing, and is not marked.
idleWatched :: (Runtime arr) => arr Conn Bool
idleWatched =
  named (Label "idle watched" "Waits until the connection has input, which may be its end, unless it has bytes in hand, with the connection marked as waiting.") $
    typed @Conn @Bool $
      step "check carry-over" held -| "bytes carried over?"
        >>> ("no" |- (marked >>> tap (connSocket ^>> awaitInput) >>> taken) ||| "yes" |- arr (const True))
  where
    marked = keeping clock >>> tap (step "mark as waiting" (\(at, conn) -> atomicWriteIORef (connMark conn) $! Idle at)) >>^ snd
    taken = step "take the mark" (\conn -> update (connMark conn) (\case Idle _ -> (Busy, True); other -> (other, False)))

-- | The connection, unless it has bytes of a request already.
held :: Conn -> IO (Either Conn ())
held conn@Conn {connCarry} = (\carried -> if B.null carried then Left conn else Right ()) <$> readIORef connCarry

-- | The watcher of a connection: it ends when the connection has waited for its next request for the idle timeout and the watcher has
-- taken it from the part that serves the requests. When it looks, it waits for as long as it takes the connection to have waited for the
-- timeout: from when it began to wait, if it does, and from now if it is busy, which has not begun to wait before now.
watching :: (Runtime arr) => Duration -> arr Conn ()
watching limit =
  named (Label "watching" "Looks at the connection when it can have waited for the idle timeout, and ends when it has.") $
    typed @Conn @() $
      iterateWith (keeping clock >>> step "look at the connection" (look limit) -| "waited for the idle timeout?") ("not yet" |- consume (Until ^>> block))

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

-- | The socket to wait for input on, unless the connection has bytes of a request already.
ready :: Conn -> IO (Either Socket ())
ready Conn {connSocket, connCarry} =
  (\carried -> if B.null carried then Left connSocket else Right ()) <$> readIORef connCarry

-- * A request

-- | A request being read: the connection, the loans of the request, the primary loan that the head is read into, and
-- how far it has come.
data Req = Req
  { reqConn :: !Conn,
    reqLoans :: !Loans,
    reqLoan :: !Loan,
    reqProgress :: !(IORef Progress)
  }

-- | The bytes in hand, where the head has been looked at, and whether the stream has ended.
data Progress = Progress
  { progressEnd :: !Int,
    progressScan :: !Scan,
    progressEnded :: !Bool
  }

-- | A request that has a head: what the head parsed to, and where it ends in the buffer.
type Good = ("parsed" := Parsed, "consumed" := Int)

-- | The ways that the head can end: the stream ended before it did ("stream ended"), the request is refused with a
-- status ("refused"), or the request has a head ("good").
type HeadEnds s = (Inj "stream ended" () s, Inj "refused" Int s, Inj "good" Good s)

-- | What the handler is run with: the call, the request, and the primary loan, if the handler is to have it back.
data Prepared = Prepared
  { preparedCall :: Call,
    preparedRequest :: Request,
    preparedLoan :: Maybe Loan
  }

-- | Ends the connection: it is not kept for another request.
quit :: (Runtime arr) => arr a Bool
quit = arr (const False) -| "keep alive"

-- | Reads a request, runs the handler, and says whether the connection is kept for another.
request :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Loans, Conn) Bool
request settings handler =
  named (Label "request" "Reads the head of a request, runs the handler, and says whether the connection is kept.") $
    (\(loans, conn) -> newRecord ["loans", "conn"] loans conn)
      ^>> adding "loan" (field "loans" >>> borrow)
      >>> adding "req" (step "copy carry-over" begin')
      >>> adding "head" (field "req" >>> readHead settings)
      >>> decide "how did the head end?" (cases "head")
      >>> ("stream ended" ?> quit ||| "refused" ?> refuse ||| "good" ?> answered settings handler)
  where
    refuse =
      step
        "render refusal"
        ( \r ->
            (,) r.conn.connSocket <$> refusal (sharedPulse r.conn.connShared) r.refused
        )
        >>> sendAll
        >>> quit
    -- The request has the connection's first bytes, which came with the one before.
    begin' :: (HasField "loan" r Loan, HasField "loans" r Loans, HasField "conn" r Conn) => r -> IO Req
    begin' r = do
      carried <- readIORef r.conn.connCarry
      unless (B.null carried) $ do
        writeIORef r.conn.connCarry B.empty
        BU.unsafeUseAsCStringLen carried $ \(source, size) ->
          withForeignPtr (bufferMemory (loanBuffer r.loan)) (\target -> copyBytes target (castPtr source) size)
      Req r.conn r.loans r.loan <$> newIORef Progress {progressEnd = B.length carried, progressScan = startScan, progressEnded = False}

-- | Receives until the head is there, or is refused, or the stream has ended.
readHead :: (Runtime arr, HeadEnds s) => Settings -> arr Req s
readHead settings =
  named (Label "readHead" "Receives until the head is there, or is refused, or the stream has ended.") $
    "Req" |- iterateWith (step "scan head" (advance settings) -| "head complete?") ("needs more" |- more)
  where
    more = first receive >>> step "record arrival" arrived
    arrived (count, req@Req {reqProgress}) = do
      progress@Progress {progressEnd} <- readIORef reqProgress
      writeIORef reqProgress $
        if count <= 0
          then progress {progressEnded = True}
          else progress {progressEnd = progressEnd + count, progressEnded = False}
      pure req

-- | Looks at the bytes that arrived: what the head came to, or the socket and the room to receive more into.
advance :: (HeadEnds s) => Settings -> Req -> IO (Step ((Socket, Room), Req) s)
advance settings req@Req {reqConn = Conn {connSocket, connPeer}, reqLoan, reqProgress} = do
  progress@Progress {progressEnd, progressScan, progressEnded} <- readIORef reqProgress
  let buffer = loanBuffer reqLoan
      bytes = view buffer progressEnd
  case scanHead (lineLimit settings) (headLimit settings) (bufferCapacity buffer) bytes progressScan of
    Refused status -> pure (Finish (inj "refused" status))
    Complete start stop spill -> do
      -- The head is copied and parsed while the bytes are there, and the request owns them.
      let !owned = joined spill (B.copy (B.take (stop - max 0 start) (B.drop (max 0 start) bytes)))
      pure . Finish $ case parseHead (bodyLimit settings) connPeer owned of
        Left status -> inj "refused" status
        Right parsed -> inj "good" (newRecord ["parsed", "consumed"] parsed stop)
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
      writeIORef reqProgress progress {progressEnd = keep, progressScan = Scanning 0 (lineStart - shift) (if requestStart == notYet then notYet else requestStart - shift) spill'}
      pure (Again ((connSocket, Room buffer keep (bufferCapacity buffer - keep)), req))
    scanning
      | progressEnded -> pure (Finish (inj "stream ended" ()))
      | otherwise ->
          Again ((connSocket, Room buffer progressEnd (bufferCapacity buffer - progressEnd)), req)
            <$ writeIORef reqProgress progress {progressScan = scanning}

-- | A request that has a head: the handler runs, and the connection is kept if the response, and the request's body,
-- leave it in a state that another request can follow.
answered ::
  (Runtime arr, HasField "req" r Req, HasField "good" r Good) =>
  Settings ->
  arr (Call, Request) () ->
  arr r Bool
answered settings handler =
  named (Label "answered" "Runs the handler on a request that has a head, and says whether the connection is kept.") $
    ( step "prepare call" (prepare settings) -| "Prepared"
        >>> decide "give back loan?" heldLoan
        >>> ("yes" |- consume release ||| "no" |- returnA)
        >>> (is "outcome" (protected (tryA handler)) &&& is "call" (arr fst))
        >>> aftermath
    )
      -| "keep alive"
  where
    -- With a shutdown event, a handler runs behind a grace of the drain's deadline: a cancellation of the server waits until it has
    -- returned, or until the deadline has passed since the shutdown began, if there is one, and then reaches it at its next wait.
    protected :: (Runtime arr) => arr a b -> arr a b
    protected = if isJust (shutdown settings) then maybe shield grace (drainTimeout settings) else id
    heldLoan Prepared {preparedCall, preparedRequest, preparedLoan} =
      maybe (Right (preparedCall, preparedRequest)) (\loan -> Left (loan, (preparedCall, preparedRequest))) preparedLoan

-- | The call: the body of the request, and the response. What follows a request that has no body goes to the next
-- request, and its primary loan is given back before the handler runs, to be used for what the handler reads.
prepare :: (HasField "req" r Req, HasField "good" r Good) => Settings -> r -> IO Prepared
prepare settings r = do
  let !Req {reqConn = Conn {..}, ..} = r.req
      !(Parsed request' framing keep) = r.good.parsed
      !consumed = r.good.consumed
  Progress {progressEnd = end} <- readIORef reqProgress
  let buffer = loanBuffer reqLoan
      Method name = method request'
  exchange <- newExchange connSocket reqLoans (name == "HEAD") (version request' == Http11) (sharedPulse connShared) keep
  case framing of
    Length size -> do
      marks <- newIORef Marks {markStart = consumed, markEnd = end, markRemaining = size}
      requestBody <- awaited request' (end == consumed) (Sized (BodyState connSocket buffer reqLoan connCarry marks))
      pure (Prepared (Call reqLoans requestBody exchange) request' Nothing)
    Chunked -> do
      marks <- newIORef Marks {markStart = consumed, markEnd = end, markRemaining = 0}
      decoding <- newIORef (AtSize 0)
      let state = BodyState connSocket buffer reqLoan connCarry marks
      requestBody <- awaited request' (end == consumed) (ChunkedBody (ChunkState state decoding (lineLimit settings) (bodyLimit settings) (headLimit settings)))
      pure (Prepared (Call reqLoans requestBody exchange) request' Nothing)
    NoContent -> do
      -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
      when (end > consumed) $ writeIORef connCarry $! B.copy (BSI.BS (bufferMemory buffer `plusForeignPtr` consumed) (end - consumed))
      pure (Prepared (Call reqLoans NoBody exchange) request' (Just reqLoan))

-- | The body, or the body that is told to go on when it is first read, if the request expects @100-continue@ in HTTP/1.1 and
-- none of the body came with the head, so that the client waits (RFC 9110 10.1.1; HTTP/1.0 ignores the expectation).
awaited :: Request -> Bool -> Body -> IO Body
awaited request' nothingYet requestBody
  | nothingYet && version request' == Http11 && any expecting (headers request') = (`Awaited` requestBody) <$> newIORef True
  | otherwise = pure requestBody
  where
    expecting (name, value) = equalsLower "expect" name && equalsLower "100-continue" value

-- | What the handler left: a connection that the client has gone from, or a response that is not there or not
-- complete, ends the connection, with a 500 if the handler had not answered; and one that is complete is kept if the
-- body of the request was read, or could be, and the response didn't end the connection.
aftermath :: (Runtime arr, HasField "outcome" r (Either SomeException ()), HasField "call" r Call) => arr r Bool
aftermath =
  named (Label "aftermath" "Decides whether the connection is kept, and answers 500 if nothing was answered.") $
    decide "how did the handler end?" byOutcome
      >>> ("connection lost" ?> quit ||| "failed" ?> failed ||| "returned" ?> finished)
  where
    -- Inlined, so that GHC fuses the classification with the branches and builds no sum.
    byOutcome r = casesBy "outcome" classify r
    {-# INLINE byOutcome #-}
    classify = \case
      Left failure
        | Just ConnectionLost <- fromException failure -> inj "connection lost" ()
        | Just (BodyRefused status) <- fromException failure -> inj "failed" status
        | otherwise -> inj "failed" (500 :: Int)
      Right () -> inj "returned" ()
    failed =
      step
        "check if answered"
        (\r -> case r.call of Call _ _ exchange -> (,) r.failed <$> unanswered exchange)
        >>> internalError
    finished =
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
        >>> ("no" |- ((500,) ^>> internalError) ||| "yes" |- completed)
    -- The body that the handler didn't read is read now and dropped, if it is not too long.
    completed =
      (\(Call _ requestBody exchange) -> (requestBody, exchange))
        ^>> first drain
        >>> step "check keep alive" (\(drained, exchange) -> (drained &&) <$> readIORef (exKeep exchange))
          -| "keep alive"
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
