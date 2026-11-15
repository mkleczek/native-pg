{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

-- | The server as a static network of parts. Each core has its workers from the start, and the first core has the
-- acceptor besides, which opens the listener when the server starts and closes it when it ends. A worker has an inbox
-- of its own for its life: it offers its address as free, in the pool that the cores share, and collects the connection
-- that the acceptor posts to it; then it serves the connection, request after request, until it ends, and offers itself
-- again. The acceptor takes a free worker of the core that has the most free workers, so that the connections are shared
-- out between the cores, and waits for one when there is none, so that past the cap on connections they wait in the
-- listen backlog.
--
-- A request is read into the primary buffer that it was admitted with. The bytes that follow it in the buffer, which
-- belong to the next request on the connection, are copied out before the buffer is used for anything else, and
-- the next request begins with them: pipelining costs a copy of the bytes that came early, and nothing otherwise.
module Network.Fulmen.Internal.Serve
  ( serve,
  )
where

import Control.Arrow (arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Exception (SomeException, fromException, try)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import qualified Data.IntMap.Strict as IntMap
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.List (findIndex)
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
    box,
    consume,
    decide,
    done,
    foreverA,
    iterateA,
    iterateWith,
    step,
    tap,
    typed,
    whether,
    withInbox,
    withTicket,
  )
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Named (Inj, adding, cases, casesBy, field, inj, is, newRecord, (:=), (?>))
import Network.Streaming.Pool (offer, queueNow, takeNowWith)

-- | The server: for each core, the workers, and for the first, the acceptor, which opens the listener.
serve :: (Runtime arr) => Settings -> Listen arr -> arr (Call, Request) () -> arr Cores ()
serve !settings listen handler =
  named (Label "serve" "Runs the workers of each core, and the acceptor besides on the first core.") $
    decide "first core?" (whether isFirst)
      >>> ("yes" |- shutting (acceptance listen : workers) ||| "no" |- shutting workers)
  where
    workers = replicate (connectionsPerCore settings) (worker settings handler)
    isFirst (Cores core _) = core == 0
    -- A server that has an event to shut down on runs its parts in a race with the wait for the event. When it fires, the server
    -- is marked as shutting down and the parts are cancelled where they wait: the acceptor, whose listener closes, the
    -- workers that wait for a connection, and the connections that wait for a request. A request in flight is behind a shield
    -- (see 'answered'), so it finishes, and the race ends when no part is left.
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

-- | Opens the listener, and then takes a free worker of the core that has the most free workers, accepts a connection,
-- and posts it to the worker's inbox, for ever. A connection that the worker is no longer there to take is closed. The listener
-- is closed when the acceptor ends, however it ends.
acceptance :: (Runtime arr) => Listen arr -> arr Cores ()
acceptance Listen {openListener, closeListener} =
  named (Label "acceptance" "Opens the listener, accepts connections, and gives each to a free worker of the core that has the most free workers.") $
    scope (done >>> openListener -| "listener") (fst ^>> closeListener) $
      tap (step "make listener non-blocking" (listening . fst)) >>> foreverA (tap place)
  where
    place =
      ( newRecord ["address", "socket"]
          <$> (snd ^>> checkout) -| "worker's address"
          <*> (fst ^>> accept) -| "accepted socket"
      )
        >>> adding "taken" ((field "address" &&& field "socket") >>> post)
        >>> decide "worker rejected?" rejected
        >>> ("yes" |- close ||| "no" |- returnA)
    accept =
      named (Label "accept" "Accepts the next connection of the listening socket, and blocks while none is waiting.") acceptor
    rejected r = if r.taken then Right () else Left r.socket

-- | The address of the inbox of a free worker of the core that has the most free workers, which is taken from the pool at once. When no
-- worker is free, the part queues the promise of a ticket of its own in the pool, which fulfils it with the worker that is released
-- first, on any core, and waits at the ticket. A worker that comes after the wait has ended is put back in the pool.
checkout :: (Runtime arr) => arr Cores (Address "connections" 'Anywhere Socket)
checkout =
  named (Label "checkout" "Takes a free worker of the core that has the most free workers at once, and else waits for the worker that is freed first, on any core, and puts back one that comes too late.") $
    alongside (step "take a free worker" (\(Cores _ shared) -> takeNowWith freest (sharedWorkers shared)))
      >>> decide "worker free now?" (\(taken, cores) -> maybe (Left cores) Right taken)
      >>> ("no" |- queued ||| "yes" |- returnA)
      >>^ freeAddress
  where
    queued = asking "free worker" (withTicket (step "queue for a worker" queue) (putBack ^>> offer)) >>^ snd
    queue (promise, Cores _ shared) = queueNow (sharedWorkers shared) 1 promise
    putBack (free, Cores _ shared) = (sharedWorkers shared, free)

-- | The position of the first free worker, in the order that the pool takes them in, of the core that has the most of
-- them, which is the first core of those that tie.
freest :: [Free] -> Maybe Int
freest workers = busiest >>= \core -> findIndex ((== core) . freeCore) workers
  where
    busiest = fst <$> IntMap.foldlWithKey' mostFree Nothing tally
    mostFree best core count = case best of
      Just (_, most) | count <= most -> best
      _ -> Just (core, count)
    tally = foldl' (\counts (Free core _) -> IntMap.insertWith (+) core (1 :: Int) counts) IntMap.empty workers

-- * The workers

-- | A worker: it opens the inbox that its connections are posted to, and offers itself as free, collects a connection,
-- and serves it, for ever. A connection that is left in the inbox when the worker ends is closed.
worker :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
worker settings handler =
  named (Label "worker" "Opens its inbox of connections, then offers itself as free, collects a connection, and serves it, for ever.") $
    withInbox @"connections" closeLateSocket (foreverA (tap (offered -| "accepted socket" >>> connection settings handler)))

-- | The socket that was left in the worker's inbox when it ended, which is closed.
closeLateSocket :: (Runtime arr) => arr (Socket, Cores) ()
closeLateSocket = box "close late socket" (fst ^>> close)

-- | The worker is free: its address is in the pool, and the connection that is posted to its inbox comes.
offered ::
  (Runtime arr) =>
  arr
    ((Inbox arr "connections" 'Anywhere Socket, Address "connections" 'Anywhere Socket), Cores)
    ("shared" := Shared, "socket" := Socket)
offered =
  named (Label "offered" "Offers the worker as free in the pool, and collects the connection that is posted to its inbox.") $
    tap (answering "free worker" (free ^>> offer)) >>> (is "shared" (arr sharedOf) &&& is "socket" (fst . fst ^>> collect))
  where
    sharedOf (_, Cores _ shared) = shared
    free ((_, address), Cores core shared) = (sharedWorkers shared, Free core address)

-- | What a connection is: its socket, the bytes of the next request that came early, which a request copies out of
-- its buffer, what the cores share, and the address that it came from.
data Conn = Conn
  { connSocket :: !Socket,
    connCarry :: !(IORef ByteString),
    connShared :: !Shared,
    connPeer :: !SockAddr
  }

-- | The connection until it ends, which closes its socket however it ends. What goes wrong on a connection is that
-- connection's: the worker goes on to the next.
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
      step "make connection state" (\r -> Conn r.socket <$> newIORef B.empty <*> pure r.shared <*> peerOf r.socket)
        -| "Conn"
        >>> tryA (requests settings handler)
        >>> done

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
    -- An idle connection waits holding no buffer: a request is admitted once there is input.
    one =
      tap idle
        >>> alongside (arr (sharedBuffers . connShared) -| "buffer pool")
        >>> withRequestBuffers (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
idle :: (Runtime arr) => arr Conn ()
idle =
  named (Label "idle" "Waits until the connection has input, which may be its end, unless it has bytes in hand.") $
    typed @Conn @() $
      step "check carry-over" ready -| "bytes carried over?"
        >>> ("no" |- awaitInput ||| "yes" |- returnA)
  where
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

-- | The cases of a sum that the head can come to: the stream ended before the head did, or there was no request ("stream
-- ended"), the request is refused with a status ("refused"), or it has a head ("good").
type Ends s = (Inj "stream ended" () s, Inj "refused" Int s, Inj "good" Good s)

-- | What the handler is run with: the call, the request, and the primary loan, if the handler is to have it back.
data Prepared = Prepared
  { preparedCall :: Call,
    preparedRequest :: Request,
    preparedLoan :: Maybe Loan
  }

-- | Reads a request, runs the handler, and says whether the connection is kept for another.
request :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Loans, Conn) Bool
request settings handler =
  named (Label "request" "Reads the head of a request, runs the handler, and says whether the connection is kept.") $
    uncurry (newRecord ["loans", "conn"])
      ^>> adding "loan" (field "loans" >>> borrow)
      >>> adding "req" (step "copy carry-over" begin')
      >>> adding "head" (field "req" >>> readHead settings)
      >>> decide "how did the head end?" (cases "head")
      >>> ("stream ended" ?> quit ||| "refused" ?> refuse ||| "good" ?> answered settings handler)
  where
    quit :: (Runtime arr) => arr a Bool
    quit = arr (const False) -| "keep alive"
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
readHead :: (Runtime arr, Ends s) => Settings -> arr Req s
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
advance :: (Ends s) => Settings -> Req -> IO (Step ((Socket, Room), Req) s)
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
answered :: (Runtime arr, HasField "req" r Req, HasField "good" r Good) => Settings -> arr (Call, Request) () -> arr r Bool
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
    -- With a shutdown event, a handler runs behind a shield: a cancellation of the server waits until it has returned.
    protected :: (Runtime arr) => arr a b -> arr a b
    protected = if isJust (shutdown settings) then shield else id
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
    -- Inlined where it is used, so that the branches take the sum apart where it is made, and it is not built.
    byOutcome r = casesBy "outcome" classify r
    {-# INLINE byOutcome #-}
    classify = \case
      Left failure
        | Just ConnectionLost <- fromException failure -> inj "connection lost" ()
        | Just (BodyRefused status) <- fromException failure -> inj "failed" status
        | otherwise -> inj "failed" (500 :: Int)
      Right () -> inj "returned" ()
    quit :: (Runtime arr) => arr a Bool
    quit = arr (const False) -| "keep alive"
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
                |- (step "render the refusal" (\(status, exchange) -> (,) (exSocket exchange) <$> refusal (exPulse exchange) status) >>> sendAll)
                ||| "no" |- returnA
            )
        >>> quit
