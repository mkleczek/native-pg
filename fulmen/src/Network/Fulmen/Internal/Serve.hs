{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
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
import Control.Exception (SomeException, fromException)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import qualified Data.IntMap.Strict as IntMap
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.List (findIndex)
import Foreign.ForeignPtr (plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Network.Fulmen.Internal
import Network.Fulmen.Internal.Accept (acceptor, listening)
import Network.Fulmen.Internal.Body (BodyState (..), ConnectionLost (..), Marks (..), drain)
import Network.Fulmen.Internal.Head (Framing (..), Parsed (..), Scan (..), parseHead, scanHead, startScan)
import Network.Fulmen.Internal.Response (Exchange (..), Phase (..), newExchange, refusal)
import Network.Fulmen.Listen (Listen (..))
import Network.Fulmen.Request (Method (..), Request (..), Version (..))
import Network.Socket (Socket)
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
import Network.Streaming.Pool (offer, queueNow, takeNowWith)

-- | The server: for each core, the workers, and for the first, the acceptor, which opens the listener.
serve :: (Runtime arr) => Settings -> Listen arr -> arr (Call, Request) () -> arr Cores ()
serve !settings listen handler =
  named (Label "serve" "Runs the workers of each core, and the acceptor besides on the first core.") $
    decide "first core?" (whether isFirst)
      >>> (taking "yes" (together (acceptance listen : workers)) ||| taking "no" (together workers))
  where
    workers = replicate (connectionsPerCore settings) (worker settings handler)
    isFirst (Cores core _) = core == 0

-- * The acceptor

-- | Opens the listener, and then takes a free worker of the core that has the most free workers, accepts a connection,
-- and posts it to the worker's inbox, for ever. A connection that the worker is no longer there to take is closed. The listener
-- is closed when the acceptor ends, however it ends.
acceptance :: (Runtime arr) => Listen arr -> arr Cores ()
acceptance Listen {openListener, closeListener} =
  named (Label "acceptance" "Opens the listener, accepts connections, and gives each to a free worker of the core that has the most free workers.") $
    scope (done >>> giving "listener" openListener) (fst ^>> closeListener) $
      tap (step "make listener non-blocking" (listening . fst)) >>> foreverA (tap place)
  where
    place =
      ( (,)
          <$> giving "worker's address" (snd ^>> checkout)
          <*> giving "accepted socket" (fst ^>> accept)
      )
        >>> alongside post
        >>> decide "worker took it?" untaken
        >>> (taking "no" close ||| taking "yes" returnA)
    accept =
      named (Label "accept" "Accepts the next connection of the listening socket, and blocks while none is waiting.") acceptor
    untaken (taken, (_, socket)) = if taken then Right () else Left socket

-- | The address of the inbox of a free worker of the core that has the most free workers, which is taken from the pool at once. When no
-- worker is free, the part queues the promise of a ticket of its own in the pool, which fulfils it with the worker that is released
-- first, on any core, and waits at the ticket. A worker that comes after the wait has ended is put back in the pool.
checkout :: (Runtime arr) => arr Cores (Address "connections" 'Anywhere Socket)
checkout =
  named (Label "checkout" "Takes a free worker of the core that has the most free workers at once, and else waits for the worker that is freed first, on any core, and puts back one that comes too late.") $
    alongside (step "take a free worker" (\(Cores _ shared) -> takeNowWith freest (sharedWorkers shared)))
      >>> decide "worker free now?" (\(taken, cores) -> maybe (Left cores) Right taken)
      >>> (taking "no" queued ||| taking "yes" returnA)
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
    withInbox @"connections" closeLateSocket (foreverA (tap (giving "accepted socket" offered >>> connection settings handler)))

-- | The socket that was left in the worker's inbox when it ended, which is closed.
closeLateSocket :: (Runtime arr) => arr (Socket, Cores) ()
closeLateSocket = box "close late socket" (fst ^>> close)

-- | The worker is free: its address is in the pool, and the connection that is posted to its inbox comes.
offered :: (Runtime arr) => arr ((Inbox arr "connections" 'Anywhere Socket, Address "connections" 'Anywhere Socket), Cores) (Shared, Socket)
offered =
  named (Label "offered" "Offers the worker as free in the pool, and collects the connection that is posted to its inbox.") $
    tap (answering "free worker" (free ^>> offer)) >>> ((,) <$> arr sharedOf <*> (fst . fst ^>> collect))
  where
    sharedOf (_, Cores _ shared) = shared
    free ((_, address), Cores core shared) = (sharedWorkers shared, Free core address)

-- | What a connection is: its socket, the bytes of the next request that came early, which a request copies out of
-- its buffer, and what the cores share.
data Conn = Conn
  { connSocket :: !Socket,
    connCarry :: !(IORef ByteString),
    connShared :: !Shared
  }

-- | The connection until it ends, which closes its socket however it ends. What goes wrong on a connection is that
-- connection's: the worker goes on to the next.
connection :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Shared, Socket) ()
connection settings handler =
  named (Label "connection" "Serves the connection until it ends, and closes its socket however it ends.") $
    scope (arr snd) (fst ^>> close) (snd ^>> serving)
  where
    serving =
      giving
        "Conn"
        (step "make connection state" (\(shared, socket) -> (\carry -> Conn socket carry shared) <$> newIORef B.empty))
        >>> tryA (requests settings handler)
        >>> done

-- | Request after request, while the connection is kept.
requests :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn ()
requests settings handler =
  named (Label "requests" "Serves request after request, while the connection is kept.") $
    typed @Conn @() $
      iterateA (giving "connection kept?" (alongside one >>^ whileKept))
  where
    whileKept (keep, conn) = if keep then Again conn else Finish ()
    -- An idle connection waits holding no buffer: a request is admitted once there is input.
    one =
      tap idle
        >>> alongside (giving "buffer pool" (arr (sharedBuffers . connShared)))
        >>> withRequestBuffers (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
idle :: (Runtime arr) => arr Conn ()
idle =
  named (Label "idle" "Waits until the connection has input, which may be its end, unless it has bytes in hand.") $
    typed @Conn @() $
      giving "bytes carried over?" (step "check carry-over" ready)
        >>> (taking "no" awaitInput ||| taking "yes" returnA)
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

-- | What the head came to.
data Head
  = -- | The stream ended before the head did, or there was no request.
    HeadEnded
  | -- | The request is refused with this status.
    HeadRefused !Int
  | -- | The request, and where its head ends in the buffer.
    HeadGood !Parsed !Int

-- | A request that has a head: the request being read, what its head parsed to, and where the head ends in the buffer.
data Headed = Headed
  { headedReq :: !Req,
    headedParsed :: !Parsed,
    headedEnd :: !Int
  }

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
    (fst &&& id)
      ^>> first borrow
      >>> giving "Req" (step "copy carry-over" begin')
      >>> readHead settings
      >>> decide "how did the head end?" byHead
      >>> ((taking "stream ended" quit ||| taking "refused" refuse) ||| taking "good" (answered handler))
  where
    byHead (req, outcome) = case outcome of
      HeadEnded -> Left (Left ())
      HeadRefused status -> Left (Right (req, status))
      HeadGood parsed consumed -> Right (Headed req parsed consumed)
    quit = giving "keep alive" (arr (const False))
    refuse =
      step
        "render refusal"
        ( \(Req {reqConn = Conn {connSocket, connShared}}, status) ->
            (,) connSocket <$> refusal (sharedDate connShared) status
        )
        >>> sendAll
        >>> quit
    -- The request has the connection's first bytes, which came with the one before.
    begin' (loan, (loans, conn@Conn {connCarry})) = do
      carried <- readIORef connCarry
      unless (B.null carried) $ do
        writeIORef connCarry B.empty
        BU.unsafeUseAsCStringLen carried $ \(source, size) ->
          withForeignPtr (bufferMemory (loanBuffer loan)) (\target -> copyBytes target (castPtr source) size)
      Req conn loans loan <$> newIORef Progress {progressEnd = B.length carried, progressScan = startScan, progressEnded = False}

-- | Receives until the head is there, or is refused, or the stream has ended.
readHead :: (Runtime arr) => Settings -> arr Req (Req, Head)
readHead settings =
  named (Label "readHead" "Receives until the head is there, or is refused, or the stream has ended.") $
    typed @Req @(Req, Head) $
      iterateWith (giving "head complete?" (step "scan head" (advance settings))) (taking "needs more" more)
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
advance :: Settings -> Req -> IO (Step ((Socket, Room), Req) (Req, Head))
advance settings req@Req {reqConn = Conn {connSocket}, reqLoan, reqProgress} = do
  progress@Progress {progressEnd, progressScan, progressEnded} <- readIORef reqProgress
  let buffer = loanBuffer reqLoan
      bytes = view buffer progressEnd
  case scanHead (lineLimit settings) (headLimit settings) (bufferCapacity buffer) bytes progressScan of
    Refused status -> pure (Finish (req, HeadRefused status))
    Complete start stop -> do
      -- The head is copied and parsed while the bytes are there, and the request owns them.
      let !owned = B.copy (B.take (stop - start) (B.drop start bytes))
      pure . Finish $ case parseHead (bodyLimit settings) owned of
        Left status -> (req, HeadRefused status)
        Right parsed -> (req, HeadGood parsed stop)
    scanning
      | progressEnded -> pure (Finish (req, HeadEnded))
      | otherwise ->
          Again ((connSocket, Room buffer progressEnd (bufferCapacity buffer - progressEnd)), req)
            <$ writeIORef reqProgress progress {progressScan = scanning}

-- | A request that has a head: the handler runs, and the connection is kept if the response, and the request's body,
-- leave it in a state that another request can follow.
answered :: (Runtime arr) => arr (Call, Request) () -> arr Headed Bool
answered handler =
  named (Label "answered" "Runs the handler on a request that has a head, and says whether the connection is kept.") $
    giving "keep alive" $
      typed @Headed @Prepared (step "prepare call" prepare)
        >>> decide "give back loan?" heldLoan
        >>> (taking "yes" (consume release) ||| taking "no" returnA)
        >>> alongside (tryA handler)
        >>> aftermath
  where
    heldLoan Prepared {preparedCall, preparedRequest, preparedLoan} =
      maybe (Right (preparedCall, preparedRequest)) (\loan -> Left (loan, (preparedCall, preparedRequest))) preparedLoan

-- | The call: the body of the request, and the response. What follows a request that has no body goes to the next
-- request, and its primary loan is given back before the handler runs, to be used for what the handler reads.
prepare :: Headed -> IO Prepared
prepare Headed {headedReq = Req {reqConn = Conn {..}, ..}, headedParsed = Parsed request' framing keep, headedEnd = consumed} = do
  Progress {progressEnd = end} <- readIORef reqProgress
  let buffer = loanBuffer reqLoan
      Method name = method request'
  exchange <- newExchange connSocket reqLoans (name == "HEAD") (version request' == Http11) (sharedDate connShared) (keep && framing /= Chunked)
  case framing of
    Length size -> do
      marks <- newIORef Marks {markStart = consumed, markEnd = end, markRemaining = size}
      pure (Prepared (Call reqLoans (Sized (BodyState connSocket buffer reqLoan connCarry marks)) exchange) request' Nothing)
    Chunked -> pure (Prepared (Call reqLoans ChunkedBody exchange) request' (Just reqLoan))
    NoContent -> do
      -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
      when (end > consumed) $ writeIORef connCarry $! B.copy (BSI.BS (bufferMemory buffer `plusForeignPtr` consumed) (end - consumed))
      pure (Prepared (Call reqLoans NoBody exchange) request' (Just reqLoan))

-- | What the handler left: a connection that the client has gone from, or a response that is not there or not
-- complete, ends the connection, with a 500 if the handler had not answered; and one that is complete is kept if the
-- body of the request was read, or could be, and the response didn't end the connection.
aftermath :: (Runtime arr) => arr (Either SomeException (), (Call, Request)) Bool
aftermath =
  named (Label "aftermath" "Decides whether the connection is kept, and answers 500 if nothing was answered.") $
    decide "how did the handler end?" byOutcome
      >>> ((taking "connection lost" quit ||| taking "failed" failed) ||| taking "returned" finished)
  where
    byOutcome (outcome, (call, _)) = case outcome of
      Left failure
        | Just ConnectionLost <- fromException failure -> Left (Left ())
        | otherwise -> Left (Right call)
      Right () -> Right call
    quit = giving "keep alive" (arr (const False))
    failed = step "check if answered" (\(Call _ _ exchange) -> unanswered exchange) >>> internalError
    finished =
      giving
        "response complete?"
        ( step
            "read response phase"
            ( \call@(Call _ _ exchange) ->
                readIORef (exPhase exchange) >>= \case
                  Ended -> pure (Right call)
                  Unanswered -> pure (Left (Just exchange))
                  _ -> pure (Left Nothing)
            )
        )
        >>> (taking "no" internalError ||| taking "yes" completed)
    -- The body that the handler didn't read is read now and dropped, if it is not too long.
    completed =
      (\(Call _ requestBody exchange) -> (requestBody, exchange))
        ^>> first drain
        >>> giving
          "keep alive"
          (step "check keep alive" (\(drained, exchange) -> (drained &&) <$> readIORef (exKeep exchange)))
    unanswered exchange = readIORef (exPhase exchange) >>= \case Unanswered -> pure (Just exchange); _ -> pure Nothing
    -- Answers 500 if nothing was answered, and ends the connection.
    internalError =
      decide "unanswered?" (maybe (Right ()) Left)
        >>> ( taking
                "yes"
                (step "render 500" (\exchange -> (,) (exSocket exchange) <$> refusal (exDate exchange) 500) >>> sendAll)
                ||| taking "no" returnA
            )
        >>> quit
