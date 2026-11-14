{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

-- | The server as a static network of parts. Each core has its workers from the start, and the first core has the
-- acceptor besides. A worker offers itself as free, in the pool that the cores share, and waits at a ticket for the
-- connection that the acceptor gives it; then it serves the connection, request after request, until it ends, and
-- offers itself again. The acceptor takes a free worker of the core that has the most free workers, so that the
-- connections are shared out between the cores, and waits for one when there is none, so that past the cap on
-- connections they wait in the listen backlog.
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
import Network.Fulmen.Request (Method (..), Request (..), Version (..))
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, consume, done, foreverA, iterateA, iterateWith, Step, tap, whether, withTicket)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Pool (queueNow, releaseNow, takeNowWith)

-- | The server: for each core, the workers, and for the first, the acceptor.
serve :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
serve !settings handler = whether isFirst ^>> (together (acceptance settings : workers) ||| together workers)
  where
    workers = replicate (connectionsPerCore settings) (worker settings handler)
    isFirst (Cores core _) = core == 0

-- * The acceptor

-- | Takes a free worker of the core that has the most free workers, accepts a connection, and gives it to the worker. A
-- connection that the worker is no longer there to take is closed.
acceptance :: (Runtime arr) => Settings -> arr Cores ()
acceptance settings = now (\cores -> cores <$ listening (listener settings)) >>> foreverA (tap place)
  where
    place =
      ((,) <$> checkout <*> (const (listener settings) ^>> acceptor))
        >>> alongside give
        >>> untaken
        ^>> (close ||| returnA)
    untaken (taken, (_, socket)) = if taken then Right () else Left socket

-- | The giver of a free worker of the core that has the most free workers, which is taken from the pool at once. When no
-- worker is free, the part queues the giver of a ticket of its own in the pool, which gives it the worker that is released
-- first, on any core, and waits at the ticket. A worker that is given after the wait has ended is put back in the pool.
checkout :: (Runtime arr) => arr Cores (Giver 'Anywhere Socket)
checkout =
  alongside (now (\(Cores _ shared) -> takeNowWith freest (sharedWorkers shared)))
    >>> (\(taken, cores) -> maybe (Left cores) Right taken)
    ^>> (queued ||| returnA)
    >>^ freeGiver
  where
    queued = withTicket (now queue) (now putBack) >>^ snd
    queue (giver, Cores _ shared) = queueNow (sharedWorkers shared) 1 giver
    putBack (free, Cores _ shared) = releaseNow (sharedWorkers shared) [free]

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

-- | A worker: it offers itself as free, waits for a connection, and serves it, for ever.
worker :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
worker settings handler = foreverA (tap (offered >>> connection settings handler))

-- | The worker is free: the giver of a new ticket is in the pool, and the connection that is given it comes. A
-- connection that is given after the wait has ended is closed.
offered :: (Runtime arr) => arr Cores (Shared, Socket)
offered = (,) <$> arr sharedOf <*> (withTicket (now offer) (fst ^>> close) >>^ snd)
  where
    sharedOf (Cores _ shared) = shared
    offer (giver, Cores core shared) = releaseNow (sharedWorkers shared) [Free core giver]

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
connection settings handler = scope (arr snd) (fst ^>> close) (snd ^>> serving)
  where
    serving =
      now (\(shared, socket) -> (\carry -> Conn socket carry shared) <$> newIORef B.empty)
        >>> tryA (requests settings handler)
        >>> done

-- | Request after request, while the connection is kept.
requests :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn ()
requests settings handler = iterateA (alongside one >>^ whileKept)
  where
    whileKept (keep, conn) = if keep then Again conn else Finish ()
    -- An idle connection waits holding no buffer: a request is admitted once there is input.
    one = tap idle >>> withRequestBuffers (buffers settings) (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
idle :: (Runtime arr) => arr Conn ()
idle = now ready >>> (awaitInput ||| returnA)
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
  (fst &&& id)
    ^>> first borrow
    >>> now begin'
    >>> readHead settings
    >>> byHead
    ^>> ((quit ||| refuse) ||| answered handler)
  where
    byHead (req, outcome) = case outcome of
      HeadEnded -> Left (Left ())
      HeadRefused status -> Left (Right (req, status))
      HeadGood parsed consumed -> Right (Headed req parsed consumed)
    quit = arr (const False)
    refuse =
      now
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
readHead settings = iterateWith (now (advance settings)) more
  where
    more = first receive >>> now arrived
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
  now prepare
    >>> heldLoan
    ^>> (consume release ||| returnA)
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
aftermath = byOutcome ^>> ((quit ||| failed) ||| finished)
  where
    byOutcome (outcome, (call, _)) = case outcome of
      Left failure
        | Just ConnectionLost <- fromException failure -> Left (Left ())
        | otherwise -> Left (Right call)
      Right () -> Right call
    quit = arr (const False)
    failed = now (\(Call _ _ exchange) -> unanswered exchange) >>> internalError
    finished =
      now
        ( \call@(Call _ _ exchange) ->
            readIORef (exPhase exchange) >>= \case
              Ended -> pure (Right call)
              Unanswered -> pure (Left (Just exchange))
              _ -> pure (Left Nothing)
        )
        >>> (internalError ||| completed)
    -- The body that the handler didn't read is read now and dropped, if it is not too long.
    completed = (\(Call _ requestBody exchange) -> (requestBody, exchange)) ^>> first drain >>> now (\(drained, exchange) -> (drained &&) <$> readIORef (exKeep exchange))
    unanswered exchange = readIORef (exPhase exchange) >>= \case Unanswered -> pure (Just exchange); _ -> pure Nothing
    -- Answers 500 if nothing was answered, and ends the connection.
    internalError = maybe (Right ()) Left ^>> ((now (\exchange -> (,) (exSocket exchange) <$> refusal (exDate exchange) 500) >>> sendAll) ||| returnA) >>> quit
