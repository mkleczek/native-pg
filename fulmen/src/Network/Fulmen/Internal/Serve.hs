{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The server as a static network of parts. Each core has its workers from the start, and the first core has the
-- acceptor besides. A worker offers itself as free, in the pool that the cores share, and waits at a ticket for the
-- connection that the acceptor gives it; then it serves the connection, request after request, until it ends, and
-- offers itself again. The acceptor takes the worker that has been free the longest, and waits for one when there is
-- none, so that past the cap on connections they wait in the listen backlog.
--
-- A request is read into the primary buffer that it was admitted with. The bytes that follow it in the buffer, which
-- belong to the next request on the connection, are copied out before the buffer is used for anything else, and
-- the next request begins with them: pipelining costs a copy of the bytes that came early, and nothing otherwise.
module Network.Fulmen.Internal.Serve
  ( serve,
  )
where

import Control.Arrow (arr, first, returnA, second, (&&&), (>>>), (^>>), (|||))
import Control.Exception (SomeException, fromException)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
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
import Network.Streaming.Combinators (together)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Pool (queueNow, releaseNow)

-- | The server: for each core, the workers, and for the first, the acceptor.
serve :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
serve settings handler =
  (\cores@(Cores core _) -> if core == 0 then Left cores else Right cores)
    ^>> (together (acceptance settings : workers) ||| together workers)
  where
    workers = replicate (connectionsPerCore settings) (worker settings handler)

-- * The acceptor

-- | Takes the worker that has been free the longest, accepts a connection, and gives it to the worker. A connection
-- that the worker is no longer there to take is closed.
acceptance :: (Runtime arr) => Settings -> arr Cores ()
acceptance settings = now (\cores -> cores <$ listening (listener settings)) >>> loop
  where
    loop = (returnA &&& place) >>> arr fst >>> loop
    place =
      checkout
        >>> (returnA &&& (arr (const (listener settings)) >>> acceptor))
        >>> (\(giver, socket) -> ((giver, socket), socket))
        ^>> first give
        >>> (\(taken, socket) -> if taken then Right () else Left socket)
        ^>> (close ||| returnA)

-- | The giver of the worker that has been free the longest. The part queues the giver of a ticket of its own in the
-- pool, which gives it the giver of a free worker at once if one is free, and else when one is released, and waits at
-- the ticket.
checkout :: (Runtime arr) => arr Cores (Giver 'Anywhere Socket)
checkout =
  (returnA &&& (arr (const ()) >>> ticket))
    >>> now (\(Cores _ shared, (waiting, giver)) -> waiting <$ queueNow (sharedWorkers shared) 1 giver)
    >>> await

-- * The workers

-- | A worker: it offers itself as free, waits for a connection, and serves it, for ever.
worker :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Cores ()
worker settings handler = loop
  where
    loop = (returnA &&& (offered >>> connection settings handler)) >>> arr fst >>> loop

-- | The worker is free: the giver of a new ticket is in the pool, and the connection that is given it comes.
offered :: (Runtime arr) => arr Cores (Shared, Socket)
offered =
  (returnA &&& (arr (const ()) >>> ticket))
    >>> now (\(Cores _ shared, (waiting, giver)) -> (shared, waiting) <$ releaseNow (sharedWorkers shared) [giver])
    >>> second await

-- | What a connection is: its socket, the bytes of the next request that came early, which a request copies out of
-- its buffer, and what the cores share.
data Conn = Conn !Socket !(IORef ByteString) !Shared

-- | The connection until it ends, which closes its socket however it ends. What goes wrong on a connection is that
-- connection's: the worker goes on to the next.
connection :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Shared, Socket) ()
connection settings handler = scope (arr snd) (arr fst >>> close) (arr snd >>> serving)
  where
    serving =
      now (\(shared, socket) -> (\carry -> Conn socket carry shared) <$> newIORef B.empty)
        >>> tryA (requests settings handler)
        >>> arr (const ())

-- | Request after request, while the connection is kept.
requests :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr Conn ()
requests settings handler = loop
  where
    loop = (returnA &&& one) >>> (\(conn, keep) -> if keep then Right conn else Left ()) ^>> (returnA ||| loop)
    -- An idle connection waits holding no buffer: a request is admitted once there is input.
    one = (returnA &&& idle) >>> arr fst >>> withRequestBuffers (buffers settings) (request settings handler)

-- | Waits until the connection has input, which may be its end, unless it has bytes of a request already.
idle :: (Runtime arr) => arr Conn ()
idle = now ready >>> (awaitInput ||| returnA)
  where
    ready (Conn socket carry _) = (\carried -> if B.null carried then Left socket else Right ()) <$> readIORef carry

-- * A request

-- | A request being read: the connection, the loans of the request, the primary loan that the head is read into, and
-- how far it has come.
data Req = Req !Conn !Loans !Loan !(IORef Progress)

-- | The bytes in hand, where the head has been looked at, and whether the stream has ended.
data Progress = Progress !Int !Scan !Bool

-- | What the head came to.
data Head
  = -- | The stream ended before the head did, or there was no request.
    HeadEnded
  | -- | The request is refused with this status.
    HeadRefused !Int
  | -- | The request, and where its head ends in the buffer.
    HeadGood !Parsed !Int

-- | Reads a request, runs the handler, and says whether the connection is kept for another.
request :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Loans, Conn) Bool
request settings handler =
  (\(loans, conn) -> (loans, (loans, conn)))
    ^>> first borrow
    >>> now begin'
    >>> readHead settings
    >>> ( \(req, outcome) -> case outcome of
            HeadEnded -> Left (Left ())
            HeadRefused status -> Left (Right (req, status))
            HeadGood parsed consumed -> Right (req, parsed, consumed)
        )
    ^>> ((quit ||| refuse) ||| answered handler)
  where
    quit = arr (const False)
    refuse =
      now (\(Req (Conn socket _ shared) _ _ _, status) -> (,) socket <$> refusal (sharedDate shared) status)
        >>> sendAll
        >>> quit
    -- The request has the connection's first bytes, which came with the one before.
    begin' (loan, (loans, conn@(Conn _ carry _))) = do
      carried <- readIORef carry
      unless (B.null carried) $ do
        writeIORef carry B.empty
        BU.unsafeUseAsCStringLen carried $ \(source, size) ->
          withForeignPtr (bufferMemory (loanBuffer loan)) (\target -> copyBytes target (castPtr source) size)
      Req conn loans loan <$> newIORef (Progress (B.length carried) startScan False)

-- | Receives until the head is there, or is refused, or the stream has ended.
readHead :: (Runtime arr) => Settings -> arr Req (Req, Head)
readHead settings = loop
  where
    loop = now (advance settings) >>> (more ||| returnA)
    more =
      (\(req@(Req (Conn socket _ _) _ _ _), room) -> ((socket, room), req))
        ^>> first receive
        >>> now arrived
        >>> loop
    arrived (count, req@(Req _ _ _ progress)) = do
      Progress end scan _ <- readIORef progress
      writeIORef progress (if count <= 0 then Progress end scan True else Progress (end + count) scan False)
      pure req

-- | Looks at the bytes that arrived: what the head came to, or the room to receive more into.
advance :: Settings -> Req -> IO (Either (Req, Room) (Req, Head))
advance settings req@(Req _ _ loan progress) = do
  Progress end scan ended <- readIORef progress
  let buffer = loanBuffer loan
      bytes = view buffer end
  case scanHead (lineLimit settings) (headLimit settings) (bufferCapacity buffer) bytes scan of
    Refused status -> pure (Right (req, HeadRefused status))
    Complete start stop -> do
      -- The head is copied and parsed while the bytes are there, and the request owns them.
      let !owned = B.copy (B.take (stop - start) (B.drop start bytes))
      pure . Right $ case parseHead (bodyLimit settings) owned of
        Left status -> (req, HeadRefused status)
        Right parsed -> (req, HeadGood parsed stop)
    scanning
      | ended -> pure (Right (req, HeadEnded))
      | otherwise -> Left (req, Room buffer end (bufferCapacity buffer - end)) <$ writeIORef progress (Progress end scanning False)

-- | A request that has a head: the handler runs, and the connection is kept if the response, and the request's body,
-- leave it in a state that another request can follow.
answered :: (Runtime arr) => arr (Call, Request) () -> arr (Req, Parsed, Int) Bool
answered handler =
  now prepare
    >>> (\(call, asked, loaned) -> maybe (Right (call, asked)) (\loan -> Left (loan, (call, asked))) loaned)
    ^>> ((first release >>> arr snd) ||| returnA)
    >>> (\(call, asked) -> ((call, asked), call))
    ^>> first (tryA handler)
    >>> aftermath

-- | The call: the body of the request, and the response. What follows a request that has no body goes to the next
-- request, and its primary loan is given back before the handler runs, to be used for what the handler reads.
prepare :: (Req, Parsed, Int) -> IO (Call, Request, Maybe Loan)
prepare (Req (Conn socket carry shared) loans loan progress, Parsed request' framing keep, consumed) = do
  Progress end _ _ <- readIORef progress
  let buffer = loanBuffer loan
      Method name = method request'
  exchange <- newExchange socket loans (name == "HEAD") (version request' == Http11) (sharedDate shared) (keep && framing /= Chunked)
  case framing of
    Length size -> do
      marks <- newIORef (Marks consumed end size)
      pure (Call loans (Sized (BodyState socket buffer loan carry marks)) exchange, request', Nothing)
    Chunked -> pure (Call loans ChunkedBody exchange, request', Just loan)
    NoContent -> do
      -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
      when (end > consumed) $ writeIORef carry $! B.copy (BSI.BS (bufferMemory buffer `plusForeignPtr` consumed) (end - consumed))
      pure (Call loans NoBody exchange, request', Just loan)

-- | What the handler left: a connection that the client has gone from, or a response that is not there or not
-- complete, ends the connection, with a 500 if the handler had not answered; and one that is complete is kept if the
-- body of the request was read, or could be, and the response didn't end the connection.
aftermath :: (Runtime arr) => arr (Either SomeException (), Call) Bool
aftermath =
  ( \(outcome, call) -> case outcome of
      Left failure
        | Just ConnectionLost <- fromException failure -> Left (Left ())
        | otherwise -> Left (Right call)
      Right () -> Right call
  )
    ^>> ((quit ||| failed) ||| finished)
  where
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
