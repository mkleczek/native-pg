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

-- | Fulmen's HTTP connection worker. It reads requests until the connection ends and handles HTTP idle timeouts
-- and graceful draining. "Network.Streaming.Server" accepts sockets and manages the worker slots.
--
-- A request is read into the primary buffer that it was admitted with. The bytes that follow it in the buffer, which
-- belong to the next request on the connection, are copied out before the buffer is used for anything else, and
-- the next request begins with them: pipelining costs a copy of the bytes that came early, and nothing otherwise.
module Network.Fulmen.Internal.Serve
  (serve)
where

import Control.Arrow (arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (SomeException, catch, fromException)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef, writeIORef)
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
import Network.Fulmen.Internal.Body (BodyRefused (..), BodyState (..), ChunkState (..), ConnectionLost (..), Decoding (..), Marks (..), drain)
import Network.Fulmen.Internal.Bytes (equalsLower)
import Network.Fulmen.Internal.Date (beginDraining)
import Network.Fulmen.Internal.Head (Framing (..), Parsed (..), Scan (..), Spill (..), joined, notYet, parseHead, scanHead, startScan)
import Network.Fulmen.Internal.Response (Exchange (..), Phase (..), newExchange, refusal)
import Network.Fulmen.Listen (Listen)
import Network.Fulmen.Request (Method (..), Request (..), Version (..))
import Network.Socket (SockAddr (..), Socket, withFdSocket)
import qualified Network.Socket.Address as SocketAddress
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators
  ( pattern Again,
    pattern Finish,
    Step,
    alongside,
    consume,
    decide,
    done,
    iterateA,
    iterateWith,
    keeping,
    step,
    tap,
  )
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Named (Inj, casesBy, inj, newRecord, type (=:), (:=), (?>))
import Network.Streaming.Server (serveConnections)
import Network.Streaming.Wiring (choice, flow, result, (-<), (<:))
import System.Posix.Types (CSsize (..))

-- | Runs the HTTP connection workers on this core. On shutdown, the dispatcher closes the listener and cancels
-- the workers at their waits. A request in flight runs behind a grace period (see 'handleRequest').
serve :: (Runtime arr) => Settings -> Listen arr -> arr (Call, Request) () -> arr Cores ()
serve !settings listen handler =
  named (Label "serve" "Runs the HTTP server and drains requests on shutdown.") $
    case shutdown settings of
      Nothing -> serving
      Just event -> race (beginning event) serving >>> done
  where
    serving = (\(Cores core shared) -> (core, shared)) ^>> serveConnections (connectionsPerCore settings) listen (connection settings handler)

-- | Waits for the event that starts the shutdown, and marks the server as shutting down: a response from then on says that its
-- connection ends.
beginning :: (Runtime arr) => Event -> arr Cores ()
beginning event =
  named (Label "shutdown" "Waits for the event that starts the shutdown, and marks the server as shutting down.") $
    tap (const event ^>> block)
      >>> step "begin the shutdown" (\(Cores _ shared) -> beginDraining (sharedPulse shared))

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

-- | Serves HTTP on a socket borrowed from the dispatcher.
connection :: (Runtime arr) => Settings -> arr (Call, Request) () -> arr (Socket, Shared) ()
connection settings handler =
  named (Label "serve connection" "Serves requests until the connection ends.") $
    uncurry (newRecord ["socket", "shared"])
      ^>> flow
        ( "connection" <: initConnection -< ("socket", "shared") >>>
          tryA runConnection             -< "connection"
        ) >>>
    done
  where
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
