{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A response in the making: the head that waits for the body's first piece, so that the two go out in one write, the
-- framing of the body, and what the response says of the connection. The head is rendered into a loan of the request's
-- buffers, and the pieces of the body are sent as they are, views and all.
module Network.Fulmen.Internal.Response
  ( Status (..),
    ResponseHeaders,
    Length (..),
    Exchange (..),
    Phase (..),
    newExchange,
    begin,
    transmit,
    refusal,
  )
where

import Control.Arrow ((>>>), (|||))
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe)
import Data.Word (Word8)
import Network.Fulmen.Internal.Bytes (commaList, equalsLower)
import Network.Fulmen.Internal.Date (Pulse, currentDate, draining)
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes (Effect (..), Link (..), Runtime)
import Network.Streaming.Combinators (tap)
import Network.Streaming.Loans (Loans, borrow, loanBuffer, release)
import Network.Streaming.Named (adding, field, newRecord, (:=))
import Network.Streaming.Output (Output (..), Uncopied (..), bytes, outputs, word8, writeOutput)

-- | A status code.
newtype Status = Status Int
  deriving (Eq, Ord, Show)

-- | The header fields of a response, in the order they are sent, besides those
-- that the server adds: @Date@, @Server@, @Connection@ and the framing.
type ResponseHeaders = [(ByteString, ByteString)]

-- | How long the response's body is: declared, or not known, which is sent
-- chunked.
data Length = Known Int | Unknown
  deriving (Eq, Show)

-- | What a response needs of its connection and of the request it answers: where to send it, what to render its
-- head in, and what the request says of its version and its method.
data Exchange = Exchange
  { exSocket :: !Socket,
    exLoans :: !Loans,
    -- | The request was a HEAD.
    exHead :: !Bool,
    exHttp11 :: !Bool,
    exPulse :: !Pulse,
    exPhase :: !(IORef Phase),
    -- | Whether the connection is kept open after the response: the request may end it, and so may the response.
    exKeep :: !(IORef Bool)
  }

-- | How far the response has come.
data Phase
  = -- | The handler has not said how it answers.
    Unanswered
  | -- | The head is held until the first piece of the body, or its end.
    Pending !Status !ResponseHeaders !Length
  | -- | The head has gone, and there is no declared length to check.
    Sending !Mode
  | -- | The head has gone, and this many bytes of the declared body are left.
    SendingFixed !Int
  | -- | The response is complete.
    Ended

-- | How the body goes out.
data Mode
  = -- | None is sent: it is a response to a HEAD, or one that has no body.
    Bodyless
  | -- | A body of this length.
    Fixed !Int
  | -- | In chunks.
    Chunking
  | -- | As it is, to the end of the connection.
    Unframed

newExchange :: Socket -> Loans -> Bool -> Bool -> Pulse -> Bool -> IO Exchange
newExchange socket loans isHead http11 pulse keep = Exchange socket loans isHead http11 pulse <$> newIORef Unanswered <*> newIORef keep

-- | The handler says how it answers. The head is sent with the first piece, or when the response ends.
begin :: Exchange -> (Status, ResponseHeaders, Length) -> IO ()
begin exchange (status, headers, len) =
  readIORef (exPhase exchange) >>= \case
    Unanswered -> writeIORef (exPhase exchange) (Pending status headers len)
    _ -> ioError (userError "the request is answered twice")

-- | The head of the response if it is not sent yet, then the pieces, framed as the response is, and then its end if
-- this is the last of it. The head, and the size of a chunk, are rendered in a loan of the request's buffers, and go
-- out in the same write as the pieces; a send that has no need of a loan has none.
transmit :: (Runtime arr) => arr (Exchange, ([ByteString], Bool)) ()
transmit = now plan >>> (sendAll ||| rendered)
  where
    rendered =
      adding "loan" (field "loans" >>> borrow)
        >>> tap (now (\r -> r.render (loanBuffer r.loan)) >>> sendAll)
        >>> field "loan"
        >>> release

-- | Decides what goes out: the pieces as they are, or what is rendered into a loan, and the pieces after it. Either way
-- it is the socket and the pieces to send.
plan ::
  (Exchange, ([ByteString], Bool)) ->
  IO (Either (Socket, [ByteString]) ("loans" := Loans, "render" := (Buffer -> IO (Socket, [ByteString]))))
plan (exchange, (given, final)) =
  readIORef (exPhase exchange) >>= \case
    Unanswered -> ioError (userError "the body of a response is sent before its head")
    Ended -> ioError (userError "the response has ended")
    Pending status headers len -> do
      let mode = modeOf exchange status len
      when (isUnframed mode) (writeIORef (exKeep exchange) False)
      when (closesAfter headers) (writeIORef (exKeep exchange) False)
      date <- currentDate (exPulse exchange)
      -- A server that is shutting down ends the connection after this response.
      draining (exPulse exchange) >>= \shuttingDown -> when shuttingDown (writeIORef (exKeep exchange) False)
      keep <- readIORef (exKeep exchange)
      body <- bodyOf exchange mode given final
      pure (outcome (Just (headOutput exchange date status headers len keep)) body)
    Sending mode -> outcome Nothing <$> bodyOf exchange mode given final
    SendingFixed remaining -> outcome Nothing <$> fixedBodyOf exchange remaining given final
  where
    isUnframed = \case Unframed -> True; _ -> False
    outcome leading (leadingBody, pieces) =
      let out = fromMaybe mempty leading <> leadingBody
       in if outputLength out == 0
            then Left (exSocket exchange, pieces)
            else Right (newRecord ["loans", "render"] (exLoans exchange) (render out pieces))
    render out pieces buffer = do
      when (outputCopied out > bufferCapacity buffer) $ ioError (userError "the head of the response doesn't fit a buffer")
      parts <- writeOutput (bufferMemory buffer) out
      pure (exSocket exchange, map (\case Left copied -> copied; Right (UncopiedBytes uncopied) -> uncopied) parts <> pieces)

-- | How the body of a response to the request goes out.
modeOf :: Exchange -> Status -> Length -> Mode
modeOf exchange (Status code) = \case
  _ | exHead exchange || bodyless code -> Bodyless
  Known size -> Fixed size
  Unknown | exHttp11 exchange -> Chunking
  Unknown -> Unframed

-- | A response that has no body, whatever it says.
bodyless :: Int -> Bool
bodyless code = code < 200 || code == 204 || code == 304

-- | The framing for this write. The send loop drops empty pieces, and an
-- empty chunk here is skipped unless it ends the response.
bodyOf :: Exchange -> Mode -> [ByteString] -> Bool -> IO (Output, [ByteString])
bodyOf exchange mode given final = case mode of
  Bodyless -> framed mempty []
  Fixed declared -> fixedBodyOf exchange declared given final
  Unframed -> framed mempty given
  Chunking
    | size > 0 -> framed (chunkSize size) (given <> [if final then "\r\n0\r\n\r\n" else "\r\n"])
    | final -> framed mempty ["0\r\n\r\n"]
    | otherwise -> framed mempty []
  where
    size = sum (map B.length given)
    framed prefix pieces =
      (prefix, pieces) <$ writeIORef (exPhase exchange) (if final then Ended else Sending mode)

-- | Checks the fixed framing before writing. A short final body closes the
-- connection so that the next response cannot be mistaken for its remainder.
fixedBodyOf :: Exchange -> Int -> [ByteString] -> Bool -> IO (Output, [ByteString])
fixedBodyOf exchange remaining given final = do
  let size = sum (map B.length given)
      remaining' = remaining - size
  when (size > remaining) $ ioError (userError "the body of a response is longer than its declared length")
  when (final && remaining' /= 0) $ writeIORef (exKeep exchange) False
  writeIORef (exPhase exchange) (if final then Ended else SendingFixed remaining')
  pure (mempty, given)

-- | The line that gives the size of a chunk.
chunkSize :: Int -> Output
chunkSize size = outputs word8 (hex size) <> bytes "\r\n"
  where
    hex :: Int -> [Word8]
    hex n = reverse (digits n)
    digits n
      | n < 16 = [digit n]
      | otherwise = digit (n `mod` 16) : digits (n `div` 16)
    digit d = fromIntegral (if d < 10 then 48 + d else 87 + d)

-- | Whether the response's own header fields say that the connection ends.
closesAfter :: ResponseHeaders -> Bool
closesAfter headers = or [("close" :: ByteString) `elem` commaList value | (name, value) <- headers, equalsLower "connection" name]

-- | The status line, the fields of the handler, those that the server adds, and the empty line.
headOutput :: Exchange -> ByteString -> Status -> ResponseHeaders -> Length -> Bool -> Output
headOutput exchange date (Status code) headers len keep =
  bytes (if exHttp11 exchange then "HTTP/1.1 " else "HTTP/1.0 ")
    <> bytes (statusText code)
    <> foldMap fieldLine headers
    <> unlessGiven "date" (bytes "Date: " <> bytes date <> bytes "\r\n")
    <> unlessGiven "server" (bytes "Server: fulmen\r\n")
    <> framing
    <> ifNot (keep || closesAfter headers) (bytes "Connection: close\r\n")
    <> bytes "\r\n"
  where
    fieldLine (name, value) = bytes (clean name) <> bytes ": " <> bytes (clean value) <> bytes "\r\n"
    given name = any (equalsLower name . fst) headers
    unlessGiven name = ifNot (given name)
    ifNot condition out = if condition then mempty else out
    -- A response to HEAD has the framing fields that the handler gave and no others: a length that the server stated might
    -- not be the length of the body of a GET.
    framing
      | bodyless code || exHead exchange = mempty
      | otherwise = case len of
          Known size -> unlessGiven "content-length" (bytes "Content-Length: " <> bytes (C.pack (show size)) <> bytes "\r\n")
          Unknown | exHttp11 exchange -> unlessGiven "transfer-encoding" (bytes "Transfer-Encoding: chunked\r\n")
          Unknown -> mempty

-- | A name or a value that can't begin another line: a carriage return, a line feed and a NUL are spaces.
clean :: ByteString -> ByteString
clean text = if B.any bad text then B.map (\byte -> if bad byte then 32 else byte) text else text
  where
    bad byte = byte == 0 || byte == 10 || byte == 13

-- | The answer to a request that can't be served, which ends the connection: the pieces of its response, which are
-- all as they are but the date.
refusal :: Pulse -> Int -> IO [ByteString]
refusal date code = (\text -> ["HTTP/1.1 ", statusText code, "Date: ", text, "\r\nServer: fulmen\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"]) <$> currentDate date

-- | The status code and its reason phrase, with the end of the line.
statusText :: Int -> ByteString
statusText = \case
  100 -> "100 Continue\r\n"
  101 -> "101 Switching Protocols\r\n"
  200 -> "200 OK\r\n"
  201 -> "201 Created\r\n"
  202 -> "202 Accepted\r\n"
  204 -> "204 No Content\r\n"
  206 -> "206 Partial Content\r\n"
  301 -> "301 Moved Permanently\r\n"
  302 -> "302 Found\r\n"
  303 -> "303 See Other\r\n"
  304 -> "304 Not Modified\r\n"
  307 -> "307 Temporary Redirect\r\n"
  308 -> "308 Permanent Redirect\r\n"
  400 -> "400 Bad Request\r\n"
  401 -> "401 Unauthorized\r\n"
  403 -> "403 Forbidden\r\n"
  404 -> "404 Not Found\r\n"
  405 -> "405 Method Not Allowed\r\n"
  406 -> "406 Not Acceptable\r\n"
  408 -> "408 Request Timeout\r\n"
  409 -> "409 Conflict\r\n"
  410 -> "410 Gone\r\n"
  412 -> "412 Precondition Failed\r\n"
  413 -> "413 Content Too Large\r\n"
  414 -> "414 URI Too Long\r\n"
  415 -> "415 Unsupported Media Type\r\n"
  416 -> "416 Range Not Satisfiable\r\n"
  422 -> "422 Unprocessable Content\r\n"
  429 -> "429 Too Many Requests\r\n"
  431 -> "431 Request Header Fields Too Large\r\n"
  500 -> "500 Internal Server Error\r\n"
  501 -> "501 Not Implemented\r\n"
  502 -> "502 Bad Gateway\r\n"
  503 -> "503 Service Unavailable\r\n"
  504 -> "504 Gateway Timeout\r\n"
  505 -> "505 HTTP Version Not Supported\r\n"
  other -> C.pack (show other) <> " \r\n"
