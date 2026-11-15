{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeApplications #-}

-- | LISTEN and NOTIFY: a connection that listens, and the notifications it is sent.
--
-- A connection that listens is one of its own, and is used as any other: it runs transactions too, for example to
-- NOTIFY. The server sends it a notification whenever it has one, so one can arrive during a statement of a transaction,
-- among that statement's replies, or after them. The transaction keeps it, with the bytes that follow its last reply, and
-- 'notification' delivers what was kept in the order it came, before it waits for the next.
--
-- Listening reads in a buffer that the connection has for it, made the first time it listens, since 'listen' and
-- 'notification' are given the connection alone, and no loans of a request. It is 16 KiB, which holds the largest
-- notification the server sends (its payload is shorter than 8000 bytes) whole.
--
-- A wait for a notification owes nothing, so a cancellation there leaves the connection usable, and a notification that
-- comes later is still delivered. A cancellation that reaches it while a message is half read doesn't: what was read of
-- the message is gone, so the connection is broken. 'listen' runs a statement, which owes its reply, so a cancellation
-- there breaks the connection, as it does for a statement of a transaction.
module Database.PostgreSQL.Notifications
  ( Notification (..),
    listen,
    notification,
  )
where

import Control.Arrow (arr, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (IOException, SomeException, displayException, fromException, throwIO, toException, try)
import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (readIORef, writeIORef)
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.Word (Word32)
import Database.PostgreSQL.Connection.Internal
import Database.PostgreSQL.Errors (ConnectionError (..), QueryError (..), ServerError (..), StatementError (..), isConnectionFatal)
import Database.PostgreSQL.Internal.Carrier (Carrier)
import Database.PostgreSQL.Internal.Diagnostics (bodyDiagnostics)
import Database.PostgreSQL.Internal.Frontend (sendOutput)
import Database.PostgreSQL.Internal.Protocol.Codec.Cursor
  ( At (..),
    Cases (..),
    CursorError (..),
    FramedMessage,
    cases,
    cstring,
    fixed,
    message,
    messages,
    remaining,
  )
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import qualified Database.PostgreSQL.Internal.Protocol.Messages as Wire
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes (Effect (..), Exit (..), Link (..), Runtime, Scope (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, done, iterateA, tap)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Reader (Reader, mark, markEnd, markLimit, markStart, newReader)
import Network.Streaming.With (With (..), asks, withOnly)

-- | A notification: the backend that sent it, the channel, and the payload.
data Notification = Notification
  { notificationProcessId :: Int32,
    notificationChannel :: ByteString,
    notificationPayload :: ByteString
  }
  deriving (Eq, Show)

-- | Listens on the channels, with one LISTEN for each, which the server answers before this returns. It fails with the
-- 'QueryError' of a channel that the server rejects, and the connection is usable then. A channel that is already
-- listened on is listened on still.
listen :: (Runtime arr) => [ByteString] -> arr Connection ()
listen [] = done
listen channels =
  scope opening (leaving True) (fst ^>> exchange) >>> now (either throwIO pure)
  where
    text = BS.intercalate ";" ["LISTEN " <> quoted channel | channel <- channels]
    output = Wire.serialize (Wire.Query text)
    -- The statement is sent, and its answer is read to the ReadyForQuery that ends it. A notification that comes
    -- meanwhile is kept, and a failure of the statement is the answer's, as a value, since the connection is as it was.
    exchange = tap sending >>> tap seeding >>> ((\listener -> (listener, Nothing)) ^>> iterateA reply)
    sending = (\Listener {listenerSession, listenerBuffer} -> (sessionCarrier listenerSession, listenerBuffer, output)) ^>> sendOutput
    reply = alongside ((listenerCursor . fst) ^>> incoming) >>> now answered
    answered (arrived, (listener, failure)) = case arrived of
      Notified body -> Again (listener, failure) <$ keepNotification (listenerSession listener) body
      Failure body -> (\found -> Again (listener, Just (fromMaybe found failure))) <$> failureOf body
      Ready _ -> pure (Finish (maybe (Right ()) (Left . QueryError text . ServerFailed) failure))
      _ -> pure (Again (listener, failure))

-- | The channel as an identifier of SQL.
quoted :: ByteString -> ByteString
quoted channel = "\"" <> BS.intercalate "\"\"" (BS.split 0x22 channel) <> "\""

-- | The next notification, waiting for it: the ones that came while statements ran are kept, and come first, in order.
-- If the server ends the session meanwhile, it fails with 'ConnectionTerminated', and if the connection breaks, with
-- 'ConnectionBroken'.
notification :: (Runtime arr) => arr Connection Notification
notification = scope opening (leaving False) (fst ^>> (tap seeding >>> tryA waiting >>> now (either (throwIO . brokenBy) pure)))

-- | A notification, a round at a time: the oldest one that was kept, or else the next message of the server, waited for
-- until the connection has input, which is not read until then.
waiting :: (Runtime arr) => arr Listener Notification
waiting = iterateA delivery
  where
    delivery =
      alongside (now (takeNotification . listenerSession))
        >>> ((\(kept, listener) -> maybe (Left listener) (Right . (listener,)) kept) ^>> (reading ||| now delivered))
    delivered (_, body) = Finish <$> decoded body
    reading =
      alongside (now frameInHand)
        >>> ((\(ready, listener) -> if ready then Right listener else Left listener) ^>> (awaitingInput ||| returnA))
        >>> (alongside (listenerCursor ^>> incoming) >>> now interpreted)
    -- Nothing is read until the connection has input, so that the wait holds no message: a cancellation there loses
    -- nothing.
    awaitingInput = tap ((sessionCarrier . listenerSession) ^>> awaitInput)
    interpreted (arrived, listener) = case arrived of
      Notified body -> Finish <$> decoded body
      Failure body -> failureOf body >>= \failure -> throwIO (unexpected ("an error while idle: " <> displayException failure))
      Skipped -> pure (Again listener)
      _ -> throwIO (unexpected "an answer to a statement that wasn't sent")

-- | Whether the next message's frame is in hand, which is when no wait is needed to read it.
frameInHand :: Listener -> IO Bool
frameInHand Listener {listenerReader = reader} = do
  start <- mark reader markStart
  limit <- mark reader markLimit
  end <- mark reader markEnd
  pure (end - max start limit >= 5)

-- | What a connection that is idle can be told, and what a LISTEN is answered with.
type Incoming =
  '[ FramedMessage 'A' 'RemainingBytesAtom,
     FramedMessage 'E' 'RemainingBytesAtom,
     FramedMessage 'C' 'CStringAtom,
     FramedMessage 'Z' ('FixedBytesAtom 1),
     FramedMessage 'N' ('PayloadStruct '[]),
     FramedMessage 'S' ('PayloadStruct '[])
   ]

-- | What a message of the server is, as far as listening cares.
data Arrived
  = -- | A NotificationResponse: its body, copied.
    Notified !ByteString
  | -- | An ErrorResponse: its body, copied.
    Failure !ByteString
  | -- | A CommandComplete.
    Completed
  | -- | A ReadyForQuery: the byte of its status.
    Ready !Int
  | -- | A NoticeResponse or a ParameterStatus, which nothing waits for.
    Skipped

-- | The next message of the server, told apart by its tag.
incoming :: (Runtime arr) => arr (At ('PayloadChoice Incoming) Carrier) Arrived
incoming =
  withOnly $
    asks message
      >>> With
        ( cases
            ( (remaining >>^ Notified)
                :> (remaining >>^ Failure)
                :> (cstring >>^ const Completed)
                :> (fixed >>^ Ready)
                :> arr (const Skipped)
                :> arr (const Skipped)
                :> NoCases
            )
        )

-- | A connection that is claimed for one use of listening, with what it reads in: the connection's buffer for it, a
-- reader of that, and the cursor of the messages that the reader stands at.
data Listener = Listener
  { listenerSession :: !Session,
    listenerBuffer :: !Buffer,
    listenerReader :: !(Reader Carrier),
    listenerCursor :: !(At ('PayloadChoice Incoming) Carrier)
  }

-- | Claims the connection, which fails if its handle is dead, or another part uses it, or it is broken, and sets up the
-- reader. A connection that can't be set up is broken.
opening :: (Runtime arr) => arr Connection Listener
opening = now open
  where
    open connection = do
      claim connection
      let session = connectionSession connection
      set <- try (setUp session)
      case set of
        Right listener -> pure listener
        Left failure -> do
          breakSession session (displayException (failure :: SomeException))
          throwIO failure
    setUp session = do
      buffer <- listeningBuffer session
      reader <- newReader (sessionCarrier session) buffer
      pure (Listener session buffer reader (messages reader))

-- | The reader starts with the bytes that the use before left, which come after what a statement is rendered in the
-- buffer with, and so are put in it when that is sent.
seeding :: (Runtime arr) => arr Listener ()
seeding = now (\Listener {listenerSession, listenerReader} -> takeCarry listenerSession >>= \carried -> unless (BS.null carried) (seedReader listenerReader carried))

-- | The buffer that the connection listens in, which it makes the first time.
listeningBuffer :: Session -> IO Buffer
listeningBuffer session = do
  kept <- readIORef (sessionKept session)
  case keptBuffer kept of
    Just buffer -> pure buffer
    Nothing -> do
      buffer <- newBuffer (positiveInt @16384)
      writeIORef (sessionKept session) kept {keptBuffer = Just buffer}
      pure buffer

-- | Gives the connection back as the use ended. A use that is done leaves it as it is, with the bytes that the server
-- sent after the last message it read. A failure breaks it. A cancellation does if a statement was in flight, as the
-- first argument says, or if a message is half read, and else it leaves the connection as it is.
leaving :: (Runtime arr) => Bool -> arr (Listener, Exit) ()
leaving statement = now (uncurry release)
  where
    release Listener {listenerSession = session, listenerReader = reader} exit = case exit of
      Done -> keep session reader
      Cancelled'
        | statement -> breakSession session "the statement was cancelled while its answer was awaited"
        | otherwise -> midMessage reader >>= \half -> if half then breakSession session "the wait for a notification was cancelled in the middle of a message" else keep session reader
      Failed failure -> breakSession session (displayException failure)
    keep session reader = do
      came <- pastLastMessage reader
      unless (BS.null came) (leaveCarry session came)
      settle session Nothing

-- | The failure that an ErrorResponse reports, as the server's: the one that ends the session is the connection's.
failureOf :: ByteString -> IO ServerError
failureOf body = case bodyDiagnostics body of
  Left reason -> throwIO (unexpected reason)
  Right diagnostics ->
    let failure = ServerError diagnostics
     in if isConnectionFatal failure then throwIO (ConnectionTerminated failure) else pure failure

-- | What the server said that it should not have: it ends the connection.
unexpected :: String -> ConnectionError
unexpected reason = ConnectionBroken ("the server broke the protocol: " <> reason)

-- | A failure of waiting, as the connection's: the end of the stream and what doesn't parse are the connection's
-- failures, and any other is as it is.
brokenBy :: SomeException -> SomeException
brokenBy failure
  | Just (CursorError reason) <- fromException failure = toException (unexpected reason)
  | Just (ioFailure :: IOException) <- fromException failure = toException (ConnectionBroken (displayException ioFailure))
  | otherwise = failure

-- | The notification of a body, which is the process id, and the channel and the payload, as C strings.
decoded :: ByteString -> IO Notification
decoded body
  | BS.length processId == 4, not (BS.null afterChannel), not (BS.null afterPayload) =
      pure (Notification (fromIntegral (BS.foldl' (\number byte -> number * 256 + fromIntegral byte) (0 :: Word32) processId)) channel payload)
  | otherwise = throwIO (unexpected "a notification that doesn't parse")
  where
    (processId, rest) = BS.splitAt 4 body
    (channel, afterChannel) = BS.break (== 0) rest
    (payload, afterPayload) = BS.break (== 0) (BS.drop 1 afterChannel)
