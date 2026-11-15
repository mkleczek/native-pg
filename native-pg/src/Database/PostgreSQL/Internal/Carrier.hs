{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE UndecidableInstances #-}

-- | What a connection sends and receives through: a plain socket, or a TLS session over one. Every call on it matches the
-- constructor once, and a plain socket's calls are the socket's own, which the instance of "Network.Streaming.Classes"
-- for a socket makes too.
module Database.PostgreSQL.Internal.Carrier
  ( Carrier (..),
    closeCarrier,
    isQuiet,
  )
where

import Control.Arrow (ArrowChoice, (>>>))
import Data.ByteString (ByteString)
import Data.Word (Word8)
import qualified Database.PostgreSQL.Internal.Socket as Probe (isQuiet)
import Database.PostgreSQL.Transport (TlsLink (..), linkReadable, linkWatchInput, linkWritable)
import Foreign.Ptr (Ptr)
import Network.Socket (Socket, unsafeFdSocket)
import qualified Network.Socket as Socket
import Network.Streaming.Classes
  ( Annotate,
    Block (..),
    Effect (..),
    Event (..),
    GiveWay,
    Label (..),
    Link (..),
    named,
    receiveFirst,
    receiveWith,
    sendAllWith,
    tryReceive,
    trySend,
  )
import Network.Streaming.Combinators (box)
import Network.Streaming.Mode (Instrumented, ModeOf)
import System.Posix.Types (Fd (..))

-- | A plain socket, or the TLS session that a TLS client made over one.
data Carrier = Plain !Socket | Secure !TlsLink

-- | A socket, or a TLS session over one, in any arrow that can run their calls, block, and give way. The calls of a socket
-- are its own, and those of a session are its link's, which never wait: while one finds nothing ready, the part blocks until
-- the socket is readable or writable.
instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => Link arr Carrier where
  receive =
    named (Label "receive" "Receives into the room, and blocks while the socket, or the TLS session over it, has no input.") $
      receiveWith (receiveOn @(ModeOf arr)) readableCarrier
  {-# INLINE receive #-}
  sendAll =
    named (Label "send" "Sends all of the pieces, and blocks while the socket takes no more.") $
      sendAllWith (sendOn @(ModeOf arr)) flushCarrier writableCarrier
  {-# INLINE sendAll #-}
  -- What the wait finds is for the receive after it to read.
  awaitInput =
    named (Label "await input" "Blocks until the socket, or the TLS session over it, has input, and takes none of it.") $
      box "watch for input" (now watchCarrier) >>> block
  {-# INLINE awaitInput #-}
  shutdownSend = box "shut down output" (now shutdownCarrier)
  {-# INLINE shutdownSend #-}
  close = box "close" (now closeCarrier)
  {-# INLINE close #-}

-- | Reads what the carrier has into the memory, without waiting: 'Nothing' if it has nothing, and 0 at the end of the stream.
receiveOn :: forall m. (Instrumented m) => Carrier -> Ptr Word8 -> Int -> IO (Maybe Int)
receiveOn carrier pointer size = case carrier of
  Plain sock -> tryReceive @m sock pointer size
  Secure link -> linkTryReceive link pointer size
{-# INLINE receiveOn #-}

-- | Sends what the carrier takes of the pieces, without waiting, and gives how many bytes it took.
sendOn :: forall m. (Instrumented m) => Carrier -> [ByteString] -> IO Int
sendOn carrier pieces = case carrier of
  Plain sock -> trySend @m sock pieces
  Secure link -> linkTrySend link pieces
{-# INLINE sendOn #-}

-- | Writes what the carrier took and has not written, without waiting: whether none is left. A socket takes everything it takes.
flushCarrier :: Carrier -> IO Bool
flushCarrier = \case
  Plain _ -> pure True
  Secure link -> linkFlush link
{-# INLINE flushCarrier #-}

-- | The events of the carrier's socket being readable, and being writable.
readableCarrier, writableCarrier :: Carrier -> IO Event
readableCarrier = \case
  Plain sock -> readable sock
  Secure link -> linkReadable link
writableCarrier = \case
  Plain sock -> writable sock
  Secure link -> linkWritable link
{-# INLINE readableCarrier #-}
{-# INLINE writableCarrier #-}

readable, writable :: Socket -> IO Event
readable sock = Readable . Fd <$> unsafeFdSocket sock
writable sock = Writable . Fd <$> unsafeFdSocket sock

-- | The event that a wait for input is for. The next receive on a socket reads before it waits, since what the wait finds
-- was not read.
watchCarrier :: Carrier -> IO Event
watchCarrier = \case
  Plain sock -> receiveFirst sock *> readable sock
  Secure link -> linkWatchInput link
{-# INLINE watchCarrier #-}

-- | Ends the carrier's output, once what was sent is written.
shutdownCarrier :: Carrier -> IO ()
shutdownCarrier = \case
  Plain sock -> Socket.shutdown sock Socket.ShutdownSend
  Secure link -> linkShutdownSend link

-- | Closes the carrier, and the socket with it.
closeCarrier :: Carrier -> IO ()
closeCarrier = \case
  Plain sock -> Socket.close sock
  Secure link -> linkEnd link

-- | Whether the carrier has nothing to read now, as a connection that waits for the next statement has: it has when the server
-- ended it, or said something that nobody asked for. A TLS session counts what it can read of what the socket has, which may
-- be records without bytes, such as TLS 1.3's session tickets, and none of the decrypted bytes.
isQuiet :: forall m. (Instrumented m) => Carrier -> IO Bool
isQuiet = \case
  Plain sock -> Probe.isQuiet @m sock
  Secure link -> not <$> linkHasInput link
