{-# LANGUAGE TypeFamilies #-}

-- | The classes of the design: a link whose calls block in the arrow that runs
-- them, what an arrow can do for a link (run an action that never waits, and
-- block until an event fires), and how directions exchange values. Two arrows
-- provide them, "Network.Directions.Threads", a thread for each concurrent
-- part, and "Network.Directions.Loop", every part on one thread; a link's
-- instance is written once for any arrow that does.
module Network.Directions.Classes
  ( Link (..),
    Concurrent (..),
    Effect (..),
    Block (..),
    Exchange (..),
    HalfClose (..),
    receiveTrying,
    sendAllTrying,
    Room (..),
    Buffer,
    newBuffer,
    bufferSize,
    roomOf,
    view,
  )
where

import Control.Arrow (ArrowChoice, first, returnA, (>>>), (^>>), (|||))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Functor ((<&>))
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Streaming.Link as S
import Network.Streaming.Watch (Watch)

-- | A byte stream whose calls block in the arrow that runs them. How they
-- block is the arrow's.
class Link arr link where
  -- | Receives into the room: how many bytes, 0 at the end of the stream.
  receive :: link -> arr Room Int

  -- | Sends all of the pieces, and writes them.
  sendAll :: link -> arr [ByteString] ()

  -- | Ends the stream's output, once what was sent is written.
  shutdownSend :: link -> arr () ()

  close :: link -> arr () ()

-- | Values that one direction gives another, once each: the way directions
-- exchange what they know. How a ticket is kept and waited for is the arrow's.
class Exchange arr where
  data Ticket arr a

  -- | A ticket, made while the program is built.
  newTicket :: IO (Ticket arr a)

  -- | Gives the ticket its value. A ticket is given one once.
  fulfil :: Ticket arr a -> arr a ()

  -- | Waits until the ticket has its value, and gives it.
  await :: Ticket arr a -> arr () a

-- | Two arrows that run at once on the same input, as '&&&' runs them one
-- after the other: the result is both outputs, once both have ended.
class Concurrent arr where
  concurrently :: arr a b -> arr a c -> arr a (b, c)

-- | An action that never waits, as an arrow.
class Effect arr where
  now :: (a -> IO b) -> arr a b

-- | Waiting until an event fires: given the registration of its watch, which
-- it releases.
class Block arr where
  block :: arr (IO Watch) ()

-- | Ends a link's output, which streaming-io's link can't do yet.
class HalfClose link where
  halfClose :: link -> IO ()

instance HalfClose Socket where
  halfClose sock = Socket.shutdown sock Socket.ShutdownSend

-- | A socket, in any arrow that can run its tries and block.
instance (ArrowChoice arr, Effect arr, Block arr) => Link arr Socket where
  receive = receiveTrying
  sendAll = sendAllTrying
  shutdownSend sock = now (\() -> halfClose sock)
  close sock = now (\() -> Socket.close sock)

-- | A receive from streaming-io's link: a try, and while nothing is ready, a
-- block until the link is readable, and the try again.
receiveTrying :: (S.Link link, ArrowChoice arr, Effect arr, Block arr) => link -> arr Room Int
receiveTrying link = loop
  where
    loop = now attempt >>> (((S.watchReadable link,) ^>> first block >>> snd ^>> loop) ||| returnA)
    attempt room@(Room memory size) =
      withForeignPtr memory (\pointer -> S.receiveReady link pointer size) <&> maybe (Left room) Right

-- | A send to streaming-io's link: it sends what the link takes and writes it,
-- and while some is left, blocks until the link is writable, and goes on.
sendAllTrying :: (S.Link link, ArrowChoice arr, Effect arr, Block arr) => link -> arr [ByteString] ()
sendAllTrying link = loop
  where
    loop = filter (not . BS.null) ^>> now attempt >>> (((S.watchWritable link,) ^>> first block >>> snd ^>> loop) ||| returnA)
    attempt pieces = case pieces of
      [] -> S.flushReady link <&> \flushed -> if flushed then Right () else Left []
      _ -> do
        sent <- S.sendReady link (take 16 pieces)
        case dropSent sent pieces of
          [] -> attempt []
          rest -> pure (Left rest)

-- | Memory to receive into, from its start: where it is and its size.
data Room = Room !(ForeignPtr Word8) !Int

-- | Memory that a direction receives into and sends from.
data Buffer = Buffer !(ForeignPtr Word8) !Int

newBuffer :: Int -> IO Buffer
newBuffer size = (`Buffer` size) <$> mallocPlainForeignPtrBytes size

bufferSize :: Buffer -> Int
bufferSize (Buffer _ size) = size

-- | All of the buffer, to receive into.
roomOf :: Buffer -> Room
roomOf (Buffer memory size) = Room memory size

-- | The buffer's first bytes, without a copy: valid until it receives again.
view :: Buffer -> Int -> ByteString
view (Buffer memory _) = BSI.BS memory

-- | What a send of the pieces left, as the pieces that remain.
dropSent :: Int -> [ByteString] -> [ByteString]
dropSent _ [] = []
dropSent sent (piece : others)
  | sent >= BS.length piece = dropSent (sent - BS.length piece) others
  | otherwise = BS.drop sent piece : others
