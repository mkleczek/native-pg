{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | The classes of the design: a link whose calls block in the arrow that runs
-- them, what an arrow can do for a link (run an action that never waits, give
-- way to the other parts, and block until an event fires), and how directions
-- exchange values. Two arrows provide them, "Network.Directions.Threads", a
-- thread for each concurrent part, and "Network.Directions.Loop", every part on
-- one thread; a link's instance is written once for any arrow that does.
module Network.Directions.Classes
  ( Link (..),
    Concurrent (..),
    Effect (..),
    GiveWay (..),
    Block (..),
    Event (..),
    Events (..),
    Exchange (..),
    Remote (..),
    Giver (..),
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
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Streaming.Link as S
import Network.Streaming.Watch (Watch)
import System.Posix.Types (Fd (..))

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
-- A ticket is made when the program runs, and passed through arrows like any
-- value, so a part can make one for each thing it waits for. An arrow whose
-- parts run on one core, as "Network.Directions.Delimited" does, makes a ticket
-- belong to that core in its type: it can be given and waited for by the parts
-- of that core only.
class Exchange arr where
  data Ticket arr a

  newTicket :: arr () (Ticket arr a)

  -- | Gives the ticket its value. A ticket is given one once.
  fulfil :: arr (Ticket arr a, a) ()

  -- | Waits until the ticket has its value, and gives it.
  await :: arr (Ticket arr a) a

-- | A ticket that the parts of other cores can give a value, for a part that
-- waits for it on its own core: one end to wait at, which belongs to that core
-- as a 'Ticket' does, and one to give at, which doesn't, so that shared
-- structures can hold it. A pool of connections keeps the givers of those that
-- wait for one.
class Remote arr where
  data Awaiting arr a

  -- | A pair of ends of a new ticket, made on this core.
  newRemote :: arr () (Awaiting arr a, Giver a)

  -- | Waits until the ticket is given its value, and gives it. Only on the core
  -- that made it, which its type says.
  awaitRemote :: arr (Awaiting arr a) a

  -- | Gives the value to the part that waits at the other end, from any core,
  -- and says whether it did: it didn't if the waiter has gone, and the value
  -- is the giver's again.
  give :: arr (Giver a, a) Bool

  -- | The waiter stops wanting the value, before it waits for it: a later
  -- 'give' says it didn't. If the value was given already, it is the waiter's,
  -- and this gives it back.
  abandon :: arr (Awaiting arr a) (Maybe a)

-- | The giving end of a remote ticket: it says whether the value was given.
-- It mentions no core, so it can be sent to others and kept in structures that
-- they share.
newtype Giver a = Giver (a -> IO Bool)

-- | Two arrows that run at once on the same input, as '&&&' runs them one
-- after the other: the result is both outputs, once both have ended.
class Concurrent arr where
  concurrently :: arr a b -> arr a c -> arr a (b, c)

-- | An action that never waits, as an arrow.
class Effect arr where
  now :: (a -> IO b) -> arr a b

-- | A point where a part gives way to the others, if it has used its turn:
-- the arrows that run every part on one thread count the points a part passes,
-- and once it has passed enough, it lets the others go on before it. Only
-- where a part does I/O, in a link's receive and send, is there one: an action
-- that never waits doesn't give way. An arrow that doesn't need it, because
-- something else preempts its parts, as GHC's scheduler does threads, makes it
-- the arrow that does nothing.
class GiveWay arr where
  giveWay :: arr a a

-- | Waiting until an event fires.
class Block arr where
  block :: arr Event ()

-- | What a part can wait for: a descriptor that becomes readable or writable,
-- which an arrow can wait for directly, or any other event, by the
-- registration of its watch, which the wait releases.
data Event = Readable !Fd | Writable !Fd | Watched (IO Watch)

-- | The events that a link's two halves wait for.
class Events link where
  readable :: link -> IO Event
  writable :: link -> IO Event

instance Events Socket where
  readable sock = Readable . Fd <$> Socket.unsafeFdSocket sock
  writable sock = Writable . Fd <$> Socket.unsafeFdSocket sock

-- | Ends a link's output, which streaming-io's link can't do yet.
class HalfClose link where
  halfClose :: link -> IO ()

instance HalfClose Socket where
  halfClose sock = Socket.shutdown sock Socket.ShutdownSend

-- | A socket, in any arrow that can run its tries, block, and give way.
instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr) => Link arr Socket where
  receive = receiveTrying
  sendAll = sendAllTrying
  shutdownSend sock = now (\() -> halfClose sock)
  close sock = now (\() -> Socket.close sock)

-- | A receive from streaming-io's link: a point to give way at, a try, and
-- while nothing is ready, a block until the link is readable, and the try
-- again. The part gives way before it tries, so that one whose link always has
-- bytes ready still does, once for each receive.
receiveTrying :: (S.Link link, Events link, ArrowChoice arr, Effect arr, GiveWay arr, Block arr) => link -> arr Room Int
receiveTrying link = giveWay >>> loop
  where
    loop = now attempt >>> ((first block >>> snd ^>> loop) ||| returnA)
    attempt room@(Room memory size) =
      withForeignPtr memory (\pointer -> S.receiveReady link pointer size) >>= \case
        Just count -> pure (Right count)
        Nothing -> (\event -> Left (event, room)) <$> readable link
{-# INLINABLE receiveTrying #-}

-- | A send to streaming-io's link: a point to give way at, then it sends what
-- the link takes and writes it, and while some is left, blocks until the link
-- is writable, and goes on.
sendAllTrying :: (S.Link link, Events link, ArrowChoice arr, Effect arr, GiveWay arr, Block arr) => link -> arr [ByteString] ()
sendAllTrying link = giveWay >>> loop
  where
    loop = filter (not . BS.null) ^>> now attempt >>> ((first block >>> snd ^>> loop) ||| returnA)
    attempt pieces = case pieces of
      [] -> S.flushReady link >>= \flushed -> if flushed then pure (Right ()) else blocked []
      _ -> do
        sent <- S.sendReady link (take 16 pieces)
        case dropSent sent pieces of
          [] -> attempt []
          rest -> blocked rest
    blocked rest = (\event -> Left (event, rest)) <$> writable link
{-# INLINABLE sendAllTrying #-}

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
