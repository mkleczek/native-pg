{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The body of a request, as the views of the request's primary buffer that the handler reads one at a time, and
-- what the server does with a body that was not read.
--
-- The bytes that came with the head are the first of the body, and the rest is received into the buffer, a piece at a
-- time, never beyond the end of the body. When the body ends, what the buffer holds after it, the start of the next
-- request on the connection, is kept for the next request, and the primary buffer is given back to the request.
module Network.Fulmen.Internal.Body
  ( Body (..),
    BodyState (..),
    Marks (..),
    ConnectionLost (..),
    drain,
  )
where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (^>>), (|||))
import Control.Exception (Exception (..), throwIO)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import Data.IORef (IORef, readIORef, writeIORef)
import Foreign.ForeignPtr (plusForeignPtr)
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes (Block, Effect (..), GiveWay, Link (..), Room (..), Stream (..))
import Network.Streaming.Loans (Loan, release)
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.NotImplemented (notImplementedIO)

-- | A request body.
data Body
  = -- | A request that has none.
    NoBody
  | -- | A body of the length that the head declared.
    Sized !BodyState
  | -- | A body in chunks, which reading fails on until the server decodes them.
    ChunkedBody

-- | What reading a sized body works with: the connection's socket, the buffer that the bytes are in and the loan of it
-- that the body gives back when it ends, where the bytes that follow the body are kept, and where the reading is.
data BodyState = BodyState
  { bodySocket :: !Socket,
    bodyBuffer :: !Buffer,
    bodyLoan :: !Loan,
    bodyCarry :: !(IORef ByteString),
    bodyMarks :: !(IORef Marks)
  }

-- | Where the bytes in hand start and end in the buffer, and how many bytes of the body have not been handed out.
data Marks = Marks !Int !Int !Int

-- | The client ended the connection before the body was complete.
data ConnectionLost = ConnectionLost
  deriving (Show)

instance Exception ConnectionLost where
  displayException ConnectionLost = "the client closed the connection inside the body of its request"

-- | What reading a body comes to once the bytes in hand are looked at: a receive, the end of the body, or a piece.
type Reading = Either (BodyState, Room) (Either BodyState (Maybe ByteString))

instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => Stream arr Body where
  next = loop
    where
      loop = now advance >>> (receiving ||| (ending ||| returnA))
      receiving =
        (\(state, room) -> ((bodySocket state, room), state))
          ^>> first receive
          >>> now arrived
          >>> loop
      ending = now (\state -> bodyLoan state <$ keepFollowing state) >>> release >>> arr (const Nothing)
  {-# INLINE next #-}

-- | The next piece of the body, from the bytes in hand if there are any, and else what to receive them into.
advance :: Body -> IO Reading
advance = \case
  NoBody -> pure (Right (Right Nothing))
  ChunkedBody -> Right (Right Nothing) <$ notImplementedIO "chunked request bodies"
  Sized state -> do
    Marks start end remaining <- readIORef (bodyMarks state)
    if
      | remaining == 0 -> pure (Right (Left state))
      | end > start -> do
          let size = min remaining (end - start)
          writeIORef (bodyMarks state) (Marks (start + size) end (remaining - size))
          pure (Right (Right (Just (BSI.BS (bufferMemory (bodyBuffer state) `plusForeignPtr` start) size))))
      | otherwise -> pure (Left (state, Room (bodyBuffer state) 0 (min (bufferCapacity (bodyBuffer state)) remaining)))

-- | The bytes that were received: they are the bytes in hand, which are the first of the rest of the body.
arrived :: (Int, BodyState) -> IO Body
arrived (count, state) = do
  when (count <= 0) (throwIO ConnectionLost)
  Marks _ _ remaining <- readIORef (bodyMarks state)
  writeIORef (bodyMarks state) (Marks 0 count remaining)
  pure (Sized state)

-- | The body has ended. What follows it in the buffer is kept for the next request, once.
keepFollowing :: BodyState -> IO ()
keepFollowing state = do
  Marks start end remaining <- readIORef (bodyMarks state)
  when (end > start) $ do
    -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
    writeIORef (bodyCarry state) $! B.copy (BSI.BS (bufferMemory (bodyBuffer state) `plusForeignPtr` start) (end - start))
    writeIORef (bodyMarks state) (Marks end end remaining)

-- | Reads what is left of the body and drops it, so that the next request on the connection begins where it should:
-- whether it did, which it doesn't if the body is longer than the server will read for a client that doesn't use it,
-- or can't be read.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => arr Body Bool
drain = (\body -> case body of ChunkedBody -> Left False; _ -> Right (body, 0 :: Int)) ^>> (returnA ||| loop)
  where
    loop =
      (\(body, total) -> (body, (body, total)))
        ^>> first next
        >>> ( \(piece, (body, total)) -> case piece of
                Nothing -> Left True
                Just view'
                  | total + B.length view' > drainLimit -> Left False
                  | otherwise -> Right (body, total + B.length view')
            )
        ^>> (returnA ||| loop)

-- | The most that is read of a body that the handler didn't read.
drainLimit :: Int
drainLimit = 262144
