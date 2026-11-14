{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE PatternSynonyms #-}
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

import Control.Arrow (ArrowChoice, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
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
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, iterateA, iterateWith)
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
data Marks = Marks
  { markStart :: !Int,
    markEnd :: !Int,
    markRemaining :: !Int
  }

-- | The client ended the connection before the body was complete.
data ConnectionLost = ConnectionLost
  deriving (Show)

instance Exception ConnectionLost where
  displayException ConnectionLost = "the client closed the connection inside the body of its request"

-- | What reading a body comes to once the bytes in hand are looked at: a receive, the end of the body, or a piece.
type Reading = Step ((Socket, Room), BodyState) (Either BodyState (Maybe ByteString))

instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => Stream arr Body where
  next = iterateWith (now advance) receiving >>> (ending ||| returnA)
    where
      receiving = first receive >>> now arrived
      ending = now (\state -> bodyLoan state <$ keepFollowing state) >>> release >>^ const Nothing
  {-# INLINE next #-}

-- | The next piece of the body, from the bytes in hand if there are any, and else the socket and the room to receive
-- them into.
advance :: Body -> IO Reading
advance = \case
  NoBody -> pure (Finish (Right Nothing))
  ChunkedBody -> Finish (Right Nothing) <$ notImplementedIO "chunked request bodies"
  Sized state -> do
    marks@Marks {markStart, markEnd, markRemaining} <- readIORef (bodyMarks state)
    if
      | markRemaining == 0 -> pure (Finish (Left state))
      | markEnd > markStart -> do
          let size = min markRemaining (markEnd - markStart)
          writeIORef (bodyMarks state) marks {markStart = markStart + size, markRemaining = markRemaining - size}
          pure (Finish (Right (Just (BSI.BS (bufferMemory (bodyBuffer state) `plusForeignPtr` markStart) size))))
      | otherwise ->
          let room = Room (bodyBuffer state) 0 (min (bufferCapacity (bodyBuffer state)) markRemaining)
           in pure (Again ((bodySocket state, room), state))

-- | The bytes that were received: they are the bytes in hand, which are the first of the rest of the body.
arrived :: (Int, BodyState) -> IO Body
arrived (count, state) = do
  when (count <= 0) (throwIO ConnectionLost)
  marks <- readIORef (bodyMarks state)
  writeIORef (bodyMarks state) $! marks {markStart = 0, markEnd = count}
  pure (Sized state)

-- | The body has ended. What follows it in the buffer is kept for the next request, once.
keepFollowing :: BodyState -> IO ()
keepFollowing state = do
  marks@Marks {markStart, markEnd} <- readIORef (bodyMarks state)
  when (markEnd > markStart) $ do
    -- Copied now, while the bytes are there: a lazy copy would be made after the buffer was used for something else.
    writeIORef (bodyCarry state) $! B.copy (BSI.BS (bufferMemory (bodyBuffer state) `plusForeignPtr` markStart) (markEnd - markStart))
    writeIORef (bodyMarks state) marks {markStart = markEnd}

-- | Reads what is left of the body and drops it, so that the next request on the connection begins where it should:
-- whether it did, which it doesn't if the body is longer than the server will read for a client that doesn't use it,
-- or can't be read.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => arr Body Bool
drain = start ^>> (returnA ||| iterateA ((fst &&& id) ^>> first next >>^ afterPiece))
  where
    start ChunkedBody = Left False
    start body = Right (body, 0 :: Int)
    afterPiece (piece, (body, total)) = case piece of
      Nothing -> Finish True
      Just view'
        | total + B.length view' > drainLimit -> Finish False
        | otherwise -> Again (body, total + B.length view')

-- | The most that is read of a body that the handler didn't read.
drainLimit :: Int
drainLimit = 262144
