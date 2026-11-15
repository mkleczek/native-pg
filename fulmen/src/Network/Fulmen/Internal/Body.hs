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

import Control.Arrow (Arrow, ArrowChoice, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception (..), throwIO)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import Data.IORef (IORef, readIORef, writeIORef)
import Foreign.ForeignPtr (plusForeignPtr)
import Network.Socket (Socket)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes
  ( Annotate (named),
    Block,
    Effect,
    GiveWay,
    Label (..),
    Link (..),
    Room (..),
    Stream (..),
    giving,
    taking,
  )
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, decide, iterateWith, step, tap)
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

instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => Stream arr Body where
  next =
    named (Label "next" "Gives the next piece of the request body, from the bytes in hand or else from the socket.") $
      iterateWith (step "look at bytes in hand" advance `giving` "bytes in hand?") (taking "no" receiving)
        `giving` "body ended?"
        >>> (taking "yes" (ending Nothing) ||| taking "no" returnA)
    where
      receiving = first receive >>> step "record arrival" arrived
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
      | otherwise -> pure (Again ((bodySocket state, receiveRoom state markRemaining), state))

-- | The room that the rest of the body is received into: the buffer, or if the rest is less than that, as much as it.
receiveRoom :: BodyState -> Int -> Room
receiveRoom state remaining = Room (bodyBuffer state) 0 (min (bufferCapacity (bodyBuffer state)) remaining)

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

-- | The body has ended: what follows it is kept for the next request, and the loan of the buffer is given back. It gives
-- the result.
ending :: (Arrow arr, Effect arr, Annotate arr) => b -> arr BodyState b
ending result = tap (step "keep carry-over" keepFollowing) >>> bodyLoan ^>> release >>^ const result

-- | Reads what is left of the body and drops it, so that the next request on the connection begins where it should:
-- whether it did, which it doesn't if the body is longer than the server will read for a client that doesn't use it,
-- or can't be read.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => arr Body Bool
drain =
  named (Label "drain" "Reads what is left of the request body and drops it, and says whether that could be done.") $
    decide "chunked?" start
      >>> ( taking "yes" returnA
              ||| taking
                "no"
                ( iterateWith dropping (taking "no" receiving) `giving` "body ended?"
                    >>> (taking "yes" (ending True) ||| taking "no" returnA)
                )
          )
  where
    start ChunkedBody = Left False
    start body = Right (body, 0 :: Int)
    -- Every way round the loop receives, and so waits: what is in hand is dropped without a wait, once for each receive.
    dropping = step "drop bytes in hand" (uncurry dropInHand) `giving` "all dropped?"
    receiving = first receive >>> step "record arrival" (\(count, (state, total)) -> (,total) <$> arrived (count, state))

-- | What dropping the bytes in hand comes to: a receive for the rest, which the drain goes on with, or the end of it, with
-- the state of the body if the body has ended, and else whether the drain could be done.
type Dropped = Step ((Socket, Room), (BodyState, Int)) (Either BodyState Bool)

-- | Drops the bytes in hand that belong to the body, and counts them towards the total. The drain is done if the body is
-- not there, or is over the limit, and the body has ended if all of it was in hand; else the rest is received.
dropInHand :: Body -> Int -> IO Dropped
dropInHand body total = case body of
  NoBody -> pure (Finish (Right True))
  ChunkedBody -> pure (Finish (Right False))
  Sized state -> do
    marks@Marks {markStart, markEnd, markRemaining} <- readIORef (bodyMarks state)
    let size = min markRemaining (markEnd - markStart)
        remaining = markRemaining - size
    writeIORef (bodyMarks state) marks {markStart = markStart + size, markRemaining = remaining}
    pure $
      if
        | total + size > drainLimit -> Finish (Right False)
        | remaining == 0 -> Finish (Left state)
        | otherwise -> Again ((bodySocket state, receiveRoom state remaining), (state, total + size))

-- | The most that is read of a body that the handler didn't read.
drainLimit :: Int
drainLimit = 262144
