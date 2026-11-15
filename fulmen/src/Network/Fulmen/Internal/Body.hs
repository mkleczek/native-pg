{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The body of a request, as the views of the request's primary buffer that the handler reads one at a time, and
-- what the server does with a body that was not read.
--
-- The bytes that came with the head are the first of the body, and the rest is received into the buffer, a piece at a
-- time. A body of a declared length is never received beyond its end. A chunked body is decoded as it is received: the
-- data of a chunk is handed out as views of the buffer, and the lines between them, the sizes and the trailer fields,
-- are read from the bytes in hand, a line that is cut by the end of what has arrived being moved to the start of the
-- buffer, where the rest of it is received after it. When the body ends, what the buffer holds after it, the start of
-- the next request on the connection, is kept for the next request, and the primary buffer is given back to the request.
module Network.Fulmen.Internal.Body
  ( Body (..),
    BodyState (..),
    ChunkState (..),
    Decoding (..),
    Marks (..),
    ConnectionLost (..),
    BodyRefused (..),
    drain,
  )
where

import Control.Arrow (Arrow, ArrowChoice, first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Exception (Exception (..), throwIO)
import Control.Monad (when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BU
import Data.IORef (IORef, readIORef, writeIORef)
import Foreign.ForeignPtr (plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (moveBytes)
import Foreign.Ptr (plusPtr)
import Network.Fulmen.Internal.Bytes (isField)
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
    trySend,
    (-|),
    (|-),
  )
import Network.Streaming.Combinators (pattern Again, pattern Finish, Step, iterateWith, step, tap)
import Network.Streaming.Loans (Loan, release)
import Network.Streaming.Mode (Instrumented, ModeOf)

-- | A request body.
data Body
  = -- | A request that has none.
    NoBody
  | -- | A body of the length that the head declared.
    Sized !BodyState
  | -- | A body in chunks.
    ChunkedBody !ChunkState
  | -- | A body that its client waits to send until it is told to, by the @100 Continue@ that the first read of the body sends, once:
    -- whether it is still owed. The body is what it is told of.
    Awaited !(IORef Bool) !Body

-- | What reading a body works with: the connection's socket, the buffer that the bytes are in and the loan of it
-- that the body gives back when it ends, where the bytes that follow the body are kept, and where the reading is.
data BodyState = BodyState
  { bodySocket :: !Socket,
    bodyBuffer :: !Buffer,
    bodyLoan :: !Loan,
    bodyCarry :: !(IORef ByteString),
    bodyMarks :: !(IORef Marks)
  }

-- | A chunked body: what a sized body has, where the decoding is, and the limits on what is decoded.
data ChunkState = ChunkState
  { chunkBody :: !BodyState,
    chunkPhase :: !(IORef Decoding),
    -- | The bytes that a chunk-size line or a trailer line may take, which a buffer may hold at most.
    chunkLineLimit :: !Int,
    -- | The bytes that the decoded body may have, if limited.
    chunkBodyLimit :: !(Maybe Int),
    -- | The bytes that the trailer section may take.
    chunkTrailerLimit :: !Int
  }

-- | Where the decoding of a chunked body is: reading a chunk-size line, the data of a chunk, the line end that follows it,
-- the trailer section, or at its end. The numbers are the bytes of the body that have been decoded, and how many of a chunk
-- or of the trailer section there are.
data Decoding
  = AtSize !Int
  | -- | The bytes of the chunk that are left, and the bytes decoded so far.
    AtData !Int !Int
  | -- | The line end after the data of a chunk: whether its CR has been read, and the bytes decoded so far.
    AtLineEnd !Bool !Int
  | AtTrailers !Int
  | AtEnd

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

-- | The body can't be read and the request is refused with this status: 400 for a chunked body that is not one, 413 for
-- one over its limit, 431 for a line that is too long.
newtype BodyRefused = BodyRefused Int
  deriving (Show)

instance Exception BodyRefused where
  displayException (BodyRefused status) = "the body of the request is refused with " <> show status

-- | What reading a body comes to once the bytes in hand are looked at: a receive, the end of the body, or a piece.
type Reading = Step ((Socket, Room), Body) (Either BodyState (Maybe ByteString))

instance (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => Stream arr Body where
  next =
    named (Label "next" "Gives the next piece of the request body, from the bytes in hand or else from the socket.") $
      iterateWith (step "look at bytes in hand" (advance @(ModeOf arr)) -| "bytes in hand?") ("no" |- receiving)
        -| "body ended?"
        >>> ("yes" |- ending Nothing ||| "no" |- returnA)
    where
      receiving = first receive >>> step "record arrival" arrived
  {-# INLINE next #-}

-- | The next piece of the body, from the bytes in hand if there are any, and else the socket and the room to receive
-- them into.
advance :: forall m. (Instrumented m) => Body -> IO Reading
advance body = case body of
  NoBody -> pure (Finish (Right Nothing))
  Awaited owed inner -> do
    -- The first read tells the client to go on, in a send that doesn't wait: a client whose socket is full of what it has not
    -- read yet goes on after its own wait.
    due <- readIORef owed
    when due $ writeIORef owed False *> (() <$ trySend @m (socketOf inner) ["HTTP/1.1 100 Continue\r\n\r\n"])
    advance @m inner
  ChunkedBody chunks -> decode body chunks
  Sized state -> do
    marks@Marks {markStart, markEnd, markRemaining} <- readIORef (bodyMarks state)
    if
      | markRemaining == 0 -> pure (Finish (Left state))
      | markEnd > markStart -> do
          let size = min markRemaining (markEnd - markStart)
          writeIORef (bodyMarks state) marks {markStart = markStart + size, markRemaining = markRemaining - size}
          pure (Finish (Right (Just (BSI.BS (bufferMemory (bodyBuffer state) `plusForeignPtr` markStart) size))))
      | otherwise -> pure (Again ((bodySocket state, receiveRoom state markRemaining), body))

-- | The socket that the body is read from.
socketOf :: Body -> Socket
socketOf = \case
  NoBody -> error "a request that has no body has none to read from a socket"
  Sized state -> bodySocket state
  ChunkedBody ChunkState {chunkBody} -> bodySocket chunkBody
  Awaited _ inner -> socketOf inner

-- | The room that the rest of the body is received into: the buffer, or if the rest is less than that, as much as it.
receiveRoom :: BodyState -> Int -> Room
receiveRoom state remaining = Room (bodyBuffer state) 0 (min (bufferCapacity (bodyBuffer state)) remaining)

-- | The bytes that were received: a sized body has them as the bytes in hand, a chunked one after what it kept.
arrived :: (Int, Body) -> IO Body
arrived (count, body) = case body of
  NoBody -> pure body
  Awaited _ inner -> arrived (count, inner)
  Sized state -> do
    when (count <= 0) (throwIO ConnectionLost)
    marks <- readIORef (bodyMarks state)
    writeIORef (bodyMarks state) $! marks {markStart = 0, markEnd = count}
    pure body
  ChunkedBody ChunkState {chunkBody = state} -> do
    when (count <= 0) (throwIO ConnectionLost)
    marks <- readIORef (bodyMarks state)
    writeIORef (bodyMarks state) $! marks {markEnd = markEnd marks + count}
    pure body

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

-- * Decoding

-- | Decodes what is in hand of a chunked body as far as it goes, in a loop that never waits: the next piece of data, the end
-- of the body, or a receive for the bytes that the decoding needs next.
decode :: Body -> ChunkState -> IO Reading
decode body ChunkState {chunkBody = state, chunkPhase, chunkLineLimit, chunkBodyLimit, chunkTrailerLimit} = go
  where
    memory = bufferMemory (bodyBuffer state)
    capacity = bufferCapacity (bodyBuffer state)
    go = do
      phase <- readIORef chunkPhase
      marks@Marks {markStart, markEnd} <- readIORef (bodyMarks state)
      let inHand = BSI.BS (memory `plusForeignPtr` markStart) (markEnd - markStart)
          consume size = writeIORef (bodyMarks state) marks {markStart = markStart + size}
          refuse status = throwIO (BodyRefused status)
      case phase of
        AtData left total
          | markEnd == markStart -> receiving 0
          | otherwise -> do
              let size = min left (markEnd - markStart)
              consume size
              writeIORef chunkPhase $! if size == left then AtLineEnd False total else AtData (left - size) total
              pure (Finish (Right (Just (BSI.BS (memory `plusForeignPtr` markStart) size))))
        AtSize total -> case B.elemIndex 10 inHand of
          Nothing -> unfinished (B.length inHand)
          Just feed -> case sizeLine (stripped (B.take feed inHand)) of
            _ | feed > chunkLineLimit + 1 -> refuse 431
            Left status -> refuse status
            Right 0 -> consume (feed + 1) *> writeIORef chunkPhase (AtTrailers 0) *> go
            Right size
              | maybe False (\limit -> size > limit - total) chunkBodyLimit -> refuse 413
              | otherwise -> consume (feed + 1) *> writeIORef chunkPhase (AtData size (total + size)) *> go
        AtLineEnd seenCR total
          | markEnd == markStart -> receiving 0
          | otherwise -> case (BU.unsafeHead inHand, seenCR) of
              (10, _) -> consume 1 *> writeIORef chunkPhase (AtSize total) *> go
              (13, False) -> consume 1 *> writeIORef chunkPhase (AtLineEnd True total) *> go
              _ -> refuse 400
        AtTrailers size -> case B.elemIndex 10 inHand of
          Nothing -> unfinished (B.length inHand)
          Just feed ->
            let line = stripped (B.take feed inHand)
             in if
                  | feed > chunkLineLimit + 1 -> refuse 431
                  | B.null line -> consume (feed + 1) *> writeIORef chunkPhase AtEnd *> pure (Finish (Left state))
                  | size + feed + 1 > chunkTrailerLimit -> refuse 431
                  | isField line -> consume (feed + 1) *> writeIORef chunkPhase (AtTrailers (size + feed + 1)) *> go
                  | otherwise -> refuse 400
        AtEnd -> pure (Finish (Left state))
    -- A line that has not ended: the bytes of it are moved to the start of the buffer and the rest is received after them, if
    -- they leave room and are not more than a line may be.
    unfinished size
      | size > chunkLineLimit + 1 || size >= capacity = throwIO (BodyRefused 431)
      | otherwise = receiving size
    -- Receives into the buffer after the bytes that are kept, which are at its start.
    receiving kept = do
      marks@Marks {markStart} <- readIORef (bodyMarks state)
      when (markStart > 0 && kept > 0) $ withForeignPtr memory (\target -> moveBytes target (target `plusPtr` markStart) kept)
      writeIORef (bodyMarks state) $! marks {markStart = 0, markEnd = kept}
      pure (Again ((bodySocket state, Room (bodyBuffer state) kept (capacity - kept)), body))

-- | A line without its CR, which is there if the line ends with it.
stripped :: ByteString -> ByteString
stripped line = if not (B.null line) && B.last line == 13 then B.init line else line

-- | The size that a chunk-size line gives, or the status that refuses it: one or more hexadecimal digits that fit in a
-- number, and then nothing or a chunk extension, which begins with a semicolon, after any whitespace.
sizeLine :: ByteString -> Either Int Int
sizeLine line
  | B.null digits = Left 400
  | otherwise = case B.foldl' accumulate (Just 0) digits of
      Nothing -> Left 400
      Just size | extension -> Right size
      _ -> Left 400
  where
    (digits, rest) = B.span isHex line
    accumulate total byte = total >>= \n -> if n > (maxBound - value byte) `div` 16 then Nothing else Just (n * 16 + value byte)
    value byte
      | byte >= 97 = fromIntegral byte - 87
      | byte >= 65 = fromIntegral byte - 55
      | otherwise = fromIntegral byte - 48
    isHex byte = (byte >= 48 && byte <= 57) || (byte >= 65 && byte <= 70) || (byte >= 97 && byte <= 102)
    extension =
      B.null rest || case B.uncons (B.dropWhile (\byte -> byte == 32 || byte == 9) rest) of
        Just (59, _) -> B.all (\byte -> byte >= 32 && byte /= 127 || byte == 9) rest
        _ -> False

-- * Draining

-- | Reads what is left of the body and drops it, so that the next request on the connection begins where it should:
-- whether it did, which it doesn't if the body is longer than the server will read for a client that doesn't use it,
-- or can't be read.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr)) => arr Body Bool
drain =
  named (Label "drain" "Reads what is left of the request body and drops it, and says whether that could be done.") $
    (,0 :: Int)
      ^>> iterateWith dropping ("no" |- receiving)
        -| "body ended?"
      >>> ("yes" |- ending True ||| "no" |- returnA)
  where
    -- Every way round the loop receives, and so waits: what is in hand is dropped without a wait, once for each receive.
    dropping = step "drop bytes in hand" (uncurry dropInHand) -| "all dropped?"
    receiving = first receive >>> step "record arrival" (\(count, (body, total)) -> (,total) <$> arrived (count, body))

-- | What dropping the bytes in hand comes to: a receive for the rest, which the drain goes on with, or the end of it, with
-- the state of the body if the body has ended, and else whether the drain could be done.
type Dropped = Step ((Socket, Room), (Body, Int)) (Either BodyState Bool)

-- | Drops the bytes in hand that belong to the body, and counts them towards the total. The drain is done if the body is
-- not there, or is over the limit, and the body has ended if all of it was in hand; else the rest is received.
dropInHand :: Body -> Int -> IO Dropped
dropInHand body total = case body of
  NoBody -> pure (Finish (Right True))
  Awaited _ inner -> dropInHand inner total
  ChunkedBody chunks -> dropChunks body chunks total
  Sized state -> do
    marks@Marks {markStart, markEnd, markRemaining} <- readIORef (bodyMarks state)
    let size = min markRemaining (markEnd - markStart)
        remaining = markRemaining - size
    writeIORef (bodyMarks state) marks {markStart = markStart + size, markRemaining = remaining}
    pure $
      if
        | total + size > drainLimit -> Finish (Right False)
        | remaining == 0 -> Finish (Left state)
        | otherwise -> Again ((bodySocket state, receiveRoom state remaining), (body, total + size))

-- | The same for a chunked body, which is decoded as far as the bytes in hand go, in a loop that never waits.
dropChunks :: Body -> ChunkState -> Int -> IO Dropped
dropChunks body chunks = go
  where
    go total =
      decode body chunks >>= \case
        Finish (Right (Just piece))
          | total + B.length piece > drainLimit -> pure (Finish (Right False))
          | otherwise -> go (total + B.length piece)
        Finish (Right Nothing) -> pure (Finish (Right True))
        Finish (Left state) -> pure (Finish (Left state))
        Again (request, body') -> pure (Again (request, (body', total)))

-- | The most that is read of a body that the handler didn't read.
drainLimit :: Int
drainLimit = 262144
