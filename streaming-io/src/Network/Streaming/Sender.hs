{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Sends outputs to a link as the link takes them, gathering their pieces
-- into as few sends as it can, and says what to wait for when it can't send
-- more. It needs no helper threads: sources are pulled when they're ready,
-- and the caller waits for the link or a source on its own thread.
module Network.Streaming.Sender
  ( Part (..),
    Unsent,
    unsentEmpty,
    discardUnsent,
    OutputBuffer (..),
    newOutputBuffer,
    outputBufferFrom,
    outputBufferPerOutput,
    unsent,
    noUnsent,
    Resume (..),
    Stalls (..),
    Pushed (..),
    stopping,
    waitingStalls,
    pushWith,
  )
where

import Control.Exception (throwIO)
import Control.Monad (unless, (>=>))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import GHC.Base (IO (..), RealWorld, State#, unIO)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Socket (Socket)
import qualified Network.Streaming.Buffer as Buffer
import Network.Streaming.Link (Blocked (..), Interest (..), Link (..), StreamLengthError (..))
import Network.Streaming.Output
  ( ByteStream (..),
    Output,
    Uncopied (..),
    WriteResult (..),
    Writer,
    outputCopied,
    outputWriter,
    runWriter,
    writeOutput,
  )
import Network.Streaming.Source (AnySource (..), AnyWaitingSource (..), Pulled (..), Source (..), WaitingSource (..))
import Network.Streaming.Watch (Watch, awaitWatch)

-- | What's left of an output to send. The server may answer the first
-- messages of an output before it has read the rest, and stop reading until
-- its answers are read. So output can be sent as the answers are read.
newtype Unsent = Unsent [Part]

unsentEmpty :: Unsent -> Bool
unsentEmpty (Unsent parts) = null parts

-- | Gives up what's left. Nothing runs on its behalf, so this drops it and
-- stops nothing: sources are only pulled by 'pushWith', and the caller's scope
-- ends them.
discardUnsent :: Unsent -> IO ()
discardUnsent _ = pure ()

-- | Bytes, or a stream with how many bytes it has given.
data Part
  = PartBytes ByteString
  | PartStream ByteStream {-# UNPACK #-} !Int
  | -- | Output rendering on in the memory from an offset, past the bytes
    -- before the part that was sent from where it is.
    PartOutput (ForeignPtr Word8) {-# UNPACK #-} !Int {-# UNPACK #-} !Int Writer
  | -- | Output that ran out of room, to go on in the memory once it is empty.
    PartOutputEmpty (ForeignPtr Word8) {-# UNPACK #-} !Int Writer
  | -- | A source to be pulled, with the bytes it declares and how many it has
    -- given.
    PartSource {-# UNPACK #-} !Int !AnySource {-# UNPACK #-} !Int
  | -- | A source to be waited for in place, with the bytes it declares and how
    -- many it has given.
    PartWaiting {-# UNPACK #-} !Int !AnyWaitingSource {-# UNPACK #-} !Int
  | -- | A chunk of a source that the link hasn't taken all of, with the part
    -- that pulls the source on, whose given bytes include the chunk. The source
    -- isn't pulled until the chunk is accepted, which ends its validity.
    PartSourceHeld !ByteString !Part

-- | Memory that outputs are written to, reused once what was written to it
-- is sent; or new memory for each output.
data OutputBuffer
  = OutputBuffer (ForeignPtr Word8) Int Bool
  | PerOutputBuffer Int

newOutputBuffer :: Int -> IO OutputBuffer
newOutputBuffer size = (\memory -> OutputBuffer memory size False) <$> mallocPlainForeignPtrBytes size

-- | The caller must keep the loan exclusive until all output has been sent.
outputBufferFrom :: Buffer.Buffer -> OutputBuffer
outputBufferFrom buffer = OutputBuffer (Buffer.bufferMemory buffer) (Buffer.bufferCapacity buffer) True

-- | New memory for each output, as large as its copied bytes, or the limit
-- if less, so that an output goes in one send unless it is longer than that.
outputBufferPerOutput :: Int -> OutputBuffer
outputBufferPerOutput = PerOutputBuffer

-- | All of the output. Caller-supplied buffers are filled incrementally and
-- reused only after their bytes have been sent. Per-output buffers are filled
-- in the same way, in memory of their own. Legacy buffers may grow to fit the
-- copied output. Long byte strings and streams retain their own storage until
-- accepted by the link.
unsent :: Maybe OutputBuffer -> Output -> IO Unsent
unsent (Just (OutputBuffer memory size True)) output
  | size < 4 = ioError (userError "output buffer capacity must be at least four bytes")
  | otherwise = pure (Unsent [PartOutput memory size 0 (outputWriter output)])
unsent (Just (PerOutputBuffer limit)) output = do
  let size = max 4 (min limit (outputCopied output))
  memory <- mallocPlainForeignPtrBytes size
  pure (Unsent [PartOutput memory size 0 (outputWriter output)])
unsent reused output = do
  memory <- case reused of
    Just (OutputBuffer memory size _) | outputCopied output <= size -> pure memory
    _ -> mallocPlainForeignPtrBytes (outputCopied output)
  parts <- writeOutput memory output
  pure (Unsent (map part parts))
  where
    part (Left copied) = PartBytes copied
    part (Right uncopied) = uncopiedPart uncopied

-- | The part that sends bytes that weren't copied.
uncopiedPart :: Uncopied -> Part
uncopiedPart = \case
  UncopiedBytes value -> PartBytes value
  UncopiedStream source -> PartStream source 0
  UncopiedSource count source -> PartSource count source 0
  UncopiedWaiting count source -> PartWaiting count source 0

-- | Renders from the offset in the memory.
renderOutput :: ForeignPtr Word8 -> Int -> Int -> Writer -> IO [Part]
renderOutput memory capacity offset writer =
  withForeignPtr memory $ \pointer -> do
    result <- runWriter writer pointer capacity offset
    let copied count parts
          | count > offset = PartBytes (BSI.BS (plusForeignPtr memory offset) (count - offset)) : parts
          | otherwise = parts
    pure $ case result of
      Finished count -> copied count []
      NeedSpace count rest -> copied count [PartOutputEmpty memory capacity rest]
      Yield count part rest -> copied count [uncopiedPart part, PartOutput memory capacity count rest]

-- | Bytes gathered for one send, and how many pieces at most. A client's
-- stream comes in chunks of up to 16 KiB from warp, and sending each on its
-- own wakes the server once per chunk.
gatherSize, gatherPieces :: Int
gatherSize = 262144
gatherPieces = 16

-- | What a push does where it can't go on: its stalls wait, and say whether to
-- try again or to stop.
data Resume = Again | Stop

data Stalls = Stalls
  { -- | The link took only part of what it was offered, or hasn't written what
    -- it took.
    stallWritable :: IO Resume,
    -- | A source has nothing ready, and what is before it is sent: waits with
    -- the registration of its watch, which it releases.
    stallSource :: IO Watch -> IO Resume
  }

-- | What a push did. Stopped gives what is left, and the registration of the
-- watch that it stopped on.
data Pushed = Pushed | Stopped !Unsent (IO Watch)

noUnsent :: Unsent
noUnsent = Unsent []

-- | The stalls of a push that never waits.
stopping :: Stalls
stopping = Stalls (pure Stop) (\_ -> pure Stop)

-- | The stalls of a push that waits for the link and for sources as long as it
-- takes, and reads nothing meanwhile.
waitingStalls :: Link link => link -> Stalls
waitingStalls link =
  Stalls
    { stallWritable = Again <$ awaitAny link (Interest False OnWritable),
      stallSource = \watching -> Again <$ awaitWatch watching
    }
{-# NOINLINE waitingStalls #-}

data Pull = Got !Pulled | Halted (IO Watch)

-- | Sends what the link takes, pulling sources as it goes, and flushes. Where
-- it can't go on it calls a stall. Each send gathers the parts up front, up to
-- 'gatherSize' bytes or 'gatherPieces' pieces. A source's chunk is gathered
-- behind the bytes before it, so that framing and the first chunk go in one
-- send, and the gather ends after it: the source isn't pulled again until the
-- link has taken the chunk. A source with nothing ready has what is gathered
-- before it sent and flushed first, so that the server starts on its statement
-- as it would without the source. A source that is waited for in place can't
-- be stopped: a stall that stops it is an error.
pushWith :: Link link => link -> Stalls -> Unsent -> IO Pushed
pushWith link stalls left = IO (pushWithIO link stalls left)
{-# INLINE pushWith #-}

-- | 'pushWith' as a function of the state token. A function that returns
-- 'IO' is compiled as a function of the token and a cast alias that every
-- caller inlines, so a specialization rule for the alias never fires and each
-- package that sends over a 'Socket' compiles a copy of its own. This
-- function has no alias, so the specialization below is the one copy that
-- they all call.
pushWithIO :: Link link => link -> Stalls -> Unsent -> State# RealWorld -> (# State# RealWorld, Pushed #)
pushWithIO link stalls (Unsent parts)
  | null parts = unIO (flushAll link stalls [])
  | otherwise = unIO (gatherFrom link stalls parts)
{-# INLINABLE pushWithIO #-}
{-# SPECIALIZE pushWithIO :: Socket -> Stalls -> Unsent -> State# RealWorld -> (# State# RealWorld, Pushed #) #-}

gatherFrom :: Link link => link -> Stalls -> [Part] -> IO Pushed
gatherFrom link stalls = go
  where
    go = gather 0 0 []
    -- Count pieces as they are gathered instead of traversing them each time.
    gather !size !pieceCount pieces remaining
      | size >= gatherSize || pieceCount >= gatherPieces = cut pieces remaining
      | otherwise = case remaining of
          [] ->
            offer pieces >>= \case
              [] -> flushAll link stalls []
              left -> stalled (map PartBytes left)
          -- Rendering in an empty buffer would overwrite gathered bytes that
          -- a partial send still owns.
          PartOutputEmpty {} : _ | not (null pieces) -> cut pieces remaining
          PartOutputEmpty memory capacity steps : others -> do
            rendered <- renderOutput memory capacity 0 steps
            gather size pieceCount pieces (rendered <> others)
          -- Past a direct part the output goes on after the gathered bytes.
          -- With none gathered, none are unsent: a partial send leaves its
          -- bytes ahead of the output.
          PartOutput memory capacity offset steps : others -> do
            rendered <- renderOutput memory capacity (if null pieces then 0 else offset) steps
            gather size pieceCount pieces (rendered <> others)
          PartSource count anySource@(AnySource source) given : others -> do
            gathered <- newIORef pieces
            pullWith source (pure . Got) (\watching again -> settleBefore link stalls gathered watching >>= maybe again (pure . Halted)) >>= \case
              Halted watch ->
                readIORef gathered >>= \left ->
                  pure (Stopped (Unsent (map PartBytes (reverse left) <> remaining)) watch)
              Got Ended -> do
                unless (given == count) (throwIO (StreamLengthError count given))
                regather size pieceCount gathered others
              Got (Pulled chunk)
                | delivered > count -> throwIO (StreamLengthError count delivered)
                | BS.null chunk -> regather size pieceCount gathered remaining
                | otherwise -> regather size pieceCount gathered (PartSourceHeld chunk (PartSource count anySource delivered) : others)
                where
                  delivered = given + BS.length chunk
          PartWaiting count anyWaiting@(AnyWaitingSource source) given : others -> do
            gathered <- newIORef pieces
            pullWaiting source (settleBefore link stalls gathered >=> maybe (pure ()) (const (ioError stopped))) >>= \case
              Nothing -> do
                unless (given == count) (throwIO (StreamLengthError count given))
                regather size pieceCount gathered others
              Just chunk
                | delivered > count -> throwIO (StreamLengthError count delivered)
                | BS.null chunk -> regather size pieceCount gathered remaining
                | otherwise -> regather size pieceCount gathered (PartSourceHeld chunk (PartWaiting count anyWaiting delivered) : others)
                where
                  delivered = given + BS.length chunk
          PartSourceHeld chunk resume : others ->
            let (prefix, suffix) = BS.splitAt (gatherSize - size) chunk
             in if BS.null suffix
                  then
                    -- Nothing may be pulled until the link has taken the
                    -- chunk, so the gather ends.
                    offer (prefix : pieces) >>= \case
                      [] -> go (resume : others)
                      left -> stalled (heldTail left resume others)
                  else cut (prefix : pieces) (PartSourceHeld suffix resume : others)
          PartBytes chunk : others
            | BS.null chunk -> gather size pieceCount pieces others
            | otherwise ->
                let (prefix, suffix) = BS.splitAt (gatherSize - size) chunk
                    rest = [PartBytes suffix | not (BS.null suffix)] <> others
                 in gather (size + BS.length prefix) (pieceCount + 1) (prefix : pieces) rest
          PartStream _ 0 : _ | not (null pieces) -> cut pieces remaining
          PartStream source given : others -> do
            chunk <- byteStreamNext source
            let delivered = given + BS.length chunk
            if
              | BS.null chunk -> do
                  unless (given == byteStreamLength source) (throwIO (StreamLengthError (byteStreamLength source) given))
                  gather size pieceCount pieces others
              | delivered > byteStreamLength source ->
                  throwIO (StreamLengthError (byteStreamLength source) delivered)
              | otherwise -> gather size pieceCount pieces (PartBytes chunk : PartStream source delivered : others)
    -- What is gathered after a pull: nothing, if its stall sent it.
    regather size pieceCount gathered rest =
      readIORef gathered >>= \case
        [] -> gather 0 0 [] rest
        pieces -> gather size pieceCount pieces rest
    stalled left =
      stallWritable stalls >>= \case
        Again -> go left
        Stop -> pure (Stopped (Unsent left) (watchWritable link))
    -- Sends the gathered pieces, which are in reverse, with something to
    -- gather after them, and gives what the link didn't take.
    offer pieces = case pieces of
      [] -> pure []
      _ -> do
        let forward = reverse pieces
        sent <- sendReady link forward
        pure (dropSent sent forward)
    -- Sends what's gathered, and gathers on if the link took it all.
    cut pieces rest =
      offer pieces >>= \case
        [] -> go rest
        left -> stalled (map PartBytes left <> rest)
    -- The last of the pieces the link didn't take is the source's chunk.
    heldTail left resume others = case left of
      [] -> others
      [chunk] -> PartSourceHeld chunk resume : others
      piece : pieces -> PartBytes piece : heldTail pieces resume others
{-# INLINE gatherFrom #-}

flushAll :: Link link => link -> Stalls -> [Part] -> IO Pushed
flushAll link stalls left =
  flushReady link >>= \case
    True -> pure Pushed
    False ->
      stallWritable stalls >>= \case
        Again -> flushAll link stalls left
        Stop -> pure (Stopped (Unsent left) (watchWritable link))
{-# INLINABLE flushAll #-}

-- | The stall of a pull of a source: sends the pieces gathered before it, which
-- are in the reference, in reverse, and flushes, and then calls the stall for
-- the source. Gives the watch that a stop was on. A stop leaves in the
-- reference what the link didn't take.
settleBefore :: Link link => link -> Stalls -> IORef [ByteString] -> IO Watch -> IO (Maybe (IO Watch))
settleBefore link stalls gathered watching = sendGathered
  where
    sendGathered =
      readIORef gathered >>= \case
        [] -> flushed
        reversed -> do
          let pieces = reverse reversed
          sent <- sendReady link pieces
          case dropSent sent pieces of
            [] -> writeIORef gathered [] *> flushed
            left -> writeIORef gathered (reverse left) *> onWritable sendGathered
    flushed =
      flushReady link >>= \case
        True ->
          stallSource stalls watching >>= \case
            Again -> pure Nothing
            Stop -> pure (Just watching)
        False -> onWritable flushed
    onWritable continue =
      stallWritable stalls >>= \case
        Again -> continue
        Stop -> pure (Just (watchWritable link))
{-# INLINABLE settleBefore #-}

-- | What a stop of a source that is waited for in place is.
stopped :: IOError
stopped = userError "a source that is waited for in place can't be stopped"

-- | What a send of the pieces left, as the pieces that remain.
dropSent :: Int -> [ByteString] -> [ByteString]
dropSent sent = \case
  chunk : others
    | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
    | otherwise -> BS.drop sent chunk : others
  [] -> []
