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
    sendSome,
    pushOutput,
    Blocked (..),
  )
where

import Control.Exception (throwIO)
import Control.Monad (unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, plusForeignPtr, withForeignPtr)
import GHC.Base (IO (..), RealWorld, State#, unIO)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Socket (Socket)
import qualified Network.Streaming.Buffer as Buffer
import Network.Streaming.Link (Blocked (..), Link (..), StreamLengthError (..))
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
import Network.Streaming.Source (AnySource (..), Pulled (..), Source (..))

-- | What's left of an output to send. The server may answer the first
-- messages of an output before it has read the rest, and stop reading until
-- its answers are read. So output can be sent as the answers are read.
newtype Unsent = Unsent [Part]

unsentEmpty :: Unsent -> Bool
unsentEmpty (Unsent parts) = null parts

-- | Gives up what's left. Nothing runs on its behalf, so this drops it and
-- stops nothing: sources are only pulled by 'sendSome', and the caller's scope
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
  | -- | A chunk of a source that the link hasn't taken all of, with the
    -- source's declared and given bytes, the chunk included. The source isn't
    -- pulled until the chunk is accepted, which ends its validity.
    PartSourceHeld !ByteString {-# UNPACK #-} !Int !AnySource {-# UNPACK #-} !Int

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
  UncopiedBorrowedStream {} -> error "a callback source can't be sent without a helper thread; use UncopiedSource"

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

-- | Sends what the link takes without waiting for room in the socket,
-- pulling sources as it goes, and gives what's left and what to wait for.
-- Each send gathers the parts up front, up to 'gatherSize' bytes or
-- 'gatherPieces' pieces. A source's chunk is gathered behind the bytes
-- before it, so that framing and the first chunk go in one send; and the
-- gather ends after the chunk, which stays valid only until the source is
-- pulled again, so the source isn't pulled again until the link has taken all
-- of it. A source that has nothing ready cuts the gather, since waiting for
-- it must not hold the bytes before it back: they are sent, the server
-- starts on its statement, and its statement_timeout, as it would without
-- the source.
sendSome :: Link link => link -> Unsent -> IO (Unsent, Blocked)
sendSome link left = IO (sendSomeIO link left)
{-# INLINE sendSome #-}

-- | 'sendSome' as a function of the state token. A function that returns
-- 'IO' is compiled as a function of the token and a cast alias that every
-- caller inlines, so a specialization rule for the alias never fires and each
-- package that sends over a 'Socket' compiles a copy of its own. This
-- function has no alias, so the specialization below is the one copy that
-- they all call.
sendSomeIO :: Link link => link -> Unsent -> State# RealWorld -> (# State# RealWorld, (Unsent, Blocked) #)
sendSomeIO link (Unsent parts) = unIO (go parts)
  where
    go = gather 0 0 []
    -- Count pieces as they are gathered instead of traversing them each time.
    gather !size !pieceCount pieces remaining
      | size >= gatherSize || pieceCount >= gatherPieces = cut pieces remaining
      | otherwise = case remaining of
          [] ->
            offer pieces >>= \case
              [] -> pure (Unsent [], Done)
              left -> pure (Unsent (map PartBytes left), OnWritable)
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
          PartSource count anySource@(AnySource source) given : others ->
            pullReady source >>= \case
              -- Waiting for the source must not hold back what's gathered.
              Pending ->
                offer pieces >>= \case
                  [] -> pure (Unsent remaining, OnSource (watchSource source))
                  left -> pure (Unsent (map PartBytes left <> remaining), OnWritable)
              Ended -> do
                unless (given == count) (throwIO (StreamLengthError count given))
                gather size pieceCount pieces others
              Pulled chunk
                | delivered > count -> throwIO (StreamLengthError count delivered)
                | BS.null chunk -> gather size pieceCount pieces remaining
                | otherwise -> gather size pieceCount pieces (PartSourceHeld chunk count anySource delivered : others)
                where
                  delivered = given + BS.length chunk
          PartSourceHeld chunk count source given : others ->
            let (prefix, suffix) = BS.splitAt (gatherSize - size) chunk
             in if BS.null suffix
                  then
                    -- Nothing may be pulled until the link has taken the
                    -- chunk, so the gather ends.
                    offer (prefix : pieces) >>= \case
                      [] -> go (PartSource count source given : others)
                      left -> pure (Unsent (heldTail left count source given others), OnWritable)
                  else cut (prefix : pieces) (PartSourceHeld suffix count source given : others)
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
    -- Sends the gathered pieces, which are in reverse, with something to
    -- gather after them, and gives what the link didn't take.
    offer pieces = case pieces of
      [] -> pure []
      _ -> do
        let gathered = reverse pieces
        sent <- sendReady link gathered
        pure (dropSent sent gathered)
    -- Sends what's gathered, and gathers on if the link took it all.
    cut pieces rest =
      offer pieces >>= \case
        [] -> go rest
        left -> pure (Unsent (map PartBytes left <> rest), OnWritable)
    -- The last of the pieces the link didn't take is the source's chunk.
    heldTail left count source given others = case left of
      [] -> others
      [chunk] -> PartSourceHeld chunk count source given : others
      piece : pieces -> PartBytes piece : heldTail pieces count source given others
    dropSent sent = \case
      chunk : others
        | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
        | otherwise -> BS.drop sent chunk : others
      [] -> []
{-# INLINABLE sendSomeIO #-}
{-# SPECIALIZE sendSomeIO :: Socket -> Unsent -> State# RealWorld -> (# State# RealWorld, (Unsent, Blocked) #) #-}

-- | 'sendSome', and then flushes what the link took but hasn't written, so
-- that waiting on what it gives is enough. Records that aren't flushed would
-- stay behind a wait for a source, so they are reported as a wait for the link
-- to be writable instead. It reports 'Done' only when nothing is left to send
-- and all is flushed. With nothing to send, it only flushes.
pushOutput :: Link link => link -> Unsent -> IO (Unsent, Blocked)
pushOutput link left = IO (pushOutputIO link left)
{-# INLINE pushOutput #-}

-- | 'pushOutput' as a function of the state token, for the reason that
-- 'sendSomeIO' is.
pushOutputIO :: Link link => link -> Unsent -> State# RealWorld -> (# State# RealWorld, (Unsent, Blocked) #)
pushOutputIO link left
  | unsentEmpty left = unIO (settle left Done)
  | otherwise = unIO $
      sendSome link left >>= \case
        -- Waiting for the link to be writable sees to the flush too.
        (rest, OnWritable) -> pure (rest, OnWritable)
        (rest, blocked) -> settle rest blocked
  where
    settle rest blocked = do
      flushed <- flushReady link
      pure (rest, if flushed then blocked else OnWritable)
{-# INLINABLE pushOutputIO #-}
{-# SPECIALIZE pushOutputIO :: Socket -> Unsent -> State# RealWorld -> (# State# RealWorld, (Unsent, Blocked) #) #-}
