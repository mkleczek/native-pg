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
    Waits (..),
    pushAnswer,
    Blocked (..),
  )
where

import Control.Exception (throwIO)
import Control.Monad (unless, void)
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
import Network.Streaming.Source (AnySource (..), AnyWaitingSource (..), Pulled (..), Source (..), WaitingSource (..))
import Network.Streaming.Watch (Watch)

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

-- | What the gather does when it can't go on without the link, or a source.
data Mode
  = -- | It gives up, and says what to wait for.
    GiveUp
  | -- | It waits where it is, and goes on: for the link with the first action,
    -- and for a source with the second, which first sends the pieces that are
    -- gathered before the source, which are kept in the reference for it.
    WaitInPlace !(IORef [ByteString]) !(IO ()) !(IO Watch -> IO ())

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
-- the source. A source that is waited for in place can't be sent, as there is
-- nothing to wait with: 'pushAnswer' sends it.
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
sendSomeIO link left = unIO (sendWith GiveUp link left)
{-# INLINABLE sendSomeIO #-}
{-# SPECIALIZE sendSomeIO :: Socket -> Unsent -> State# RealWorld -> (# State# RealWorld, (Unsent, Blocked) #) #-}

-- | The gather, for 'sendSome', which gives up, and for 'pushAnswer', which waits
-- where it is. It is inlined into each, so that the mode is known where it
-- is used, and a send that gives up has nothing of the other.
sendWith :: Link link => Mode -> link -> Unsent -> IO (Unsent, Blocked)
sendWith mode link (Unsent parts) = go parts
  where
    go = gather 0 0 []
    -- Count pieces as they are gathered instead of traversing them each time.
    gather !size !pieceCount pieces remaining
      | size >= gatherSize || pieceCount >= gatherPieces = cut pieces remaining
      | otherwise = case remaining of
          [] ->
            offer pieces >>= \case
              [] -> pure (Unsent [], Done)
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
          PartSource count anySource@(AnySource source) given : others ->
            pullReady source >>= \case
              -- Waiting for the source must not hold back what's gathered.
              Pending -> case mode of
                GiveUp ->
                  offer pieces >>= \case
                    [] -> pure (Unsent remaining, OnSource (watchSource source))
                    left -> pure (Unsent (map PartBytes left <> remaining), OnWritable)
                WaitInPlace gathered _ settle -> do
                  writeIORef gathered pieces
                  settle (watchSource source)
                  regather size pieceCount gathered remaining
              Ended -> do
                unless (given == count) (throwIO (StreamLengthError count given))
                gather size pieceCount pieces others
              Pulled chunk
                | delivered > count -> throwIO (StreamLengthError count delivered)
                | BS.null chunk -> gather size pieceCount pieces remaining
                | otherwise -> gather size pieceCount pieces (PartSourceHeld chunk (PartSource count anySource delivered) : others)
                where
                  delivered = given + BS.length chunk
          PartWaiting count anyWaiting@(AnyWaitingSource source) given : others -> case mode of
            GiveUp -> throwIO (userError "a source that is waited for in place can't be sent by a send that gives up")
            WaitInPlace gathered _ settle -> do
              writeIORef gathered pieces
              pullWaiting source settle >>= \case
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
    -- Goes on after a wait for a source with what is still gathered before it:
    -- nothing, if the wait sent it, and then the counts start again.
    regather size pieceCount gathered rest =
      readIORef gathered >>= \case
        [] -> gather 0 0 [] rest
        pieces -> gather size pieceCount pieces rest
    -- What is left when the link took only part of what it was offered: it is
    -- given back, or waited for.
    stalled left = case mode of
      GiveUp -> pure (Unsent left, OnWritable)
      WaitInPlace _ writable _ -> writable *> go left
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
        left -> stalled (map PartBytes left <> rest)
    -- The last of the pieces the link didn't take is the source's chunk.
    heldTail left resume others = case left of
      [] -> others
      [chunk] -> PartSourceHeld chunk resume : others
      piece : pieces -> PartBytes piece : heldTail pieces resume others
{-# INLINE sendWith #-}

-- | What a send of the pieces left, as the pieces that remain.
dropSent :: Int -> [ByteString] -> [ByteString]
dropSent sent = \case
  chunk : others
    | sent >= BS.length chunk -> dropSent (sent - BS.length chunk) others
    | otherwise -> BS.drop sent chunk : others
  [] -> []

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

-- | What a push waits with, on the caller's thread: for the link to take more,
-- and for a source that has nothing ready, with the registration of the watch
-- to wait on, which the wait releases. The caller has what it waits for,
-- such as a client's input and a timeout, besides.
data Waits = Waits
  { waitWritable :: IO (),
    waitSource :: IO Watch -> IO ()
  }

-- | Sends the output and flushes what the link took, and gives nothing back
-- until all is sent: when the link takes only part, it waits for it with
-- 'waitWritable', and when a source has nothing ready, it is pulled with
-- 'pullWaiting' and 'waitSource', which are given the pieces gathered before
-- it sent first, as 'sendSome' sends them before it gives up on a source. The
-- source isn't pulled again before the link has taken its chunk, so a wait never
-- overlaps a chunk that is being sent. A source that gives up is waited for too.
pushAnswer :: Link link => link -> Waits -> Unsent -> IO ()
pushAnswer link waits left = IO (pushAnswerIO link waits left)
{-# INLINE pushAnswer #-}

-- | 'pushAnswer' as a function of the state token, for the reason that
-- 'sendSomeIO' is.
pushAnswerIO :: Link link => link -> Waits -> Unsent -> State# RealWorld -> (# State# RealWorld, () #)
pushAnswerIO link waits left = unIO $ do
  unless (unsentEmpty left) $ do
    gathered <- newIORef []
    void (sendWith (WaitInPlace gathered (waitWritable waits) (settleBefore link waits gathered)) link left)
  flushAll link waits
{-# INLINABLE pushAnswerIO #-}
{-# SPECIALIZE pushAnswerIO :: Socket -> Waits -> Unsent -> State# RealWorld -> (# State# RealWorld, () #) #-}

-- | The wait for a source that has nothing ready: sends the pieces that are
-- gathered before it, and flushes what the link took, waiting for it to be
-- writable as long as it takes, and then waits for the source.
settleBefore :: Link link => link -> Waits -> IORef [ByteString] -> IO Watch -> IO ()
settleBefore link waits gathered watch = do
  pieces <- readIORef gathered
  unless (null pieces) $ do
    writeIORef gathered []
    sendAll (reverse pieces)
  flushAll link waits
  waitSource waits watch
  where
    sendAll pieces = do
      sent <- sendReady link pieces
      case dropSent sent pieces of
        [] -> pure ()
        left -> waitWritable waits *> sendAll left
{-# INLINABLE settleBefore #-}

-- | Flushes what the link took, waiting for it to be writable until it has.
flushAll :: Link link => link -> Waits -> IO ()
flushAll link waits = flushReady link >>= \flushed -> unless flushed (waitWritable waits *> flushAll link waits)
{-# INLINABLE flushAll #-}
