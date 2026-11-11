{-# LANGUAGE LambdaCase #-}

-- | Stages at the ends of a direction: one that receives from a link, one that
-- sends to a link, and two that give and keep bytes in memory. The links are
-- streaming-io's, as they are.
module Network.Directions.Ends
  ( HalfClose (..),
    receiving,
    sending,
    giving,
    keeping,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef (IORef, modifyIORef')
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Network.Directions.Stage
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Link (Link (..))

-- | Ends a link's output: what was sent arrives, then the end of the stream.
-- streaming-io's 'Link' has no such call yet.
class HalfClose link where
  shutdownSend :: link -> IO ()

instance HalfClose Socket where
  shutdownSend sock = Socket.shutdown sock Socket.ShutdownSend

-- | Receives from the link into its output, until the link's input ends.
receiving :: (Link link) => link -> Stage
receiving link = Stage (pure ()) step
  where
    step () _ output =
      withRoom output $ \pointer room ->
        if room == 0
          then pure (Await () [Room])
          else
            receiveReady link pointer room >>= \case
              Nothing -> pure (Await () [Event (watchReadable link)])
              Just 0 -> pure Done
              Just count -> Again () <$ produce output count

-- | Sends its input to the link, and ends the link's output once its input
-- ends.
sending :: (Link link, HalfClose link) => link -> Stage
sending link = Stage (pure ()) step
  where
    step () input _ = do
      waiting <- pendingBytes input
      if BS.null waiting
        then
          inputEnded input >>= \case
            True -> Done <$ shutdownSend link
            False -> pure (Await () [Input])
        else
          sendReady link [waiting] >>= \case
            0 -> pure (Await () [Event (watchWritable link)])
            sent -> Again () <$ consume input sent

-- | Gives the bytes as its output, then ends.
giving :: ByteString -> Stage
giving whole = Stage (pure whole) step
  where
    step left _ output
      | BS.null left = pure Done
      | otherwise =
          withRoom output $ \pointer room ->
            if room == 0
              then pure (Await left [Room])
              else do
                let (now, rest) = BS.splitAt room left
                BSU.unsafeUseAsCStringLen now $ \(from, count) -> copyBytes pointer (castPtr from) count
                Again rest <$ produce output (BS.length now)

-- | Keeps a copy of its input, the latest chunk first, until its input ends.
keeping :: IORef [ByteString] -> Stage
keeping kept = Stage (pure ()) step
  where
    step () input _ = do
      chunk <- pendingBytes input
      if BS.null chunk
        then (\ended -> if ended then Done else Await () [Input]) <$> inputEnded input
        else do
          -- The chunk is a view of the gap, so it's copied now, not later.
          let !copy = BS.copy chunk
          modifyIORef' kept (copy :)
          Again () <$ consume input (BS.length chunk)
