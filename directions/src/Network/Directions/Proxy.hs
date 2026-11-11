{-# LANGUAGE LambdaCase #-}

-- | The proxy of the design notes, and what it is tried with: a server that
-- echoes, a client in two directions, the same client as one stage, and a
-- stage that transforms bytes on their way.
module Network.Directions.Proxy
  ( proxy,
    proxyThrough,
    echo,
    exchange,
    sendThenReceive,
    upper,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef (IORef, modifyIORef')
import Foreign.Marshal.Alloc (allocaBytes)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Network.Directions.Ends
import Network.Directions.Stage
import Network.Streaming.Link (Link (..))

-- | A proxy between a client's link and a server's: a direction each way, each
-- a receiving stage and a sending stage with a bounded gap between them.
proxy :: (Link c, HalfClose c, Link s, HalfClose s) => c -> s -> [Stage]
proxy fromClient toServer = [receiving fromClient ~> sending toServer, receiving toServer ~> sending fromClient]

-- | 'proxy', with a stage that the client's bytes go through on their way.
proxyThrough :: (Link c, HalfClose c, Link s, HalfClose s) => Stage -> c -> s -> [Stage]
proxyThrough through fromClient toServer =
  [receiving fromClient ~> through ~> sending toServer, receiving toServer ~> sending fromClient]

-- | A server that sends back what it receives, until its input ends: one
-- direction, from its link back to it.
echo :: (Link e, HalfClose e) => e -> Stage
echo end = receiving end ~> sending end

-- | A client in two directions: one sends the request and ends it, the other
-- keeps the answer until it ends.
exchange :: (Link e, HalfClose e) => e -> ByteString -> IORef [ByteString] -> [Stage]
exchange end request kept = [giving request ~> sending end, receiving end ~> keeping kept]

-- | The same client as one stage, the way one stack would write it: it sends
-- all of the request, and only then reads the answer. While it sends, it reads
-- nothing, so once the buffers on the way back are full, it and the server
-- wait for each other.
sendThenReceive :: (Link e, HalfClose e) => e -> ByteString -> IORef [ByteString] -> Stage
sendThenReceive end request kept = Stage (pure (Sending request)) step
  where
    step phase _ _ = case phase of
      Sending left
        | BS.null left -> Again Receiving <$ shutdownSend end
        | otherwise ->
            sendReady end [left] >>= \case
              0 -> pure (Await phase [Event (watchWritable end)])
              sent -> pure (Again (Sending (BS.drop sent left)))
      Receiving ->
        allocaBytes 4096 $ \buffer ->
          receiveReady end buffer 4096 >>= \case
            Nothing -> pure (Await Receiving [Event (watchReadable end)])
            Just 0 -> pure Done
            Just count -> Again Receiving <$ (BS.packCStringLen (castPtr buffer, count) >>= modifyIORef' kept . (:))

data Phase = Sending !ByteString | Receiving

-- | Upper-cases ASCII letters on their way: a stage with an input and an
-- output.
upper :: Stage
upper = Stage (pure ()) step
  where
    step () input output = do
      waiting <- pendingBytes input
      if BS.null waiting
        then (\ended -> if ended then Done else Await () [Input]) <$> inputEnded input
        else withRoom output $ \pointer room ->
          if room == 0
            then pure (Await () [Room])
            else do
              let converted = BS.map toUpper (BS.take room waiting)
                  count = BS.length converted
              BSU.unsafeUseAsCStringLen converted $ \(from, _) -> copyBytes pointer (castPtr from) count
              consume input count
              Again () <$ produce output count
    toUpper byte
      | byte >= 97 && byte <= 122 = byte - 32
      | otherwise = byte
