{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A link that a spec feeds with what the server sends, as it likes, and the
-- messages of a server, for specs of what is read without waiting.
module FeedSupport
  ( FeedLink,
    newFeedLink,
    feed,
    setWritable,
    setReceiveLimit,
    endFeed,
    sentBytes,
    emptyReceives,
    withFeed,
    withOwnedFeed,
    scarceSize,
    withScarceRequests,
    queueFor,
    withRequestFeed,
    message,
    number,
    dataRow,
    command,
    rows,
    readyTx,
    ready,
    errorMessage,
  )
where

import AcquisitionTestSupport (getAcquisition)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Database.PostgreSQL.Internal.Channel (newChannel)
import Database.PostgreSQL.Internal.Context (Context, newConnection, newOwnedContext, withContext, withRequestContext)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.BufferPool (BufferAccess, PoolSettings (..), borrowBuffer, newBufferPool, watchBorrow, withRequestBuffers)
import Network.Streaming.Link (Link (..))
import Network.Streaming.PositiveInt (mkPositiveInt, positiveInt)
import Network.Streaming.Watch (Watch (..))

-- | A link whose input is what the spec feeds it. It takes everything it is
-- offered while it is writable, and nothing while it isn't. A receive gives
-- what is fed, up to its size, or to the limit that was set.
data FeedLink = FeedLink
  { feedInput :: TVar BS.ByteString,
    feedWritable :: TVar Bool,
    feedSent :: IORef BS.ByteString,
    feedEmpty :: IORef Int,
    feedLimit :: IORef Int,
    feedEnded :: TVar Bool
  }

newFeedLink :: IO FeedLink
newFeedLink = FeedLink <$> newTVarIO BS.empty <*> newTVarIO True <*> newIORef BS.empty <*> newIORef 0 <*> newIORef maxBound <*> newTVarIO False

-- | The most that one receive gives, as a socket gives what one segment of
-- the sender holds, however much is ready.
setReceiveLimit :: FeedLink -> Int -> IO ()
setReceiveLimit = writeIORef . feedLimit

-- | What the server sends next.
feed :: FeedLink -> BS.ByteString -> IO ()
feed link bytes = atomically $ readTVar (feedInput link) >>= writeTVar (feedInput link) . (<> bytes)

setWritable :: FeedLink -> Bool -> IO ()
setWritable link = atomically . writeTVar (feedWritable link)

-- | The server's connection is lost: once what was fed is received, a receive
-- gives the end of the stream, as a socket does.
endFeed :: FeedLink -> IO ()
endFeed = atomically . flip writeTVar True . feedEnded

-- | What the link took.
sentBytes :: FeedLink -> IO BS.ByteString
sentBytes = readIORef . feedSent

-- | How many times a receive found nothing.
emptyReceives :: FeedLink -> IO Int
emptyReceives = readIORef . feedEmpty

instance Link FeedLink where
  receiveReady link pointer size = do
    -- Looking before it takes keeps a receive that finds nothing cheap, which
    -- a spec of what such a receive costs a pull relies on.
    available <- readTVarIO (feedInput link)
    if BS.null available
      then do
        ended <- readTVarIO (feedEnded link)
        if ended then pure (Just 0) else Nothing <$ modifyIORef' (feedEmpty link) (+ 1)
      else do
        limit <- readIORef (feedLimit link)
        taken <- atomically $ do
          bytes <- readTVar (feedInput link)
          let (now, later) = BS.splitAt (min size limit) bytes
          writeTVar (feedInput link) later
          pure now
        BSU.unsafeUseAsCStringLen taken $ \(origin, count) -> copyBytes pointer (castPtr origin) count
        pure (Just (BS.length taken))
  sendReady link pieces = do
    writable <- readTVarIO (feedWritable link)
    if not writable
      then pure 0
      else do
        let bytes = BS.concat pieces
        atomicModifyIORef' (feedSent link) (\sent -> (sent <> bytes, ()))
        pure (BS.length bytes)
  flushReady _ = pure True
  watchReadable link =
    pure $
      Watch
        ( do
            bytes <- readTVar (feedInput link)
            ended <- readTVar (feedEnded link)
            check (not (BS.null bytes) || ended)
        )
        (pure ())
  watchWritable link = pure (Watch (readTVar (feedWritable link) >>= check) (pure ()))
  closeLink _ = pure ()

-- | A connection over a new link, with an input buffer of this size, and the
-- context that borrows its buffers, for the action.
withFeed :: Int -> (FeedLink -> Context -> IO a) -> IO a
withFeed size action = do
  link <- newFeedLink
  input <- buffer size
  output <- buffer 64
  connection <- newConnection (newChannel link) size
  withContext connection input output (action link)
  where
    buffer count = maybe (ioError (userError "no buffer")) newBuffer (mkPositiveInt (toInteger count))

-- | A connection over a new link, and the owned context that allocates its input
-- as it needs it, with a first allocation of this size, for the action.
withOwnedFeed :: Int -> (FeedLink -> Context -> IO a) -> IO a
withOwnedFeed size action = do
  link <- newFeedLink
  connection <- newConnection (newChannel link) size
  context <- newOwnedContext connection
  action link context

-- | A connection over a new link, whose input is a loan of the request's
-- buffers, for the action.
withRequestFeed :: BufferAccess -> (FeedLink -> Context -> IO a) -> IO a
withRequestFeed access action = do
  link <- newFeedLink
  connection <- newConnection (newChannel link) scarceSize
  withRequestContext connection access (action link)

-- | The size of the buffers of 'withScarceRequests'.
scarceSize :: Int
scarceSize = 4096

-- | Three requests of a pool of four buffers, of 'scarceSize' bytes. Each
-- request owns one, and the fourth is what a request can borrow in addition to
-- its own, so that a spec exhausts the pool with two of them.
withScarceRequests :: (BufferAccess -> BufferAccess -> BufferAccess -> IO a) -> IO a
withScarceRequests action = do
  waiting <- getAcquisition
  pool <-
    newBufferPool waiting PoolSettings
      { bufferSize = positiveInt @4096,
        ordinaryBufferCount = positiveInt @3,
        reserveBufferCount = positiveInt @1,
        setupBufferCount = positiveInt @1
      }
  withRequestBuffers pool $ \first ->
    withRequestBuffers pool $ \second ->
      withRequestBuffers pool (action first second)

-- | Takes the request's own buffer, and queues it for one more, which it is
-- granted when the pool has one. Releasing the watch gives the buffer back, if
-- it holds one.
queueFor :: BufferAccess -> IO Watch
queueFor access = borrowBuffer access *> watchBorrow access

message :: Word8 -> BS.ByteString -> BS.ByteString
message tag body = BS.singleton tag <> number (4 + BS.length body) <> body

number :: Int -> BS.ByteString
number n = BS.pack [fromIntegral (n `div` 16777216), fromIntegral (n `div` 65536), fromIntegral (n `div` 256), fromIntegral n]

dataRow :: [Maybe BS.ByteString] -> BS.ByteString
dataRow columns =
  message 68 $
    BS.pack [0, fromIntegral (length columns)]
      <> foldMap (maybe (number (-1)) (\value -> number (BS.length value) <> value)) columns

-- | The answer to a statement without rows: Parse, Bind and the command.
command :: BS.ByteString -> BS.ByteString
command tag = message 49 "" <> message 50 "" <> message 67 (tag <> "\0")

-- | The answer to a statement with these rows.
rows :: [[Maybe BS.ByteString]] -> BS.ByteString
rows values = message 49 "" <> message 50 "" <> foldMap dataRow values <> message 67 "SELECT 1\0"

-- | ReadyForQuery, in a transaction that is open, and out of one.
readyTx, ready :: BS.ByteString
readyTx = message 90 "T"
ready = message 90 "I"

-- | An error of the server, with its SQLSTATE.
errorMessage :: BS.ByteString -> BS.ByteString
errorMessage code = message 69 ("SERROR\0C" <> code <> "\0Mthe server refused\0\0")
