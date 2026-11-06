{-# LANGUAGE LambdaCase #-}

-- | A link that records what it's offered, and sources with scripted pulls,
-- for specs of sources and of the sender.
module FakeStreams
  ( FakeLink,
    newFakeLink,
    linkOffers,
    linkAddresses,
    linkReceived,
    setFlushed,
    flushCount,
    FakeSource,
    newFakeSource,
    newClosedSource,
    openSource,
    pullCount,
    watchCounts,
    sourceOutput,
    sendAll,
  )
where

import Control.Exception (evaluate)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr, castPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Streaming.Link (Blocked (..), Interest (..), Link (..))
import Network.Streaming.Output (Output (..), Uncopied (..), WriteResult (..), Writer (..))
import Network.Streaming.Sender (Unsent, sendSome)
import Network.Streaming.Source (AnySource (..), Pulled (..), Source (..))
import Network.Streaming.Watch (Watch (..), readyNow)

-- | A link that takes as many bytes from each send as it's told to, and then
-- everything, and keeps what it was offered. It has nothing to flush, unless
-- it's told otherwise.
data FakeLink = FakeLink
  { fakeTakes :: IORef [Int],
    fakeOffers :: IORef [[ByteString]],
    fakeAddresses :: IORef [[(Ptr Word8, Int)]],
    fakeReceived :: IORef ByteString,
    fakeFlushed :: IORef Bool,
    fakeFlushes :: IORef Int
  }

-- | The link takes this many bytes from its sends in turn, at most what each
-- offers. An infinite list makes it take at most that many from every send.
newFakeLink :: [Int] -> IO FakeLink
newFakeLink takes =
  FakeLink <$> newIORef takes <*> newIORef [] <*> newIORef [] <*> newIORef BS.empty <*> newIORef True <*> newIORef 0

-- | The pieces of each send as the link was offered them, copied, in order.
linkOffers :: FakeLink -> IO [[ByteString]]
linkOffers link = reverse <$> readIORef (fakeOffers link)

-- | Where the pieces of each send were, and their lengths.
linkAddresses :: FakeLink -> IO [[(Ptr Word8, Int)]]
linkAddresses link = reverse <$> readIORef (fakeAddresses link)

-- | What the link took.
linkReceived :: FakeLink -> IO ByteString
linkReceived = readIORef . fakeReceived

-- | Whether a flush finds nothing left to write.
setFlushed :: FakeLink -> Bool -> IO ()
setFlushed = writeIORef . fakeFlushed

-- | How many times the link was asked to flush.
flushCount :: FakeLink -> IO Int
flushCount = readIORef . fakeFlushes

instance Link FakeLink where
  sendReady link pieces = do
    -- A copy, because the memory may be reused once it's accepted.
    offered <- traverse (evaluate . BS.copy) pieces
    addresses <- traverse (\piece -> BSU.unsafeUseAsCStringLen piece (\(pointer, size) -> pure (castPtr pointer, size))) pieces
    limit <- atomicModifyIORef' (fakeTakes link) $ \case
      [] -> ([], maxBound)
      count : rest -> (rest, count)
    let taken = BS.take limit (BS.concat offered)
    modifyIORef' (fakeOffers link) (offered :)
    modifyIORef' (fakeAddresses link) (addresses :)
    modifyIORef' (fakeReceived link) (<> taken)
    pure (BS.length taken)
  flushReady link = modifyIORef' (fakeFlushes link) (+ 1) *> readIORef (fakeFlushed link)
  watchWritable _ = pure readyNow
  receiveReady _ _ _ = fail "unexpected receive"
  watchReadable _ = fail "unexpected read wait"
  closeLink _ = pure ()

-- | A source that gives its script's results in turn, then ends. Every chunk
-- is written over the memory of the one before, after clearing it, so a chunk
-- that's used after the next pull shows as wrong bytes. While it's closed it
-- gives 'Pending', and its watch fires once it's opened.
data FakeSource = FakeSource
  { sourceMemory :: ForeignPtr Word8,
    sourceCapacity :: Int,
    sourceScript :: IORef [Pulled],
    sourcePulls :: IORef Int,
    sourceIsOpen :: TVar Bool,
    sourceRegistered :: IORef Int,
    sourceReleased :: IORef Int
  }

-- | An open source.
newFakeSource :: [Pulled] -> IO FakeSource
newFakeSource script = do
  let capacity = maximum (1 : [BS.length chunk | Pulled chunk <- script])
  FakeSource
    <$> mallocPlainForeignPtrBytes capacity
    <*> pure capacity
    <*> newIORef script
    <*> newIORef 0
    <*> newTVarIO True
    <*> newIORef 0
    <*> newIORef 0

-- | A source that has nothing until it's opened.
newClosedSource :: [Pulled] -> IO FakeSource
newClosedSource script = do
  source <- newFakeSource script
  atomically (writeTVar (sourceIsOpen source) False)
  pure source

openSource :: FakeSource -> IO ()
openSource source = atomically (writeTVar (sourceIsOpen source) True)

-- | How many times it was pulled, pending pulls included.
pullCount :: FakeSource -> IO Int
pullCount = readIORef . sourcePulls

-- | How many watches were registered, and how many releases were asked for.
watchCounts :: FakeSource -> IO (Int, Int)
watchCounts source = (,) <$> readIORef (sourceRegistered source) <*> readIORef (sourceReleased source)

instance Source FakeSource where
  pullReady source = do
    modifyIORef' (sourcePulls source) (+ 1)
    withForeignPtr (sourceMemory source) $ \memory -> fillBytes memory 35 (sourceCapacity source)
    readTVarIO (sourceIsOpen source) >>= \case
      False -> pure Pending
      True ->
        atomicModifyIORef' (sourceScript source) (\case [] -> ([], Ended); step : rest -> (rest, step)) >>= \case
          Pulled chunk -> do
            withForeignPtr (sourceMemory source) $ \memory ->
              BSU.unsafeUseAsCStringLen chunk $ \(origin, size) -> copyBytes memory (castPtr origin) size
            pure (Pulled (BSI.BS (sourceMemory source) (BS.length chunk)))
          other -> pure other
  watchSource source = do
    modifyIORef' (sourceRegistered source) (+ 1)
    pure (Watch (readTVar (sourceIsOpen source) >>= check) (modifyIORef' (sourceReleased source) (+ 1)))

-- | An output that sends the source, which declares to give this many bytes.
sourceOutput :: Source source => Int -> source -> Output
sourceOutput count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedSource count (AnySource source)) rest)

-- | Sends everything, waiting on sources as the sender asks. The link is
-- always writable.
sendAll :: Link link => link -> Unsent -> IO ()
sendAll link left =
  sendSome link left >>= \case
    (_, Done) -> pure ()
    (rest, blocked) -> awaitAny link (Interest False blocked) *> sendAll link rest
