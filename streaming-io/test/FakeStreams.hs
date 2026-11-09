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
    writableWatchCount,
    FakeSource,
    newFakeSource,
    newClosedSource,
    openSource,
    pullCount,
    watchCounts,
    sourceOutput,
    waitingOutput,
    Stalled (..),
    sendSome,
    sendAll,
  )
where

import Control.Exception (evaluate)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Data.ByteString (ByteString)
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr, castPtr)
import GHC.ForeignPtr (mallocPlainForeignPtrBytes)
import Network.Streaming.Link (Link (..))
import Network.Streaming.Output (Output (..), Uncopied (..), WriteResult (..), Writer (..))
import Network.Streaming.Sender (Pushed (..), Resume (..), Stalls (..), Unsent, noUnsent, pushWith, waitingStalls)
import Network.Streaming.Source (AnySource (..), AnyWaitingSource (..), Pulled (..), Source (..), Waited (..), WaitingSource (..), retrying)
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
    fakeFlushes :: IORef Int,
    fakeWritableWatches :: IORef Int
  }

-- | The link takes this many bytes from its sends in turn, at most what each
-- offers. An infinite list makes it take at most that many from every send.
newFakeLink :: [Int] -> IO FakeLink
newFakeLink takes =
  FakeLink <$> newIORef takes <*> newIORef [] <*> newIORef [] <*> newIORef BS.empty <*> newIORef True <*> newIORef 0 <*> newIORef 0

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

-- | How many watches for the link to be writable were registered.
writableWatchCount :: FakeLink -> IO Int
writableWatchCount = readIORef . fakeWritableWatches

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
  watchWritable link = modifyIORef' (fakeWritableWatches link) (+ 1) $> readyNow
  receiveReady _ _ _ = fail "unexpected receive"
  watchReadable _ = fail "unexpected read wait"
  closeLink _ = pure ()

-- | A source that gives its script's results in turn, then ends. Every chunk
-- is written over the memory of the one before, after clearing it, so a chunk
-- that's used after the next pull shows as wrong bytes. While it's closed it
-- has nothing ready, and its watch fires once it's opened.
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

-- | A pull that doesn't wait: what the script gives next, or nothing while
-- the source is closed.
pullNow :: FakeSource -> IO (Maybe Pulled)
pullNow source = do
  modifyIORef' (sourcePulls source) (+ 1)
  withForeignPtr (sourceMemory source) $ \memory -> fillBytes memory 35 (sourceCapacity source)
  readTVarIO (sourceIsOpen source) >>= \case
    False -> pure Nothing
    True ->
      atomicModifyIORef' (sourceScript source) (\case [] -> ([], Ended); step : rest -> (rest, step)) >>= \case
        Pulled chunk -> do
          withForeignPtr (sourceMemory source) $ \memory ->
            BSU.unsafeUseAsCStringLen chunk $ \(origin, size) -> copyBytes memory (castPtr origin) size
          pure (Just (Pulled (BSI.BS (sourceMemory source) (BS.length chunk))))
        ended -> pure (Just ended)

-- | The registration of a watch for the source to be open.
watchNow :: FakeSource -> IO Watch
watchNow source = do
  modifyIORef' (sourceRegistered source) (+ 1)
  pure (Watch (readTVar (sourceIsOpen source) >>= check) (modifyIORef' (sourceReleased source) (+ 1)))

instance Source FakeSource where
  pullWith source = retrying (pullNow source) (watchNow source)

-- | An output that sends the source, which declares to give this many bytes.
sourceOutput :: Source source => Int -> source -> Output
sourceOutput count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedSource count (AnySource source)) rest)

-- | A source that is waited for in place, with the wait that it is given, which
-- always runs again.
instance WaitingSource FakeSource where
  pullWaiting source = pullWaiting (Waited source)

-- | An output that sends the source, which declares to give this many bytes, to
-- be waited for in place.
waitingOutput :: Int -> FakeSource -> Output
waitingOutput count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedWaiting count (AnyWaitingSource source)) rest)

-- | What a push that stops at its first stall was stalled on.
data Stalled
  = -- | All is sent.
    AllSent
  | -- | The link took only part, or has not written what it took.
    StalledOnLink
  | -- | A source has nothing ready: the registration of its watch.
    StalledOnSource (IO Watch)

-- | Pushes until the first stall, where it stops, and gives what is left and
-- what it was stalled on.
sendSome :: Link link => link -> Unsent -> IO (Unsent, Stalled)
sendSome link left = do
  stalled <- newIORef AllSent
  let stalls =
        Stalls
          { stallWritable = Stop <$ writeIORef stalled StalledOnLink,
            stallSource = \register -> Stop <$ writeIORef stalled (StalledOnSource register)
          }
  pushWith link stalls left >>= \case
    Pushed -> pure (noUnsent, AllSent)
    Stopped rest _ -> (,) rest <$> readIORef stalled

-- | Sends everything, waiting on sources as the sender asks. The link is
-- always writable.
sendAll :: Link link => link -> Unsent -> IO ()
sendAll link left =
  pushWith link (waitingStalls link) left >>= \case
    Pushed -> pure ()
    Stopped _ _ -> fail "a push that waits stopped"
