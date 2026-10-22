{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module OutputBufferSpec (spec) where

import Control.Exception (evaluate)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import Control.Monad (forM_)
import qualified Database.PostgreSQL.Internal.Protocol.Codec as Output
import Database.PostgreSQL.Internal.Context
import Network.Streaming.Resources.Internal (bufferMemory, bufferCapacity, newBuffer)
import Database.PostgreSQL.Internal.Protocol (Sync (..), SyncType (SyncType), Parse (..), ParseType (ParseType), Query (..), QueryType (QueryType), Execute (..), ExecuteType (ExecuteType))
import Database.PostgreSQL.Internal.Protocol.Codec (serialize)
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (castPtr, plusPtr)
import Skeletest hiding (context)

spec :: Spec
spec = describe "caller-owned output storage" $ do
  it "serializes a large batch through the same small buffer" $ do
      buffer <- newBuffer 16
      addresses <- newIORef True
      sent <- newIORef BS.empty
      let link = Sender $ \chunks -> do
            withForeignPtr (bufferMemory buffer) $ \start ->
              mapM_ (\chunk -> BSU.unsafeUseAsCStringLen chunk $ \(pointer, size) ->
                modifyIORef' addresses (&& (castPtr pointer >= start && castPtr pointer `plusPtr` size <= start `plusPtr` bufferCapacity buffer))) chunks
            accepted <- evaluate (BS.copy (BS.concat chunks))
            modifyIORef' sent (<> accepted)
            pure (sum (map BS.length chunks))
      channelSendBuffered (newChannel link) (outputBufferFrom buffer) batch
      readIORef sent `shouldReturn` wire
      readIORef addresses `shouldReturn` True

  it "sends through the current context's supplied workspace" $ do
    input <- newBuffer 16
    output <- newBuffer 16
    supplied <- newIORef True
    sent <- newIORef BS.empty
    let link = Sender $ \chunks -> do
          withForeignPtr (bufferMemory output) $ \start ->
            mapM_ (\chunk -> BSU.unsafeUseAsCStringLen chunk $ \(pointer, size) ->
              modifyIORef' supplied (&& (castPtr pointer >= start && castPtr pointer `plusPtr` size <= start `plusPtr` bufferCapacity output))) chunks
          accepted <- evaluate (BS.copy (BS.concat chunks))
          modifyIORef' sent (<> accepted)
          pure (BS.length accepted)
    connection <- newConnection (newChannel link) 16
    withContext connection input output $ \context ->
      runContextOperation context (sendContextOutput context batch)
    readIORef sent `shouldReturn` wire
    readIORef supplied `shouldReturn` True

  it "preserves unsent bytes across partial writes" $ do
    buffer <- newBuffer 16
    sent <- newIORef BS.empty
    let link = Sender $ \chunks -> do
          prefix <- evaluate (BS.copy (BS.take 3 (BS.concat chunks)))
          modifyIORef' sent (<> prefix)
          pure (BS.length prefix)
    channelSendBuffered (newChannel link) (outputBufferFrom buffer) batch
    readIORef sent `shouldReturn` wire
  it "matches contiguous serialization for complete messages at small capacities" $ do
    let output = serialize @(Parse BS.ByteString) @'ParseType (Parse ("" :: BS.ByteString) "SELECT $1" [])
          <> serialize @(Query BS.ByteString) @'QueryType (Query (BS.replicate 9000 65))
          <> serialize @(Execute BS.ByteString) @'ExecuteType (Execute ("" :: BS.ByteString) 17)
          <> serialize @Sync @'SyncType Sync
        expected = Output.outputByteString output
    forM_ [4, 7, 256] $ \capacity -> do
      buffer <- newBuffer capacity
      received <- newIORef BS.empty
      let sender = Sender $ \chunks -> do
            prefix <- evaluate (BS.copy (BS.take 3 (BS.concat chunks)))
            modifyIORef' received (<> prefix)
            pure (BS.length prefix)
      channelSendBuffered (newChannel sender) (outputBufferFrom buffer) output
      readIORef received `shouldReturn` expected

  where
    batch = mconcat (replicate 40 (serialize @Sync @'SyncType Sync))
    wire = BS.concat (replicate 40 "S\0\0\0\4")

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  awaitWritable _ = pure ()
  receiveReady _ _ _ = fail "unexpected receive"
  awaitReadable _ = fail "unexpected read wait"
  awaitReadableOrWritable _ = fail "unexpected readiness wait"
  closeLink _ = pure ()
