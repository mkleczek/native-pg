{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module BorrowedOutputSpec (spec) where

import Control.Exception (evaluate)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Data.IORef
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Buffer (bufferMemory, newBuffer)
import Database.PostgreSQL.Internal.Protocol (Parameter (..))
import Database.PostgreSQL.Internal.Protocol.Codec
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (MaybeBytes32Atom))
import Database.PostgreSQL.Transport hiding (newChannel)
import Database.PostgreSQL.Internal.Channel
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (fillBytes)
import Skeletest

spec :: Spec
spec = describe "borrowed parameter streams" $ do
  it "keeps a reused source buffer unchanged across partial writes" $ do
      buffer <- newBuffer (positiveInt @32)
      pulls <- newIORef (0 :: Int)
      received <- newIORef BS.empty
      let next = do
            count <- atomicModifyIORef' pulls (\n -> (n + 1, n))
            if count >= 3 then pure BS.empty else do
              withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer (fromIntegral count + 65) 32
              pure (BSI.BS (bufferMemory buffer) 32)
          source = ByteStream 96 next
          output = serialize @(Maybe Parameter) @'MaybeBytes32Atom (Just (ParameterBorrowedStream source))
          sender = Sender $ \pieces -> do
            prefix <- evaluate (BS.copy (BS.take 7 (BS.concat pieces)))
            modifyIORef' received (<> prefix)
            pure (BS.length prefix)
      channelSend (newChannel sender) output
      readIORef received `shouldReturn` ("\0\0\0`" <> BS.replicate 32 65 <> BS.replicate 32 66 <> BS.replicate 32 67)
      readIORef pulls `shouldReturn` 4

newtype Sender = Sender ([BS.ByteString] -> IO Int)

instance Link Sender where
  sendReady (Sender send) = send
  flushReady _ = pure True
  awaitWritable _ = pure ()
  receiveReady _ _ _ = fail "unexpected receive"
  awaitReadable _ = fail "unexpected read wait"
  awaitReadableOrWritable _ = fail "unexpected combined wait"
  closeLink _ = pure ()
