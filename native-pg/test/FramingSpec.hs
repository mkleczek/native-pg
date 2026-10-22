{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module FramingSpec (spec) where

import Control.Exception (try)
import Control.Monad (replicateM)
import Database.PostgreSQL.Buffer (newBuffer)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32)
import Data.Word (Word8)
import Database.PostgreSQL.Protocol
import Database.PostgreSQL.Protocol.Codec.Payload (Payload (..))
import Data.IORef (newIORef, modifyIORef', readIORef)
import Database.PostgreSQL.Protocol.Codec
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import Skeletest

spec :: Spec
spec = describe "protocol framing" $ do
  it "rejects reading beyond an enclosing protocol frame" $ do
    source <- bytesSource "abcd"
    inputResult (runInput source emptyInputBuffer (withInputLimit 3 (readInputExactly 4)))
      `shouldReturn` Left "unexpected end of a bounded part: needed 4 bytes"

  it "requires a frame parser to consume its whole body" $ do
    source <- bytesSource "abcd"
    inputResult (runInput source emptyInputBuffer (withInputLimit 4 (readInputExactly 3)))
      `shouldReturn` Left "bounded parser left 1 unconsumed bytes"

  it "permits a streamed row body across source chunks" $ do
    let column = BS.replicate 1024 120
        body = outputByteString (serialize @Int16 @('FixedBytesAtom 2) 1 <> serialize @Int32 @('FixedBytesAtom 4) 1024) <> column
    source <- bytesSource (frameHeader 68 body <> body)
    inputResult (runInputUntil source maxBound emptyInputBuffer $ do
      _ <- parser @DataRowHeader @('PayloadField 'DataRowHeaderType)
      size <- parser @Int32 @('FixedBytesAtom 4)
      skipInput (fromIntegral size)
      pure "streamed") `shouldReturn` Right "streamed"

  it "batches primitive reads inside a frame without pulling its successor" $ do
    let body = outputByteString (mconcat (replicate 100 (serialize @Int32 @('FixedBytesAtom 4) 7)))
        envelope = outputByteString (serialize @Int32 @('FixedBytesAtom 4) 400) <> body
        bytes = envelope <> "next"
    offset <- newIORef 0
    requests <- newIORef []
    storage <- newBuffer 16
    let source = InputSource (\pointer size -> do
          modifyIORef' requests (size :)
          consumed <- readIORef offset
          let chunk = BS.take size (BS.drop consumed bytes)
          BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
          modifyIORef' offset (+ BS.length chunk)
          pure (BS.length chunk)) 16
    (values, _) <- runInputFramedUntil source maxBound (reusableInputBuffer storage) $ do
      length' <- parser @Int32 @('FixedBytesAtom 4)
      withInputLimit (fromIntegral length') (replicateM 100 (parser @Int32 @('FixedBytesAtom 4)))
    values `shouldBe` replicate 100 7
    readIORef offset `shouldReturn` BS.length envelope
    calls <- readIORef requests
    (length calls <= 28) `shouldBe` True
    maximum calls `shouldBe` 16

frameHeader :: Word8 -> ByteString -> ByteString
frameHeader tag body = BS.singleton tag <> outputByteString (serialize @Int32 @('FixedBytesAtom 4) (fromIntegral (BS.length body + 4)))

inputResult :: IO (ByteString, InputBuffer) -> IO (Either String ByteString)
inputResult action = either (\(InputError reason) -> Left reason) (Right . fst) <$> try action

bytesSource :: ByteString -> IO InputSource
bytesSource bytes = do
  consumed <- newIORef (0 :: Int)
  pure $ InputSource
    (\pointer size -> do
      offset <- readIORef consumed
      let chunk = BS.take size (BS.drop offset bytes)
      BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
      modifyIORef' consumed (+ BS.length chunk)
      pure (BS.length chunk))
    16
