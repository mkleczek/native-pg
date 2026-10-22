{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

module FramingSpec (spec) where

import Control.Exception (try)
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
