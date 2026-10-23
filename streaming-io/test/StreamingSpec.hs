{-# LANGUAGE LambdaCase #-}
module StreamingSpec (spec) where

import Data.IORef (newIORef, readIORef, atomicModifyIORef', modifyIORef')
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Foreign.Ptr (castPtr)
import Data.Word (Word8)
import Foreign.ForeignPtr (mallocForeignPtrBytes, withForeignPtr)
import Foreign.Storable (poke, peek)
import Network.Streaming (Source (..), forward)
import Skeletest

spec :: Spec
spec = describe "borrowed stream forwarding" $ do
  it "consumes each borrowed chunk before the next pull overwrites it" $ do
    memory <- mallocForeignPtrBytes 1
    remaining <- newIORef [11, 22, 33]
    consumed <- newIORef []
    let source = Source $ \use -> do
          next <- atomicModifyIORef' remaining $ \case
            [] -> ([], Nothing)
            value : rest -> (rest, Just value)
          case next of
            Nothing -> use BS.empty
            Just value -> do
              withForeignPtr memory $ \pointer -> poke pointer value
              use (BSI.fromForeignPtr memory 0 1)
        sink bytes = do
          value <- BS.useAsCString bytes (peek . castPtr) :: IO Word8
          modifyIORef' consumed (++ [value])
    forward source sink
    readIORef consumed `shouldReturn` [11, 22, 33]
  it "stops at the first empty chunk without sending it" $ do
    pulls <- newIORef (0 :: Int)
    writes <- newIORef (0 :: Int)
    forward (Source $ \use -> modifyIORef' pulls (+ 1) >> use BS.empty)
      (const $ modifyIORef' writes (+ 1))
    readIORef pulls `shouldReturn` 1
    readIORef writes `shouldReturn` 0

  it "returns from the chunk callback before pulling the next chunk" $ do
    pulls <- newIORef (0 :: Int)
    inCallback <- newIORef False
    let source = Source $ \use -> do
          readIORef inCallback `shouldReturn` False
          number <- atomicModifyIORef' pulls (\n -> (n + 1, n))
          modifyIORef' inCallback (const True)
          result <- use (if number == 0 then BS.singleton 42 else BS.empty)
          modifyIORef' inCallback (const False)
          pure result
    forward source (const $ readIORef inCallback `shouldReturn` True)
    readIORef pulls `shouldReturn` 2
