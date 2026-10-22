module StreamingSpec (spec) where

import Data.IORef (newIORef, readIORef, atomicModifyIORef', modifyIORef')
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import Foreign.ForeignPtr (mallocForeignPtrBytes, withForeignPtr)
import Foreign.Storable (poke)
import Network.Streaming (forward)
import Skeletest

spec :: Spec
spec = describe "borrowed stream forwarding" $ do
  it "consumes each borrowed chunk before the next pull overwrites it" $ do
    memory <- mallocForeignPtrBytes 1
    remaining <- newIORef [11, 22, 33]
    consumed <- newIORef []
    let source = do
          next <- atomicModifyIORef' remaining $ \values -> case values of
            [] -> ([], Nothing)
            value : rest -> (rest, Just value)
          case next of
            Nothing -> pure BS.empty
            Just value -> do
              withForeignPtr memory $ \pointer -> poke pointer value
              pure (BSI.fromForeignPtr memory 0 1)
        sink bytes = do
          let value = BS.unpack bytes
          value `shouldNotBe` []
          modifyIORef' consumed (++ value)
    forward source sink
    readIORef consumed `shouldReturn` [11, 22, 33]
  it "stops at the first empty chunk without sending it" $ do
    pulls <- newIORef (0 :: Int)
    writes <- newIORef (0 :: Int)
    forward (modifyIORef' pulls (+ 1) >> pure BS.empty)
      (const $ modifyIORef' writes (+ 1))
    readIORef pulls `shouldReturn` 1
    readIORef writes `shouldReturn` 0
