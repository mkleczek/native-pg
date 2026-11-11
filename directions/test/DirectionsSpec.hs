module DirectionsSpec (spec) where

import Control.Arrow (ArrowChoice)
import Control.Exception (ErrorCall (..), bracket, throwIO, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, newIORef, readIORef)
import Data.Word (Word64)
import Network.Directions.Classes
import Network.Directions.Loop
import Network.Directions.Proxy
import Network.Directions.Simulated
import Network.Directions.Threads
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "directions" $ do
  describe "the proxy, between a client and an echo server" $ do
    it "brings the request back, with threads" $
      within (snd <$> roundTrip plain request threaded) `shouldReturn` Just request

    it "brings the request back, with every part on one thread" $
      within (snd <$> roundTrip plain request looped) `shouldReturn` Just request

    it "brings the request back in a simulation, whatever order the seed gives" $
      forM_ [1 .. 20] $ \seed ->
        roundTrip plain request (simulate seed) `shouldReturn` (Completed (), request)

    it "brings the request back over sockets, with threads and on one thread" $ do
      within (overSockets threaded) `shouldReturn` Just request
      within (overSockets looped) `shouldReturn` Just request

    it "changes the client's bytes on their way" $ do
      let through = Setup exchange (proxyThrough (BS.map toUpper))
      roundTrip through request (simulate 7) `shouldReturn` (Completed (), BS.map toUpper request)
      within (snd <$> roundTrip through request looped) `shouldReturn` Just (BS.map toUpper request)

  describe "the simulation" $ do
    it "runs the same way for the same seed, and differently for another" $ do
      first <- simulatedTrace . fst <$> roundTrip plain request (\program -> runSimulation 1 limit program ())
      again <- simulatedTrace . fst <$> roundTrip plain request (\program -> runSimulation 1 limit program ())
      other <- simulatedTrace . fst <$> roundTrip plain request (\program -> runSimulation 2 limit program ())
      again `shouldBe` first
      (other == first) `shouldBe` False

    it "finds that a client that sends all before it reads waits for the server, which waits for it" $ do
      (outcome, kept) <- roundTrip oneDirection request (simulate 1)
      kept `shouldBe` BS.empty
      case outcome of
        Deadlocked parts -> length parts `shouldBe` 4
        other -> failTest ("the run ended as " <> show other)

    -- A pump holds what one receive brought, and four lanes of 1000 bytes and
    -- three pumps hold about 7000 bytes.
    it "lets the same client finish while its request fits in the buffers on the way" $
      roundTrip oneDirection (BS.take 5000 request) (simulate 1) `shouldReturn` (Completed (), BS.take 5000 request)

  describe "threads" $
    it "stop every part when one fails, and rethrow its failure" $ do
      (_, end) <- connect 1000
      buffer <- newBuffer 1024
      kept <- newIORef []
      let failing = now (\() -> throwIO (ErrorCall "the part failed"))
      within (try (runThreads (together [receiveAll buffer end kept, failing]) ())) `shouldReturn` Just (Left (ErrorCall "the part failed"))

-- | How the client and the proxy are made.
data Setup = Setup
  { setupClient :: forall arr end. (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () (),
    setupProxy :: forall arr end. (ArrowChoice arr, Concurrent arr, Link arr end) => Buffer -> Buffer -> end -> end -> arr () ()
  }

plain, oneDirection :: Setup
plain = Setup exchange proxy
oneDirection = Setup sendThenReceive proxy

request :: ByteString
request = BS.pack (take 100000 (cycle [97 .. 122]))

-- | The whole round trip as one program: the client, the proxy, and an echo
-- server, over the four ends.
world :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Setup -> ByteString -> IORef [ByteString] -> (end, end, end, end) -> IO (arr () ())
world setup bytes kept (client, front, back, server) = do
  forClient <- newBuffer 16384
  requests <- newBuffer 16384
  answers <- newBuffer 16384
  forServer <- newBuffer 16384
  pure $
    together
      [ setupClient setup forClient client bytes kept,
        setupProxy setup requests answers front back,
        echo forServer server
      ]

-- | The round trip over simulated links that hold 1000 bytes each way: what
-- the run gave, and what the client kept.
roundTrip :: (ArrowChoice arr, Concurrent arr, Effect arr, Block arr) => Setup -> ByteString -> (arr () () -> IO a) -> IO (a, ByteString)
roundTrip setup bytes run = do
  (client, front) <- connect 1000
  (back, server) <- connect 1000
  kept <- newIORef []
  result <- run =<< world setup bytes kept (client, front, back, server)
  (,) result . BS.concat . reverse <$> readIORef kept

-- | The same over socket pairs: what the client kept.
overSockets :: (ArrowChoice arr, Concurrent arr, Effect arr, Block arr) => (arr () () -> IO ()) -> IO ByteString
overSockets run =
  withPair $ \client front -> withPair $ \back server -> do
    kept <- newIORef []
    run =<< world plain request kept (client, front, back, server)
    BS.concat . reverse <$> readIORef kept
  where
    withPair :: (Socket -> Socket -> IO a) -> IO a
    withPair =
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
        . uncurry

threaded :: Threads () () -> IO ()
threaded program = runThreads program ()

looped :: Loop () () -> IO ()
looped program = fst <$> runLoop program ()

simulate :: Word64 -> Loop () () -> IO (Outcome ())
simulate seed program = simulatedOutcome <$> runSimulation seed limit program ()

limit :: Int
limit = 10000000

within :: IO a -> IO (Maybe a)
within = timeout 20000000

toUpper :: (Ord a, Num a) => a -> a
toUpper byte
  | byte >= 97 && byte <= 122 = byte - 32
  | otherwise = byte
