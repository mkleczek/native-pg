module DirectionsSpec (spec) where

import Control.Exception (ErrorCall (..), bracket, throwIO, try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, newIORef, readIORef)
import Data.Word (Word64)
import Network.Directions.Drive
import Network.Directions.Ends
import Network.Directions.Proxy
import Network.Directions.Simulated
import Network.Directions.Stage
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "directions" $ do
  describe "the proxy, between a client and an echo server" $ do
    it "brings the request back, with a thread for each direction" $
      within (snd <$> roundTrip plain runThreads) `shouldReturn` Just request

    it "brings the request back, with every direction on one thread" $
      within (snd <$> roundTrip plain runLoop) `shouldReturn` Just request

    it "brings the request back in a simulation, whatever order the seed gives" $
      forM_ [1 .. 20] $ \seed ->
        roundTrip plain (simulate seed) `shouldReturn` (Completed, request)

    it "brings the request back over sockets, with each driver" $ do
      within (overSockets runThreads) `shouldReturn` Just request
      within (overSockets runLoop) `shouldReturn` Just request

    it "passes the client's bytes through a stage on their way" $ do
      let through = Setup (proxyThrough upper) exchange
      roundTrip through (simulate 7) `shouldReturn` (Completed, BS.map toUpper request)
      within (snd <$> roundTrip through runLoop) `shouldReturn` Just (BS.map toUpper request)

  describe "the simulation" $ do
    it "runs the same way for the same seed, and differently for another" $ do
      first <- simulatedTrace . fst <$> roundTrip plain (runSimulation 1 limit)
      again <- simulatedTrace . fst <$> roundTrip plain (runSimulation 1 limit)
      other <- simulatedTrace . fst <$> roundTrip plain (runSimulation 2 limit)
      again `shouldBe` first
      (other == first) `shouldBe` False

    it "finds that a client that sends all before it reads waits for the server, which waits for it" $
      roundTrip oneStage (simulate 1) `shouldReturn` (Deadlocked [0, 1, 2, 3], BS.empty)

    it "lets the same client finish while its request fits in the buffers on the way" $
      roundTripWith (BS.take 10000 request) oneStage (simulate 1) `shouldReturn` (Completed, BS.take 10000 request)

  describe "threads" $
    it "stop every direction when one fails, and rethrow its failure" $ do
      (_, end) <- connect 1000
      kept <- newIORef []
      let failing = Stage (pure ()) (\() _ _ -> throwIO (ErrorCall "the direction failed"))
          stuck = receiving end ~> keeping kept
      within (try (runThreads [stuck, failing])) `shouldReturn` Just (Left (ErrorCall "the direction failed"))

-- | How the client and the proxy are made.
data Setup = Setup
  { setupProxy :: SimEnd -> SimEnd -> [Stage],
    setupClient :: SimEnd -> ByteString -> IORef [ByteString] -> [Stage]
  }

plain, oneStage :: Setup
plain = Setup proxy exchange
oneStage = Setup proxy (\end bytes kept -> [sendThenReceive end bytes kept])

request :: ByteString
request = BS.pack (take 100000 (cycle [97 .. 122]))

-- | The request from a client, through the proxy, to an echo server and back,
-- over simulated links that hold 1000 bytes each way: what the run gave, and
-- what the client kept.
roundTrip :: Setup -> ([Stage] -> IO a) -> IO (a, ByteString)
roundTrip = roundTripWith request

roundTripWith :: ByteString -> Setup -> ([Stage] -> IO a) -> IO (a, ByteString)
roundTripWith bytes setup run = do
  (client, front) <- connect 1000
  (back, server) <- connect 1000
  kept <- newIORef []
  result <- run (setupClient setup client bytes kept <> setupProxy setup front back <> [echo server])
  (,) result . BS.concat . reverse <$> readIORef kept

-- | The same over sockets: what the client kept.
overSockets :: ([Stage] -> IO a) -> IO ByteString
overSockets run =
  withPair $ \client front -> withPair $ \back server -> do
    kept <- newIORef []
    _ <- run (exchange client request kept <> proxy front back <> [echo server])
    BS.concat . reverse <$> readIORef kept
  where
    withPair :: (Socket -> Socket -> IO a) -> IO a
    withPair =
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
        . uncurry

simulate :: Word64 -> [Stage] -> IO Outcome
simulate seed stages = simulatedOutcome <$> runSimulation seed limit stages

limit :: Int
limit = 10000000

within :: IO a -> IO (Maybe a)
within = timeout 20000000

toUpper :: (Ord a, Num a) => a -> a
toUpper byte
  | byte >= 97 && byte <= 122 = byte - 32
  | otherwise = byte
