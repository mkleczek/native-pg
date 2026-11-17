{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Where a server that runs on several cores puts its new connections, and how many it serves at once: a client opens
-- connections and keeps them, and each core is to serve as many of them as the other, instead of one core serving them
-- all while the other has room. A connection goes to the core that has the fewest when it is accepted; past the cap on
-- connections the server accepts none until one closes; and a shutdown that comes while every core is full ends the
-- server.
module PlacementSpec (spec) where

import Control.Arrow ((&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (forkFinally, getNumCapabilities, killThread, myThreadId, rtsSupportsBoundThreads, setNumCapabilities, threadCapability, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.Exception (SomeException, bracket)
import Control.Monad (forM_, replicateM, void, when)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (sort)
import Network.Fulmen (Call, Request)
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.BufferPool (BufferPoolSettings (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (consume, keeping, sleep, step, tap)
import Network.Streaming.Delimited (runCores)
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

spec :: Spec
spec = do
  placementSpec
  bufferSpec

placementSpec :: Spec
placementSpec = parallelCores "a core's thread is pinned to the capability of its number, and the non-threaded run time has one" $ do
  describe "placement" $ do
    it "gives new connections that are kept open to the cores in turn, when the connections that closed were all of one core" $
      withServer Fulmen.defaultSettings {Fulmen.connectionsPerCore = 4} 2 $ \port _ -> do
        -- Both cores wait for a connection from the start, and the connections come when they do.
        threadDelay 200000
        placed <- timeout 20000000 $
          -- Each core serves four connections, so both are full, and the acceptor waits for the first core that has
          -- room: the one that closes a connection first, which is core 0. Then the connections of core 1 close, and
          -- last the others of core 0: both cores have none, and the connections that come go to them in turn.
          bracket (replicateM 8 (opened port)) (mapM_ (Socket.close . fst)) $ \held -> do
            let (firstClosed, lastClosed) = splitAt 1 (on 0 held)
            forM_ [firstClosed, on 1 held, lastClosed] $ \group -> do
              mapM_ (Socket.close . fst) group
              threadDelay 100000
            bracket (replicateM 4 (opened port)) (mapM_ (Socket.close . fst)) (pure . sort . map snd)
        placed `shouldBe` Just [0, 0, 1, 1 :: Int]

    -- The core is picked when the connection is accepted, from what the cores have then: the connection that comes
    -- after the connections of core 1 closed goes to core 1, which has the fewest, and not to the core that had more
    -- room before.
    it "gives a connection to the core that has the fewest when it comes, after the connections of the other were closed" $
      withServer Fulmen.defaultSettings {Fulmen.connectionsPerCore = 4} 2 $ \port _ -> do
        threadDelay 200000
        placed <- timeout 20000000 $
          bracket (replicateM 6 (opened port)) (mapM_ (Socket.close . fst)) $ \held -> do
            -- Three connections on each core, and then none on core 1.
            sort (map snd held) `shouldBe` [0, 0, 0, 1, 1, 1]
            mapM_ (Socket.close . fst) (on 1 held)
            threadDelay 300000
            bracket (opened port) (Socket.close . fst) (pure . snd)
        placed `shouldBe` Just 1

  describe "the cap on connections" $
    it "accepts no connection past the cap until one that is served has closed" $
      -- One core, with room for two connections.
      withServer Fulmen.defaultSettings {Fulmen.connectionsPerCore = 2} 1 $ \port _ -> do
        threadDelay 200000
        served <- timeout 20000000 $
          bracket (replicateM 2 (opened port)) (mapM_ (Socket.close . fst)) $ \held ->
            bracket (connected port) Socket.close $ \waiting -> do
              sendRequest waiting
              -- The third waits in the listen backlog, and is not served while the two are open.
              early <- timeout 400000 (answerOf waiting)
              mapM_ (Socket.close . fst) (take 1 held)
              late <- timeout 10000000 (answerOf waiting)
              pure (early, late)
        served `shouldBe` Just (Nothing, Just 0)

  describe "a shutdown that comes while every core is full" $
    it "ends the server, closes the connection that is idle, and serves none that waits in the backlog" $
      bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(reader, writer) -> Socket.close reader *> Socket.close writer) $ \(reader, writer) -> do
        fd <- Socket.unsafeFdSocket reader
        let settings = Fulmen.defaultSettings {Fulmen.connectionsPerCore = 1, Fulmen.shutdown = Just (Readable (Fd fd))}
        withServer settings 1 $ \port stopped -> do
          threadDelay 200000
          bracket (opened port) (Socket.close . fst) $ \(idle, _) ->
            -- The only core is full, so a second connection waits in the backlog, where the acceptor doesn't take it.
            bracket (connected port) Socket.close $ \waiting -> do
              sendRequest waiting
              threadDelay 200000
              SocketBytes.sendAll writer "x"
              ended <- timeout 5000000 (readMVar stopped)
              fmap (either (const False) (const True)) ended `shouldBe` Just True
              closed <- timeout 3000000 (SocketBytes.recv idle 1)
              closed `shouldBe` Just B.empty
              answered <- timeout 500000 (answerOf waiting)
              answered `shouldBe` Nothing

-- | A server on this many cores that runs for as long as the action does, on a listening socket of its own, which the action
-- is given the port of, and the box that says how the server ended. Each core runs on the capability of its number.
withServer :: Fulmen.Settings -> Int -> (Socket.PortNumber -> MVar (Either SomeException ()) -> IO a) -> IO a
withServer settings cores use = do
  getNumCapabilities >>= \capabilities -> when (capabilities < cores) (setNumCapabilities cores)
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    let serving shared =
          runCores cores (pure shared) (\made core -> const (Fulmen.onCore made core) ^>> Fulmen.serve settings (Fulmen.borrowed accepting) answerWithCore)
    shared <- Fulmen.newCores settings cores
    stopped <- newEmptyMVar
    bracket (forkFinally (void (serving shared)) (putMVar stopped)) killThread $ \_ -> use port stopped

-- | Where the request buffers of a server that runs on several cores are: a request is admitted from the pool of its own core,
-- so that the buffer that a request gives back goes to the next request that waits on its core, and a core that has requests
-- waiting never idles because the buffers went to requests of another core.
bufferSpec :: Spec
bufferSpec = parallelCores "a core's thread is pinned to the capability of its number, and the non-threaded run time has one" $ describe "request buffers" $
  it "gives the buffer that a request gives back to the request that waits on its own core, though a request of the other core waited longer" $ do
      getNumCapabilities >>= \capabilities -> when (capabilities < 2) (setNumCapabilities 2)
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
        Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
        Socket.listen accepting 128
        port <- Socket.socketPort accepting
        -- Two ordinary buffers for the two cores, one each, and one in reserve, which an admission leaves.
        let settings =
              Fulmen.defaultSettings
                { Fulmen.connectionsPerCore = 4,
                  Fulmen.bufferPool = BufferPoolSettings (positiveInt @16384) (positiveInt @2) (positiveInt @1) (positiveInt @1)
                }
        started <- newIORef []
        cores <- Fulmen.newCores settings 2
        stopped <- newEmptyMVar
        let serving = runCores 2 (pure cores) (\shared core -> const (Fulmen.onCore shared core) ^>> Fulmen.serve settings (Fulmen.borrowed accepting) (holding started))
        bracket (forkFinally (void serving) (putMVar stopped)) killThread $ \_ -> do
          threadDelay 200000
          order <- timeout 30000000 $ bracket (replicateM 4 (opened port)) (mapM_ (Socket.close . fst)) $ \held ->
            case (on 0 held, on 1 held) of
              (zeroFirst : zeroSecond : _, oneFirst : oneSecond : _) -> do
                -- The first two requests are admitted, one on each core. A request of core 1 waits next, and then one of core 0, which came
                -- later. The one on core 0 ends first, and its buffer is for the request that waits on core 0.
                writeIORef started []
                forM_ [(zeroFirst, 250000), (oneFirst, 150000), (oneSecond, 100000), (zeroSecond, 1800000)] $ \((sock, _), pause) -> do
                  SocketBytes.sendAll sock "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
                  threadDelay pause
                reverse <$> readIORef started
              _ -> failTest ("the connections were not placed two to a core: " <> show (map snd held))
          order `shouldBe` Just [0, 1, 0, 1 :: Int]

-- | Notes the core that a request starts on, holds it for 800 milliseconds, and answers with the number of that core.
holding :: (Runtime arr) => IORef [Int] -> arr (Call, Request) ()
holding started =
  tap (step "note the core" (\_ -> myThreadId >>= threadCapability >>= \(capability, _) -> atomicModifyIORef' started (\noted -> (capability : noted, ()))))
    >>> (keeping (sleep (millis 800)) >>^ snd)
    >>> answerWithCore

-- | Answers with the number of the core that runs the handler, as a digit: a core's thread is pinned to the capability
-- of its number.
answerWithCore :: (Runtime arr) => arr (Call, Request) ()
answerWithCore =
  (,(Fulmen.Status 200, [], Fulmen.Known 1))
    . fst
    ^>> Fulmen.respond
    >>> keeping (now (const core))
    >>> ((\(digit, answering) -> (answering, [digit])) &&& snd)
    ^>> consume Fulmen.send
    >>> Fulmen.finish
  where
    core = (\(capability, _) -> C.pack (show capability)) <$> (threadCapability =<< myThreadId)

-- | The connections that the server's core of this number serves.
on :: Int -> [(Socket, Int)] -> [(Socket, Int)]
on core = filter ((== core) . snd)

-- | A new connection to the server, and the number of the core that answers a request on it.
opened :: Socket.PortNumber -> IO (Socket, Int)
opened port = do
  sock <- connected port
  sendRequest sock
  (,) sock <$> answerOf sock

-- | A new connection to the server, with nothing sent on it.
connected :: Socket.PortNumber -> IO Socket
connected port = do
  sock <- Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol
  Socket.connect sock (Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1)))
  pure sock

-- | Sends a request on the connection.
sendRequest :: Socket -> IO ()
sendRequest sock = SocketBytes.sendAll sock "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"

-- | The number of the core that answered the request that was sent on the connection.
answerOf :: Socket -> IO Int
answerOf sock = do
  answer <- readAnswer B.empty
  pure (read (C.unpack (B.takeEnd 1 answer)))
  where
    readAnswer sofar
      | B.length (snd (B.breakSubstring "\r\n\r\n" sofar)) > 4 = pure sofar
      | otherwise = SocketBytes.recv sock 4096 >>= \bytes -> if B.null bytes then failTest ("the connection ended before the answer: " <> show sofar) else readAnswer (sofar <> bytes)

-- | Skips the case on the non-threaded run time, which has one capability.
parallelCores :: String -> Spec -> Spec
parallelCores reason = if rtsSupportsBoundThreads then id else skip reason
