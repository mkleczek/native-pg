{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The conformance cases of "ConformanceCases", run against a fulmen server: each case starts a server with its
-- handler and settings on a free port, sends its steps over one TCP connection, and checks what came back, as the
-- header of "ConformanceCases" says.
--
-- Each case is run under each runner of the arrow runtime. A server that stops while a case runs fails the case at once,
-- with what it stopped with, so that a server that is not there yet is told from one that answers wrongly.
--
-- A case that needs a part of the server that is not there yet is marked as an expected failure, and the mark comes
-- off when the part lands.
module ConformanceSpec (spec) where

import Control.Arrow (first, returnA, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkFinally, forkIO, killThread, rtsSupportsBoundThreads, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (IOException, SomeException, bracket, displayException, fromException, throwIO, try)
import Control.Monad (foldM, forM_, void, when)
import ConformanceCases
import ConformanceWire
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Char (toLower)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Tuple (swap)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Network.Fulmen (Call, Cores, Request (..))
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, consume, iterateA, iterateWith, keeping, sleep, tap)
import Network.Streaming.Counters (Counter (..), countOf, since, snapshot)
import Network.Streaming.Compiled (runCompiled, runCompiledM)
import Network.Streaming.Delimited (runCores, runDelimited, runDelimitedM)
import Network.Streaming.Mode (Mode (..))
import Network.Streaming.Threads (runThreads, runThreadsM)
import Skeletest
import System.Info (os)
import System.Directory (findExecutable)
import System.IO (hGetLine)
import System.Posix.Resource (Resource (..), ResourceLimits (..), getResourceLimit, setResourceLimit)
import System.Posix.Signals (raiseSignal, sigTERM)
import System.Posix.Types (Fd (..))
import System.IO.Error (ioeGetErrorString, isAlreadyInUseError)
import System.IO.Unsafe (unsafePerformIO)
import System.Process (CreateProcess (..), StdStream (..), createProcess, proc, terminateProcess, waitForProcess)
import System.Timeout (timeout)

spec :: Spec
spec = conformance *> counted *> listenerFailure *> peerAddress *> shutdownSpec *> idleSpec *> manyWorkers

conformance :: Spec
conformance = describe "HTTP/1.1 conformance" $ forM_ runners $ \runner@(Runner runnerName _) -> describe runnerName $ do
  forM_ [minBound .. maxBound] $ \area ->
    case filter ((== area) . caseArea) cases of
      [] -> pure ()
      inArea -> describe (areaTitle area) $ forM_ inArea $ \c -> marked c $ it (caseName c) (runCase runner c)
  it "serves connections one after another, each closed by its client, in one run of the server" (oneAfterAnother runner)

-- | The calls that accepting makes, counted by the runners of 'Debug'.
counted :: Spec
counted = describe "accepting" $ forM_ debugRunners $ \runner@(Runner runnerName _) -> describe runnerName $
  it "takes each connection with one call, and where accept4 is there with none to fcntl" $ do
    before <- snapshot
    oneAfterAnother runner
    counts <- (`since` before) <$> snapshot
    countOf Accepts counts `shouldBe` 30
    when (os == "linux") $ countOf Fcntls counts `shouldBe` 0

-- | A listener that fails to open ends the server with that failure, and the server doesn't wait for a connection: the
-- first core runs the acceptor, which opens the listener, and the other parts of the server don't keep it from ending.
listenerFailure :: Spec
listenerFailure =
  describe "a listener that fails to open" $ forM_ (runners ++ [coresRunner]) $ \runner@(Runner runnerName _) ->
    describe runnerName $ do
      it "ends the server with the failure of the provider" $
        endedWith runner (Listening failing) >>= shouldEndWith ((== "the listener failed") . ioeGetErrorString)
      it "ends the server with the failure to bind to a port that is in use" $
        bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \occupant -> do
          Socket.bind occupant (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
          Socket.listen occupant 128
          port <- Socket.socketPort occupant
          let address =
                Socket.defaultHints
                  { Socket.addrFamily = Socket.AF_INET,
                    Socket.addrSocketType = Socket.Stream,
                    Socket.addrAddress = Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1))
                  }
          endedWith runner (Listening (Fulmen.tcp address 128)) >>= shouldEndWith isAlreadyInUseError
  where
    failing :: (Runtime arr) => Fulmen.Listen arr
    failing =
      Fulmen.Listen
        { Fulmen.openListener = now (const (ioError (userError "the listener failed"))),
          Fulmen.closeListener = now (const (pure ()))
        }

-- | The request has the address that its connection came from, as the server saw it: the client's own address.
peerAddress :: Spec
peerAddress = describe "the peer address" $ forM_ runners $ \runner@(Runner runnerName _) ->
  describe runnerName $
    it "of a request is the address that its client connected from" (peerOf runner)

peerOf :: Runner -> IO ()
peerOf (Runner _ runServer) =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    cores <- Fulmen.newCores Fulmen.defaultSettings 1
    stopped <- newEmptyMVar
    let serving = runServer Fulmen.defaultSettings (Listening (Fulmen.borrowed accepting)) (Handling peerHandler) cores
    bracket (forkFinally serving (putMVar stopped)) killThread $ \_ ->
      withClient port $ \client -> do
        own <- Socket.getSocketName (socket client)
        let outcome = Outcome [ResponseExpect (Status 200) Nothing [] (BodyIs (C.pack (show own))) False] ConnOpen
            c =
              Case
                { caseName = "the peer address",
                  caseArea = ARequestLine,
                  caseOrigin = Spec "the peer address of a request",
                  caseHandler = Fail,
                  caseSettings = noSettings,
                  caseSteps = [Send "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"],
                  caseExpect = [outcome],
                  caseNote = ""
                }
        exchange c client stopped
  where
    -- Answers with the address of the peer, as the network package shows it.
    peerHandler :: (Runtime arr) => arr (Call, Request) ()
    peerHandler = (\(call, request) -> (call, (Fulmen.Status 200, [], C.pack (show (Fulmen.peer request))))) ^>> reply

-- | A graceful shutdown: it starts when the event of the server's settings fires, which a byte on a socket does here. The
-- listener is closed, a connection that waits for its next request is closed, a request in flight finishes and says that
-- the connection ends, and the server ends when none is left.
shutdownSpec :: Spec
shutdownSpec = describe "a graceful shutdown" $ forM_ (runners ++ [coresRunner]) $ \runner@(Runner runnerName _) ->
  describe runnerName $ do
    it "ends the server and closes its listener, with no connection open" $
      withStoppable runner (handling (Fixed 200 [] "ok")) $ \shutting -> do
        stopServer shutting
        ended <- timeout 3000000 (readMVar (serverStopped shutting))
        fmap (either (const False) (const True)) ended `shouldBe` Just True
        readIORef (listenerClosed shutting) >>= (`shouldBe` True)
    it "closes a connection that waits for its next request" $
      withStoppable runner (handling (Fixed 200 [] "ok")) $ \shutting ->
        withClient (serverPort shutting) $ \client -> do
          SocketBytes.sendAll (socket client) "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
          awaitBytes client "ok"
          stopServer shutting
          closed <- timeout 3000000 (waitUntil (readIORef (ended client)))
          closed `shouldBe` Just ()
          ended' <- timeout 3000000 (readMVar (serverStopped shutting))
          fmap (either (const False) (const True)) ended' `shouldBe` Just True
    it "lets a request in flight finish, and says that the connection ends" $
      withStoppable runner quickOrSlow $ \shutting ->
        withClient (serverPort shutting) $ \client -> do
          SocketBytes.sendAll (socket client) "GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\n"
          threadDelay 150000
          stopServer shutting
          closed <- timeout 5000000 (waitUntil (readIORef (ended client)))
          closed `shouldBe` Just ()
          bytes <- readIORef (received client)
          case parsedResponses (parseResponses [False] True bytes) of
            [response] -> do
              responseStatus response `shouldBe` 200
              responseBody response `shouldBe` "onetwo"
              [value | (name, value) <- responseFields response, C.map toLower name == "connection"] `shouldBe` ["close"]
            others -> failTest ("expected one response, got " <> show (length others))
          ended' <- timeout 3000000 (readMVar (serverStopped shutting))
          fmap (either (const False) (const True)) ended' `shouldBe` Just True
    it "closes the idle connection at once while another request is in flight, which then finishes" $
      withStoppable runner quickOrSlow $ \shutting ->
        withClient (serverPort shutting) $ \busy -> withClient (serverPort shutting) $ \idle -> do
          -- The connection that is idle has served a request, and waits for the next.
          SocketBytes.sendAll (socket idle) "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
          awaitBytes idle "ok"
          SocketBytes.sendAll (socket busy) "GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\n"
          threadDelay 200000
          stopServer shutting
          idleClosed <- timeout 1000000 (waitUntil (readIORef (ended idle)))
          idleClosed `shouldBe` Just ()
          busyClosed <- readIORef (ended busy)
          busyClosed `shouldBe` False
          finished <- timeout 6000000 (waitUntil (readIORef (ended busy)))
          finished `shouldBe` Just ()
          bytes <- readIORef (received busy)
          fmap responseBody (parsedResponses (parseResponses [False] True bytes)) `shouldBe` ["onetwo"]
    -- A request whose handler never returns: with a drain deadline the server ends when it has passed, and the handler is cancelled
    -- where it waits, so that the scope that it holds is released.
    it "ends within the drain deadline when a request in flight never returns, and releases what its handler held" $ do
      noted <- newIORef []
      withStoppableWith (\settings -> settings {Fulmen.drainTimeout = Just (millis 300)}) runner (hanging noted) $ \shutting ->
        withClient (serverPort shutting) $ \client -> do
          SocketBytes.sendAll (socket client) "GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n"
          began <- timeout 3000000 (waitUntil ((== ["began"]) <$> readIORef noted))
          began `shouldBe` Just ()
          begun <- getMonotonicTimeNSec
          stopServer shutting
          ended' <- timeout 5000000 (readMVar (serverStopped shutting))
          finished <- getMonotonicTimeNSec
          fmap (either (const False) (const True)) ended' `shouldBe` Just True
          -- It waited for the deadline, and not for ever.
          (finished - begun >= 300000000) `shouldBe` True
          (finished - begun < 3000000000) `shouldBe` True
          readIORef noted `shouldReturn` ["left: cancelled", "began"]
          closed <- timeout 3000000 (waitUntil (readIORef (ended client)))
          closed `shouldBe` Just ()
    it "starts when the process is sent SIGTERM, with the event of terminating" $ do
      event <- Fulmen.terminating
      withStoppableOn id event runner (handling (Fixed 200 [] "ok")) $ \shutting -> do
        raiseSignal sigTERM
        ended <- timeout 3000000 (readMVar (serverStopped shutting))
        fmap (either (const False) (const True)) ended `shouldBe` Just True

-- | The idle timeout: a connection that waits for its next request for longer than it is closed, and its worker serves the
-- connection that waits for one, while a connection that is serving a request, or that waits for less, is not closed, and a
-- shutdown closes an idle connection at once whatever the timeout is.
idleSpec :: Spec
idleSpec = describe "an idle timeout" $ forM_ (runners ++ [coresRunner]) $ \runner@(Runner runnerName _) ->
  describe runnerName $ do
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
        withIdle limit workers = withStoppableWith (\settings -> settings {Fulmen.idleTimeout = limit, Fulmen.connectionsPerCore = workers}) runner
        notYet = xfail "the idle timeout is not there yet"
    notYet $
      it "closes a connection that waits for its next request for longer, and its worker serves the connection that waits for one" $
        withIdle (Just (millis 300)) 1 (handling (Fixed 200 [] "ok")) $ \shutting ->
          withClient (serverPort shutting) $ \idle -> withClient (serverPort shutting) $ \waiting -> do
            SocketBytes.sendAll (socket idle) request
            awaitBytes idle "ok"
            started <- getMonotonicTimeNSec
            -- The only worker has the idle connection, so this one waits in the listen backlog.
            SocketBytes.sendAll (socket waiting) request
            arrived <- timeout 5000000 (waitUntil (("ok" `B.isInfixOf`) <$> readIORef (received waiting)))
            arrived `shouldBe` Just ()
            answered <- getMonotonicTimeNSec
            -- Not before the connection had waited the timeout.
            (answered - started >= 250000000) `shouldBe` True
            closed <- timeout 1000000 (waitUntil (readIORef (ended idle)))
            closed `shouldBe` Just ()
    notYet $
      it "closes a connection that says nothing at all, after the timeout" $
        withIdle (Just (millis 300)) 4 (handling (Fixed 200 [] "ok")) $ \shutting ->
          withClient (serverPort shutting) $ \silent -> do
            threadDelay 100000
            readIORef (ended silent) `shouldReturn` False
            closed <- timeout 3000000 (waitUntil (readIORef (ended silent)))
            closed `shouldBe` Just ()
    it "does not close a connection whose request is in flight for longer than the timeout" $
      withIdle (Just (millis 300)) 4 quickOrSlow $ \shutting ->
        withClient (serverPort shutting) $ \busy -> do
          SocketBytes.sendAll (socket busy) "GET /slow HTTP/1.1\r\nHost: localhost\r\n\r\n"
          threadDelay 1500000
          readIORef (ended busy) `shouldReturn` False
          -- The body comes in two chunks, so the response is parsed to see it whole.
          finished <- timeout 3000000 (waitUntil ((== ["onetwo"]) . map responseBody . parsedResponses . parseResponses [False] True <$> readIORef (received busy)))
          finished `shouldBe` Just ()
    it "does not close a connection whose waits for its next request are shorter than the timeout, however long it is kept" $
      withIdle (Just (millis 400)) 4 (handling (Fixed 200 [] "ok")) $ \shutting ->
        withClient (serverPort shutting) $ \client -> do
          forM_ [1 .. 8 :: Int] $ \_ -> do
            SocketBytes.sendAll (socket client) request
            threadDelay 150000
          readIORef (ended client) `shouldReturn` False
          bytes <- readIORef (received client)
          length (parsedResponses (parseResponses (replicate 8 False) True bytes)) `shouldBe` 8
    it "does not close an idle connection when it has no idle timeout" $
      withIdle Nothing 4 (handling (Fixed 200 [] "ok")) $ \shutting ->
        withClient (serverPort shutting) $ \client -> do
          SocketBytes.sendAll (socket client) request
          awaitBytes client "ok"
          threadDelay 700000
          readIORef (ended client) `shouldReturn` False
          SocketBytes.sendAll (socket client) request
          timeout 3000000 (waitUntil ((\bytes -> length (parsedResponses (parseResponses [False, False] True bytes)) == 2) <$> readIORef (received client))) `shouldReturn` Just ()
    it "closes the idle connection at once at a shutdown, whatever the timeout is" $
      withIdle (Just (secs 60)) 4 (handling (Fixed 200 [] "ok")) $ \shutting ->
        withClient (serverPort shutting) $ \idle -> do
          SocketBytes.sendAll (socket idle) request
          awaitBytes idle "ok"
          stopServer shutting
          closed <- timeout 1000000 (waitUntil (readIORef (ended idle)))
          closed `shouldBe` Just ()
          stopped <- timeout 3000000 (readMVar (serverStopped shutting))
          fmap (either (const False) (const True)) stopped `shouldBe` Just True

-- | A server that is running, and what a test needs to stop it and to see how it ended.
data Stoppable = Stoppable
  { serverPort :: Socket.PortNumber,
    stopServer :: IO (),
    serverStopped :: MVar (Either SomeException ()),
    listenerClosed :: IORef Bool
  }

-- | A server whose shutdown starts when a byte is written to a socket, as the event of its settings says.
withStoppable :: Runner -> Handling -> (Stoppable -> IO a) -> IO a
withStoppable = withStoppableWith id

-- | The same, with the settings that the function makes of the test's.
withStoppableWith :: (Fulmen.Settings -> Fulmen.Settings) -> Runner -> Handling -> (Stoppable -> IO a) -> IO a
withStoppableWith adjust runner handler use =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(reader, writer) -> Socket.close reader *> Socket.close writer) $ \(reader, writer) -> do
    fd <- Socket.unsafeFdSocket reader
    withStoppableOn adjust (Readable (Fd fd)) runner handler (\shutting -> use shutting {stopServer = void (try @IOException (SocketBytes.sendAll writer "x"))})

-- | A server whose shutdown starts when the event fires, with the settings that the function makes of the test's. It listens on a
-- socket that it closes as it ends, so that the test sees that it did.
withStoppableOn :: (Fulmen.Settings -> Fulmen.Settings) -> Event -> Runner -> Handling -> (Stoppable -> IO a) -> IO a
withStoppableOn adjust event (Runner _ runServer) handler use =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    closed <- newIORef False
    -- Several workers, so that a connection that is idle and one that is busy are both served.
    let settings = adjust Fulmen.defaultSettings {Fulmen.shutdown = Just event, Fulmen.connectionsPerCore = 4}
        listener =
          Listening
            Fulmen.Listen
              { Fulmen.openListener = now (const (pure accepting)),
                Fulmen.closeListener = now (\sock -> writeIORef closed True *> Socket.close sock)
              }
    cores <- Fulmen.newCores settings 1
    stopped <- newEmptyMVar
    bracket (forkFinally (runServer settings listener handler cores) (putMVar stopped)) killThread $ \_ ->
      use (Stoppable port (pure ()) stopped closed)

-- | Waits, a little at a time, until the check holds.
waitUntil :: IO Bool -> IO ()
waitUntil check = check >>= \holds -> if holds then pure () else threadDelay 5000 *> waitUntil check

-- | Waits until what the client has received has the bytes.
awaitBytes :: Client -> ByteString -> IO ()
awaitBytes client needle = do
  arrived <- timeout 3000000 (waitUntil ((needle `B.isInfixOf`) <$> readIORef (received client)))
  when (isNothing arrived) (failTest ("waited for " <> show needle <> ", which did not arrive"))

-- | A handler that answers /slow in two chunks, a second before each, and anything else at once.
quickOrSlow :: Handling
quickOrSlow = Handling program
  where
    program :: (Runtime arr) => arr (Call, Request) ()
    program =
      (\input@(_, request) -> if target request == "/slow" then Left input else Right input)
        ^>> (streamedWith [pause 1000 >>> writing "one", pause 1000 >>> writing "two"] ||| ((, (Fulmen.Status 200, [], "ok")) . fst ^>> reply))

-- | A handler that notes that it began, in a scope that notes how it was left, and waits for ever.
hanging :: IORef [String] -> Handling
hanging noted = Handling program
  where
    program :: (Runtime arr) => arr (Call, Request) ()
    program =
      scope
        (now (const (note "began")))
        (now (\(_, exit) -> note ("left: " <> describeExit exit)))
        (now (const neverReadable) >>> block)
    note entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))
    describeExit = \case
      Done -> "done"
      Failed _ -> "failed"
      Cancelled' -> "cancelled"

-- | An event that never fires: the readability of a socket that nothing is sent to, which is kept.
neverReadable :: IO Event
neverReadable = do
  (one, other) <- Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol
  atomicModifyIORef' keptSockets (\sockets -> (one : other : sockets, ()))
  Readable . Fd <$> Socket.unsafeFdSocket one

keptSockets :: IORef [Socket]
keptSockets = unsafePerformIO (newIORef [])
{-# NOINLINE keptSockets #-}

-- | A server of thousands of workers starts and serves that many connections at once, under the small stack that a dev
-- build runs with. Its workers are made at startup, and a loop that is not a tail loop overflows that stack, which the parts
-- that run masked don't report: the server goes on with the workers that it had made, and spins. It is a process of its
-- own, so that its spin doesn't hold up the test: it is the server of @fulmen-bench@.
manyWorkers :: Spec
manyWorkers = describe "a server of thousands of workers, under a small stack" $ forM_ ["threads", "delimited"] $ \kind ->
  describe kind $
    it "serves that many connections that are open at once" (serveMany kind 2500)

-- | A server of this many workers, and the same many clients that connect at once and ask it for the answer of 200: how
-- many got it, within the time that they have.
serveMany :: String -> Int -> IO ()
serveMany kind count = do
  -- Both ends of every connection are descriptors of this process.
  limit <- getResourceLimit ResourceOpenFiles
  setResourceLimit ResourceOpenFiles limit {softLimit = hardLimit limit}
  bench <- findExecutable "fulmen-bench" >>= maybe (failTest "fulmen-bench is not on the path") pure
  -- The server has ten capabilities where the run time has them: the non-threaded one refuses the flag, and has one.
  let capabilities = ["-N10" | rtsSupportsBoundThreads]
  bracket
    (createProcess (proc bench (["server", kind, "1", show count, "+RTS", "-T"] <> capabilities <> ["-K1K", "-RTS"])) {std_out = CreatePipe})
    (\(_, _, _, handle) -> terminateProcess handle *> void (waitForProcess handle))
    $ \(_, out, _, _) -> do
      port <- maybe (failTest "no output of the server") (fmap (fromIntegral . (read :: String -> Int) . drop 5 . filter (/= '\r') . takeWhile (/= '\n')) . hGetLine) out
      -- Loops that keep no stack of their own: the stack of the test is small, too, as the stack of a dev build is.
      answers <- foldM (\collected _ -> (: collected) <$> newEmptyMVar) [] [1 .. count]
      forM_ answers $ \answer -> forkIO (asking port >>= putMVar answer)
      answered <- timeout 30000000 (foldM (\total answer -> (\got -> if got then total + 1 else total) <$> readMVar answer) (0 :: Int) answers)
      answered `shouldBe` Just count
  where
    asking port = (either (const False) id <$>) . try @IOException $
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
        Socket.connect sock (Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1)))
        SocketBytes.sendAll sock "GET /16k HTTP/1.1\r\nHost: localhost\r\n\r\n"
        ("HTTP/1.1 200" `B.isPrefixOf`) <$> SocketBytes.recv sock 4096

-- | How the server ended, if it did in time: the runner runs it with a handler that answers 200, on one core made for it.
endedWith :: Runner -> Listening -> IO (Maybe (Either SomeException ()))
endedWith (Runner _ runServer) listening = do
  cores <- Fulmen.newCores Fulmen.defaultSettings 1
  stopped <- newEmptyMVar
  let serving = runServer Fulmen.defaultSettings listening (handling (Fixed 200 [] "ok")) cores
  bracket (forkFinally serving (putMVar stopped)) killThread $ \_ -> timeout 5000000 (readMVar stopped)

-- | Fails the test unless the server ended with an I/O failure of this kind.
shouldEndWith :: (IOException -> Bool) -> Maybe (Either SomeException ()) -> IO ()
shouldEndWith kind = \case
  Nothing -> failTest "the server did not end in time"
  Just (Right ()) -> failTest "the server returned"
  Just (Left failure) -> case fromException failure of
    Just ioFailure | kind ioFailure -> pure ()
    _ -> failTest ("the server ended with another failure: " <> displayException failure)

-- | Marks a case that is expected to fail, and says why. A case that then passes unexpectedly fails the suite.
marked :: Case -> Spec -> Spec
marked c = maybe id xfail (expectedFailure c)

-- | Why a case is expected to fail, if it is: it needs a part of the server that is not there yet. @Expect: 100-continue@
-- and the timeouts are step 3.
expectedFailure :: Case -> Maybe String
expectedFailure c
  | caseName c `elem` served = Nothing
  | caseArea c == ATimeouts = Just "the client timeout is not there yet (step 3)"
  | otherwise = Nothing

-- | The cases of the areas that are not there yet that pass all the same: the server doesn't wait for a client
-- that sends its body at once, or before the timeout.
served :: [String]
served =
  [ "a client that sends the body without waiting gets the answer, with or without 100 Continue",
    "a handler that ignores the body of an expecting request does not leave the connection in limbo",
    "an HTTP/1.0 request with Expect: 100-continue and its body is answered",
    "a body that arrives slowly but within the timeout is read in full"
  ]

-- * Runners

-- | A way to run the server: an arrow's runner, given the settings, the listener, the handler and the cores.
data Runner = Runner String (Fulmen.Settings -> Listening -> Handling -> Cores -> IO ())

-- | A handler, for any arrow that has what 'Runtime' bundles.
newtype Handling = Handling (forall arr. (Runtime arr) => arr (Call, Request) ())

-- | A provider of the listening socket, for any arrow that has what 'Runtime' bundles.
newtype Listening = Listening (forall arr. (Runtime arr) => Fulmen.Listen arr)

runners :: [Runner]
runners =
  [ Runner
      "threads"
      (\settings (Listening listen) (Handling handler) cores -> runThreads (Fulmen.serve settings listen handler) cores),
    Runner
      "delimited"
      (\settings (Listening listen) (Handling handler) cores -> void (runDelimited (Fulmen.serve settings listen handler) cores)),
    Runner
      "compiled"
      (\settings (Listening listen) (Handling handler) cores -> void (runCompiled (Fulmen.serve settings listen handler) cores))
  ]

-- | The same in 'Debug', which counts the calls that the server makes.
debugRunners :: [Runner]
debugRunners =
  [ Runner
      "threads"
      (\settings (Listening listen) (Handling handler) cores -> runThreadsM @'Debug (Fulmen.serve settings listen handler) cores),
    Runner
      "delimited"
      ( \settings (Listening listen) (Handling handler) cores ->
          void (runDelimitedM @'Debug (Fulmen.serve settings listen handler) cores)
      ),
    Runner
      "compiled"
      ( \settings (Listening listen) (Handling handler) cores ->
          void (runCompiledM @'Debug (Fulmen.serve settings listen handler) cores)
      )
  ]

-- | The runner of two cores, the first of which runs the acceptor.
coresRunner :: Runner
coresRunner =
  Runner
    "two cores"
    ( \settings (Listening listen) (Handling handler) cores ->
        void (runCores 2 (pure cores) (\cores' core -> const (Fulmen.onCore cores' core) ^>> Fulmen.serve settings listen handler))
    )

-- * The handlers of the cases

-- | The handler of the case, as the header of "ConformanceCases" describes it.
handling :: Handler -> Handling
handling handler = case handler of
  Echo -> Handling echo
  Fixed status fields content -> Handling ((, (Fulmen.Status status, fields, content)) . fst ^>> reply)
  Streamed chunks -> Handling (streamedWith [writing chunk | chunk <- chunks])
  Headers -> Handling (second ((Fulmen.Status 200, [],) . headersOf) ^>> reply)
  Ignore -> Handling ((, (Fulmen.Status 200, [], ignoreBody)) . fst ^>> reply)
  Slow delay chunks -> Handling (streamedWith [pause delay >>> writing chunk | chunk <- chunks])
  EchoStreaming -> Handling echoStreaming
  Fail -> Handling (now (\_ -> throwIO (userError "the handler failed")))
  where
    headersOf request =
      let Fulmen.Method name = method request
       in headersBody name (target request) (headers request)

-- | The head, the body, and the end of the response, when the body is known.
reply :: (Runtime arr) => arr (Call, (Fulmen.Status, Fulmen.ResponseHeaders, ByteString)) ()
reply =
  withLength
    ^>> first Fulmen.respond
    >>> (second (: []) &&& fst)
    ^>> consume Fulmen.send
    >>> Fulmen.finish
  where
    withLength (call, (status, fields, content)) = ((call, (status, fields, Fulmen.Known (B.length content))), content)

-- | The whole request body, as the response.
echo :: (Runtime arr) => arr (Call, Request) ()
echo =
  fst
    ^>> alongside (Fulmen.body >>> gather)
    >>> second (Fulmen.Status 200, [],) . swap
    ^>> reply

-- | A 200 of unknown length, whose body the steps send, one after the other.
streamedWith :: (Runtime arr) => [arr Fulmen.Answering Fulmen.Answering] -> arr (Call, Request) ()
streamedWith pieces =
  (, (Fulmen.Status 200, [], Fulmen.Unknown)) . fst
    ^>> Fulmen.respond
    >>> foldr (>>>) returnA pieces
    >>> Fulmen.finish

-- | Writes the chunk, flushed. A chunk that is nothing is written as nothing: it must not end a chunked body.
writing :: (Runtime arr) => ByteString -> arr Fulmen.Answering Fulmen.Answering
writing chunk
  | B.null chunk = returnA
  | otherwise = tap ((, [chunk]) ^>> Fulmen.send)

-- | Waits this many milliseconds, which is a wait of the arrow.
pause :: (Runtime arr) => Int -> arr a a
pause delay = keeping (sleep (millis (fromIntegral delay))) >>^ snd

-- | Each piece of the request body, sent as soon as it has arrived.
echoStreaming :: (Runtime arr) => arr (Call, Request) ()
echoStreaming =
  fst
    ^>> alongside Fulmen.body
    >>> second ((, (Fulmen.Status 200, [], Fulmen.Unknown)) ^>> Fulmen.respond)
    >>> iterateWith (alongside (fst ^>> next) >>^ byPiece) (consume Fulmen.send)
    >>> Fulmen.finish
  where
    byPiece (piece, (requestBody, answering)) =
      maybe (Finish answering) (\view' -> Again ((answering, [view']), (requestBody, answering))) piece

-- | The pieces of the stream, copied out of the views and put together.
gather :: (Runtime arr, Stream arr s) => arr s ByteString
gather =
  (,[])
    ^>> iterateA
      ( alongside (fst ^>> next)
          >>> now
            ( \(piece, (source, copies)) -> case piece of
                Nothing -> pure (Finish copies)
                Just view' -> let !copy = B.copy view' in pure (Again (source, copy : copies))
            )
      )
    >>^ (B.concat . reverse)

-- * Running a case

-- | How long the client waits for what it expects: the case's silence timeout comes on top.
patience :: Case -> Word64
patience c = 3000 + maybe 0 fromIntegral (timeoutMs (caseSettings c))

runCase :: Runner -> Case -> IO ()
runCase (Runner _ runServer) c =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    let settings = settingsOf (caseSettings c)
        serving cores = runServer settings (Listening (Fulmen.borrowed accepting)) (handling (caseHandler c)) cores
    cores <- Fulmen.newCores settings 1
    stopped <- newEmptyMVar
    bracket (forkFinally (serving cores) (putMVar stopped)) killThread $ \_ ->
      withClient port $ \client -> exchange c client stopped

-- | Connections one after another, each with one request and closed by its client, in one run of the server. The server's
-- sockets get the numbers of those it closed before, and its waits on them must not be lost.
oneAfterAnother :: Runner -> IO ()
oneAfterAnother (Runner _ runServer) =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    cores <- Fulmen.newCores Fulmen.defaultSettings 1
    stopped <- newEmptyMVar
    let serving = runServer Fulmen.defaultSettings (Listening (Fulmen.borrowed accepting)) (handling (Fixed 200 [] "ok")) cores
    bracket (forkFinally serving (putMVar stopped)) killThread $ \_ -> do
      answers <- timeout 20000000 (mapM (const (once port)) [1 .. 30 :: Int])
      fmap (all answered) answers `shouldBe` Just True
  where
    -- One connection: it connects, waits a little so that the server waits for its input, sends a request, reads the
    -- answer and closes.
    once port = bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
      Socket.connect sock (Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1)))
      threadDelay 10000
      SocketBytes.sendAll sock "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"
      readAnswer sock B.empty
    readAnswer sock sofar
      | "\r\n\r\nok" `B.isSuffixOf` sofar = pure sofar
      | otherwise = SocketBytes.recv sock 4096 >>= \bytes -> if B.null bytes then pure sofar else readAnswer sock (sofar <> bytes)
    answered answer = "HTTP/1.1 200" `B.isPrefixOf` answer && "\r\n\r\nok" `B.isSuffixOf` answer

-- | The settings of the server: the defaults, and the case's where it names any.
settingsOf :: Settings -> Fulmen.Settings
settingsOf given =
  base
    { Fulmen.lineLimit = fromMaybe (Fulmen.lineLimit base) (lineLimit given),
      Fulmen.headLimit = fromMaybe (Fulmen.headLimit base) (headLimit given),
      Fulmen.bodyLimit = bodyLimit given,
      Fulmen.clientTimeout = maybe (Fulmen.clientTimeout base) (Just . millis . fromIntegral) (timeoutMs given)
    }
  where
    base = Fulmen.defaultSettings

-- | What the client of a case has: its socket, what it has received, whether the server ended the connection, whether it
-- reads, and when what it has received last grew.
data Client = Client
  { socket :: Socket,
    received :: IORef ByteString,
    ended :: IORef Bool,
    reading :: IORef Bool,
    grew :: IORef Word64
  }

withClient :: Socket.PortNumber -> (Client -> IO a) -> IO a
withClient port use = bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
  Socket.connect sock (Socket.SockAddrInet port (Socket.tupleToHostAddress (127, 0, 0, 1)))
  client <- Client sock <$> newIORef B.empty <*> newIORef False <*> newIORef True <*> (getMonotonicTimeNSec >>= newIORef)
  bracket (forkIO (collecting client)) killThread (\_ -> use client)

-- | Collects every byte that the server sends, from the moment of connecting.
collecting :: Client -> IO ()
collecting client = loop
  where
    loop = do
      allowed <- readIORef (reading client)
      if allowed
        then do
          chunk <- try (SocketBytes.recv (socket client) 4096)
          case chunk of
            Left (_ :: IOException) -> writeIORef (ended client) True
            Right bytes
              | B.null bytes -> writeIORef (ended client) True
              | otherwise -> do
                  modifyIORef' (received client) (<> bytes)
                  getMonotonicTimeNSec >>= writeIORef (grew client)
                  loop
        else threadDelay 1000 >> loop

-- | The steps of the case, and then what the server answered: a case passes if the exchange is one of the outcomes that it
-- accepts.
exchange :: Case -> Client -> MVar (Either SomeException ()) -> IO ()
exchange c client stopped = do
  forM_ (caseSteps c) (step c client stopped)
  closed <- settle c client stopped
  bytes <- readIORef (received client)
  let attempts = [(outcome, matchOutcome outcome (parseResponses (map rsToHead (outResponses outcome)) closed bytes) closed) | outcome <- caseExpect c]
  case attempts of
    _ | any (either (const False) (const True) . snd) attempts -> pure ()
    (outcome, Left why) : _ ->
      failTest
        ( "the exchange is none of the "
            <> show (length attempts)
            <> " accepted outcomes; the first: "
            <> why
            <> "\nobserved: "
            <> describeObserved (parseResponses (map rsToHead (outResponses outcome)) closed bytes) closed
        )
    _ -> failTest "the case accepts no outcome"

-- | One step of the client.
step :: Case -> Client -> MVar (Either SomeException ()) -> Step -> IO ()
step c client stopped s = case s of
  Send bytes -> do
    -- A write that the server doesn't take blocks, so it is made by a thread of its own, and the case waits for it as it
    -- waits for the server's answer. A server that has closed the connection already doesn't take it: that is for the
    -- outcome to say.
    written <- newEmptyMVar
    bracket (forkIO (try @IOException (SocketBytes.sendAll (socket client) bytes) >>= putMVar written)) killThread $ \_ -> do
      arrived <- waitFor c client stopped (\_ _ -> False) (isJust <$> tryReadMVar written)
      if arrived then pure () else failTest ("the server did not take " <> show (B.length bytes) <> " bytes of the request")
  PauseMs delay -> threadDelay (delay * 1000)
  AwaitReceived needle -> do
    arrived <- waitFor c client stopped (\bytes closed -> needle `B.isInfixOf` bytes || closed) (pure False)
    bytes <- readIORef (received client)
    if arrived && needle `B.isInfixOf` bytes
      then pure ()
      else failTest ("waited for " <> show needle <> ", which did not arrive; received " <> show (B.take 200 bytes))
  HalfClose -> void (try @IOException (Socket.shutdown (socket client) Socket.ShutdownSend))
  PauseReadingMs delay -> do
    writeIORef (reading client) False
    threadDelay (delay * 1000)
    writeIORef (reading client) True

-- | Waits until the condition holds of what has been received, and whether the server ended the connection, or the other
-- check is true, or the patience runs out: whether one held.
waitFor :: Case -> Client -> MVar (Either SomeException ()) -> (ByteString -> Bool -> Bool) -> IO Bool -> IO Bool
waitFor c client stopped condition other = do
  started <- getMonotonicTimeNSec
  let loop = do
        failIfStopped stopped
        bytes <- readIORef (received client)
        closed <- readIORef (ended client)
        finished <- other
        now' <- getMonotonicTimeNSec
        if condition bytes closed || finished
          then pure True
          else
            if now' - started > patience c * 1000000
              then pure False
              else threadDelay 5000 >> loop
  loop

-- | After the last step: reads on until the server closes the connection, or until it has said what it is going to, which
-- is when the responses of an outcome are all there and nothing has come for 300 ms, since the last byte or the last
-- step, whichever is later: a response that came before the last step doesn't end the wait for its answer. Whether the
-- server closed it.
settle :: Case -> Client -> MVar (Either SomeException ()) -> IO Bool
settle c client stopped = do
  started <- getMonotonicTimeNSec
  let loop = do
        failIfStopped stopped
        bytes <- readIORef (received client)
        closed <- readIORef (ended client)
        lastGrew <- readIORef (grew client)
        now' <- getMonotonicTimeNSec
        let complete = any (`sayingAll` bytes) (caseExpect c)
        if closed
          then pure True
          else
            if (complete && now' - max lastGrew started > 300 * 1000000) || now' - started > patience c * 1000000
              then pure False
              else threadDelay 5000 >> loop
  loop
  where
    -- The bytes are exactly the responses of the outcome, each of them complete.
    sayingAll outcome bytes =
      let parsed = parseResponses (map rsToHead (outResponses outcome)) False bytes
       in length (parsedResponses parsed) == length (outResponses outcome) && B.null (parsedRest parsed)

-- | Fails the case if the server has stopped: it serves until it is killed, so one that is not serving can't answer.
failIfStopped :: MVar (Either SomeException ()) -> IO ()
failIfStopped stopped =
  tryReadMVar stopped >>= \case
    Nothing -> pure ()
    Just (Left failure) -> failTest ("the server stopped before the exchange ended: " <> displayException failure)
    Just (Right ()) -> failTest "the server returned before the exchange ended"
