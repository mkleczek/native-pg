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

import Control.Arrow (first, returnA, second, (&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (forkFinally, forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, tryReadMVar)
import Control.Exception (IOException, SomeException, bracket, displayException, fromException, throwIO, try)
import Control.Monad (forM_, void, when)
import ConformanceCases
import ConformanceWire
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isJust)
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
import System.IO.Error (ioeGetErrorString, isAlreadyInUseError)
import System.Timeout (timeout)

spec :: Spec
spec = conformance *> counted *> listenerFailure

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

-- | Why a case is expected to fail, if it is: it needs a part of the server that is not there yet. Chunked request
-- bodies, @Expect: 100-continue@ and the timeouts are step 3.
expectedFailure :: Case -> Maybe String
expectedFailure c
  | caseName c `elem` served = Nothing
  | caseArea c == AChunked || caseName c `elem` needingChunkedBodies = Just "chunked request bodies are not there yet (step 3)"
  | caseArea c == AExpect = Just "Expect: 100-continue is not there yet (step 3)"
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

-- | The cases of the other areas that send a chunked request body that is to be decoded.
needingChunkedBodies :: [String]
needingChunkedBodies =
  [ "a chunked body over the limit is answered 413",
    "a chunked body exactly at the limit is echoed",
    "the coding \"chunked \" (trailing space) frames a real chunked body",
    "the coding \"chunked\" followed by a tab frames a real chunked body",
    "the coding \"chunked,\" (empty element after) frames a real chunked body",
    "the coding \", chunked\" (empty element before) frames a real chunked body",
    "a small chunked body the handler ignores is drained and the next request answered",
    "non-pipelining, chunked body, read (Echo)",
    "non-pipelining, chunked body, ignore (Ignore)",
    "pipelining, chunked body, read (Echo)",
    "pipelining, chunked body, ignore (Ignore)",
    "pipelined requests of different kinds are answered in order",
    "a chunked request body is echoed as it arrives, before the last chunk"
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
