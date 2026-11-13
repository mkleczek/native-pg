{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The conformance cases of "ConformanceCases", run against a fulmen server: each case starts a server with its
-- handler and settings on a free port, sends its steps over one TCP connection, and checks what came back, as the
-- header of "ConformanceCases" says.
--
-- Each case is run under each runner of the arrow runtime. A server that stops while a case runs fails the case at once,
-- with what it stopped with, so that a server that is not there yet is told from one that answers wrongly.
--
-- Every area of cases is marked as an expected failure until the server is there.
module ConformanceSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Arrow (arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkFinally, forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, tryReadMVar)
import Control.Exception (IOException, SomeException, bracket, displayException, throwIO, try)
import Control.Monad (forM_, void)
import ConformanceCases
import ConformanceWire
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isJust)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import Network.Fulmen (Call, Cores, Request (..))
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Network.Streaming.BufferPool (BufferPool, PoolSettings (..), newBufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (keeping, sleep)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads (runThreads)
import Skeletest

spec :: Spec
spec = describe "HTTP/1.1 conformance" $ forM_ runners $ \runner@(Runner runnerName _) -> describe runnerName $
  forM_ [minBound .. maxBound] $ \area ->
    case filter ((== area) . caseArea) cases of
      [] -> pure ()
      inArea -> notYet $ describe (areaTitle area) $ forM_ inArea $ \c -> it (caseName c) (runCase runner c)

-- | Marks every case of the area as an expected failure: the server is not there yet. The mark comes off as the part
-- that the area covers lands, and a case that then passes unexpectedly fails the suite.
notYet :: Spec -> Spec
notYet = xfail "v2: not implemented"

-- * Runners

-- | A way to run the server: an arrow's runner, given the settings, the handler and the cores.
data Runner = Runner String (Fulmen.Settings -> Handling -> Cores -> IO ())

-- | A handler, for any arrow that has what 'Runtime' bundles.
newtype Handling = Handling (forall arr. (Runtime arr) => arr (Call, Request) ())

runners :: [Runner]
runners =
  [ Runner "threads" (\settings (Handling handler) cores -> runThreads (Fulmen.serve settings handler) cores),
    Runner "delimited" (\settings (Handling handler) cores -> void (runDelimited (Fulmen.serve settings handler) cores)),
    Runner "compiled" (\settings (Handling handler) cores -> void (runCompiled (Fulmen.serve settings handler) cores))
  ]

-- * The handlers of the cases

-- | The handler of the case, as the header of "ConformanceCases" describes it.
handling :: Handler -> Handling
handling handler = case handler of
  Echo -> Handling echo
  Fixed status fields content -> Handling ((\(call, _) -> (call, (Fulmen.Status status, fields, content))) ^>> reply)
  Streamed chunks -> Handling (streamedWith [writing chunk | chunk <- chunks])
  Headers -> Handling ((\(call, request) -> (call, (Fulmen.Status 200, [], headersOf request))) ^>> reply)
  Ignore -> Handling ((\(call, _) -> (call, (Fulmen.Status 200, [], ignoreBody))) ^>> reply)
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
  (\(call, (status, fields, content)) -> ((call, (status, fields, Fulmen.Known (B.length content))), content))
    ^>> first Fulmen.respond
    >>> (\(answering, content) -> (answering, [content]))
    ^>> (Fulmen.send &&& arr fst)
    >>> arr snd
    >>> Fulmen.finish

-- | The whole request body, as the response.
echo :: (Runtime arr) => arr (Call, Request) ()
echo =
  fst
    ^>> (returnA &&& (Fulmen.body >>> collect))
    >>> (\(call, content) -> (call, (Fulmen.Status 200, [], content)))
    ^>> reply

-- | A 200 of unknown length, whose body the steps send, one after the other.
streamedWith :: (Runtime arr) => [arr Fulmen.Answering Fulmen.Answering] -> arr (Call, Request) ()
streamedWith pieces =
  (\(call, _) -> (call, (Fulmen.Status 200, [], Fulmen.Unknown)))
    ^>> Fulmen.respond
    >>> foldr (>>>) returnA pieces
    >>> Fulmen.finish

-- | Writes the chunk, flushed. A chunk that is nothing is written as nothing: it must not end a chunked body.
writing :: (Runtime arr) => ByteString -> arr Fulmen.Answering Fulmen.Answering
writing chunk
  | B.null chunk = returnA
  | otherwise = (, [chunk]) ^>> (Fulmen.send &&& arr fst) >>^ snd

-- | Waits this many milliseconds, which is a wait of the arrow.
pause :: (Runtime arr) => Int -> arr a a
pause delay = keeping (sleep (millis (fromIntegral delay))) >>^ snd

-- | Each piece of the request body, sent as soon as it has arrived.
echoStreaming :: (Runtime arr) => arr (Call, Request) ()
echoStreaming =
  fst
    ^>> (returnA &&& Fulmen.body)
    >>> first ((, (Fulmen.Status 200, [], Fulmen.Unknown)) ^>> Fulmen.respond)
    >>> (\(answering, requestBody) -> (requestBody, answering))
    ^>> loop
  where
    loop =
      (\(requestBody, answering) -> (requestBody, (requestBody, answering)))
        ^>> first next
        >>> (\(piece, (requestBody, answering)) -> maybe (Left answering) (\view' -> Right (view', requestBody, answering)) piece)
        ^>> (Fulmen.finish ||| ((\(view', requestBody, answering) -> ((answering, [view']), (requestBody, answering))) ^>> first Fulmen.send >>> arr snd >>> loop))

-- | The pieces of the stream, copied out of the views and put together.
collect :: (Runtime arr, Stream arr s) => arr s ByteString
collect = (,[]) ^>> loop >>^ (B.concat . reverse)
  where
    loop =
      (\(source, copies) -> (source, (source, copies)))
        ^>> first next
        >>> now
          ( \(piece, (source, copies)) -> case piece of
              Nothing -> pure (Left copies)
              Just view' -> let !copy = B.copy view' in pure (Right (source, copy : copies))
          )
        >>> (returnA ||| loop)

-- * Running a case

-- | How long the client waits for what it expects: the case's silence timeout comes on top.
patience :: Case -> Word64
patience c = 3000 + maybe 0 fromIntegral (timeoutMs (caseSettings c))

runCase :: Runner -> Case -> IO ()
runCase (Runner _ runServer) c = do
  acquisition <- getAcquisition
  pool <- newBufferPool acquisition (PoolSettings (positiveInt @16384) (positiveInt @16) (positiveInt @2) (positiveInt @2))
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \accepting -> do
    Socket.bind accepting (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen accepting 128
    port <- Socket.socketPort accepting
    cores <- Fulmen.newCores 1
    stopped <- newEmptyMVar
    bracket (forkFinally (runServer (settingsOf (caseSettings c) accepting pool) (handling (caseHandler c)) cores) (putMVar stopped)) killThread $ \_ ->
      withClient port $ \client -> exchange c client stopped

-- | The settings of the server: the defaults, and the case's where it names any.
settingsOf :: Settings -> Socket -> BufferPool -> Fulmen.Settings
settingsOf given accepting pool =
  base
    { Fulmen.lineLimit = fromMaybe (Fulmen.lineLimit base) (lineLimit given),
      Fulmen.headLimit = fromMaybe (Fulmen.headLimit base) (headLimit given),
      Fulmen.bodyLimit = bodyLimit given,
      Fulmen.clientTimeout = maybe (Fulmen.clientTimeout base) (Just . millis . fromIntegral) (timeoutMs given)
    }
  where
    base = Fulmen.defaultSettings accepting pool

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
-- is when the responses of an outcome are all there and nothing has come for 300 ms. Whether the server closed it.
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
            if (complete && now' - lastGrew > 300 * 1000000) || now' - started > patience c * 1000000
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
