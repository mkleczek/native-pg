{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- | PostgREST's request shape on fulmen and native-pg v2, as small as it can be, to measure what the runtime gives
-- (data-api-e2e's @load/experiments/v2compare@ runs it against postgrestream).
--
-- @GET /rows_16k@ and @GET /rows_1m@ (the query string is ignored) run @select json_agg(t) from rows_16k t@ or @rows_1m@ in
-- one read-only transaction on a pooled connection, take the one field and answer with its bytes: the pieces of the
-- field go from the reply of the database, where they are views of the request's buffer, to @writev@, without a copy.
-- @GET /metrics@ gives the bytes that the runtime allocated and, in debug mode, what the runtime counts (its polls, its
-- armings of descriptors, and the calls on sockets that cost a system call), and any other request is answered 404.
--
-- It does far less than PostgREST (no JWT, no role switch, no query planning, no @set_config@): what it gives is a bound
-- on what the runtime can, not a like-for-like comparison.
--
-- > v2-slice --listen 127.0.0.1:3000 --database postgres://postgres@127.0.0.1:5432/postgres [--runtime threads|delimited|compiled]
-- >          [--mode debug|release] [--cores N] [--pool N] [--connections N]
--
-- The debug mode is built only with @-f debug-mode@: a binary that holds the programs of both modes runs the 'Compiled' one of 'Release'
-- allocating 6% more than a binary that holds the one, which is not what a release build is for. The arrow is chosen where the program is built, as in @fulmen-bench@, so that GHC specialises the handler to it:
-- @threads@ is GHC threads, which the RTS runs on the cores of @+RTS -N@, @delimited@ is one thread for each of
-- @--cores@ cores (2 by default), and @compiled@ is one core. The flags of the run time are not defaults of the binary:
-- they are @GHCRTS@, and @-T@ is what makes the allocation counter of @\/metrics@.
module Main (main) where

import Control.Arrow (arr, first, returnA, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Monad (void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import Database.PostgreSQL.Connection (ConnectSettings)
import Database.PostgreSQL.ConnectionString (Defaults (..), fromConnectionString, readConnectionString)
import Database.PostgreSQL.Pool (ConnectionPool, connectionPoolSettings, withConnection, withConnectionPool)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Stats (RTSStats (allocated_bytes), getRTSStats, getRTSStatsEnabled)
import Network.Fulmen (Call, Method (..), Request (..))
import qualified Network.Fulmen as Fulmen
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes (Effect (..), Runtime, Scope (..), Stream (..))
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, consume, iterateWith, keeping)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Counters (Counts, counterName, toList)
import Network.Streaming.Delimited (runCores)
import Network.Streaming.Mode (Instrumented (counted), Mode (..))
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt)
import Network.Streaming.Threads (runThreads)
#if defined(DEBUG_MODE)
import Network.Streaming.Compiled (runCompiledM)
import Network.Streaming.Delimited (runCoresM)
import Network.Streaming.Threads (runThreadsM)
#endif
import Network.Streaming.With (With)
import System.Environment (getArgs)
import System.Exit (die)
import System.IO (BufferMode (..), hSetBuffering, stdout)
import System.Mem (performMinorGC)
import Text.Read (readMaybe)

-- * The handler

-- | What a request is answered with: the rows of the tables that the list names, which are fixed, and the metrics. The
-- arrow is the caller's, which has to know it for the program to be specialised: see 'main'.
handler :: (Runtime arr) => IO (Maybe Counts) -> ConnectionPool -> arr (Call, Request) ()
handler counts pool =
  routed
    [ ("/rows_16k", rows pool "select json_agg(t) from rows_16k t"),
      ("/rows_1m", rows pool "select json_agg(t) from rows_1m t"),
      ("/metrics", metrics counts)
    ]
{-# INLINABLE handler #-}

-- | Runs the program of the path that a GET asks for, and answers 404 to any other request. The query string is not
-- part of the path.
routed :: (Runtime arr) => [(ByteString, arr Call ())] -> arr (Call, Request) ()
routed = foldr step missing
  where
    step (path, serving) rest = (\(call, request) -> if asks path request then Left call else Right (call, request)) ^>> (serving ||| rest)
    asks path request = method request == Method "GET" && B.takeWhile (/= questionMark) (target request) == path
    questionMark = 63
    missing = (,(404, "not found\n")) . fst ^>> plain

-- | The rows of a table: one read-only transaction on a pooled connection, in the loans of the call, whose pipeline
-- carries the COMMIT. Its reading program takes the one field of the one row and answers with it. A failure of the
-- transaction, of the pool or of the answer is a 500 if the head has not been given, and else the connection ends,
-- which fulmen does when a handler gives the head twice.
rows :: (Runtime arr) => ConnectionPool -> ByteString -> arr Call ()
rows pool sql =
  alongside (tryA (Fulmen.loans &&& arr ((),) >>> withConnection pool (Tx.transaction Tx.ReadCommitted Tx.Read (Tx.pipeline Tx.Final (Tx.statement query) reading))))
    >>> unlessSucceeded
    ^>> (failed ||| returnA)
  where
    unlessSucceeded (Right (Right (Right ())), _) = Right ()
    unlessSucceeded (_, call) = Left call
    query = Tx.Statement sql (const []) (Tx.one Tx.field)
    failed = (,(500, "internal error\n")) ^>> plain

-- | Takes the field of the result, says how long it is with the head, sends its pieces as they arrive, and ends the
-- response.
reading :: (Runtime arr) => Tx.Result arr Tx.Field -> With Tx.Tx arr Call ()
reading result =
  keeping (Tx.single result)
    >>> withHead
    ^>> first Fulmen.respond
    >>> pieces
  where
    withHead (field, call) =
      ((call, (Fulmen.Status 200, [("Content-Type", "application/json; charset=utf-8")], Fulmen.Known (Tx.fieldLength field))), field)

-- | The pieces of the field, one after the other, each sent before the next is taken: they are views, which are valid
-- until the next step on the field. At its end, the response ends.
pieces :: (Runtime arr, Stream arr s) => arr (Fulmen.Answering, s) ()
pieces = iterateWith (withSource ^>> first next >>^ chunked) (consume Fulmen.send) >>> Fulmen.finish
  where
    withSource (answering, source) = (source, (answering, source))
    chunked (Nothing, (answering, _)) = Finish answering
    chunked (Just chunk, (answering, source)) = Again ((answering, [chunk]), (answering, source))

-- | The metrics: the bytes that the runtime allocated, and in debug mode the counts of "Network.Streaming.Counters", as Prometheus text.
-- It needs @+RTS -T@: without it the answer says so, and is a 503. The runtime counts what a nursery held when it is
-- collected, and not before: with the nurseries of @-A16m@ the counter lags by up to as many bytes as there are cores times
-- 16 MiB, which is a good part of what a few thousand small requests allocate. So a minor collection is made before it is
-- read.
metrics :: (Runtime arr) => IO (Maybe Counts) -> arr Call ()
metrics counts = ((,) <$> returnA <*> now (const exposition)) >>> plain
  where
    exposition =
      getRTSStatsEnabled >>= \case
        False -> pure (503, "the runtime keeps no statistics: run with +RTS -T\n")
        True -> do
          performMinorGC
          stats <- getRTSStats
          counted' <- foldMap (\(counter, count) -> counting ("streaming_io_" <> counterName counter <> "_total") (toInteger count)) . maybe [] toList <$> counts
          pure (200, counting "ghc_allocated_bytes_total" (fromIntegral (allocated_bytes stats)) <> counted')
    counting :: String -> Integer -> ByteString
    counting name count = C.pack ("# TYPE " <> name <> " counter\n" <> name <> " " <> show count <> "\n")

-- | An answer of one piece of text: the head with its length, the text and the end.
plain :: (Runtime arr) => arr (Call, (Int, ByteString)) ()
plain =
  withHead
    ^>> first Fulmen.respond
    >>> withBody
    ^>> consume Fulmen.send
    >>> Fulmen.finish
  where
    withHead (call, (status, text)) =
      ((call, (Fulmen.Status status, [("Content-Type", "text/plain; charset=utf-8")], Fulmen.Known (B.length text))), text)
    withBody (answering, text) = ((answering, [text]), answering)

-- * The flags

-- | The arrow that runs the server.
data Kind = Threads | Delimited | Compiled

data Options = Options
  { optListen :: Maybe (String, String),
    optDatabase :: Maybe String,
    optRuntime :: Kind,
    optMode :: Mode,
    optCores :: Int,
    optPool :: Int,
    optConnections :: Int
  }

usage :: String
usage =
  unlines
    [ "usage: v2-slice --listen HOST:PORT --database postgres://USER@HOST:PORT/DB [--runtime threads|delimited|compiled]",
      "                 [--mode debug|release] [--cores N] [--pool N] [--connections N]",
      "  --listen       a numeric address and a port",
      "  --runtime      the arrow that runs the server (default delimited)",
      "  --mode         debug counts what the runtime does, and /metrics lists the counts; release (the default) counts nothing",
      "  --cores        the cores of delimited (default 2): threads takes its cores from +RTS -N, and compiled has one",
      "  --pool         the connections to the database (default 10)",
      "  --connections  the connections that the server holds at once (default 1200)"
    ]

parseOptions :: [String] -> Either String Options
parseOptions = go (Options Nothing Nothing Delimited Release 2 10 1200)
  where
    go options = \case
      [] -> Right options
      "--listen" : value : rest -> address value >>= \listen -> go options {optListen = Just listen} rest
      "--database" : value : rest -> go options {optDatabase = Just value} rest
      "--runtime" : value : rest -> kind value >>= \chosen -> go options {optRuntime = chosen} rest
      "--mode" : value : rest -> chosenMode value >>= \chosen -> go options {optMode = chosen} rest
      "--cores" : value : rest -> count "--cores" value >>= \n -> go options {optCores = n} rest
      "--pool" : value : rest -> count "--pool" value >>= \n -> go options {optPool = n} rest
      "--connections" : value : rest -> count "--connections" value >>= \n -> go options {optConnections = n} rest
      flag : _ -> Left ("what is " <> flag <> "?")
    kind = \case
      "threads" -> Right Threads
      "delimited" -> Right Delimited
      "compiled" -> Right Compiled
      other -> Left ("--runtime is threads, delimited or compiled, not " <> other)
    chosenMode = \case
      "debug" -> Right Debug
      "release" -> Right Release
      other -> Left ("--mode is debug or release, not " <> other)
    count flag text = case readMaybe text of
      Just n | n > 0 -> Right n
      _ -> Left (flag <> " takes a number above 0, not " <> text)

-- | The host and the port of @HOST:PORT@, where the host may be an IPv6 address in brackets.
address :: String -> Either String (String, String)
address text = case break (== ':') (reverse text) of
  (port@(_ : _), ':' : host@(_ : _)) -> Right (unbracketed (reverse host), reverse port)
  _ -> Left ("--listen takes HOST:PORT, not " <> text)
  where
    unbracketed = \case
      '[' : inner | not (null inner), last inner == ']' -> init inner
      host -> host

-- * The server

-- | A socket that listens on the numeric address.
listening :: (String, String) -> IO Socket
listening (host, port) = do
  let hints = Socket.defaultHints {Socket.addrFlags = [Socket.AI_NUMERICHOST, Socket.AI_NUMERICSERV, Socket.AI_PASSIVE], Socket.addrSocketType = Socket.Stream}
  Socket.getAddrInfo (Just hints) (Just host) (Just port) >>= \case
    [] -> die ("no address for " <> host <> ":" <> port)
    found : _ -> do
      socket <- Socket.socket (Socket.addrFamily found) Socket.Stream Socket.defaultProtocol
      Socket.setSocketOption socket Socket.ReuseAddr 1
      Socket.bind socket (Socket.addrAddress found)
      Socket.listen socket 2048
      pure socket

-- | How the pool connects: the settings of the connection string, which v2compare gives with trust authentication.
connectSettingsOf :: String -> IO ConnectSettings
connectSettingsOf uri = readConnectionString (Defaults "localhost" Nothing) (C.pack uri) >>= either die (either die pure . fromConnectionString)

positive :: Int -> IO PositiveInt
positive n = maybe (die ("not a positive count: " <> show n)) pure (mkPositiveInt (toInteger n))

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  options <- getArgs >>= either (\problem -> die (problem <> "\n" <> usage)) pure . parseOptions
  (listen, uri) <- maybe (die ("--listen and --database are needed\n" <> usage)) pure ((,) <$> optListen options <*> optDatabase options)
  connect <- connectSettingsOf uri
  listener <- listening listen
  -- One worker for each connection that is held at once, the connections being shared out among the cores that run them
  -- (the other runners have one core, which is all of them), and a buffer of the pool for each, since a request holds one.
  let runningCores = case optRuntime options of Delimited -> optCores options; _ -> 1
      perCore = (optConnections options + runningCores - 1) `div` runningCores
  -- The buffers of requests in flight, a reserve for the second loan of a request (the head of a response, while its
  -- transaction holds the first), and the buffers that a connection is set up with, which a pool of connections has of
  -- its own as well.
  size <- positive 16384
  ordinary <- positive (perCore * runningCores)
  reserve <- positive 16
  setup <- positive (optPool options)
  buffers <- newBufferPool (BufferPoolSettings size ordinary reserve setup)
  -- The client timeout is off, as fulmen-bench has it: the server doesn't enforce it yet.
  let settings = (Fulmen.defaultSettings listener buffers) {Fulmen.connectionsPerCore = perCore, Fulmen.clientTimeout = Nothing}
  shared <- Fulmen.newCores runningCores
  putStrLn ("listening on " <> fst listen <> ":" <> snd listen)
  -- The arrow is known where the handler is built, so that GHC specialises it to the arrow.
  withConnectionPool (connectionPoolSettings (optPool options) connect) $ \pool -> case (optMode options, optRuntime options) of
    (Release, Threads) -> runThreads (Fulmen.serve settings (handler (counted @'Release) pool)) shared
    (Release, Delimited) -> void (runCores runningCores (pure shared) (\cores core -> const (Fulmen.onCore cores core) ^>> Fulmen.serve settings (handler (counted @'Release) pool)))
    (Release, Compiled) -> void (runCompiled (Fulmen.serve settings (handler (counted @'Release) pool)) shared)
#if defined(DEBUG_MODE)
    (Debug, Threads) -> runThreadsM @'Debug (Fulmen.serve settings (handler (counted @'Debug) pool)) shared
    (Debug, Delimited) -> void (runCoresM @'Debug runningCores (pure shared) (\cores core -> const (Fulmen.onCore cores core) ^>> Fulmen.serve settings (handler (counted @'Debug) pool)))
    (Debug, Compiled) -> void (runCompiledM @'Debug (Fulmen.serve settings (handler (counted @'Debug) pool)) shared)
#else
    (Debug, _) -> die "this build has no debug mode: build the slice with -f debug-mode"
#endif
