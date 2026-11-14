{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeApplications #-}

-- | What a request costs: transactions per second and bytes allocated for each, over a Unix-domain socket and a pool of
-- one connection, under each arrow, for a statement that gives one number and for one that gives a field of about 16 KiB
-- and of about 1 MiB, which a request streams and drops. It starts a cluster of its own, as the integration tests do.
-- Every request is as a server's: a transaction on a pooled connection, in loans of its own, whose pipeline carries the
-- COMMIT. Nothing is gated: the numbers are what it prints.
--
-- Each arrow runs a program that is built where the arrow is known, as a server builds its own, so that GHC specialises
-- the program to the arrow: one that is given the arrow's dictionaries at run time allocates a few times as much.
--
-- Usage: @native-pg-bench [scale]@, which multiplies the number of requests of each workload (1 by default).
module Main (main) where

import Control.Arrow (returnA, second, (>>>), (>>^), (^>>))
import Control.Monad (forM_, unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Int (Int32)
import Database.PostgreSQL.Connection (Address (..), ConnectSettings (..), connectSettings)
import Database.PostgreSQL.Pool (ConnectionPool, connectionPoolSettings, withConnection, withConnectionPool)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import GHC.Stats (allocated_bytes, getRTSStats, getRTSStatsEnabled)
import Network.Streaming.BufferPool (BufferPool, BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, done, iterateA, times)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Loans (withRequestBuffers)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads (runThreads)
import Network.Streaming.With (With)
import System.Environment (getArgs)
import System.Exit (die)
import System.FilePath (takeDirectory)
import System.Mem (performGC)
import System.Process (readProcess)
import TestCluster (withCluster)
import Text.Printf (printf)

-- | What a request asks the server for.
data Workload = SelectOne | Field16 | Field1M
  deriving (Eq, Show, Enum, Bounded)

-- | The name of a workload, and how many requests a measurement makes of it.
label :: Workload -> String
label = \case
  SelectOne -> "select 1"
  Field16 -> "json field, ~16 KiB"
  Field1M -> "json field, ~1 MiB"

requestsOf :: Workload -> Int
requestsOf = \case
  SelectOne -> 20000
  Field16 -> 10000
  Field1M -> 500

-- | The arrows, each of which runs a request of a workload the given number of times, for a pool of buffers and a pool
-- of connections. A server builds its program where its arrow is known, and so does each of these: 'build' is for any
-- arrow, and is specialised to each.
runners :: [(String, Workload -> BufferPool -> ConnectionPool -> Int -> IO ())]
runners =
  [ ("threads", \workload buffers pool n -> runThreads (times n (build workload buffers pool)) ()),
    ("delimited", \workload buffers pool n -> void (runDelimited (times n (build workload buffers pool)) ())),
    ("compiled", \workload buffers pool n -> void (runCompiled (times n (build workload buffers pool)) ()))
  ]

main :: IO ()
main = do
  enabled <- getRTSStatsEnabled
  unless enabled (die "native-pg-bench needs +RTS -T")
  scale <- maybe 1 read . headMaybe <$> getArgs
  withCluster $ \socketPath port -> do
    let settings = (connectSettings (UnixAddress socketPath) "postgres") {connectDatabase = Just "postgres"}
    buffers <- newBufferPool (BufferPoolSettings (positiveInt @16384) (positiveInt @4) (positiveInt @2) (positiveInt @2))
    -- The tables that the fields are made of: 352 rows of about 46 bytes in JSON, which is 16 KiB, and 22,500, which is 1 MiB.
    withConnectionPool (connectionPoolSettings 1 settings) $ \pool ->
      runThreads
        ( commands
            buffers
            pool
            [ "CREATE TABLE small AS SELECT g AS id, repeat('x', 20) AS value FROM generate_series(1, 352) g",
              "CREATE TABLE large AS SELECT g AS id, repeat('x', 20) AS value FROM generate_series(1, 22500) g"
            ]
        )
        ()
    -- What the fields are, as the server says.
    forM_ ["small", "large"] $ \table -> do
      size <- readProcess "psql" ["--host", takeDirectory socketPath, "--port", show port, "--username", "postgres", "--dbname", "postgres", "--no-psqlrc", "--tuples-only", "--no-align", "--command", "SELECT length(json_agg(" <> table <> ")::text) FROM " <> table] ""
      printf "the field of %s has %s bytes\n" (table :: String) (filter (/= '\n') size)
    printf "%-10s %-22s %12s %14s\n" ("arrow" :: String) ("workload" :: String) ("tx/s" :: String) ("bytes/tx" :: String)
    forM_ runners $ \(name, run) -> forM_ [minBound .. maxBound] $ \workload ->
      -- A pool for each measurement, so that its connection is made under the arrow that measures it.
      withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> measure name workload (requestsOf workload * scale) (run workload buffers pool)

-- | A statement that gives one number, and ones that give one JSON value, of the tables' rows, as a field.
build :: (Runtime arr) => Workload -> BufferPool -> ConnectionPool -> arr () ()
build workload buffers pool = case workload of
  SelectOne -> request buffers pool (Tx.Statement "SELECT 1" (const []) (Tx.one (Tx.column @_ @Int32))) (\result -> Tx.single result >>> done)
  Field16 -> field "SELECT json_agg(small) FROM small"
  Field1M -> field "SELECT json_agg(large) FROM large"
  where
    field sql = request buffers pool (Tx.Statement sql (const []) (Tx.one Tx.field)) (\result -> Tx.single result >>> drained >>> done)
{-# INLINABLE build #-}

-- | One request: a transaction on a pooled connection, in loans of its own, whose pipeline carries the COMMIT, and the
-- reading program that takes its one result.
request ::
  (Runtime arr) =>
  BufferPool ->
  ConnectionPool ->
  Tx.Statement arr () row ->
  (Tx.Result arr row -> With Tx.Tx arr () ()) ->
  arr () ()
request buffers pool given reading =
  (buffers,)
    ^>> withRequestBuffers (second (const ((), ())) ^>> withConnection pool (Tx.transaction Tx.ReadCommitted Tx.Read (Tx.pipeline Tx.Final (Tx.statement given) reading)))
    >>> now (\case Right (Right ()) -> pure (); other -> die ("a request failed: " <> show other))

-- | The pieces of the field, one after the other, dropped: how many bytes they had.
drained :: (Runtime arr, Stream arr s) => arr s Int
drained = (,0) ^>> iterateA (alongside (fst ^>> next) >>^ counted)
  where
    counted (piece, (source, total)) = maybe (Finish total) (\chunk -> Again (source, total + BS.length chunk)) piece

-- | Runs the commands in a transaction of their own, one after the other.
commands :: (Runtime arr) => BufferPool -> ConnectionPool -> [ByteString] -> arr () ()
commands buffers pool texts =
  (buffers,)
    ^>> withRequestBuffers (second (const ((), ())) ^>> withConnection pool (Tx.transaction Tx.ReadCommitted Tx.Write (Tx.pipeline Tx.Final (traverse Tx.command texts) (foldr ((>>>) . Tx.discard) returnA))))
    >>> now (\case Right (Right ()) -> pure (); other -> die ("the setup failed: " <> show other))

-- | The workload under the arrow: transactions per second, and the bytes that each allocated. The first requests make
-- the pool's connection, and are not measured.
measure :: String -> Workload -> Int -> (Int -> IO ()) -> IO ()
measure arrowName workload requests run = do
  run 100
  performGC
  before <- allocated_bytes <$> getRTSStats
  started <- getMonotonicTime
  run requests
  ended <- getMonotonicTime
  performGC
  after <- allocated_bytes <$> getRTSStats
  printf "%-10s %-22s %12.0f %14.0f\n" arrowName (label workload) (fromIntegral requests / (ended - started)) (fromIntegral (after - before) / fromIntegral requests :: Double)

headMaybe :: [a] -> Maybe a
headMaybe = \case [] -> Nothing; first' : _ -> Just first'
