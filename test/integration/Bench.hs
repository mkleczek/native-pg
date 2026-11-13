{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | What a request costs: transactions per second and bytes allocated for each, over a Unix-domain socket and a pool of
-- one connection, under each arrow, for a statement that gives one number and for one that gives a field of about 16 KiB
-- and of about 1 MiB, which a request streams and drops. It starts a cluster of its own, as the integration tests do.
-- Every request is as a server's: a transaction on a pooled connection, in loans of its own, whose pipeline carries the
-- COMMIT. Nothing is gated: the numbers are what it prints.
--
-- Usage: @native-pg-bench [scale]@, which multiplies the number of requests of each workload (1 by default).
module Main (main) where

import Control.Arrow (first, returnA, (>>>), (>>^), (^>>), (|||))
import Control.Monad (forM_, unless)
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
import Network.Streaming.Combinators (times)
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

-- | A program for any arrow that has what 'Runtime' bundles.
type Program a = forall arr. (Runtime arr) => arr () a

-- | A way to run a program: an arrow's runner.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners = [Runner "threads" onThreads, Runner "delimited" onDelimited, Runner "compiled" onCompiled]

-- | Each arrow runs a program for the unit: a function of its own, since a program is for any arrow.
onThreads, onDelimited, onCompiled :: Program a -> IO a
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimited program ()
onCompiled program = fst <$> runCompiled program ()

-- | What a request asks the server for: its name, how many requests a measurement makes of it, and the request, for a
-- pool of buffers and a pool of connections.
data Workload = Workload String Int (forall arr. (Runtime arr) => BufferPool -> ConnectionPool -> arr () ())

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
    forM_ runners $ \runner -> forM_ (workloads scale) $ \workload ->
      -- A pool for each measurement, so that its connection is made under the arrow that measures it.
      withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> measure runner buffers pool workload

-- | A statement that gives one number, and ones that give one JSON value, of the tables' rows, as a field.
workloads :: Int -> [Workload]
workloads scale =
  [ Workload "select 1" (20000 * scale) (\buffers pool -> request buffers pool (Tx.Statement "SELECT 1" (const []) (Tx.one (Tx.column @_ @Int32))) (\result -> Tx.single result >>^ const ())),
    Workload "json field, ~16 KiB" (10000 * scale) (field "SELECT json_agg(small) FROM small"),
    Workload "json field, ~1 MiB" (500 * scale) (field "SELECT json_agg(large) FROM large")
  ]
  where
    field sql buffers pool = request buffers pool (Tx.Statement sql (const []) (Tx.one Tx.field)) (\result -> Tx.single result >>> drained >>^ const ())

-- | One request: a transaction on a pooled connection, in loans of its own, whose pipeline carries the COMMIT, and the
-- reading program that takes its one result.
request ::
  (Runtime arr) =>
  BufferPool ->
  ConnectionPool ->
  Tx.Statement arr () row ->
  (Tx.Result row -> With Tx.Tx arr () ()) ->
  arr () ()
request buffers pool given reading =
  withRequestBuffers
    buffers
    ((\(loans, ()) -> (loans, ((), ()))) ^>> withConnection pool (Tx.transaction Tx.ReadCommitted Tx.Read (Tx.pipeline Tx.Final (Tx.statement given) reading)))
    >>> now (\case Right (Right ()) -> pure (); other -> die ("a request failed: " <> show other))

-- | The pieces of the field, one after the other, dropped: how many bytes they had.
drained :: (Runtime arr, Stream arr s) => arr s Int
drained = (,0) ^>> loop
  where
    loop =
      (\(source, total) -> (source, (source, total)))
        ^>> first next
        >>> (\(piece, (source, total)) -> maybe (Left total) (\chunk -> Right (source, total + BS.length chunk)) piece)
        ^>> (returnA ||| loop)

-- | Runs the commands in a transaction of their own, one after the other.
commands :: (Runtime arr) => BufferPool -> ConnectionPool -> [ByteString] -> arr () ()
commands buffers pool texts =
  withRequestBuffers
    buffers
    ((\(loans, ()) -> (loans, ((), ()))) ^>> withConnection pool (Tx.transaction Tx.ReadCommitted Tx.Write (Tx.pipeline Tx.Final (traverse Tx.command texts) (foldr (\result rest -> Tx.discard result >>> rest) returnA))))
    >>> now (\case Right (Right ()) -> pure (); other -> die ("the setup failed: " <> show other))

-- | The workload under the arrow: transactions per second, and the bytes that each allocated. The first requests make
-- the pool's connection, and are not measured.
measure :: Runner -> BufferPool -> ConnectionPool -> Workload -> IO ()
measure (Runner arrowName run) buffers pool (Workload label requests build) = do
  run (times 100 (build buffers pool))
  performGC
  before <- allocated_bytes <$> getRTSStats
  started <- getMonotonicTime
  run (times requests (build buffers pool))
  ended <- getMonotonicTime
  performGC
  after <- allocated_bytes <$> getRTSStats
  printf "%-10s %-22s %12.0f %14.0f\n" arrowName label (fromIntegral requests / (ended - started)) (fromIntegral (after - before) / fromIntegral requests :: Double)

headMaybe :: [a] -> Maybe a
headMaybe = \case [] -> Nothing; first' : _ -> Just first'
