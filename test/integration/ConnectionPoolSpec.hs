{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | The connection pool against a real server: how a checkout ends, and what the pool does with its connections as it
-- closes and as they age. Every case is run by each arrow.
--
-- A case that the v2 code can't pass yet is marked with what it needs ('needs'): the marks come off as the part that they
-- wait for lands.
module ConnectionPoolSpec (spec) where

import Control.Arrow ((>>>), (^>>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_)
import qualified Data.ByteString.Char8 as BC
import Data.Int (Int64)
import Data.Text (Text)
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Pool (ConnectionPool, ConnectionPoolSettings (..), connectionPoolSettings, withConnection, withConnectionPool)
import qualified Database.PostgreSQL.Transaction as Tx
import GHC.Clock (getMonotonicTime)
import Network.Socket (PortNumber)
import Network.Streaming.BufferPool (BufferPoolSettings (..))
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import TestPrograms

spec :: FilePath -> PortNumber -> Spec
spec socketPath _port = describe "the connection pool" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let settings = unixSettings socketPath "postgres" Nothing
      -- The pool of this size, for the use, with the buffers that a request runs in, and the way to run a request.
      pooled :: (ConnectionPoolSettings -> ConnectionPoolSettings) -> (Env -> IO a) -> IO a
      pooled change use = do
        buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
        withConnectionPool (change (connectionPoolSettings 1 settings)) $ \pool -> use (Env runner buffers pool)
      -- What the server says a backend is doing, asked on a connection of its own: no row once the backend is gone.
      stateOf :: Int64 -> IO (Either QueryError [Text])
      stateOf pid =
        ordinary runner (session settings (query (plain ("SELECT state FROM pg_stat_activity WHERE pid = " <> BC.pack (show pid)) single)))
      gone :: Int64 -> IO Bool
      gone pid = (== Right []) <$> stateOf pid

  -- How a checkout ends: by the deadline of the pool, or of the arrow that runs it, or by the pool's closing.
  describe "a checkout" $ do
    it "gives CheckoutTimedOut once the pool's timeout has passed, and not before" $
      pooled (\s -> s {poolCheckoutTimeout = Just (millis 100)}) $ \env -> do
        -- The only connection is held by the outer request, so the inner one waits for it.
        started <- getMonotonicTime
        onEnv env (withConnection (envPool env) (snd ^>> withConnection (envPool env) done)) `shouldReturn` Right (Left CheckoutTimedOut)
        ended <- getMonotonicTime
        (ended - started >= 0.1) `shouldBe` True
        (ended - started < 5) `shouldBe` True

    it "gives up its place when the deadline of an enclosing 'within' cancels it, and keeps the connection for the next" $
      pooled id $ \env -> do
        pid <- pidOf env
        onEnv env (withConnection (envPool env) (snd ^>> withinFor (millis 50) (withConnection (envPool env) done)))
          `shouldReturn` Right Nothing
        pidOf env `shouldReturn` pid

    it "gives each of several waiting checkouts its deadline, and serves the one that comes after them" $
      pooled (\s -> s {poolCheckoutTimeout = Just (millis 100)}) $ \env -> do
        pid <- pidOf env
        let waiting :: (Runtime arr) => arr Loans (Either PoolError ())
            waiting = withConnection (envPool env) done
        onEnv env (withConnection (envPool env) (snd ^>> steps [waiting `yields` Left CheckoutTimedOut, waiting `yields` Left CheckoutTimedOut]))
          `shouldReturn` Right ()
        pidOf env `shouldReturn` pid

    it "gives PoolClosed once the pool is closed, though a request still holds its connection" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection, so the checkout doesn't wait for it.
      runOn runner buffers (withConnection pool done) `shouldReturn` Left PoolClosed
      finishHolding holder

  -- What the pool does with its connections as it closes: those that are free close at once, and one that a request still
  -- has closes as the request gives it back.
  describe "closing" $ do
    it "closes the connections that are free" $ do
      pid <- pooled id pidOf
      eventually 5 (gone pid) `shouldReturn` True

    needs "pool eviction" $ it "closes the connection that a request still has, as the request gives it back" $ do
      buffers <- newPool (BufferPoolSettings (positiveInt @1024) (positiveInt @4) (positiveInt @2) (positiveInt @2))
      (pool, holder) <- withConnectionPool (connectionPoolSettings 1 settings) $ \pool -> (pool,) <$> startHolding (Env runner buffers pool)
      -- The pool's bracket has ended while the request holds the only connection: it is still open.
      stateOf (holderBackend holder) `shouldReturn` Right ["idle"]
      finishHolding holder
      eventually 5 (gone (holderBackend holder)) `shouldReturn` True
      -- The pool is used last: while it is reachable, its connection is not closed by the collection of its socket.
      runOn runner buffers (withConnection pool done) `shouldReturn` Left PoolClosed

-- * The pool of the cases

-- | The pool of real connections over the request buffers, and the way to run a program over them.
data Env = Env
  { envRunner :: Runner,
    envBuffers :: BufferPool,
    envPool :: ConnectionPool
  }

-- | The program in one request of its own, on the runner of the environment.
onEnv :: Env -> Program a -> IO a
onEnv env = runOn (envRunner env) (envBuffers env)

-- | The backend of the connection that a request runs on.
pidOf :: Env -> IO Int64
pidOf env = onEnv env (withConnection (envPool env) backend) >>= either (\failure -> failTest ("no connection: " <> show failure)) (either (\failure -> failTest ("no backend: " <> show failure)) pure)

-- | The backend of the connection that a request runs on, as a step on it.
backend :: (Runtime arr) => OnConn arr (Either QueryError Int64)
backend = committed Tx.Read (Tx.statement (plain "SELECT pg_backend_pid()::bigint" single)) Tx.single

-- * A request that outlives its pool's bracket

-- | A request on a thread of its own, which holds the pool's connection until it is told to give it back.
data Holder = Holder
  { -- | The backend of the connection that it holds.
    holderBackend :: Int64,
    holderGoOn :: MVar (),
    holderEnded :: MVar (Either SomeException ())
  }

-- | Starts the request, and gives once it holds the connection.
startHolding :: Env -> IO Holder
startHolding env = do
  held <- newEmptyMVar
  goOn <- newEmptyMVar
  ended <- newEmptyMVar
  let holdIt :: (Runtime arr) => OnConn arr ()
      holdIt = backend >>> now (either (throwIO . userError . show) (\pid -> putMVar held pid *> takeMVar goOn))
  _ <- forkIO (try (onEnv env (withConnection (envPool env) holdIt >>> now (either throwIO pure))) >>= putMVar ended)
  pid <- takeMVar held
  pure (Holder pid goOn ended)

-- | Tells the request to give the connection back, and waits until it has ended.
finishHolding :: Holder -> IO ()
finishHolding holder = putMVar (holderGoOn holder) () *> takeMVar (holderEnded holder) >>= either throwIO pure


-- | Looks until the check is true, for at most this many seconds.
eventually :: Double -> IO Bool -> IO Bool
eventually seconds check = do
  started <- getMonotonicTime
  let go =
        check >>= \ok ->
          if ok
            then pure True
            else getMonotonicTime >>= \time -> if time - started > seconds then pure False else threadDelay 20000 *> go
  go
