{-# LANGUAGE LambdaCase #-}

module PoolReservationSpec (spec) where

import Control.Concurrent.Resource.Waiting qualified as Waiting
import Control.Concurrent (forkFinally, forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (atomically, newTVarIO, orElse, readTVar, retry, writeTVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO, try)
import Control.Monad (void)
import Data.Either (isLeft)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (isNothing)
import Data.Text qualified as Text
import Database.PostgreSQL.Pool
import Skeletest

spec :: Spec
spec = readySpec *> openingSpec

readySpec :: Spec
readySpec = describe "ready connection reservations" $ do
  it "takes only ready connections and returns one exactly once" $ Waiting.withWaiting $ \waiting -> do
    (pool, opened, _) <- harness waiting id
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    useOnce pool `shouldReturn` Right 1
    reservation <- ready pool
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    validateReservation reservation `shouldReturn` True
    useReservation reservation (\connection -> pure (Succeeded, connection)) `shouldReturn` 1
    finishReservation reservation Succeeded
    validateReservation reservation `shouldReturn` False
    rejected <- try @ReservationError (useReservation reservation (\_ -> pure (Succeeded, ())))
    rejected `shouldBe` Left ReservationFinished
    useOnce pool `shouldReturn` Right 1
    readIORef opened `shouldReturn` 1
    release pool

  it "rolls back the ready claim when another STM resource cannot be acquired" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, _) <- harness waiting id
    void (useOnce pool)
    permit <- newTVarIO False
    combined <- atomically $ (do
      reservation <- tryReserveReady pool
      available <- readTVar permit
      if available then pure reservation else retry) `orElse` pure Nothing
    isNothing combined `shouldBe` True
    atomically (writeTVar permit True)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    finishReservation reservation Succeeded
    release pool

  it "validates idleness at the time of use and frees invalid capacity once" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, closed) <- harness waiting (\config -> config{configMaxIdletime = 0.01})
    void (useOnce pool)
    reservation <- ready pool
    threadDelay 30_000
    validateReservation reservation `shouldReturn` False
    validateReservation reservation `shouldReturn` False
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "invalidates a tentative claim across pool release" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    release pool
    validateReservation reservation `shouldReturn` False
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "keeps validated ownership through release and closes at finish" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    release pool
    validateReservation reservation `shouldReturn` True
    finishReservation reservation Succeeded
    finishReservation reservation Succeeded
    readIORef closed `shouldReturn` [1]
    useOnce pool `shouldReturn` Right 2
    release pool

  it "returns an exception-usable connection and rethrows the action exception" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, closed) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    result <- try @ErrorCall (useReservation reservation (\_ -> throwIO (ErrorCall "action failed")) :: IO ())
    result `shouldBe` Left (ErrorCall "action failed")
    readIORef closed `shouldReturn` []
    useOnce pool `shouldReturn` Right 1
    release pool

  it "frees capacity even when closing a broken reservation throws" $ Waiting.withWaiting $ \waiting -> do
    (pool, opened, _) <- harness waiting (\config -> config{configClose = const (throwIO (ErrorCall "close failed"))})
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    void (try @SomeException (finishReservation reservation (Broke Nothing)))
    finishReservation reservation Succeeded
    useOnce pool `shouldReturn` Right 2
    readIORef opened `shouldReturn` 2
    void (try @SomeException (release pool))

  it "hands the connection to an existing use waiter before a new reservation" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, _) <- harness waiting id
    void (useOnce pool)
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    entered <- newEmptyMVar
    done <- newEmptyMVar
    result <- newEmptyMVar
    _ <- forkIO $ use pool (\connection -> putMVar entered () *> takeMVar done *> pure (Succeeded, connection)) >>= putMVar result
    threadDelay 20_000
    finishReservation reservation Succeeded
    takeMVar entered
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    putMVar done ()
    takeMVar result `shouldReturn` Right 1
    release pool

ready :: Pool Int String -> IO (Reservation Int String)
ready pool = atomically (tryReserveReady pool) >>= maybe (fail "expected a ready reservation") pure

useOnce :: Pool Int String -> IO (Either (UsageError String) Int)
useOnce pool = use pool (\connection -> pure (Succeeded, connection))

harness :: Waiting.Waiting -> (Config Int String -> Config Int String) -> IO (Pool Int String, IORef Int, IORef [Int])
harness waiting configure = do
  opened <- newIORef 0
  closed <- newIORef []
  pool <- newPool waiting . configure $ Config
    { configSize = 1
    , configAcquisitionTimeout = 1
    , configMaxLifetime = 3600
    , configMaxIdletime = 3600
    , configConnect = Right <$> atomicModifyIORef' opened (\count -> (count + 1, count + 1))
    , configDescribeFailure = Just . Text.pack
    , configClose = \connection -> atomicModifyIORef' closed (\connections -> (connections <> [connection], ()))
    , configUsableAfterException = const (pure True)
    , configObserve = const (pure ())
    }
  pure (pool, opened, closed)

opening :: Pool Int String -> IO (Opening Int String)
opening pool = atomically (tryReserveOpening pool) >>= maybe (fail "expected an opening slot") pure

openingSpec :: Spec
openingSpec = describe "independent connection startup" $ do
  it "opens a ready connection without occupying a request reservation" $ Waiting.withWaiting $ \waiting -> do
    (pool, opened, _) <- harness waiting id
    startup <- opening pool
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    prepareOpening startup `shouldReturn` Right ()
    rejected <- try @OpeningError (prepareOpening startup)
    rejected `shouldBe` Left OpeningFinished
    abandonOpening startup
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    reservation <- ready pool
    validateReservation reservation `shouldReturn` True
    useReservation reservation (\connection -> pure (Succeeded, connection)) `shouldReturn` 1
    readIORef opened `shouldReturn` 1
    release pool

  it "abandons an unused opening token exactly once" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, _) <- harness waiting id
    first <- opening pool
    abandonOpening first
    abandonOpening first
    second <- opening pool
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    rejected <- try @OpeningError (prepareOpening first)
    rejected `shouldBe` Left OpeningFinished
    prepareOpening second `shouldReturn` Right ()
    useOnce pool `shouldReturn` Right 1
    release pool

  it "returns capacity after a reported startup failure" $ Waiting.withWaiting $ \waiting -> do
    failNext <- newIORef True
    (pool, _, _) <- harness waiting (\config -> config{configConnect = do
      failing <- atomicModifyIORef' failNext (\value -> (False, value))
      if failing then pure (Left "offline") else configConnect config})
    first <- opening pool
    prepareOpening first `shouldReturn` Left (ConnectionFailed "offline")
    second <- opening pool
    prepareOpening second `shouldReturn` Right ()
    useOnce pool `shouldReturn` Right 1
    release pool

  it "returns capacity when a connecting thread is cancelled" $ Waiting.withWaiting $ \waiting -> do
    started <- newEmptyMVar
    gate <- newEmptyMVar
    finished <- newEmptyMVar
    (pool, _, _) <- harness waiting (\config -> config{configConnect = putMVar started () *> takeMVar gate *> configConnect config})
    startup <- opening pool
    thread <- forkFinally (prepareOpening startup) (putMVar finished)
    takeMVar started
    killThread thread
    isLeft <$> takeMVar finished `shouldReturn` True
    abandonOpening startup
    replacement <- opening pool
    isNothing <$> atomically (tryReserveOpening pool) `shouldReturn` True
    abandonOpening replacement
    release pool

  it "closes a connection opened across release instead of recycling its old generation" $ Waiting.withWaiting $ \waiting -> do
    started <- newEmptyMVar
    gate <- newEmptyMVar
    finished <- newEmptyMVar
    (pool, _, closed) <- harness waiting (\config -> config{configConnect = putMVar started () *> takeMVar gate *> configConnect config})
    startup <- opening pool
    _ <- forkIO (prepareOpening startup >>= putMVar finished)
    takeMVar started
    release pool
    putMVar gate ()
    takeMVar finished `shouldReturn` Right ()
    readIORef closed `shouldReturn` [1]
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    replacement <- opening pool
    abandonOpening replacement
    release pool

  it "returns a startup connection to an existing use waiter" $ Waiting.withWaiting $ \waiting -> do
    (pool, _, _) <- harness waiting id
    startup <- opening pool
    entered <- newEmptyMVar
    done <- newEmptyMVar
    result <- newEmptyMVar
    _ <- forkIO $ use pool (\connection -> putMVar entered () *> takeMVar done *> pure (Succeeded, connection)) >>= putMVar result
    threadDelay 20_000
    prepareOpening startup `shouldReturn` Right ()
    takeMVar entered
    isNothing <$> atomically (tryReserveReady pool) `shouldReturn` True
    putMVar done ()
    takeMVar result `shouldReturn` Right 1
    release pool
