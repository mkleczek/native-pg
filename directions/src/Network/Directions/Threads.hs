{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | A thread for each concurrent part: an arrow is a function to IO, and a
-- link blocks its thread.
module Network.Directions.Threads
  ( Threads,
    runThreads,
  )
where

import Control.Arrow (Arrow, ArrowChoice, Kleisli (..), returnA)
import Control.Category (Category)
import Control.Concurrent (forkIO, killThread, threadWaitRead, threadWaitWrite)
import Control.Concurrent.Chan (newChan, readChan, writeChan)
import Control.Concurrent.STM (TMVar, atomically, newEmptyTMVarIO, readTMVar, tryPutTMVar)
import Control.Exception (SomeException, finally, throwIO, try)
import Control.Monad (unless)
import Network.Directions.Classes
import Network.Streaming.Watch (awaitWatch)

-- | An arrow whose concurrent parts run on threads of their own.
newtype Threads a b = Threads (Kleisli IO a b)
  deriving newtype (Category, Arrow, ArrowChoice)

runThreads :: Threads a b -> a -> IO b
runThreads (Threads (Kleisli run)) = run

threads :: (a -> IO b) -> Threads a b
threads = Threads . Kleisli

instance Effect Threads where
  now = threads

-- | GHC's scheduler preempts a thread, so a part has no turn to give way at.
instance GiveWay Threads where
  giveWay = returnA

-- | Each on a thread of its own. A failure of either stops both, and is
-- rethrown.
instance Concurrent Threads where
  concurrently one two = threads $ \input -> do
    results <- newChan
    started <-
      traverse
        forkIO
        [ try (Left <$> runThreads one input) >>= writeChan results,
          try (Right <$> runThreads two input) >>= writeChan results
        ]
    let collect (Just b) (Just d) = pure (b, d)
        collect b d =
          readChan results >>= \case
            Left (failure :: SomeException) -> throwIO failure
            Right (Left b') -> collect (Just b') d
            Right (Right d') -> collect b (Just d')
    collect Nothing Nothing `finally` mapM_ killThread started

-- | Blocks the thread: on a descriptor directly, as 'threadWaitRead' does, and
-- on any other event by its watch.
instance Block Threads where
  block = threads $ \case
    Readable fd -> threadWaitRead fd
    Writable fd -> threadWaitWrite fd
    Watched register -> awaitWatch register

-- | A ticket is a 'TMVar', which blocks the thread that waits for it.
instance Exchange Threads where
  newtype Ticket Threads a = ThreadTicket (TMVar a)
  newTicket = ThreadTicket <$> newEmptyTMVarIO
  fulfil (ThreadTicket var) = threads $ \value -> do
    given <- atomically (tryPutTMVar var value)
    unless given $ ioError (userError "a ticket was given a value twice")
  await (ThreadTicket var) = threads (\() -> atomically (readTMVar var))
