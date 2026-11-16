{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}

-- | 'update' of a reference that holds a thunk: a writer that stores the value of a computation, and not the value, as a
-- connection's mark was stored. The change gives the same result whichever way the compiler builds it: matched on the value inside
-- the loop of the compare-and-swap, called as a function that the loop knows nothing of, or run as a step of a program of each arrow.
module AtomicSpec (spec) where

import Control.Arrow (ArrowChoice)
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_)
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Data.Version (makeVersion)
import GHC.Exts (noinline)
import Network.Streaming.Classes (Annotate, Effect)
import Network.Streaming.Combinators (step)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Internal.Atomic (update)
import Network.Streaming.Threads
import Skeletest
import System.IO.Unsafe (unsafeInterleaveIO)
import System.Info (compilerVersion)
import System.Timeout (timeout)

-- | A connection's mark, as the idle timeout of a server keeps it.
data Mark = Idle !Int | Busy | TimedOut
  deriving (Eq, Show)

-- | A reference that holds a thunk, which makes the mark when it is evaluated, and not the mark.
holdingThunk :: Mark -> IO (IORef Mark)
holdingThunk mark = do
  ref <- newIORef Busy
  thunk <- unsafeInterleaveIO (pure mark)
  atomicWriteIORef ref thunk
  pure ref

-- | Takes the mark if the connection waits, as the part that serves requests does: the change matches on the value.
takeIdle :: IORef Mark -> IO Bool
takeIdle ref = update ref (\case Idle _ -> (Busy, True); other -> (other, False))
{-# NOINLINE takeIdle #-}

-- | The same with a change that the compare-and-swap is not compiled with.
takeIdleWith :: (Mark -> (Mark, Bool)) -> IORef Mark -> IO Bool
takeIdleWith change ref = update ref change
{-# NOINLINE takeIdleWith #-}

-- | What the cases' programs need of an arrow.
type Program = forall arr. (ArrowChoice arr, Effect arr, Annotate arr) => arr (IORef Mark) Bool

-- | The program of the part that serves requests: a step that takes the mark.
taken :: Program
taken = step "take the mark" (\ref -> update ref (\case Idle _ -> (Busy, True); other -> (other, False)))

data Runner = Runner String (Program -> IORef Mark -> IO Bool)

runners :: [Runner]
runners =
  [ Runner "threads" (\program ref -> runThreads program ref),
    Runner "delimited" (\program ref -> fst <$> runDelimited program ref),
    Runner "compiled" (\program ref -> fst <$> runCompiled program ref)
  ]

-- | A case that gives up after a few seconds: a compare-and-swap that never succeeds spins for ever.
within10s :: IO a -> IO a
within10s action = timeout 10000000 action >>= maybe (failTest "the compare-and-swap of 'update' does not end") pure

-- | The same for an action that runs masked, which a timeout cannot interrupt: it runs on a thread of its own, which is left behind if
-- it does not end.
withinOnAThread10s :: IO a -> IO a
withinOnAThread10s action = do
  finished <- newEmptyMVar
  _ <- forkIO (try action >>= putMVar finished)
  timeout 10000000 (takeMVar finished) >>= \case
    Nothing -> failTest "the compare-and-swap of 'update' does not end"
    Just (Left failure) -> throwIO (failure :: SomeException)
    Just (Right result) -> pure result

-- | Marks a case that fails on GHC 9.12 and later, whose compare-and-swap does not end on a reference that holds a thunk. The mark
-- comes off when it ends, and a case that then passes unexpectedly fails the suite.
spinsOn912 :: Spec -> Spec
spinsOn912 = if compilerVersion >= makeVersion [9, 12] then xfail "the compare-and-swap of 'update' spins on a thunk (GHC 9.12)" else id

spec :: Spec
spec = describe "update of a reference that holds a thunk" $ do
  spinsOn912 $ it "takes the mark when the change matches on the value" $ do
    ref <- holdingThunk (Idle 7)
    within10s (takeIdle ref) `shouldReturn` True
    readIORef ref `shouldReturn` Busy

  spinsOn912 $ it "leaves the value when the change matches on it and keeps it" $ do
    ref <- holdingThunk TimedOut
    within10s (takeIdle ref) `shouldReturn` False
    readIORef ref `shouldReturn` TimedOut

  it "takes the mark when the change is a function that is not known" $ do
    ref <- holdingThunk (Idle 7)
    within10s (takeIdleWith (\case Idle _ -> (Busy, True); other -> (other, False)) ref) `shouldReturn` True
    readIORef ref `shouldReturn` Busy

  it "takes the mark when 'update' is not inlined into the caller" $ do
    ref <- holdingThunk (Idle 7)
    within10s (noinline update ref (\case Idle _ -> (Busy, True); other -> (other, False))) `shouldReturn` True
    readIORef ref `shouldReturn` Busy

  it "replaces the thunk when the change does not look at it" $ do
    ref <- holdingThunk (Idle 7)
    within10s (update ref (const (TimedOut, ()))) `shouldReturn` ()
    readIORef ref `shouldReturn` TimedOut

  forM_ runners $ \(Runner name run) -> describe name $
    spinsOn912 $ it "takes the mark in a step of a program" $ do
      ref <- holdingThunk (Idle 7)
      withinOnAThread10s (run taken ref) `shouldReturn` True
      readIORef ref `shouldReturn` Busy
