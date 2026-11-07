{-# LANGUAGE LambdaCase #-}

module DriveSpec (spec) where

import Control.Exception (Exception, throwIO)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Network.Streaming.Drive
import Network.Streaming.Link (Blocked (..), Interest (..), Link (..))
import Skeletest
import qualified Skeletest.Predicate as P

-- | A link that only counts the waits it's asked for, with what they were
-- for, and runs an action for each.
data WaitLink = WaitLink
  { waitsLog :: IORef [(Bool, String)],
    onWait :: IO ()
  }

newWaitLink :: IO () -> IO WaitLink
newWaitLink action = (`WaitLink` action) <$> newIORef []

-- | The waits asked for, in order.
waits :: WaitLink -> IO [(Bool, String)]
waits = fmap reverse . readIORef . waitsLog

instance Link WaitLink where
  awaitAny link (Interest input output) = do
    modifyIORef' (waitsLog link) ((input, name output) :)
    onWait link
  watchReadable _ = fail "unexpected read watch"
  watchWritable _ = fail "unexpected write watch"
  receiveReady _ _ _ = fail "unexpected receive"
  sendReady _ _ = fail "unexpected send"
  flushReady _ = fail "unexpected flush"
  closeLink _ = pure ()

newtype Boom = Boom String deriving (Show)

instance Exception Boom

spec :: Spec
spec = describe "driving a step" $ do
  it "returns what a ready step gives, without waiting" $ do
    link <- newWaitLink (pure ())
    drive link (\() -> pure (Ready 'x')) () `shouldReturn` 'x'
    waits link `shouldReturn` []

  it "steps again from the state it made progress to, without waiting" $ do
    link <- newWaitLink (pure ())
    seen <- newIORef []
    let step n = do
          modifyIORef' seen (n :)
          pure (if n < 3 then Again (n + 1) else Ready (n * 10))
    drive link step (0 :: Int) `shouldReturn` 30
    readIORef seen `shouldReturn` [3, 2, 1, 0]
    waits link `shouldReturn` []

  it "makes progress without growing the stack" $ do
    link <- newWaitLink (pure ())
    let step n = pure (if n == (1000000 :: Int) then Ready n else Again (n + 1))
    drive link step 0 `shouldReturn` 1000000
    waits link `shouldReturn` []

  it "waits once with the interest it's blocked on, then steps again from its state" $ do
    link <- newWaitLink (pure ())
    seen <- newIORef []
    let step n = do
          modifyIORef' seen (n :)
          pure (if n == 0 then Blocked (n + 1) (Interest True OnWritable) else Ready n)
    drive link step (0 :: Int) `shouldReturn` 1
    readIORef seen `shouldReturn` [1, 0]
    waits link `shouldReturn` [(True, "writable")]

  it "waits each time it's blocked, for what each interest includes, and for no other reason" $ do
    link <- newWaitLink (pure ())
    let step = \case
          0 -> pure (Again 1)
          1 -> pure (Blocked 2 (Interest True Done))
          2 -> pure (Again 3)
          3 -> pure (Blocked 4 (Interest False OnWritable))
          4 -> pure (Blocked 5 (Interest False (OnSource (fail "unexpected source watch"))))
          n -> pure (Ready (n :: Int))
    drive link step 0 `shouldReturn` 5
    waits link `shouldReturn` [(True, "done"), (False, "writable"), (False, "source")]

  it "lets the exception of a step propagate, without waiting" $ do
    link <- newWaitLink (pure ())
    drive link (\() -> throwIO (Boom "step") :: IO (Progress () ())) ()
      `shouldSatisfy` P.throws (P.con (Boom (P.eq "step")))
    waits link `shouldReturn` []

  it "lets the exception of a step after progress propagate" $ do
    link <- newWaitLink (pure ())
    let step n = if n < 2 then pure (Again (n + 1)) else throwIO (Boom "later") :: IO (Progress Int ())
    drive link step (0 :: Int) `shouldSatisfy` P.throws (P.con (Boom (P.eq "later")))
    waits link `shouldReturn` []

  it "lets the exception of a wait propagate, and doesn't step again" $ do
    link <- newWaitLink (throwIO (Boom "wait"))
    steps <- newIORef (0 :: Int)
    let step () = modifyIORef' steps (+ 1) *> pure (Blocked () (Interest True Done) :: Progress () ())
    drive link step () `shouldSatisfy` P.throws (P.con (Boom (P.eq "wait")))
    readIORef steps `shouldReturn` 1
    waits link `shouldReturn` [(True, "done")]

  describe "polling a step" $ do
    xfail "poll: it is not implemented yet" $ it "gives what a ready step gives" $ do
      poll (\() -> pure (Ready 'x')) () >>= \case
        Got 'x' -> pure ()
        _ -> failTest "a ready step gave nothing"

    xfail "poll: it is not implemented yet" $ it "steps again from the state it made progress to" $ do
      seen <- newIORef []
      let step n = do
            modifyIORef' seen (n :)
            pure (if n < 3 then Again (n + 1) else Ready (n * 10))
      poll step (0 :: Int) >>= \case
        Got 30 -> pure ()
        _ -> failTest "the step did not get to its result"
      readIORef seen `shouldReturn` [3, 2, 1, 0]

    xfail "poll: it is not implemented yet" $ it "makes progress without growing the stack" $ do
      let step n = pure (if n == (1000000 :: Int) then Ready n else Again (n + 1))
      poll step 0 >>= \case
        Got 1000000 -> pure ()
        _ -> failTest "the step did not get to its result"

    xfail "poll: it is not implemented yet" $ it "gives up where drive would wait, with what it is blocked on, and doesn't wait" $ do
      seen <- newIORef []
      let step n = do
            modifyIORef' seen (n :)
            pure (if n == 0 then Again (n + 1) else Blocked (n + 1) (Interest True OnWritable) :: Progress Int ())
      poll step (0 :: Int) >>= \case
        Stalled (Interest input output) -> (input, name output) `shouldBe` (True, "writable")
        Got () -> failTest "a blocked step was ready"
      -- It gave up at the block, and didn't try again from the state it gave
      readIORef seen `shouldReturn` [1, 0]

    xfail "poll: it is not implemented yet" $ it "lets the exception of a step propagate" $
      poll (\() -> throwIO (Boom "step") :: IO (Progress () ())) ()
        `shouldSatisfy` P.throws (P.con (Boom (P.eq "step")))

name :: Blocked -> String
name = \case
  Done -> "done"
  OnWritable -> "writable"
  OnSource _ -> "source"
