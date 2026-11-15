{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | The unwinding of a failure through the frames of a part: the releases that are due run innermost first, down to the
-- mark of the catcher, or to the bottom of a part that has none.
module UnwindSpec (spec) where

import Control.Exception (SomeException, fromException, toException, try)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Network.Streaming.Classes (Exit, ReleaseFailed (..))
import Network.Streaming.Internal.Cancel (Frame (..))
import Network.Streaming.Internal.Scheduler
import Network.Streaming.Mode (Mode (..))
import Skeletest

spec :: Spec
spec = describe "the unwinding of a failure" $ do
  it "runs the releases that are due, innermost first, down to the mark, and leaves the mark and what is below it" $
    withRun defaultTurn $ \run -> do
      part <- newPart run
      noted <- newIORef []
      mapM_ (pushFrame run part) [Owes (notes noted "outer"), Marked, Owes (notes noted "second"), Shielded, Owes (notes noted "first")]
      left <- unwindFrames run part AtMark (userFailure "x")
      show left `shouldBe` "user error (x)"
      reverse <$> readIORef noted `shouldReturn` ["first: Failed user error (x)", "second: Failed user error (x)"]
      frames part `shouldReturn` ["marked", "owes", "inside"]

  it "gives the releases that follow one that failed its failure, with how the body ended" $
    withRun defaultTurn $ \run -> do
      part <- newPart run
      noted <- newIORef []
      mapM_ (pushFrame run part) [Marked, Owes (notes noted "second"), Owes (\exit -> notes noted "first" exit *> ioError (userError "release failed"))]
      left <- unwindFrames run part AtMark (userFailure "body failed")
      case fromException left of
        Just (ReleaseFailed _ released) -> show released `shouldBe` "user error (release failed)"
        Nothing -> failTest ("unwound to " <> show left)
      reverse <$> readIORef noted
        `shouldReturn` [ "first: Failed user error (body failed)",
                         "second: Failed (ReleaseFailed (Failed user error (body failed)) user error (release failed))"
                       ]

  it "leaves no frame, and the part in no scope, when it unwinds to the bottom" $
    withRun defaultTurn $ \run -> do
      part <- newPart run
      noted <- newIORef []
      mapM_ (pushFrame run part) [Owes (notes noted "outer"), Shielded, Owes (notes noted "inner")]
      _ <- unwindFrames run part AtBottom (userFailure "x")
      reverse <$> readIORef noted `shouldReturn` ["inner: Failed user error (x)", "outer: Failed user error (x)"]
      frames part `shouldReturn` []

  it "finds, in a run that observes, that the base of a part that is not the first has no mark" $
    withRun defaultTurn $ \run -> do
      parent <- newPart run
      noted <- newIORef []
      pushFrame run parent (Owes (notes noted "parent"))
      scope' <- newScope run
      child <- childPart run parent scope'
      -- The child's frames are its parent's, and it has no mark of its own.
      observing @'Debug run
      missing <- try (unwindFrames run child AtMark (userFailure "x"))
      either (\failure -> show (failure :: SomeException)) (const "no failure") missing `shouldBe` "user error (a failure unwound the frames of a part to the bottom, and found no mark at its base)"
  it "unwinds what it finds, where nothing observes, whether it finds the mark or not" $
    withRun defaultTurn $ \run -> do
      part <- newPart run
      noted <- newIORef []
      observing @'Release run
      pushFrame run part (Owes (notes noted "owed"))
      left <- unwindFrames run part AtMark (userFailure "x")
      show left `shouldBe` "user error (x)"
      reverse <$> readIORef noted `shouldReturn` ["owed: Failed user error (x)"]

-- | A release that notes how the body ended.
notes :: IORef [String] -> String -> Exit -> IO ()
notes noted name exit = modifyIORef' noted ((name <> ": " <> show exit) :)

userFailure :: String -> SomeException
userFailure = toException . userError

-- | What the frames of the part are, innermost first.
frames :: Part -> IO [String]
frames part = map named <$> readIORef (partFrames part)
  where
    named = \case Inside _ -> "inside"; Shielded -> "shielded"; Graced _ -> "graced"; Held -> "held"; Owes _ -> "owes"; Marked -> "marked"
