-- | Steps that can't wait: 'undoing' cleans up after a step that fails, with no scope, and 'through' and 'on' run an arrow on a
-- part of the input. The same programs, written once for any arrow that has what a step that can't wait has, are run by each
-- arrow. That a step or a cleanup that can wait doesn't compile is checked by @test/check-steps.sh@.
module StepsSpec (spec) where

import Control.Arrow (Arrow, arr, (>>>), (^>>))
import Control.Exception (Exception, SomeException, fromException, throwIO)
import Control.Monad (forM_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, on, through, undoing, _1, _2)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest

-- | The arrow of a step that can't wait.
type Program a = forall arr. (Steps arr) => arr () a

-- | A way to run a program: an arrow's.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" (\program -> runThreads program ()),
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

-- | Marks a case that is expected to fail while 'undoing' and 'on' are stubs. The mark comes off as the code lands, and a
-- case that then passes unexpectedly fails the suite.
stub :: Spec -> Spec
stub = xfail "steps: undoing and on are stubs, which fail as not implemented"

spec :: Spec
spec = describe "steps" $ forM_ runners $ \(Runner name run) -> describe name $ do
  describe "undoing" $ do
    stub $ it "gives the result of the step when it succeeds, and doesn't run the cleanup" $ do
      noted <- newLog
      run (const 7 ^>> undoing (succeeds noted) (cleaning noted)) `shouldReturn` 8
      readLog noted `shouldReturn` ["step 7"]

    stub $ it "runs the cleanup once, on the input of the step, when it fails, and then fails with the same exception" $ do
      noted <- newLog
      result <- run (tryA (const 7 ^>> undoing (fails noted) (cleaning noted)))
      failureOf result `shouldBe` Just (Boom 7)
      readLog noted `shouldReturn` ["step 7", "cleanup 7"]

    stub $ it "fails with the exception of the cleanup, when the cleanup fails too" $ do
      noted <- newLog
      result <- run (tryA (const 7 ^>> undoing (fails noted) (cleaning noted >>> now (const (throwIO (userError "cleanup failed"))))))
      describeFailure result `shouldBe` "Left user error (cleanup failed)"
      readLog noted `shouldReturn` ["step 7", "cleanup 7"]

    -- The inner one is a step of the outer one, which a step that waits could not be.
    stub $ it "can be the step of another undoing: the cleanups run innermost first, and the exception is the same" $ do
      noted <- newLog
      failed <- run (tryA (const 7 ^>> undoing (undoing (fails noted) (cleaning noted)) (noting' noted "outer cleanup")))
      failureOf failed `shouldBe` Just (Boom 7)
      readLog noted `shouldReturn` ["step 7", "cleanup 7", "outer cleanup 7"]
      succeeded <- newLog
      run (const 7 ^>> undoing (undoing (succeeds succeeded) (cleaning succeeded)) (noting' succeeded "outer cleanup")) `shouldReturn` 8
      readLog succeeded `shouldReturn` ["step 7"]

  describe "through" $
    it "runs the arrow on the part that the lens picks, and keeps the rest" $ do
      noted <- newLog
      run (const (1 :: Int, (2 :: Int, "rest")) ^>> through (_2 . _1) (seeing noted)) `shouldReturn` (1, (20, "rest"))
      readLog noted `shouldReturn` ["saw 2"]
      run (const (1 :: Int, (2 :: Int, "rest")) ^>> through _1 (arr (+ 1))) `shouldReturn` (2, (2, "rest"))

  describe "on" $
    stub $ it "runs the arrow on the part that the lens picks, and gives the unit" $ do
      noted <- newLog
      run (const (1 :: Int, (2 :: Int, "rest")) ^>> on (_2 . _1) (seeing noted >>> done)) `shouldReturn` ()
      readLog noted `shouldReturn` ["saw 2"]

-- | A failure of a step, which is told apart from another by its number.
newtype Boom = Boom Int
  deriving (Eq, Show)

instance Exception Boom

-- | The failure of the result, if it is a 'Boom'.
failureOf :: Either SomeException a -> Maybe Boom
failureOf = either fromException (const Nothing)

-- | A failure, as the text of what it was, or of the result.
describeFailure :: (Show a) => Either SomeException a -> String
describeFailure = either (\failure -> "Left " <> show failure) (\result -> "Right " <> show result)

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

-- | Adds to the log.
note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

-- | Notes the entry and the input, and gives nothing.
noting' :: (Arrow arr, Effect arr, Show a) => Log -> String -> arr a ()
noting' noted entry = now (\input -> note noted (entry <> " " <> show input))

-- | A step that notes its input, and gives it plus one.
succeeds :: (Arrow arr, Effect arr) => Log -> arr Int Int
succeeds noted = now (\input -> note noted ("step " <> show input) *> pure (input + 1))

-- | A step that notes its input, and fails with it.
fails :: (Arrow arr, Effect arr) => Log -> arr Int Int
fails noted = now (\input -> note noted ("step " <> show input) *> throwIO (Boom input))

-- | A cleanup that notes its input.
cleaning :: (Arrow arr, Effect arr) => Log -> arr Int ()
cleaning noted = noting' noted "cleanup"

-- | A step that notes its input, and gives it times ten.
seeing :: (Arrow arr, Effect arr) => Log -> arr Int Int
seeing noted = now (\input -> note noted ("saw " <> show input) *> pure (input * 10))
