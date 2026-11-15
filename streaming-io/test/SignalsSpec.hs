{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | A run that waits for a descriptor ends when the handler of a signal throws to the thread that runs it, as GHC's
-- handler of SIGINT does: at once when the signal arrives while the scheduler waits in its poller, and within a
-- second when it arrived before the wait, while a part ran. The non-threaded run time has one OS thread, which the
-- scheduler's wait holds, so the handler's thread runs only once the scheduler has given the thread back.
module SignalsSpec (spec) where

import Control.Arrow (ArrowChoice, (>>>), (^>>))
import Control.Concurrent (myThreadId, throwTo)
import Control.Exception (ErrorCall (..), bracket, try)
import Control.Monad (forM_, void)
import Foreign.C.Types (CUInt (..))
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (newBuffer)
import Network.Streaming.Classes
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.PositiveInt (positiveInt)
import Skeletest
import System.Posix.Signals (Handler (Catch), installHandler, raiseSignal, sigALRM)

-- | Sends SIGALRM to the process after this many microseconds, and says how many were left of the one that was set.
foreign import ccall unsafe "ualarm"
  ualarm :: CUInt -> CUInt -> IO CUInt

-- | An arrow that has what the program uses.
type Rich arr = (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr (Local arr), GiveWay arr, Block arr, Annotate arr, Instrumented (ModeOf arr))

-- | A program made in IO that each arrow runs: it runs an action, and then waits for a byte that never comes.
type Waiting = forall arr. (Rich arr) => IO (arr () ())

-- | A runner of the programs of one arrow, which ends when the program does or the run fails.
data Runner = Runner String (Waiting -> IO ())

runners :: [Runner]
runners = [Runner "delimited" onDelimited, Runner "compiled" onCompiled]

onDelimited, onCompiled :: Waiting -> IO ()
onDelimited built = void (runDelimitedWith built ())
onCompiled built = void (built >>= \program -> runCompiled program ())

spec :: Spec
spec = describe "a signal" $
  forM_ runners $ \(Runner name run) -> describe name $ do
    it "ends the run as soon as it arrives while the scheduler waits for a descriptor" $ do
      withPair $ \waiting _ -> do
        let delay = 500000
        due <- (+ fromIntegral delay * 1000) <$> monotonicNanoseconds
        result <- handlerStopping $ try (ualarm delay 0 *> run (waitingFor waiting (pure ())))
        ended <- monotonicNanoseconds
        result `shouldBe` Left (ErrorCall "the handler ran")
        -- The handler's thread runs at once, not at the scheduler's next timeout.
        (ended - due < 50000000) `shouldBe` True

    it "ends the run within a second when it arrived while a part ran, before the scheduler waits" $ do
      withPair $ \waiting _ -> do
        started <- monotonicNanoseconds
        result <- handlerStopping $ try (run (waitingFor waiting (raiseSignal sigALRM)))
        ended <- monotonicNanoseconds
        result `shouldBe` Left (ErrorCall "the handler ran")
        (ended - started < 1000000000) `shouldBe` True

-- | The run waits for a byte on the socket, after the action.
waitingFor :: (Rich arr) => Socket -> IO () -> IO (arr () ())
waitingFor waiting action = do
  buffer <- newBuffer (positiveInt @16)
  pure $ now (const action) >>> const (waiting, roomOf buffer) ^>> receive >>> now (const (pure ()))

-- | The action, with the handler of SIGALRM that throws to the thread that runs it, which restores what it replaced.
handlerStopping :: IO a -> IO a
handlerStopping action = do
  runner <- myThreadId
  bracket
    (installHandler sigALRM (Catch (throwTo runner (ErrorCall "the handler ran"))) Nothing)
    (\previous -> ualarm 0 0 *> installHandler sigALRM previous Nothing)
    (const action)

-- | A pair of connected sockets, closed after the action.
withPair :: (Socket -> Socket -> IO a) -> IO a
withPair =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) (\(a, b) -> Socket.close a *> Socket.close b)
    . uncurry
