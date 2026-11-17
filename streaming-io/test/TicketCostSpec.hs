-- | What an exchange on a ticket costs, in bytes allocated and in time, under each arrow that runs programs: a ticket used once, as a
-- request and its answer use one, and a ticket used again and again. The cases measure and don't compare: with @TICKET_COST_FILE@ set,
-- each appends a line with what it measured to that file, where the numbers of the tree before and after a change are taken from.
-- What a case does assert is that an exchange costs a bounded number of bytes, so that a leak of a part's frames, or a queue that
-- grows, is found.
module TicketCostSpec (spec) where

import Control.Arrow (ArrowChoice, first, (>>>), (>>^), (^>>))
import Control.Monad (forM_, replicateM)
import Data.Word (Word64)
import GHC.Stats (RTSStats (..), getRTSStats)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, expect, localTicket, tap, timesWith)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Environment (lookupEnv)
import System.Mem (performMajorGC)

-- | What the programs need of an arrow.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Scope arr, Annotate arr, Exchange arr (Local arr))

type Program = forall arr. (Rich arr) => Int -> arr () ()

-- | An arrow, and how it runs a program.
data Runner = Runner String (Program -> Int -> IO ())

runners :: [Runner]
runners =
  [ Runner "threads" (\program count -> runThreads (program count) ()),
    Runner "delimited" (\program count -> () <$ runDelimited (program count) ()),
    Runner "compiled" (\program count -> () <$ runCompiled (program count) ()),
    Runner "loop" (\program count -> () <$ runLoop (program count) ())
  ]

-- | A request that carries the promise of its answer, and an answer that carries the promise of the next request.
newtype Ping r = Ping (Promise r (Pong r))

newtype Pong r = Pong (Promise r (Ping r))

-- | Two parts of one core that give each other a ticket and wait for the answer, this many times: two tickets are made, used once, and
-- awaited in each round.
usedOnce :: (Rich arr) => Int -> arr () ()
usedOnce count = localTicket >>> first willAwait >>> together [snd ^>> client, fst ^>> server]
  where
    client = timesWith count (expect Ping >>> await >>^ \(Pong following) -> following) >>> done
    server = timesWith count (await >>> (\(Ping answer) -> answer) ^>> expect Pong) >>> done

-- | Two parts that hand over this many values on one ticket, and take them.
usedAgain :: (Rich arr) => Int -> arr () ()
usedAgain count =
  scope localTicket ((\((future, _), _) -> future) ^>> abandon >>> done) $
    together [(\((_, promise), ()) -> promise) ^>> sender, (\((future, _), ()) -> future) ^>> receiver]
  where
    sender = timesWith count (tap ((,1 :: Int) ^>> handWaiting >>> done)) >>> done
    receiver = timesWith count (tap (await >>> done)) >>> done

spec :: Spec
spec = describe "an exchange on a ticket" $ forM_ runners $ \(Runner name run) -> describe name $ do
  it "used once costs a bounded number of bytes and a bounded time" $ do
    (bytes, nanos') <- measured (run usedOnce)
    note' name "used once" bytes nanos' (2 * rounds)
    (bytes `div` fromIntegral (2 * rounds) < 16384) `shouldBe` True

  it "used again costs a bounded number of bytes and a bounded time" $ do
    (bytes, nanos') <- measured (run usedAgain)
    note' name "used again" bytes nanos' rounds
    (bytes `div` fromIntegral rounds < 16384) `shouldBe` True
  where
    rounds = 100000

-- | The bytes allocated and the nanoseconds taken by the run of the program with this many rounds, the least over five runs, as the
-- runs are alike and the host is not.
measured :: (Int -> IO ()) -> IO (Word64, Word64)
measured run = do
  runs <- replicateM 5 $ do
    performMajorGC
    before <- allocated_bytes <$> getRTSStats
    started <- monotonicNanoseconds
    run 100000
    ended <- monotonicNanoseconds
    after <- allocated_bytes <$> getRTSStats
    pure (after - before, ended - started)
  pure (minimum (fst <$> runs), minimum (snd <$> runs))

-- | Appends what a case measured, per exchange, to the file that the run asks for.
note' :: String -> String -> Word64 -> Word64 -> Int -> IO ()
note' name kind bytes nanos' exchanges =
  lookupEnv "TICKET_COST_FILE" >>= \asked ->
    forM_ asked $ \path ->
      appendFile path (name <> " | " <> kind <> " | " <> show (fromIntegral bytes / fromIntegral exchanges :: Double) <> " B/exchange | " <> show (fromIntegral nanos' / fromIntegral exchanges :: Double) <> " ns/exchange\n")
