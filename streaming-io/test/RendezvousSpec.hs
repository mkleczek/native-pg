{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}

-- | A ticket as a rendezvous, for every arrow that runs programs: a place where one part hands a value to another, over and over,
-- and keeps none. The same programs, written once for any arrow that has what they need, are run by each.
module RendezvousSpec (spec) where

import Control.Arrow (ArrowChoice, first, second, (>>>), (^>>))
import Control.Monad (forM_, when)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (isInfixOf, sort)
import Data.Maybe (isNothing)
import Data.Word (Word64)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats)
import LoansTestSupport (ensureCores)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, done, iterateWith, sleep, step, tap, timesWith, withTicket)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMajorGC)
import System.Timeout (timeout)

-- | An arrow that has everything the programs use, and tickets of its own reach.
type Rich arr =
  ( ArrowChoice arr,
    ArrowApplicative arr,
    Effect arr,
    GiveWay arr,
    Block arr,
    Concurrent arr,
    Exchange arr (Local arr),
    Scope arr,
    Race arr,
    Time arr,
    Within arr,
    Annotate arr
  )

type Program a = forall arr. (Rich arr) => arr () a

-- | A way to run a program: an arrow's.
data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" (\program -> runThreads program ()),
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

spec :: Spec
spec = do
  describe "a ticket as a rendezvous" $ do
    forM_ runners $ \(Runner name run) -> describe name $ do
      it "makes the sender wait until the receiver takes the value" $ do
        noted <- newLog
        run (handWaits noted)
        entries <- readLog noted
        (take 1 entries, sort (drop 1 entries)) `shouldBe` (["receiver takes"], ["handed Nothing", "took 7"])

      it "makes the receiver wait until a sender hands a value over" $ do
        noted <- newLog
        run (awaitWaits noted)
        entries <- readLog noted
        (take 1 entries, sort (drop 1 entries)) `shouldBe` (["sender hands"], ["handed Nothing", "took 7"])

      it "gives the value of a fulfil to a receiver that is parked, and not to a ticket that has none, by the arrow and by the action, and keeps none" $ do
        noted <- newLog
        run (fulfils noted)
        sort <$> readLog noted
          `shouldReturn` [ "action, nobody parked: False",
                           "action, receiver parked: True",
                           "arrow, nobody parked: False",
                           "arrow, receiver parked: True",
                           "took 3",
                           "took 4"
                         ]

      it "can be awaited again after an exchange, as new" $ do
        noted <- newLog
        run (awaitedAgain noted)
        sort <$> readLog noted `shouldReturn` ["fulfil after the exchange: False", "took 1", "took 2"]

      describe "that awaits and registers" $ do
        it "runs the action once, when the receiver parks, with the promise, which a hand reaches the receiver through" $ do
          noted <- newLog
          count <- newIORef (0 :: Int)
          boxed <- newIORef Nothing
          run (registers noted count boxed)
          sort <$> readLog noted `shouldReturn` ["fulfilled True", "took 5"]
          readIORef count `shouldReturn` 1

        it "doesn't park the receiver when the action gives a value, which is the await's, and a fulfil then isn't taken" $ do
          noted <- newLog
          run (registrationGives noted)
          readLog noted `shouldReturn` ["took 9", "fulfil: False"]

        it "doesn't run the action when a sender waits already, and takes the sender's value" $ do
          noted <- newLog
          count <- newIORef (0 :: Int)
          run (registrationSkipped noted count)
          sort <$> readLog noted `shouldReturn` ["handed Nothing", "took 6"]
          readIORef count `shouldReturn` 0

      describe "that is abandoned" $ do
        it "gives the value back to a sender that waits, and to a sender that comes later, at once" $ do
          noted <- newLog
          run (abandonedWhileWaiting noted)
          run (abandonedBefore noted)
          readLog noted `shouldReturn` ["handed Just 2 while it waited", "handed Just 1 after", "fulfil: False after"]

        it "lets a part that watches go" $ do
          noted <- newLog
          run (abandonLetsWatcherGo noted)
          readLog noted `shouldReturn` ["watch ended at the abandon"]

      -- A part that Loop has parked at a ticket is not cancelled: a deadline can't end its wait. The cases that cancel a part
      -- that waits are for the other arrows.
      when (name /= "loop") $ do
        it "lets a cancelled receiver go: a fulfil to it isn't taken, and the ticket serves the next" $ do
          noted <- newLog
          run (cancelledReceiver noted)
          readLog noted `shouldReturn` ["await cancelled", "fulfil: False", "took 6"]

        it "lets a cancelled sender keep its value, which a scope's release sees, and hands it to no receiver" $ do
          noted <- newLog
          run (cancelledSender noted)
          readLog noted `shouldReturn` ["released 7 cancelled", "took 8"]

        it "leaves each value with exactly one of the sender and a receiver that is cancelled while fulfils arrive, many times" $ do
          received <- newIORef []
          returned <- newIORef []
          run (racing received returned 300)
          (,) <$> readIORef received <*> readIORef returned >>= \(taken, given) -> do
            reported name (length taken) (length given)
            sort (taken <> given) `shouldBe` [1 .. 300]

      it "returns the value of an await that a fulfil has delivered, though a cancellation came after it" $ do
        noted <- newLog
        run (deliveryWins noted)
        readLog noted `shouldReturn` ["took 7", "fulfil again: False"]

      it "lets a part wait until the receiver is parked, takes nothing, and has one watcher at a time" $ do
        noted <- newLog
        run (watched noted)
        entries <- readLog noted
        (take 2 entries, sort (drop 2 entries)) `shouldBe` (["second watch failed", "receiver parks"], ["fulfil: True", "took 5", "watch ended"])

      it "fails a second sender that waits while one waits, and goes on with the first" $ do
        noted <- newLog
        run (twoSenders noted)
        sort <$> readLog noted `shouldReturn` ["first handed Nothing", "second hand failed", "took 1"]

      -- A loop of Loop keeps a frame for each round, as its choice has no branch of its own: the cases are for the arrows that run
      -- a loop in constant space.
      when (name /= "loop") $ do
        it "hands 100,000 values over one after the other, and holds no more than a ticket that handed none over" $
          staysSmall name run (\probes -> exchangesMany probes 100000)

        it "makes 100,000 tickets one after the other, each used once, with a hand and an await, and holds no more than none" $
          staysSmall name run (\probes -> usedOnceMany probes 100000)

    describe "with the parts on different cores, as the reach is any core" $ do
      it "hands a value from a part of one core to a receiver of another, which makes the ticket where it awaits" $ do
        ensureCores
        box' <- newIORef Nothing
        results <- timeout 10000000 (runCores 2 (pure box') acrossCores)
        results `shouldBe` Just [1, 5]

      it "gives a value to a receiver of another core by a fulfil, once a watch has seen the receiver park" $ do
        ensureCores
        box' <- newIORef Nothing
        results <- timeout 10000000 (runCores 2 (pure box') watchedAcrossCores)
        results `shouldBe` Just [1, 5]

      it "never misses with a fulfil through the promise that the receiver registered, though it is made as the receiver parks" $ do
        ensureCores
        box' <- newIORef Nothing
        missed <- newIORef (0 :: Int)
        results <- timeout 60000000 (runCores 2 (pure (box', missed)) registeredAcrossCores)
        results `shouldBe` Just [rounds, rounds]
        readIORef missed `shouldReturn` 0

  -- What a ticket's registration step is held to, and held to before: a promise that the step puts where others fulfil it from is
  -- never fulfilled in vain, whether the receiver has parked yet or not.
  describe "the registration step of a ticket" $
    it "has a fulfil through the promise that it registered reach the receiver, though the receiver is made at once" $ do
      ensureCores
      box' <- newIORef Nothing
      missed <- newIORef (0 :: Int)
      results <- timeout 60000000 (runCores 2 (pure (box', missed)) registeredByWithTicket)
      results `shouldBe` Just [rounds, rounds]
      readIORef missed `shouldReturn` 0
  where
    rounds = 1000

-- | Runs the program, which takes the live heap before its first use of a ticket and after its last, and fails if the heap grew.
staysSmall :: String -> (forall a. Program a -> IO a) -> (IORef [Word64] -> Program ()) -> IO ()
staysSmall name run program = do
  probes <- newIORef []
  finished <- timeout 120000000 (run (program probes))
  finished `shouldBe` Just ()
  reverse <$> readIORef probes >>= \case
    [start, end] -> do
      reportedHeap name start end
      if end < start + 16384
        then pure ()
        else failTest ("the live heap grew from " <> show start <> " to " <> show end <> " bytes")
    found -> failTest ("the heap was taken " <> show (length found) <> " times, and not twice")

-- | Says what the memory case measured, if the run asks for it: with @RENDEZVOUS_REPORT@ set, on stderr.
reportedHeap :: String -> Word64 -> Word64 -> IO ()
reportedHeap name start end =
  lookupEnv "RENDEZVOUS_REPORT" >>= \asked ->
    when (asked /= Nothing) (hPutStrLn stderr ("rendezvous " <> name <> " | live bytes at the start " <> show start <> " | at the end " <> show end))

-- | Says how a race of the cancelled receiver and the fulfils came out, if the run asks for it.
reported :: String -> Int -> Int -> IO ()
reported name taken given =
  lookupEnv "RENDEZVOUS_REPORT" >>= \asked ->
    when (asked /= Nothing) (hPutStrLn stderr ("rendezvous " <> name <> " | race: " <> show taken <> " taken by the receiver, " <> show given <> " not taken"))

-- | The two ends of a ticket of numbers, of the arrow's own reach.
type End arr = (Future arr (Local arr) Int, Promise (Local arr) Int)

-- | The input of a part that has one ticket: its ends, and nothing else.
type Ends arr = (End arr, ())

-- | The body in a scope that has a ticket of numbers, which it is given the two ends of, and which the scope abandons as it ends.
withJobs :: forall arr c. (Rich arr) => arr (Ends arr) c -> arr () c
withJobs = scope (ticket @arr @(Local arr) @Int) ((\((future, _), _) -> future) ^>> abandon >>> done)

-- | Hands the number over, and gives what the hand gives: nothing if the receiver took it.
handing :: (Rich arr) => Int -> arr (Ends arr) (Maybe Int)
handing value = (\((_, promise), ()) -> (promise, value)) ^>> handWaiting

-- | Fulfils with the number, as an arrow, and says whether the receiver took it.
fulfilling :: (Rich arr) => Int -> arr (Ends arr) Bool
fulfilling value = (\((_, promise), ()) -> (promise, value)) ^>> fulfil

-- | Fulfils with the number, as an action, and says whether the receiver took it.
fulfillingNow :: (Rich arr) => Int -> arr (Ends arr) Bool
fulfillingNow value = now (\((_, promise), ()) -> fulfilNow promise value)

-- | Awaits the number that a sender hands over.
awaiting :: (Rich arr) => arr (Ends arr) Int
awaiting = (\((future, _), ()) -> future) ^>> await

-- | Abandons the ticket, as its owner does.
abandoning :: (Rich arr) => arr (Ends arr) (Maybe Int)
abandoning = (\((future, _), ()) -> future) ^>> abandon

-- | Waits until the receiver is parked.
watching :: (Rich arr) => arr (Ends arr) ()
watching = (\((_, promise), ()) -> promise) ^>> untilParked

-- | Waits for this long, and keeps the input.
pausing :: (Rich arr) => Duration -> arr a a
pausing duration = tap (done >>> sleep duration)

-- | A sender that hands a value over at once, and a receiver that comes after a time and takes it.
handWaits :: (Rich arr) => Log -> arr () ()
handWaits noted =
  withJobs . together $
    [ handing 7 >>> noting noted (\result -> "handed " <> show result),
      pausing (millis 30) >>> tap (saying noted "receiver takes") >>> awaiting >>> noting noted (\value -> "took " <> show value)
    ]

-- | A receiver that waits at once, and a sender that comes after a time and hands a value over.
awaitWaits :: (Rich arr) => Log -> arr () ()
awaitWaits noted =
  withJobs . together $
    [ awaiting >>> noting noted (\value -> "took " <> show value),
      pausing (millis 30) >>> tap (saying noted "sender hands") >>> handing 7 >>> noting noted (\result -> "handed " <> show result)
    ]

-- | Fulfils with no receiver parked, by the arrow and by the action; then to a receiver that is parked, by each.
fulfils :: (Rich arr) => Log -> arr () ()
fulfils noted =
  withJobs $
    tap (fulfilling 1 >>> noting noted (\taken -> "arrow, nobody parked: " <> show taken))
      >>> tap (fulfillingNow 2 >>> noting noted (\taken -> "action, nobody parked: " <> show taken))
      >>> tap (together [awaiting >>> noting noted (\value -> "took " <> show value), pausing (millis 20) >>> fulfilling 3 >>> noting noted (\taken -> "arrow, receiver parked: " <> show taken)])
      >>> together [awaiting >>> noting noted (\value -> "took " <> show value), pausing (millis 20) >>> fulfillingNow 4 >>> noting noted (\taken -> "action, receiver parked: " <> show taken)]

-- | An exchange, a second exchange on the same ticket, and a fulfil after them that no receiver takes.
awaitedAgain :: (Rich arr) => Log -> arr () ()
awaitedAgain noted =
  withJobs $
    tap (together [awaiting >>> noting noted (\value -> "took " <> show value), pausing (millis 10) >>> handing 1 >>> done])
      >>> tap (together [awaiting >>> noting noted (\value -> "took " <> show value), pausing (millis 10) >>> handing 2 >>> done])
      >>> fulfillingNow 3
      >>> noting noted (\taken -> "fulfil after the exchange: " <> show taken)

-- | A receiver that awaits and registers: the action keeps in a box a way to fulfil the promise that it is given, and gives nothing.
-- A sender waits until that is in the box, and fulfils through it.
registers :: (Rich arr) => Log -> IORef Int -> IORef (Maybe (Int -> IO Bool)) -> arr () ()
registers noted count boxed =
  withJobs . together $
    [ (\((future, _), ()) -> (future, registration)) ^>> awaitRegistering >>> noting noted (\value -> "took " <> show value),
      untilBoxed >>> now (\send -> send 5) >>> noting noted (\taken -> "fulfilled " <> show taken)
    ]
  where
    registration promise = atomicModifyIORef' count (\n -> (n + 1, ())) *> writeIORef boxed (Just (fulfilNow promise)) *> pure Nothing
    untilBoxed = done >>> iterateWith (now (const (maybe (Again ()) Finish <$> readIORef boxed))) (sleep (micros 50))

-- | A receiver whose action gives a value at once takes it without parking: a fulfil after that isn't taken.
registrationGives :: (Rich arr) => Log -> arr () ()
registrationGives noted =
  withJobs $
    tap ((\((future, _), ()) -> (future, \_ -> pure (Just 9))) ^>> awaitRegistering >>> noting noted (\value -> "took " <> show value))
      >>> fulfillingNow 4
      >>> noting noted (\taken -> "fulfil: " <> show taken)

-- | A sender waits already: the receiver takes its value, and the action is not run.
registrationSkipped :: (Rich arr) => Log -> IORef Int -> arr () ()
registrationSkipped noted count =
  withJobs . together $
    [ handing 6 >>> noting noted (\result -> "handed " <> show result),
      pausing (millis 20) >>> (\((future, _), ()) -> (future, \_ -> atomicModifyIORef' count (\n -> (n + 1, ())) *> pure Nothing)) ^>> awaitRegistering >>> noting noted (\value -> "took " <> show value)
    ]

-- | A ticket that is abandoned while the sender waits gives the value back to it.
abandonedWhileWaiting :: (Rich arr) => Log -> arr () ()
abandonedWhileWaiting noted =
  withJobs . together $
    [ handing 2 >>> noting noted (\result -> "handed " <> show result <> " while it waited"),
      pausing (millis 20) >>> abandoning >>> done
    ]

-- | A ticket that is abandoned gives the value back, to a hand and to a fulfil, at once.
abandonedBefore :: (Rich arr) => Log -> arr () ()
abandonedBefore noted =
  withJobs $
    tap (abandoning >>> done)
      >>> tap (handing 1 >>> noting noted (\result -> "handed " <> show result <> " after"))
      >>> fulfillingNow 3
      >>> noting noted (\taken -> "fulfil: " <> show taken <> " after")

-- | A part that watches is let go when the ticket is abandoned: no receiver will park.
abandonLetsWatcherGo :: (Rich arr) => Log -> arr () ()
abandonLetsWatcherGo noted =
  withJobs . together $
    [ watching >>> saying noted "watch ended at the abandon",
      pausing (millis 20) >>> abandoning >>> done
    ]

-- | An await that nobody hands over to is cancelled after a time: a fulfil isn't taken, and a number goes over after.
cancelledReceiver :: (Rich arr) => Log -> arr () ()
cancelledReceiver noted =
  withJobs $
    tap (within (millis 20) awaiting >>> now (\outcome -> isNothing outcome `shouldBe` True) >>> saying noted "await cancelled")
      >>> tap (fulfillingNow 5 >>> noting noted (\taken -> "fulfil: " <> show taken))
      >>> together [handLater, awaiting >>> noting noted (\value -> "took " <> show value)]
  where
    handLater = pausing (millis 10) >>> handing 6 >>> done

-- | A sender that waits, in a scope that notes what it was released with and how its body ended, is cancelled after a time:
-- then a number is handed over and taken, and it is the later one.
cancelledSender :: (Rich arr) => Log -> arr () ()
cancelledSender noted =
  withJobs $
    tap (within (millis 20) (scope (returning 7) release sending) >>> done)
      >>> together [handing 8 >>> done, awaiting >>> noting noted (\value -> "took " <> show value)]
  where
    release = noting noted (\(held, exit) -> "released " <> show held <> " " <> describeExit exit)
    sending = (\(held, ((_, promise), ())) -> (promise, held)) ^>> handWaiting

-- | A receiver that is parked and cancelled after a time, and a sender that fulfils after a time that goes round: before, at and
-- after the cancellation. Each round has its own value, which the receiver notes as soon as its await returns it, and the sender
-- notes when it was not taken.
racing :: (Rich arr) => IORef [Int] -> IORef [Int] -> Int -> arr () ()
racing received returned count = withJobs (foldr (\value rest -> tap (aRound value) >>> rest) done [1 .. count])
  where
    aRound value =
      together
        [ within (micros 300) (awaiting >>> now (\taken -> atomicModifyIORef' received (\values -> (taken : values, ())))) >>> done,
          pausing (micros (fromIntegral (value `mod` 7) * 100)) >>> fulfillingNow value >>> now (\taken -> if taken then pure () else atomicModifyIORef' returned (\values -> (value : values, ())))
        ]

-- | A receiver awaits and then waits for a long time; a sender fulfils and ends at once, which ends the race that the receiver is the
-- other half of, and cancels the receiver. The value was delivered first: the await returns it, and the cancellation reaches the
-- receiver at its next wait. A fulfil after that isn't taken.
deliveryWins :: (Rich arr) => Log -> arr () ()
deliveryWins noted =
  withJobs $
    tap (race (awaiting >>> noting noted (\value -> "took " <> show value) >>> done >>> sleep (secs 10)) (pausing (millis 20) >>> fulfillingNow 7 >>> done) >>> done)
      >>> fulfillingNow 8
      >>> noting noted (\taken -> "fulfil again: " <> show taken)

-- | A part watches from the start, and a second watches after a time and fails; the receiver parks later, which ends the first watch:
-- it takes nothing, and a fulfil then gives the value to the receiver.
watched :: (Rich arr) => Log -> arr () ()
watched noted =
  withJobs . together $
    [ tap (watching >>> saying noted "watch ended") >>> fulfillingNow 5 >>> noting noted (\taken -> "fulfil: " <> show taken),
      pausing (millis 10) >>> tap (tryA watching >>> noting noted (either (\failure -> if "one watcher at a time" `isInfixOf` show failure then "second watch failed" else "second watch failed another way: " <> show failure) (const "second watch ended"))) >>> done,
      pausing (millis 30) >>> tap (saying noted "receiver parks") >>> awaiting >>> noting noted (\value -> "took " <> show value)
    ]

-- | A sender waits, and a second hand fails with the message while it does; the first goes on, and its value is taken.
twoSenders :: (Rich arr) => Log -> arr () ()
twoSenders noted =
  withJobs . together $
    [ handing 1 >>> noting noted (\result -> "first handed " <> show result),
      pausing (millis 20) >>> tap (tryA (handing 2) >>> noting noted failed) >>> awaiting >>> noting noted (\value -> "took " <> show value)
    ]
  where
    failed = either (\failure -> if "one sender at a time" `isInfixOf` show failure then "second hand failed" else "failed another way: " <> show failure) (const "went in")

-- | Two parts that hand over this many numbers, and take them, one after the other, with the live heap taken before the first and
-- after the last.
exchangesMany :: (Rich arr) => IORef [Word64] -> Int -> arr () ()
exchangesMany probes count =
  withJobs $
    tap (heapProbe probes)
      >>> tap (together [timesWith count (tap (handing 1 >>> done)) >>> done, timesWith count (tap (awaiting >>> done)) >>> done])
      >>> tap (heapProbe probes)
      >>> done

-- | A requester that makes a ticket for each of this many requests, as a ticket is made, and hands its promise to a responder over a
-- ticket that lasts; the responder hands the answer through it. The live heap is taken before the first request and after the last.
usedOnceMany :: forall arr. (Rich arr) => IORef [Word64] -> Int -> arr () ()
usedOnceMany probes count =
  scope (ticket @arr @(Local arr) @(Promise (Local arr) Int)) ((\((requests, _), _) -> requests) ^>> abandon >>> done) $
    tap (heapProbe probes)
      >>> tap (together [requester, responder])
      >>> tap (heapProbe probes)
      >>> done
  where
    requester :: arr (Requests arr) ()
    requester = timesWith count (tap oneRequest) >>> done
    oneRequest :: arr (Requests arr) ()
    oneRequest =
      scope
        (done >>> ticket @arr @(Local arr) @Int)
        ((\((answers, _), _) -> answers) ^>> abandon >>> done)
        ((\((answers, answering), ((_, requesting), ())) -> ((requesting, answering), answers)) ^>> first handWaiting >>> second await >>> done)
    responder :: arr (Requests arr) ()
    responder = timesWith count (tap ((\((requests, _), ()) -> requests) ^>> await >>> (\answering -> (answering, 1)) ^>> handWaiting >>> done)) >>> done

-- | The input of a part that has the ticket of requests, which carry the promise of their answers: its ends, and nothing else.
type Requests arr = ((Future arr (Local arr) (Promise (Local arr) Int), Promise (Local arr) (Promise (Local arr) Int)), ())

-- | Takes the live heap, after a major collection, and notes it.
heapProbe :: (Effect arr) => IORef [Word64] -> arr a ()
heapProbe probes = now (const (performMajorGC *> (gcdetails_live_bytes . gc <$> getRTSStats) >>= \live -> atomicModifyIORef' probes (\taken -> (live : taken, ()))))

-- | The part of a core: core 0 waits until the promise is in the box, and hands a number over; the other core makes the ticket, puts
-- its promise in the box, and awaits a number. Each gives a number: 1 if the hand was taken, and the number taken.
acrossCores :: IORef (Maybe (Promise 'Anywhere Int)) -> Int -> Delimited s () Int
acrossCores box' core
  | core == 0 = untilThere box' >>> (\promise -> (promise, 5)) ^>> handWaiting >>> now (\result -> pure (if isNothing result then 1 else 0))
  | otherwise = scope (ticket @_ @'Anywhere @Int) ((\((future, _), _) -> future) ^>> abandon >>> done) (tap (now (\((_, promise), ()) -> writeIORef box' (Just promise))) >>> (\((future, _), ()) -> future) ^>> await)

-- | The part of a core: core 0 waits until the promise is in the box, watches until the receiver is parked, and fulfils with a number,
-- which the receiver, which is parked, takes: it gives 1 if it did. The other core is as in 'acrossCores'.
watchedAcrossCores :: IORef (Maybe (Promise 'Anywhere Int)) -> Int -> Delimited s () Int
watchedAcrossCores box' core
  | core == 0 = untilThere box' >>> tap untilParked >>> now (\promise -> (\taken -> if taken then 1 else 0) <$> fulfilNow promise 5)
  | otherwise = acrossCores box' core

-- | The part of a core, for this many rounds: core 1 awaits with a registration that puts the promise in the box, and gives nothing;
-- core 0 waits until a promise is in the box, empties it, and fulfils it as soon as it sees it. Each round, the fulfil must reach
-- the receiver, which has registered the promise, whether it has parked yet or not. Each core gives the number of rounds it ran, and
-- core 0 counts the fulfils that were not taken.
registeredAcrossCores :: (IORef (Maybe (Promise 'Anywhere Int)), IORef Int) -> Int -> Delimited s () Int
registeredAcrossCores (box', missed) core
  | core == 0 = timesWith 1000 (tap (untilTaken box' >>> now (\promise -> fulfilNow promise 1 >>= \taken -> if taken then pure () else atomicModifyIORef' missed (\n -> (n + 1, ()))))) >>> now (const (pure 1000))
  | otherwise = scope (ticket @_ @'Anywhere @Int) ((\((future, _), _) -> future) ^>> abandon >>> done) (timesWith 1000 (tap ((\((future, _), ()) -> (future, \promise -> Nothing <$ writeIORef box' (Just promise))) ^>> awaitRegistering >>> done))) >>> now (const (pure 1000))

-- | The same, for the registration step of 'withTicket', which puts the promise in the box, in each of the rounds.
registeredByWithTicket :: (IORef (Maybe (Promise 'Anywhere Int)), IORef Int) -> Int -> Delimited s () Int
registeredByWithTicket (box', missed) core
  | core == 0 = timesWith 1000 (tap (untilTaken box' >>> now (\promise -> fulfilNow promise 1 >>= \taken -> if taken then pure () else atomicModifyIORef' missed (\n -> (n + 1, ()))))) >>> now (const (pure 1000))
  | otherwise = timesWith 1000 (tap (withTicket (step "register" (\(promise, ()) -> writeIORef box' (Just promise))) (step "dispose" (const (pure ()))) >>> done)) >>> now (const (pure 1000))

-- | Waits until a promise is in the box, and empties it. It sleeps between looks, which is a wait, so that the part is cancelled there.
untilTaken :: (Rich arr) => IORef (Maybe (Promise 'Anywhere Int)) -> arr () (Promise 'Anywhere Int)
untilTaken box' = done >>> iterateWith (now (const (maybe (Again ()) Finish <$> atomicModifyIORef' box' (\found -> (Nothing, found))))) (sleep (micros 20))

-- | Looks in the box until the promise is there, which it leaves, and between looks sleeps, so that the part is cancelled there.
untilThere :: (Rich arr) => IORef (Maybe (Promise 'Anywhere Int)) -> arr () (Promise 'Anywhere Int)
untilThere box' = done >>> iterateWith (now (const (maybe (Again ()) Finish <$> readIORef box'))) (sleep (micros 50))

-- | A step that gives the value.
returning :: (Effect arr) => a -> arr x a
returning value = now (const (pure value))

-- | What a program noted, in order.
type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

-- | Notes what is made of the input.
noting :: (Effect arr) => Log -> (a -> String) -> arr a ()
noting noted say = now (\input -> atomicModifyIORef' noted (\entries -> (say input : entries, ())))

-- | Notes the text.
saying :: (Effect arr) => Log -> String -> arr x ()
saying noted = noting noted . const

describeExit :: Exit -> String
describeExit = \case
  Done -> "done"
  Failed failure -> "failed " <> show failure
  Cancelled' -> "cancelled"
