{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | Scopes, failures and cancellation, for every arrow that has them: the same programs, written once for any
-- arrow that has what they need, are run by each.
module ScopesSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, returnA, second, (>>>), (^>>), (|||))
import Control.Concurrent (forkIO, getNumCapabilities, setNumCapabilities, threadDelay)
import Control.Exception (SomeException, fromException, throwIO, try)
import Control.Monad (forM_, unless, void, when)
import qualified Data.ByteString as BS
import Data.Functor (($>))
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.List (sort)
import Data.Word (Word64)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as Socket
import Network.Streaming.Classes hiding (within)
import Network.Streaming.Combinators (times, together, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Pool
import Network.Streaming.Threads
import Skeletest
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Exchange arr (Local arr), Scope arr, Race arr)

-- | The same, for the pool, which parts of several cores share.
type Pooled arr = (Rich arr, Exchange arr 'Anywhere)

type Program a = forall arr. (Rich arr) => arr () a

-- | A way to run a program: an arrow's.
data Runner = Runner String (forall a. Program a -> IO a)

-- | Each arrow runs a program for the unit: a function of its own, since a program is for any arrow.
onThreads :: Program a -> IO a
onThreads program = runThreads program ()

runners :: [Runner]
runners =
  [ Runner "threads" onThreads,
    Runner "delimited" (\program -> fst <$> runDelimited program ()),
    Runner "compiled" (\program -> fst <$> runCompiled program ()),
    Runner "loop" (\program -> fst <$> runLoop program ())
  ]

spec :: Spec
spec = describe "scopes" $ forM_ runners $ \(Runner name run) -> describe name $ do
  it "release once, on Done, on a failure and on a cancellation, also a release that waits" $
    forM_ [False, True] $ \waits -> do
      let work :: Program ()
          work = if waits then slowWait else returnA
      done <- newLog
      run (bracketed done "r" work (arr (const (1 :: Int)))) `shouldReturn` 1
      readLog done `shouldReturn` ["acquire r", "release r: done"]
      failed <- newLog
      failure <- run (tryA (bracketed failed "r" work (boom "boom" :: Program Int)))
      describeFailure failure `shouldBe` "Left user error (boom)"
      readLog failed `shouldReturn` ["acquire r", "release r: failed user error (boom)"]
      cancelled <- newLog
      (winner <$> run (race (bracketed cancelled "r" work waitsForEver) giveWayAndEnd)) `shouldReturn` "second"
      readLog cancelled `shouldReturn` ["acquire r", "release r: cancelled"]

  it "report a failure of the release, with how the body ended" $ do
    after <- run (tryA (scope returnA (boom "release failed") (arr (const (1 :: Int)))))
    failure <- either pure (const (failTest "the release's failure was not reported")) after
    case fromException failure of
      Just (ReleaseFailed Done inner) -> show inner `shouldBe` "user error (release failed)"
      _ -> failTest ("reported as " <> show failure)
    both <- run (tryA (scope returnA (boom "release failed") (boom "body failed" >>> arr (const (0 :: Int)))))
    failure' <- either pure (const (failTest "the release's failure was not reported")) both
    case fromException failure' of
      Just (ReleaseFailed (Failed body) inner) -> (show body, show inner) `shouldBe` ("user error (body failed)", "user error (release failed)")
      _ -> failTest ("reported as " <> show failure')

  it "catch a failure with tryA, but not a cancellation" $ do
    run (tryA (boom "x" >>> arr (const (1 :: Int)))) >>= (`shouldBe` "Left user error (x)") . describeFailure
    run (tryA (arr (const (1 :: Int)))) >>= (`shouldBe` "Right 1") . describeFailure
    returned <- newLog
    (winner <$> run (race (tryA waitsForEver >>> noting returned "tryA returned") giveWayAndEnd)) `shouldReturn` "second"
    readLog returned `shouldReturn` []

  describe "concurrently" $ do
    it "cancels the other half at its wait when a half fails, waits for its release, and rethrows the failure" $ do
      for' <- newLog
      result <- run (tryA (concurrently (bracketed for' "other" slowWait waitsForEver :: Program ()) (slowWait >>> boom "first" :: Program ())))
      describeFailure result `shouldBe` "Left user error (first)"
      readLog for' `shouldReturn` ["acquire other", "release other: cancelled"]

    it "rethrows the first failure when both fail" $ do
      for' <- newLog
      -- The second half fails as it is cancelled: its release fails.
      result <- run (tryA (concurrently (boom "first" :: Program ()) (scope (noting for' "acquire" >>> arr (const ())) (boom "second") (arr snd >>> waitsForEver) :: Program ())))
      describeFailure result `shouldBe` "Left user error (first)"
      readLog for' `shouldReturn` ["acquire"]

    it "give both results when neither fails" $
      run (concurrently (slowWait >>> arr (const (1 :: Int))) (arr (const 'x'))) `shouldReturn` (1, 'x')

  describe "race" $ do
    it "gives the first to end, and releases the loser before it returns" $ do
      loser <- newLog
      (winner <$> run (race (bracketed loser "loser" slowWait waitsForEver) (arr (const 'x')))) `shouldReturn` "second"
      readLog loser `shouldReturn` ["acquire loser", "release loser: cancelled"]

    it "rethrows the failure of the first to end, after the other is released" $ do
      loser <- newLog
      result <- run (tryA (race (bracketed loser "loser" slowWait waitsForEver :: Program ()) (boom "winner" :: Program ())))
      describeFailure result `shouldBe` "Left user error (winner)"
      readLog loser `shouldReturn` ["acquire loser", "release loser: cancelled"]

    it "lets the loser finish a release that waits, which a shield protects" $ do
      loser <- newLog
      started <- monotonicNanoseconds
      (winner <$> run (race (bracketed loser "loser" slowWait waitsForEver) giveWayAndEnd)) `shouldReturn` "second"
      ended <- monotonicNanoseconds
      readLog loser `shouldReturn` ["acquire loser", "release loser: cancelled"]
      -- The release waits 20 ms, and race waited for it.
      (ended - started >= 15000000) `shouldBe` True

  describe "cancellation" $ do
    it "doesn't interrupt a long now, and reaches the part at its next wait" $ do
      noted <- newLog
      result <-
        run $
          tryA
            ( concurrently
                (noting noted "start" >>> now (\() -> spinFor 100000000 *> note noted "now done") >>> noting noted "after" >>> waitsForEver :: Program ())
                (slowWait >>> boom "first" :: Program ())
            )
      describeFailure result `shouldBe` "Left user error (first)"
      -- The long action ended; the part didn't go on past its next wait.
      readLog noted `shouldReturn` ["start", "now done", "after"]

    it "reaches a part that waits at once" $ do
      started <- monotonicNanoseconds
      (winner <$> run (race waitsForEver (slowWait :: Program ()))) `shouldReturn` "second"
      ended <- monotonicNanoseconds
      (ended - started < 2000000000) `shouldBe` True

  describe "shield" $ do
    it "keeps an outside cancellation until it ends" $ do
      noted <- newLog
      (winner <$> run (race (shield (slowWait >>> noting noted "shielded done") >>> waitsForEver) giveWayAndEnd)) `shouldReturn` "second"
      readLog noted `shouldReturn` ["shielded done"]

    it "doesn't keep a cancellation that is not outside it" $ do
      noted <- newLog
      (winner <$> run (race (shield (race waitsForEver (slowWait :: Program ()) >>> arr (const ()) >>> noting noted "inner race ended") >>> waitsForEver) giveWayAndEnd)) `shouldReturn` "second"
      readLog noted `shouldReturn` ["inner race ended"]

    it "is needed: without it the wait is cancelled" $ do
      noted <- newLog
      (winner <$> run (race (slowWait >>> noting noted "not cancelled" >>> waitsForEver) giveWayAndEnd)) `shouldReturn` "second"
      readLog noted `shouldReturn` []

  -- A part of a Loop that waits for a ticket is held by the ticket and is not cancelled there.
  when (name /= "loop") $
    describe "a part that waits for a ticket of the arrow's own core" $ do
      it "is cancelled where it waits, and a value given to its ticket afterwards is not given to it, but is its waiter's to take back" $ do
        noted <- newLog
        run (cancelledWaiterLocal noted False) `shouldReturn` ("second", True, Just 7)
        readLog noted `shouldReturn` ["acquire", "release: cancelled"]

      it "gives up its ticket when it is cancelled, and a value given to the ticket afterwards is told it isn't taken" $ do
        noted <- newLog
        run (cancelledWaiterLocal noted True) `shouldReturn` ("second", False, Nothing)
        readLog noted `shouldReturn` ["acquire", "release: cancelled"]

  when (name `elem` ["threads", "delimited", "compiled"]) $
    describe "a part that waits for a ticket of any core" $ do
      it "is cancelled where it waits, and a value given to its ticket afterwards is not given to it, but is its waiter's to take back" $ do
        noted <- newLog
        runAnywhere name (cancelledWaiter @'Anywhere noted False) `shouldReturn` ("second", True, Just 7)
        readLog noted `shouldReturn` ["acquire", "release: cancelled"]

      it "gives up its ticket when it is cancelled, and a value given to the ticket afterwards is told it isn't taken" $ do
        noted <- newLog
        runAnywhere name (cancelledWaiter @'Anywhere noted True) `shouldReturn` ("second", False, Nothing)
        readLog noted `shouldReturn` ["acquire", "release: cancelled"]

  it "lose and duplicate no connection of a pool, and let no part wait for ever, whatever cancels the parts" $ do
    ensureCores
    forM_ [1, 2] $ \seed -> do
      world <- newWorld 3 seed
      let parts = 4 :: Int
      case name of
        "threads" -> within (runThreads (together [times turns (turn world) | _ <- [1 .. parts * 2]]) ()) >>= (`shouldBe` Just ())
        "compiled" -> within (fst <$> runCompiled (together [times turns (turn world) | _ <- [1 .. parts * 2]]) ()) >>= (`shouldBe` Just ())
        _ -> within (runCores 2 (pure world) (\shared _ -> together [times turns (turn shared) | _ <- [1 .. parts]])) >>= (`shouldBe` Just [(), ()])
      checkWorld world

  when (name == "delimited") $
    describe "a run of cores" $
      it "runs the release of every part when a core fails" $ do
        ensureCores
        noted <- newLog
        result <-
          within . try $
            runCores 3 (pure noted) $ \noted' core ->
              if core == 2
                then slowWait >>> boom "core 2 failed" >>> arr (const 0)
                else bracketed noted' ("core " <> show core) returnA waitsForEver >>> arr (const core)
        fmap (either (\failure -> show (failure :: SomeException)) (const "ended")) result `shouldBe` Just "user error (core 2 failed)"
        sort <$> readLog noted `shouldReturn` ["acquire core 0", "acquire core 1", "release core 0: cancelled", "release core 1: cancelled"]

  when (name /= "loop") $ it "runs the release of a part that waits when the run is stopped from outside" $ do
    noted <- newLog
    result <- timeout 300000 (run (bracketed noted "r" returnA waitsForEver :: Program ()))
    result `shouldBe` Nothing
    entries <- readLog noted
    -- The release ran once, before the stop returned.
    length entries `shouldBe` 2
    take 1 entries `shouldBe` ["acquire r"]

  when (name /= "loop") $ it "runs the release of both halves of a pair when the run is stopped from outside" $ do
    noted <- newLog
    result <- timeout 300000 (run (concurrently (bracketed noted "a" slowWait waitsForEver) (bracketed noted "b" slowWait waitsForEver) >>> arr (const ())))
    result `shouldBe` Nothing
    entries <- readLog noted
    sort entries `shouldBe` ["acquire a", "acquire b", "release a: cancelled", "release b: cancelled"]

  it "cancel every one of a thousand parts that wait in a scope, and run their releases" $ do
    forever <- readableAfter (-1)
    waiting <- newCounter
    cancelled <- newCounter
    let count = 1000
        -- The part that ends the race waits until every other has begun to wait.
        everyoneWaits :: Program ()
        everyoneWaits = untilJust ((\n -> if n == count then Just () else Nothing) <$> readCounter waiting)
        waiter :: Program ()
        waiter = counting waiting >>> arr (const forever) >>> block
    result <- within (run (race (balanced (replicate count (holding cancelled waiter))) everyoneWaits))
    (winner <$> result) `shouldBe` Just "second"
    readCounter cancelled `shouldReturn` count

  when (name /= "loop") $ it "runs the release of every one of a thousand parts that hold one when the run is stopped from outside" $ do
    forever <- readableAfter (-1)
    released <- newCounter
    let count = 1000
    result <- timeout 500000 (run (balanced (replicate count (holding released (arr (const forever) >>> block)))))
    result `shouldBe` Nothing
    readCounter released `shouldReturn` count

-- | A failure, as the text of what it was, or of the result.
describeFailure :: (Show a) => Either SomeException a -> String
describeFailure = either (\failure -> "Left " <> show failure) (\result -> "Right " <> show result)

-- | A count that parts on several cores add to.
type Counter = IORef Int

newCounter :: IO Counter
newCounter = newIORef 0

readCounter :: Counter -> IO Int
readCounter = readIORef

-- | A part adds one to the count and goes on with what it was given.
counting :: (Effect arr) => Counter -> arr a a
counting counter = now (\value -> atomicModifyIORef' counter (\n -> (n + 1, ())) $> value)

-- | A part that holds a resource while its body runs, and counts the releases that follow a cancellation.
holding :: (Rich arr) => Counter -> arr () () -> arr () ()
holding counter body =
  scope returnA (now (\(_, exit) -> case exit of Cancelled' -> atomicModifyIORef' counter (\n -> (n + 1, ())); _ -> pure ())) (arr snd >>> body)

-- | The arrows at once, as a tree of pairs, so that the parts are not nested as deep as there are of them.
balanced :: (Rich arr) => [arr () ()] -> arr () ()
balanced [] = returnA
balanced [one] = one
balanced parts' = let (front, back) = splitAt (length parts' `div` 2) parts' in concurrently (balanced front) (balanced back) >>> arr (const ())

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

noting :: (Effect arr) => Log -> String -> arr a a
noting noted entry = now (\value -> note noted entry $> value)

-- | Adds to the log: parts on several cores write to it.
note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

-- | Fails with the text.
boom :: (Effect arr) => String -> arr a b
boom text = now (\_ -> throwIO (userError text))

-- | A resource that is acquired and released with notes, and whose release does the work before it notes how
-- the body ended.
bracketed :: (Rich arr) => Log -> String -> arr () () -> arr () b -> arr () b
bracketed noted name work body =
  scope
    (noting noted ("acquire " <> name))
    ((\(_, exit) -> ((), exit)) ^>> first work >>> now (\((), exit) -> note noted ("release " <> name <> ": " <> describeExit exit)))
    (arr snd >>> body)

describeExit :: Exit -> String
describeExit = \case
  Done -> "done"
  Failed failure -> "failed " <> show failure
  Cancelled' -> "cancelled"

-- | A program that parts of every core can share tickets in: the arrows of cores, of threads and of one thread run it.
runAnywhere :: String -> (forall arr. (Pooled arr) => arr () a) -> IO a
runAnywhere name program = case name of
  "threads" -> runThreads program ()
  "compiled" -> fst <$> runCompiled program ()
  _ -> fst <$> runDelimited program ()

-- | The waiting end of a ticket, waited for in a resource whose release notes how the wait ended and, if it is
-- told to, gives up the ticket.
waiterIn :: forall r arr. (Rich arr, Exchange arr r) => Log -> Bool -> arr (Waiting arr r Int) Int
waiterIn noted giveUp =
  scope
    (noting noted "acquire")
    ((if giveUp then first abandon else first (arr (const Nothing))) >>> now (\(_, exit) -> note noted ("release: " <> describeExit exit)))
    (arr snd >>> await >>> noting noted "resumed")

-- | A part waits for a ticket in a race that the other half ends at once, so that it is cancelled where it waits.
-- Then the ticket is given a value, and its waiting end abandoned: whether the give was taken, and what the abandon
-- gave back.
cancelledWaiter :: forall r arr. (Rich arr, Exchange arr r) => Log -> Bool -> arr () (String, Bool, Maybe Int)
cancelledWaiter noted giveUp =
  ticket @arr @r
    >>> (\ends -> (ends, ends))
    ^>> first (race (arr fst >>> waiterIn @r noted giveUp) (arr (const ()) >>> giveWayAndEnd))
    >>> (\(outcome, (waiting, giver)) -> ((giver, 7), (winner outcome, waiting)))
    ^>> first give
    >>> (\(taken, (who, waiting)) -> ((who, taken), waiting))
    ^>> second abandon
    >>> arr (\((who, taken), returned) -> (who, taken, returned))

-- | The same at the reach of the arrow's own core.
cancelledWaiterLocal :: forall arr. (Rich arr) => Log -> Bool -> arr () (String, Bool, Maybe Int)
cancelledWaiterLocal = cancelledWaiter @(Local arr)

-- | A wait of about 20 ms, for a descriptor that another thread makes readable.
slowWait :: (Rich arr) => arr () ()
slowWait = now (\() -> readableAfter 20000) >>> block

-- | A wait that nothing ends.
waitsForEver :: (Rich arr) => arr a ()
waitsForEver = now (\_ -> readableAfter (-1)) >>> block

-- | A descriptor that becomes readable after that many microseconds, or never.
readableAfter :: Int -> IO Event
readableAfter microseconds = do
  (one, other) <- Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol
  -- The descriptor is only its socket's while the socket is alive.
  atomicModifyIORef' kept (\sockets -> (one : sockets, ()))
  if microseconds < 0
    then atomicModifyIORef' kept (\sockets -> (other : sockets, ()))
    else void (forkIO (threadDelay microseconds *> Socket.sendAll other (BS.pack [1])))
  Readable . Fd <$> Socket.unsafeFdSocket one

-- | The far ends of descriptors that never become readable: they stay open while the tests run.
kept :: IORef [Socket.Socket]
kept = unsafePerformIO (newIORef [])
{-# NOINLINE kept #-}

-- | A wait that another thread ends at once: it may or may not find the value there.
quickWait :: (Pooled arr) => arr () ()
quickWait = ticket @_ @'Anywhere >>> now (\(awaiting, giver) -> awaiting <$ forkIO (void (runThreads give (giver, ())))) >>> await

-- | Which half of a race was the first to end.
winner :: Either a b -> String
winner = either (const "first") (const "second")

-- | A part that ends after a point to give way at.
giveWayAndEnd :: (Rich arr) => arr () ()
giveWayAndEnd = giveWay

-- | Keeps the thread busy for that many nanoseconds, in an action that doesn't wait.
spinFor :: Word64 -> IO ()
spinFor nanoseconds = do
  started <- monotonicNanoseconds
  let go = monotonicNanoseconds >>= \now' -> when (now' - started < nanoseconds) go
  go

ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

within :: IO a -> IO (Maybe a)
within = timeout 60000000

-- | A connection, and whether a part holds it now.
data Conn = Conn {connId :: !Int, connBusy :: !(IORef Bool)}

data World = World {worldPool :: Pool Conn, worldConns :: [Conn], worldRandom :: IORef Word64, worldTurns :: IORef Int, worldCancelled :: IORef Int}

newWorld :: Int -> Word64 -> IO World
newWorld count seed = do
  conns <- mapM (\ident -> Conn ident <$> newIORef False) [0 .. count - 1]
  World <$> newPool conns <*> pure conns <*> newIORef seed <*> newIORef 0 <*> newIORef 0

turns :: Int
turns = 300

-- | A connection while the body runs: no two parts hold one, and it goes back to the pool however the body ends.
withConn :: (Pooled arr) => World -> arr () a -> arr () a
withConn world body =
  scope
    (checkout (worldPool world) >>> now (\conn -> atomicModifyIORef' (connBusy conn) (True,) >>= \was -> when was (throwIO (userError "a connection is held twice")) $> conn))
    (arr fst >>> now (\conn -> atomicModifyIORef' (connBusy conn) (False,) >>= \was -> unless was (throwIO (userError "a connection was not held")) $> conn) >>> release (worldPool world))
    (arr snd >>> body)

-- | A part's turn: it borrows a connection and waits while it holds it, and something cancels it after a random
-- number of points to give way at.
turn :: (Pooled arr) => World -> arr () ()
turn world =
  race (withConn world (now (\() -> pure ()) >>> quickWait >>> quickWait)) (now (\() -> randomBelow (worldRandom world) 40) >>> countdown)
    >>> now (either (const (bump (worldTurns world))) (const (bump (worldTurns world) *> bump (worldCancelled world))))
  where
    bump counter = atomicModifyIORef' counter (\n -> (n + 1, ()))
    countdown = (\n -> if n <= (0 :: Int) then Left () else Right (n - 1)) ^>> (returnA ||| (giveWay >>> countdown))

randomBelow :: IORef Word64 -> Int -> IO Int
randomBelow state limit = atomicModifyIORef' state (\s -> let s' = s * 6364136223846793005 + 1442695040888963407 in (s', fromIntegral (s' `div` 65536 `mod` fromIntegral limit)))

-- | Every connection is in the pool once, none is held, and every part ran its turns.
checkWorld :: World -> IO ()
checkWorld world = do
  freed <- free (worldPool world)
  sort (map connId freed) `shouldBe` sort (map connId (worldConns world))
  forM_ (worldConns world) $ \conn -> readIORef (connBusy conn) `shouldReturn` False
  readIORef (worldCancelled world) >>= \cancelled -> (cancelled > 0) `shouldBe` True
  readIORef (worldTurns world) >>= \done -> done `shouldBe` 8 * turns
