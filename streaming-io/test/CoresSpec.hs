{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}

module CoresSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, first, second, (&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO, getNumCapabilities, newEmptyMVar, putMVar, setNumCapabilities, takeMVar, threadDelay, yield)
import Control.Exception (ErrorCall (..), IOException, evaluate, finally, throwIO, try)
import Control.Monad (forM_, unless, void, when, (>=>))
import Data.Bool (bool)
import Data.Either (fromLeft)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef)
import Data.List (isInfixOf, sort)
import Data.Maybe (fromMaybe)
import Data.Tuple (swap)
import Network.Streaming.Classes hiding (within)
import Network.Streaming.Combinators (concurrently, keeping, tap, times, timesWith, untilJust)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Pool
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "cores" $ do
  describe "tickets of any core" $ do
    it "give a value to a part that waits on the same core, which goes on in a later round" $ do
      fst <$> runDelimited (sameCore @'Anywhere) () `shouldReturn` (42, True)
      fst <$> runCompiled (sameCore @'Anywhere) () `shouldReturn` (42, True)
      runThreads (sameCore @'Anywhere) () `shouldReturn` (42, True)

    it "give a value to a part on another core, whose scheduler waits in its poller" $ do
      ensureCores
      shelf <- newIORef Nothing
      results <-
        within $ runCores 2 (pure shelf) $ \box core ->
          if core == 0
            then ticket @_ @'Anywhere >>> second (now (atomicWriteIORef box . Just)) >>> fst ^>> await
            else untilJust (readIORef box) >>> now (\giver -> (giver, 7) <$ threadDelay 20000) >>> give >>^ bool 0 1
      results `shouldBe` Just [7, 1 :: Int]

    it "give a value from another thread to a part on a scheduler that waits in its poller, and to one that finds it given before it waits" $ do
      fst <$> runDelimited givenAfterTheWait () `shouldReturn` (7, True)
      fst <$> runCompiled givenAfterTheWait () `shouldReturn` (7, True)
      runThreads givenAfterTheWait () `shouldReturn` (7, True)
      fst <$> runDelimited givenBeforeTheWait () `shouldReturn` (7, True)
      fst <$> runCompiled givenBeforeTheWait () `shouldReturn` (7, True)
      runThreads givenBeforeTheWait () `shouldReturn` (7, True)

    it "give every value that another thread gives the moment it finds the ticket, whenever that is in the part's wait" $ do
      ensureCores
      withGiverThread $ \shelf -> do
        fst <$> runDelimited (racing shelf 100000) () `shouldReturn` 100000
        fst <$> runCompiled (racing shelf 100000) () `shouldReturn` 100000
        runThreads (racing shelf 100000) () `shouldReturn` 100000

  describe "tickets of the arrow's own core" $ do
    it "give a value to a part that waits on the same core, which goes on in a later round, with every arrow that has them" $ do
      fst <$> runDelimited sameCoreLocal () `shouldReturn` (42, True)
      fst <$> runCompiled sameCoreLocal () `shouldReturn` (42, True)
      fst <$> runLoop sameCoreLocal () `shouldReturn` (42, True)

  describe "the arrows that have what Runtime bundles" $
    it "give a program tickets of both reaches" $ do
      runThreads ticketsOfBothReaches () `shouldReturn` ((42, True), (42, True))
      fst <$> runDelimited ticketsOfBothReaches () `shouldReturn` ((42, True), (42, True))
      fst <$> runCompiled ticketsOfBothReaches () `shouldReturn` ((42, True), (42, True))

  describe "a ticket of either reach" $ do
    it "fails when a value is given twice" $ do
      forM_ [void (runDelimited givenTwiceLocal ()), void (runCompiled givenTwiceLocal ()), void (runLoop givenTwiceLocal ()), void (runDelimited (givenTwice @'Anywhere) ()), void (runCompiled (givenTwice @'Anywhere) ()), void (runThreads (givenTwice @'Anywhere) ())] $ \run -> do
        outcome <- try run
        either (\failure -> "twice" `isInfixOf` show (failure :: IOException)) (const False) outcome `shouldBe` True

    it "tells the giver that the waiter has gone, and gives the waiter a value that came before it went" $ do
      fst <$> runDelimited goneLocal () `shouldReturn` (Nothing, False)
      fst <$> runCompiled goneLocal () `shouldReturn` (Nothing, False)
      fst <$> runLoop goneLocal () `shouldReturn` (Nothing, False)
      fst <$> runDelimited (gone @'Anywhere) () `shouldReturn` (Nothing, False)
      fst <$> runCompiled (gone @'Anywhere) () `shouldReturn` (Nothing, False)
      runThreads (gone @'Anywhere) () `shouldReturn` (Nothing, False)
      fst <$> runDelimited givenFirstLocal () `shouldReturn` (True, Just (5 :: Int))
      fst <$> runCompiled givenFirstLocal () `shouldReturn` (True, Just 5)
      fst <$> runLoop givenFirstLocal () `shouldReturn` (True, Just 5)
      fst <$> runDelimited (givenFirst @'Anywhere) () `shouldReturn` (True, Just 5)
      fst <$> runCompiled (givenFirst @'Anywhere) () `shouldReturn` (True, Just 5)
      runThreads (givenFirst @'Anywhere) () `shouldReturn` (True, Just 5)

  describe "the pool of connections" $ do
    it "loses and duplicates no connection, and no part waits for ever, with parts on 2 and 4 cores" $ do
      ensureCores
      forM_ [2, 4] $ \cores -> do
        world <- newWorld cores 8 3
        result <- within (runCores cores (pure world) (\shared core -> together [times turns (borrowing (worldPool shared) done) | done <- worldDone shared !! core]))
        result `shouldBe` Just (replicate cores ())
        checkWorld world cores 8

    it "does the same with the parts as threads" $ do
      ensureCores
      world <- newWorld 4 8 3
      result <- within (runThreads (together [times turns (borrowing (worldPool world) done) | done <- concat (worldDone world)]) ())
      result `shouldBe` Just ()
      checkWorld world 4 8

    it "gives the connection to the next waiter when the first has abandoned its place, with the parts on one core" $ do
      pool <- newPool [7 :: Int]
      fst <$> runDelimited (nextWaiter pool) () `shouldReturn` (Nothing, 7, [])
      pool' <- newPool [7 :: Int]
      runThreads (nextWaiter pool') () `shouldReturn` (Nothing, 7, [])

    it "frees the connection when the only waiter has abandoned its place" $ do
      pool <- newPool [7 :: Int]
      fst <$> runDelimited (toFreeList pool) () `shouldReturn` (Nothing, [7])
      pool' <- newPool [7 :: Int]
      runThreads (toFreeList pool') () `shouldReturn` (Nothing, [7])

    it "gives a waiter that abandons its place a connection that came before it did, to release" $ do
      pool <- newPool [7 :: Int]
      fst <$> runDelimited (givenBeforeAbandoned pool) () `shouldReturn` (Just 7, [7])
      pool' <- newPool [7 :: Int]
      runThreads (givenBeforeAbandoned pool') () `shouldReturn` (Just 7, [7])

    it "hands a connection to the next waiter on another core when the first has abandoned its place" $ do
      ensureCores
      pool <- newPool [7 :: Int]
      flags <- Flags <$> newFlag <*> newFlag <*> newFlag <*> newFlag
      results <- within (runCores 3 (pure (pool, flags)) acrossCores)
      results `shouldBe` Just [[], [], [7]]
      free pool `shouldReturn` [7]

    it "frees the connection when the only waiter, on another core, has abandoned its place" $ do
      ensureCores
      pool <- newPool [7 :: Int]
      flags <- Flags <$> newFlag <*> newFlag <*> newFlag <*> newFlag
      results <- within (runCores 2 (pure (pool, flags)) acrossCoresOnly)
      results `shouldBe` Just [[], []]
      free pool `shouldReturn` [7]

  describe "a run of cores" $
    it "fails when a core fails, instead of waiting for the cores that wait for ever" $ do
      ensureCores
      started <- monotonicNanoseconds
      outcome <- within (try (runCores 3 (pure ()) (\() core -> if core == 1 then now (const (throwIO (ErrorCall "core 1 failed"))) else waitsForEver)))
      ended <- monotonicNanoseconds
      outcome `shouldBe` Just (Left (ErrorCall "core 1 failed") :: Either ErrorCall [Int])
      (ended - started < 5000000000) `shouldBe` True

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

within :: IO a -> IO (Maybe a)
within = timeout 60000000

-- | Runs the effect, and goes on with what came in.
afterwards :: (Arrow arr) => arr () () -> arr a a
afterwards effect = keeping effect >>^ snd

waitsForEver :: (Arrow arr, Exchange arr 'Anywhere) => arr () Int
waitsForEver = ticket @_ @'Anywhere >>> fst ^>> await

-- | A ticket given a value by the other half of a pair on its core, in a part that waits for it.
sameCore :: forall r arr. (ArrowChoice arr, Concurrent arr, Exchange arr r, Exchange arr (Local arr)) => arr () (Int, Bool)
sameCore = ticket @arr @r >>> concurrently (fst ^>> await) ((,42) . snd ^>> give)

givenTwice :: forall r arr. (ArrowChoice arr, Exchange arr r) => arr () Bool
givenTwice = ticket @arr @r >>> ((,1 :: Int) &&& id) . snd ^>> first give >>> (,2) . snd ^>> give

-- | The waiter abandons before anything is given: nothing for it, and the giver is told.
gone :: forall r arr. (ArrowChoice arr, Exchange arr r) => arr () (Maybe Int, Bool)
gone = ticket @arr @r >>> first abandon >>> second (,5) ^>> second give

-- | The value is given before the waiter abandons: the waiter has it.
givenFirst :: forall r arr. (ArrowChoice arr, Exchange arr r) => arr () (Bool, Maybe Int)
givenFirst = ticket @arr @r >>> second (,5) ^>> second give >>> swap ^>> second abandon

-- | A ticket of any core that a thread of its own gives a value after a while, which the part waits for: the
-- scheduler of an arrow that runs every part on one thread waits in its poller by then, and the give wakes it.
givenAfterTheWait :: (ArrowChoice arr, Effect arr, Exchange arr 'Anywhere) => arr () (Int, Bool)
givenAfterTheWait =
  ticket @_ @'Anywhere
    >>> now (\(awaiting, giver) -> newEmptyMVar >>= \taken -> (awaiting, taken) <$ forkIO (threadDelay 20000 *> runThreads give (giver, 7) >>= putMVar taken))
    >>> first await
    >>> now (\(value, taken) -> (,) value <$> takeMVar taken)

-- | The same, with the value given by a thread before the part waits: it finds the value at its look.
givenBeforeTheWait :: (ArrowChoice arr, Effect arr, Exchange arr 'Anywhere) => arr () (Int, Bool)
givenBeforeTheWait =
  ticket @_ @'Anywhere
    >>> now (\(awaiting, giver) -> newEmptyMVar >>= \taken -> (awaiting,) <$> (forkIO (runThreads give (giver, 7) >>= putMVar taken) *> takeMVar taken))
    >>> first await

-- | Runs the action with a thread that gives a value to every giver that is left on the shelf, as soon as it is
-- there, and stops the thread afterwards: with a flag, since a thread that spins like this one may never reach a
-- point where an exception could stop it.
withGiverThread :: (IORef (Maybe (Giver 'Anywhere Int)) -> IO a) -> IO a
withGiverThread action = do
  shelf <- newIORef Nothing
  stopped <- newIORef False
  let spin = readIORef stopped >>= \done -> unless done (atomicModifyIORef' shelf (Nothing,) >>= maybe yield (\giver -> void (runThreads give (giver, 1))) >> spin)
  _ <- forkIO spin
  action shelf `finally` atomicWriteIORef stopped True

-- | Makes a ticket of any core this many times, leaves its giver on the shelf, and waits for it at once, while a
-- thread gives it a value the moment it sees the giver: the give comes before the part looks at the ticket, between
-- its look and its wait, or after, and the part gets every value.
racing :: (ArrowChoice arr, Effect arr, Exchange arr 'Anywhere, Annotate arr) => IORef (Maybe (Giver 'Anywhere Int)) -> Int -> arr () Int
racing shelf count = const 0 ^>> timesWith count (keeping ticketOnShelf >>^ uncurry (+))
  where
    -- Makes a ticket, leaves its giver on the shelf, and waits for the ticket.
    ticketOnShelf = ticket @_ @'Anywhere >>> second (now (atomicWriteIORef shelf . Just)) >>> fst ^>> await

-- | The programs of the two reaches at once, for any arrow that has what 'Runtime' bundles.
ticketsOfBothReaches :: (Runtime arr) => arr () ((Int, Bool), (Int, Bool))
ticketsOfBothReaches = sameCoreLocal &&& sameCore @'Anywhere

-- | Those four at the reach of the arrow's own core.
sameCoreLocal :: forall arr. (ArrowChoice arr, Concurrent arr, Exchange arr (Local arr)) => arr () (Int, Bool)
sameCoreLocal = sameCore @(Local arr)

givenTwiceLocal :: forall arr. (ArrowChoice arr, Exchange arr (Local arr)) => arr () Bool
givenTwiceLocal = givenTwice @(Local arr)

goneLocal :: forall arr. (ArrowChoice arr, Exchange arr (Local arr)) => arr () (Maybe Int, Bool)
goneLocal = gone @(Local arr)

givenFirstLocal :: forall arr. (ArrowChoice arr, Exchange arr (Local arr)) => arr () (Bool, Maybe Int)
givenFirstLocal = givenFirst @(Local arr)

-- | A part's place in the pool's queue, and no connection: for tests that ask for one when none is free.
queued :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere) => Pool conn -> arr () (Waiting arr 'Anywhere conn)
queued pool = reserve pool >>^ fromLeft (error "a connection was free")

-- | One part that holds a connection, queues two others behind it, one of which abandons its place, and releases.
nextWaiter :: (ArrowChoice arr, Annotate arr, ArrowApplicative arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Int -> arr () (Maybe Int, Int, [Int])
nextWaiter pool =
  checkout pool
    >>> keeping (queued pool)
    >>> keeping (queued pool)
    >>> ( (,,)
            <$> (fst . snd ^>> abandon)
            <* (snd . snd ^>> release pool)
            <*> (fst ^>> await)
            <*> now (const (free pool))
        )

toFreeList :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Int -> arr () (Maybe Int, [Int])
toFreeList pool =
  checkout pool
    >>> keeping (queued pool)
    >>> first abandon
    >>> second (release pool)
    >>> now (\(abandoned, ()) -> (,) abandoned <$> free pool)

givenBeforeAbandoned :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Int -> arr () (Maybe Int, [Int])
givenBeforeAbandoned pool =
  checkout pool
    >>> keeping (queued pool)
    >>> second (release pool)
    >>> fst ^>> abandon
    >>> (id &&& fromMaybe 0) ^>> second (release pool)
    >>> now (\(given, ()) -> (,) given <$> free pool)

-- | What the cores of a pool test wait for each other with: a flag for each thing that has happened.
data Flags = Flags {heldFlag :: IORef Bool, firstQueuedFlag :: IORef Bool, secondQueuedFlag :: IORef Bool, firstGoneFlag :: IORef Bool}

newFlag :: IO (IORef Bool)
newFlag = newIORef False

raise :: (Effect arr) => IORef Bool -> arr () ()
raise flag = now (const (atomicWriteIORef flag True))

-- | Waits, giving way, until the flag is raised.
spinUntil :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr) => IORef Bool -> arr () ()
spinUntil flag = untilJust (bool Nothing (Just ()) <$> readIORef flag)

-- | A core holds the only connection; a second queues for it, then a third, the second abandons its place, and
-- the first releases the connection: the third has it.
acrossCores :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Annotate arr) => (Pool Int, Flags) -> Int -> arr () [Int]
acrossCores (pool, flags) core = case core of
  0 -> checkout pool >>> afterwards (raise (heldFlag flags) >>> spinUntil (firstGoneFlag flags)) >>> release pool >>^ const []
  1 -> spinUntil (heldFlag flags) >>> queued pool >>> afterwards (raise (firstQueuedFlag flags) >>> spinUntil (secondQueuedFlag flags)) >>> abandon >>> afterwards (raise (firstGoneFlag flags)) >>^ maybe [] pure
  _ -> spinUntil (firstQueuedFlag flags) >>> queued pool >>> afterwards (raise (secondQueuedFlag flags)) >>> await >>> tap (release pool) >>^ pure

-- | The same with no third core: the connection is free afterwards.
acrossCoresOnly :: (ArrowChoice arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr, Annotate arr) => (Pool Int, Flags) -> Int -> arr () [Int]
acrossCoresOnly (pool, flags) core = case core of
  0 -> checkout pool >>> afterwards (raise (heldFlag flags) >>> spinUntil (firstGoneFlag flags)) >>> release pool >>^ const []
  _ -> spinUntil (heldFlag flags) >>> queued pool >>> abandon >>> afterwards (raise (firstGoneFlag flags)) >>^ maybe [] pure

-- | A connection, and what the test counts of it: whether a part holds it now, and how often one did.
data Conn = Conn {connId :: !Int, connBusy :: !(IORef Bool), connUses :: !(IORef Int)}

-- | The pool, its connections, and a counter for each part of each core.
data World = World {worldPool :: Pool Conn, worldConns :: [Conn], worldDone :: [[IORef Int]]}

-- | A world for this many cores, parts on each, and connections.
newWorld :: Int -> Int -> Int -> IO World
newWorld cores parts connections = do
  conns <- mapM (\ident -> Conn ident <$> newIORef False <*> newIORef 0) [0 .. connections - 1]
  pool <- newPool conns
  done <- mapM (const (mapM (const (newIORef 0)) [1 .. parts])) [1 .. cores]
  pure (World pool conns done)

turns :: Int
turns = 2000

-- | One turn of a part: it checks a connection out, holds it for a little work, and releases it.
borrowing :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr, Exchange arr 'Anywhere, Scope arr) => Pool Conn -> IORef Int -> arr () ()
borrowing pool done = checkout pool >>> now (acquire >=> work >=> unacquire) >>> release pool
  where
    acquire conn = do
      wasBusy <- atomicModifyIORef' (connBusy conn) (True,)
      when wasBusy $ throwIO (userError "a connection is held by two parts at once")
      modifyIORef' (connUses conn) (+ 1)
      modifyIORef' done (+ 1)
      pure conn
    work conn = conn <$ evaluate (foldl' (+) 0 [1 .. 50 :: Int])
    unacquire conn = do
      wasBusy <- atomicModifyIORef' (connBusy conn) (False,)
      unless wasBusy $ throwIO (userError "a connection was released that no part held")
      pure conn

-- | Every part ran every turn, no connection is missing or twice in the pool, none is held, and the
-- connections were used as often as the parts took them.
checkWorld :: World -> Int -> Int -> IO ()
checkWorld world cores parts = do
  freed <- free (worldPool world)
  sort (map connId freed) `shouldBe` sort (map connId (worldConns world))
  forM_ (worldConns world) $ \conn -> readIORef (connBusy conn) `shouldReturn` False
  uses <- mapM (readIORef . connUses) (worldConns world)
  sum uses `shouldBe` cores * parts * turns
  forM_ (concat (worldDone world)) $ \done -> readIORef done `shouldReturn` turns
