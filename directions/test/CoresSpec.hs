module CoresSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, second, (>>>), (^>>), (|||))
import Control.Concurrent (getNumCapabilities, setNumCapabilities, threadDelay)
import Control.Exception (ErrorCall (..), IOException, evaluate, throwIO, try)
import Control.Monad (forM_, unless, when)
import Data.Either (fromLeft)
import Data.IORef (IORef, atomicModifyIORef', atomicWriteIORef, modifyIORef', newIORef, readIORef)
import Data.List (isInfixOf, sort)
import Data.Maybe (fromMaybe)
import GHC.Clock (getMonotonicTimeNSec)
import Network.Directions.Classes
import Network.Directions.Compiled (runCompiled)
import Network.Directions.Delimited
import Network.Directions.Loop (Report (..), runLoop)
import Network.Directions.PingPong
import Network.Directions.Pool
import Network.Directions.Proxy (times, together)
import Network.Directions.Threads
import Skeletest
import System.Timeout (timeout)

spec :: Spec
spec = describe "cores" $ do
  describe "tickets that are made as the program runs" $
    it "let two parts ping-pong, with every arrow, with tickets made for each round trip" $ do
      runThreads (pingPong 1000) () `shouldReturn` ()
      fst <$> runLoop (pingPong 1000) () `shouldReturn` ()
      fst <$> runCompiled (pingPong 1000) () `shouldReturn` ()
      fst <$> runDelimited (pingPong 1000) () `shouldReturn` ()

  describe "remote tickets" $ do
    it "give a value to a part that waits on the same core, which goes on in a later round" $ do
      fst <$> runDelimited sameCore () `shouldReturn` (42, True)
      runThreads sameCore () `shouldReturn` (42, True)

    it "give a value to a part on another core, whose scheduler waits in its poller" $ do
      ensureCores
      shelf <- newIORef Nothing
      results <-
        within $ runCores 2 (pure shelf) $ \box core ->
          if core == 0
            then newRemote >>> now (\(awaiting, giver) -> awaiting <$ atomicWriteIORef box (Just giver)) >>> awaitRemote
            else spinUntilJust box >>> now (\giver -> (giver, 7) <$ threadDelay 20000) >>> give >>> arr (\given -> if given then 1 else 0)
      results `shouldBe` Just [7, 1 :: Int]

    it "fail when a value is given twice" $ do
      twice <- try (runDelimited givenTwice ())
      either (\failure -> "twice" `isInfixOf` show (failure :: IOException)) (const False) twice `shouldBe` True
      thrice <- try (runThreads givenTwice ())
      either (\failure -> "twice" `isInfixOf` show (failure :: IOException)) (const False) thrice `shouldBe` True

    it "tell the giver that the waiter has gone, and give the waiter a value that came before it went" $ do
      fst <$> runDelimited gone () `shouldReturn` (Nothing, False)
      runThreads gone () `shouldReturn` (Nothing, False)
      fst <$> runDelimited givenFirst () `shouldReturn` (True, Just (5 :: Int))
      runThreads givenFirst () `shouldReturn` (True, Just 5)

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

  describe "a core that waits in its poller" $ do
    it "is woken by a give from another core within a few milliseconds, every time" $ do
      ensureCores
      wake <- newWake
      let samplesWanted = 40
      reports <- within (runCoresReport 2 (pure wake) (\shared core -> if core == 0 then sleeper shared samplesWanted else waker shared 3000 samplesWanted))
      waits <- case reports of
        Just (((), report) : _) -> pure (reportWaits report)
        _ -> failTest "the run didn't end"
      -- Core 0 blocked in its poller for (nearly) every sample.
      (waits >= samplesWanted - 5) `shouldBe` True
      latencies <- sort <$> wakeLatencies wake
      length latencies `shouldBe` samplesWanted
      -- The median is far below a millisecond; the slowest tenth, on a busy host, still well under 10 ms.
      (latencies !! (samplesWanted `div` 2) < 1000000) `shouldBe` True
      (latencies !! (samplesWanted * 9 `div` 10) < 10000000) `shouldBe` True

  describe "a run of cores" $
    it "fails when a core fails, instead of waiting for the cores that wait for ever" $ do
      ensureCores
      started <- getMonotonicTimeNSec
      outcome <- within (try (runCores 3 (pure ()) (\() core -> if core == 1 then now (\() -> throwIO (ErrorCall "core 1 failed")) >>> arr (const 0) else waitsForEver)))
      ended <- getMonotonicTimeNSec
      outcome `shouldBe` Just (Left (ErrorCall "core 1 failed") :: Either ErrorCall [Int])
      (ended - started < 5000000000) `shouldBe` True

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

within :: IO a -> IO (Maybe a)
within = timeout 60000000

-- | Runs the effect, and goes on with what came in.
afterwards :: (Arrow arr) => arr () () -> arr a a
afterwards effect = arr (,()) >>> second effect >>> arr fst

waitsForEver :: (Arrow arr, Remote arr) => arr () Int
waitsForEver = newRemote >>> arr fst >>> awaitRemote

sameCore :: (ArrowChoice arr, Concurrent arr, Remote arr) => arr () (Int, Bool)
sameCore = newRemote >>> concurrently (arr fst >>> awaitRemote) (arr (\(_, giver) -> (giver, 42)) >>> give)

givenTwice :: (ArrowChoice arr, Remote arr) => arr () Bool
givenTwice = newRemote >>> (\(_, giver) -> ((giver, 1 :: Int), giver)) ^>> first give >>> (\(_, giver) -> (giver, 2)) ^>> give

-- | The waiter abandons before anything is given: nothing for it, and the giver is told.
gone :: (ArrowChoice arr, Remote arr) => arr () (Maybe Int, Bool)
gone = newRemote >>> first abandon >>> (\(abandoned, giver) -> (abandoned, (giver, 5))) ^>> second give

-- | The value is given before the waiter abandons: the waiter has it.
givenFirst :: (ArrowChoice arr, Remote arr) => arr () (Bool, Maybe Int)
givenFirst = newRemote >>> (\(awaiting, giver) -> (awaiting, (giver, 5))) ^>> second give >>> (\(awaiting, taken) -> (taken, awaiting)) ^>> second abandon

-- | A part's place in the pool's queue, and no connection: for tests that ask for one when none is free.
queued :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool conn -> arr () (Awaiting arr conn)
queued pool = reserve pool >>> arr (fromLeft (error "a connection was free"))

-- | One part that holds a connection, queues two others behind it, one of which abandons its place, and releases.
nextWaiter :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool Int -> arr () (Maybe Int, Int, [Int])
nextWaiter pool =
  checkout pool
    >>> (,())
    ^>> second (queued pool)
    >>> (\(held, first') -> ((held, first'), ()))
    ^>> second (queued pool)
    >>> (\((held, first'), second') -> (first', (held, second')))
    ^>> first abandon
    >>> (\(abandoned, (held, second')) -> (held, (abandoned, second')))
    ^>> first (release pool)
    >>> (\((), (abandoned, second')) -> (second', abandoned))
    ^>> first awaitRemote
    >>> now (\(taken, abandoned) -> (,,) abandoned taken <$> free pool)

toFreeList :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool Int -> arr () (Maybe Int, [Int])
toFreeList pool =
  checkout pool
    >>> (,())
    ^>> second (queued pool)
    >>> (\(held, waiting) -> (waiting, held))
    ^>> first abandon
    >>> (\(abandoned, held) -> (held, abandoned))
    ^>> first (release pool)
    >>> now (\((), abandoned) -> (,) abandoned <$> free pool)

givenBeforeAbandoned :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool Int -> arr () (Maybe Int, [Int])
givenBeforeAbandoned pool =
  checkout pool
    >>> (,())
    ^>> second (queued pool)
    >>> first (release pool)
    >>> (\((), waiting) -> waiting)
    ^>> abandon
    >>> (\given -> (given, fromMaybe 0 given))
    ^>> second (release pool)
    >>> now (\(given, ()) -> (,) given <$> free pool)

-- | What the cores of a pool test wait for each other with: a flag for each thing that has happened.
data Flags = Flags {heldFlag :: IORef Bool, firstQueuedFlag :: IORef Bool, secondQueuedFlag :: IORef Bool, firstGoneFlag :: IORef Bool}

newFlag :: IO (IORef Bool)
newFlag = newIORef False

raise :: (Effect arr) => IORef Bool -> arr () ()
raise flag = now (\() -> atomicWriteIORef flag True)

-- | Waits, giving way, until the flag is raised.
spinUntil :: (ArrowChoice arr, Effect arr, GiveWay arr) => IORef Bool -> arr () ()
spinUntil flag = loop
  where
    loop = now (\() -> (\raised -> if raised then Right () else Left ()) <$> readIORef flag) >>> ((giveWay >>> loop) ||| returnA)

-- | Waits, giving way, until the reference holds a value.
spinUntilJust :: (ArrowChoice arr, Effect arr, GiveWay arr) => IORef (Maybe a) -> arr () a
spinUntilJust box = loop
  where
    loop = now (\() -> maybe (Left ()) Right <$> readIORef box) >>> ((giveWay >>> loop) ||| returnA)

-- | A core holds the only connection; a second queues for it, then a third, the second abandons its place, and
-- the first releases the connection: the third has it.
acrossCores :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => (Pool Int, Flags) -> Int -> arr () [Int]
acrossCores (pool, flags) core = case core of
  0 -> checkout pool >>> afterwards (raise (heldFlag flags) >>> spinUntil (firstGoneFlag flags)) >>> release pool >>> arr (const [])
  1 -> spinUntil (heldFlag flags) >>> queued pool >>> afterwards (raise (firstQueuedFlag flags) >>> spinUntil (secondQueuedFlag flags)) >>> abandon >>> afterwards (raise (firstGoneFlag flags)) >>> arr (maybe [] pure)
  _ -> spinUntil (firstQueuedFlag flags) >>> queued pool >>> afterwards (raise (secondQueuedFlag flags)) >>> awaitRemote >>> (\taken -> (taken, taken)) ^>> second (release pool) >>> arr (pure . fst)

-- | The same with no third core: the connection is free afterwards.
acrossCoresOnly :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => (Pool Int, Flags) -> Int -> arr () [Int]
acrossCoresOnly (pool, flags) core = case core of
  0 -> checkout pool >>> afterwards (raise (heldFlag flags) >>> spinUntil (firstGoneFlag flags)) >>> release pool >>> arr (const [])
  _ -> spinUntil (heldFlag flags) >>> queued pool >>> abandon >>> afterwards (raise (firstGoneFlag flags)) >>> arr (maybe [] pure)

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
borrowing :: (ArrowChoice arr, Effect arr, GiveWay arr, Remote arr) => Pool Conn -> IORef Int -> arr () ()
borrowing pool done = checkout pool >>> now acquire >>> now work >>> now unacquire >>> release pool
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
