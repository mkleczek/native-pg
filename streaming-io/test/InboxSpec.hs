{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}

-- | Inboxes: a channel of one value, owned by the part that collects from it, with an address that others post to. The
-- same programs, written once for any arrow that has what they need, are run by each arrow, at each reach it has.
module InboxSpec (spec) where

import Control.Arrow (Arrow, ArrowChoice, arr, second, (>>>), (>>^), (^>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar)
import Control.Exception (SomeException, throwIO)
import Control.Monad (forM_, when)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Proxy (Proxy (..))
import Network.Streaming.Classes
import Network.Streaming.Combinators (concurrently, done, sleep, tap, withInbox)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop (runLoop)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, ArrowApplicative arr, Effect arr, GiveWay arr, Block arr, Time arr, Concurrent arr, Exchange arr (Local arr), Inboxes arr (Local arr), Scope arr, Race arr, Annotate arr)

-- | The same, for the parts of several cores, which share the inboxes of any core.
type Pooled arr = (Rich arr, Inboxes arr 'Anywhere)

-- | An arrow that has an inbox of the reach.
type Boxed r arr = (Rich arr, Inboxes arr r)

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
spec = describe "inboxes" $ forM_ runners $ \(Runner name run) -> describe name $ do
  forM_ (reachings name run) $ \(Reaching reach runAt) -> describe reach $ do
    it "gives the value that is posted, and leaves none for the disposal" $ do
      noted <- newLog
      promptly (runAt (Reached (postedThenCollected noted))) `shouldReturn` Just (True, 7)
      readLog noted `shouldReturn` []

    it "makes a collect wait until a value is posted" $ do
      noted <- newLog
      promptly (runAt (Reached (collectWaitsForPost noted))) `shouldReturn` Just (7, True)
      readLog noted `shouldReturn` ["posting", "collected"]

    it "makes a post wait while the inbox is full, until the value in it is collected" $ do
      noted <- newLog
      promptly (runAt (Reached (postWaitsWhileFull noted))) `shouldReturn` Just (True, ((1, 2), True))
      readLog noted `shouldReturn` ["collecting", "posted 2"]

    it "serves the posters that wait in the order they came" $ do
      noted <- newLog
      promptly (runAt (Reached (postersInOrder noted))) `shouldReturn` Just ([0, 1, 2, 3], [True, True, True, True])
      readLog noted `shouldReturn` []

    -- A part of a Loop that waits at an inbox is held by it and is not cancelled there, as at a ticket.
    when (name /= "loop") $ do
      it "leaves the value that is posted after a collect that waited was cancelled for the next collect" $ do
        noted <- newLog
        promptly (runAt (Reached (cancelledCollect noted))) `shouldReturn` Just ("second", True, 7)
        readLog noted `shouldReturn` []

      it "passes over a post that is cancelled while it waits, and serves the posters after it" $
        promptly (runAt (Reached cancelledPost)) `shouldReturn` Just (True, (True, ("second", (1, 3, Nothing))))

    it "leaves the value in the inbox when a collect is cancelled before it takes it" $ do
      noted <- newLog
      promptly (runAt (Reached (cancelledBeforeTaking noted))) `shouldReturn` Just (True, "second", 5)
      readLog noted `shouldReturn` []

    it "gives the value left in it to the disposal, with the input, when its scope ends" $ do
      noted <- newLog
      promptly (runAt (Reached (leftAtTheEnd noted))) `shouldReturn` Just True
      readLog noted `shouldReturn` ["disposed 7 of request"]

    it "gives the value left in it to the disposal, with the input, when the body fails too" $ do
      noted <- newLog
      promptly (runAt (Reached (leftAtAFailure noted))) `shouldReturn` Just "Left user error (body failed)"
      readLog noted `shouldReturn` ["disposed 7 of request"]

    it "gives back the value left in it when it is closed, and nothing the second time" $
      promptly (runAt (Reached closedWithLeftover)) `shouldReturn` Just (True, Just 7, Nothing)

    it "releases a post that waits with False when it is closed, and says False to a post after that" $
      promptly (runAt (Reached closedUnderPoster)) `shouldReturn` Just (True, (False, Just 1), False)

    it "says False to a post to the address of an inbox whose scope has ended" $ do
      noted <- newLog
      promptly (runAt (Reached (afterTheScope noted))) `shouldReturn` Just False
      readLog noted `shouldReturn` []

  when (name == "delimited") $
    it "wakes the owner that waits on one core for a post from another, and the poster that waits on another core for a collect" $ do
      ensureCores
      shelf <- newEmptyMVar
      promptly (runCores 2 (pure shelf) acrossCores) `shouldReturn` Just [[1, 2, 3], [1, 1, 1]]

-- | The reaches that a runtime has inboxes of, as the scoped-ticket tests have them: its own core's, and, if it has cores,
-- any core's.
reachings :: String -> (forall a. Program a -> IO a) -> [Reaching]
reachings name run =
  Reaching "of the arrow's own core" (\reached -> run (atOwnCore reached))
    : [Reaching "of any core" (\reached -> runAnywhere name (atAnyCore reached)) | name /= "loop"]

-- | A program for an inbox of any reach. The proxy says which, since nothing else in the program does.
newtype Reached a = Reached (forall r arr. (Boxed r arr) => Proxy r -> arr () a)

-- | A way to run a program for the inboxes of one reach.
data Reaching = Reaching String (forall a. Reached a -> IO a)

-- | The program for the inboxes of the arrow's own core.
atOwnCore :: forall a arr. (Rich arr) => Reached a -> arr () a
atOwnCore (Reached program) = program (Proxy @(Local arr))

-- | The program for the inboxes of any core.
atAnyCore :: forall a arr. (Pooled arr) => Reached a -> arr () a
atAnyCore (Reached program) = program (Proxy @'Anywhere)

-- | A program that parts of every core can share inboxes in: the arrows of cores, of threads and of one thread run it.
runAnywhere :: String -> (forall arr. (Pooled arr) => arr () a) -> IO a
runAnywhere name program = case name of
  "threads" -> runThreads program ()
  "compiled" -> fst <$> runCompiled program ()
  _ -> fst <$> runDelimited program ()

-- | Runs the action, which gives nothing if it waits for ever.
promptly :: IO a -> IO (Maybe a)
promptly = timeout 10000000

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

-- | The inbox of a pair of it and its address, as the body of a scope is given it. The programs use one name.
type Pair r arr = (Inbox arr "numbers" r Int, Address "numbers" r Int)

-- | Posts the value to the address of the pair.
posting :: (Arrow arr, Inboxes arr r) => Int -> arr (Pair r arr) Bool
posting value = (,value) . snd ^>> post

-- | Collects from the inbox of the pair.
collecting :: (Arrow arr, Inboxes arr r) => arr (Pair r arr) Int
collecting = fst ^>> collect

-- | 'withInbox' at the reach of the proxy.
withInboxAt :: (ArrowChoice arr, Annotate arr, Scope arr, Inboxes arr r) => Proxy r -> arr (a, b) () -> arr ((Inbox arr "numbers" r a, Address "numbers" r a), b) c -> arr b c
withInboxAt _ = withInbox @"numbers"

-- | The value is posted, and then collected: nothing waits.
postedThenCollected :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () (Bool, Int)
postedThenCollected noted proxy = withInboxAt proxy (disposed noted) (fst ^>> ((,) <$> posting 7 <*> collecting))

-- | The collect begins before the post, and waits for it.
collectWaitsForPost :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () (Int, Bool)
collectWaitsForPost noted proxy =
  withInboxAt proxy (disposed noted) (fst ^>> concurrently (collecting >>> noting noted "collected") (tap (done >>> sleep (millis 30) >>> noting noted "posting") >>> posting 7))

-- | The inbox is full after the first post, and the second waits, until the collect that follows a pause takes the first
-- value and lets it in.
postWaitsWhileFull :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () (Bool, ((Int, Int), Bool))
postWaitsWhileFull noted proxy =
  withInboxAt proxy (disposed noted) $
    fst ^>> ((,) <$> posting 1 <*> concurrently collectingTwice (posting 2 >>> noting noted "posted 2"))
  where
    collectingTwice = tap (done >>> sleep (millis 40) >>> noting noted "collecting") >>> ((,) <$> collecting <*> collecting)

-- | The first post fills the inbox, and three posters come after it, 20 ms apart, and wait: the collects, which begin once
-- all of them wait, give the values in the order the posters came.
postersInOrder :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () ([Int], [Bool])
postersInOrder noted proxy =
  withInboxAt proxy (disposed noted) $
    fst ^>> ((,) <$> posting 0 <*> concurrently collectingAll posters) >>^ gathered
  where
    posters = concurrently (after 20 1) (concurrently (after 40 2) (after 60 3))
    after ms value = tap (done >>> sleep (millis ms)) >>> posting value
    collectingAll = tap (done >>> sleep (millis 120)) >>> ((\a b c d -> [a, b, c, d]) <$> collecting <*> collecting <*> collecting <*> collecting)
    gathered (filled, (values, (one, (two, three)))) = (values, [filled, one, two, three])

-- | A part that waits to collect is cancelled by a race that the other half ends at once. The value that is posted after it
-- is still there for the next collect.
cancelledCollect :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () (String, Bool, Int)
cancelledCollect noted proxy =
  withInboxAt proxy (disposed noted) $
    fst ^>> ((,,) <$> (race collecting (done >>> sleep (millis 20)) >>^ winner) <*> posting 7 <*> collecting)

-- | The part is cancelled behind a shield, which keeps the cancellation until it is out. Its collect finds a value, and the
-- cancellation is raised there, before it takes it: the next collect does.
cancelledBeforeTaking :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () (Bool, String, Int)
cancelledBeforeTaking noted proxy =
  withInboxAt proxy (disposed noted) $
    fst ^>> ((,,) <$> posting 5 <*> (race (tap (shield (done >>> sleep (millis 20))) >>> collecting) (done >>> giveWay) >>^ winner) <*> collecting)

-- | The inbox is full, and two posters wait: the one that is cancelled while it waits is passed over, and the other is
-- served, so that the inbox is empty after two collects.
cancelledPost :: forall r arr. (Boxed r arr) => Proxy r -> arr () (Bool, (Bool, (String, (Int, Int, Maybe Int))))
cancelledPost _ =
  inbox @arr @r @"numbers" >>> ((,) <$> posting 1 <*> concurrently (posting 3) (concurrently cancelled collector))
  where
    cancelled = race (posting 2) (done >>> sleep (millis 20)) >>^ winner
    collector = tap (done >>> sleep (millis 60)) >>> ((,,) <$> collecting <*> collecting <*> (fst ^>> closeInbox))

-- | A value is left in the inbox when the body ends, and goes to the disposal, with the input.
leftAtTheEnd :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () Bool
leftAtTheEnd noted proxy = const "request" ^>> withInboxAt proxy (disposedOf noted) (fst ^>> posting 7)

-- | The same when the body fails after the post.
leftAtAFailure :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () String
leftAtAFailure noted proxy =
  const "request" ^>> tryA (withInboxAt proxy (disposedOf noted) (fst ^>> posting 7 >>> (boom "body failed" :: arr Bool Bool))) >>^ describeFailure

-- | An inbox that is made and closed by hand: closing gives back the value left in it, once.
closedWithLeftover :: forall r arr. (Boxed r arr) => Proxy r -> arr () (Bool, Maybe Int, Maybe Int)
closedWithLeftover _ = inbox @arr @r @"numbers" >>> ((,,) <$> posting 7 <*> (fst ^>> closeInbox) <*> (fst ^>> closeInbox))

-- | The inbox is full, and a second post waits; the part that owns it closes it after a pause, which releases that post
-- with False, and gives back the value in it. A post after that says False at once.
closedUnderPoster :: forall r arr. (Boxed r arr) => Proxy r -> arr () (Bool, (Bool, Maybe Int), Bool)
closedUnderPoster _ =
  inbox @arr @r @"numbers" >>> ((,,) <$> posting 1 <*> concurrently (posting 2) (fst ^>> tap (done >>> sleep (millis 30)) >>> closeInbox) <*> posting 3)

-- | The address leaves the scope of its inbox, and a post to it after the scope has ended says False.
afterTheScope :: forall r arr. (Boxed r arr) => Log -> Proxy r -> arr () Bool
afterTheScope noted proxy = withInboxAt proxy (disposed noted) (arr (snd . fst)) >>> (,3) ^>> post

-- | The owner on core 0 collects three values that a poster on core 1 posts, pausing between its collects, so that it waits
-- when the inbox is empty and the poster waits when it is full.
acrossCores :: MVar (Address "numbers" 'Anywhere Int) -> Int -> Delimited s () [Int]
acrossCores shelf = \case
  0 ->
    inbox @_ @'Anywhere @"numbers"
      >>> second (now (putMVar shelf))
      >>> fst ^>> ((\a b c -> [a, b, c]) <$> pausing collect <*> pausing collect <*> pausing collect)
  _ ->
    now (const (readMVar shelf))
      >>> tap (now (const (threadDelay 20000)))
      >>> ((\a b c -> map fromEnum [a, b, c]) <$> ((,1) ^>> post) <*> ((,2) ^>> post) <*> ((,3) ^>> post))
  where
    pausing step = step >>> tap (now (const (threadDelay 10000)))

-- | Which half of a race was the first to end.
winner :: Either a b -> String
winner = either (const "first") (const "second")

-- | A failure, as the text of what it was, or of the result.
describeFailure :: (Show a) => Either SomeException a -> String
describeFailure = either (\failure -> "Left " <> show failure) (\result -> "Right " <> show result)

type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

readLog :: Log -> IO [String]
readLog = fmap reverse . readIORef

noting :: (Arrow arr, Effect arr) => Log -> String -> arr a a
noting noted entry = tap (now (const (note noted entry)))

-- | Adds to the log: parts on several cores write to it.
note :: Log -> String -> IO ()
note noted entry = atomicModifyIORef' noted (\entries -> (entry : entries, ()))

-- | Fails with the text.
boom :: (Effect arr) => String -> arr a b
boom text = now (const (throwIO (userError text)))

-- | A disposal that notes the value it was given.
disposed :: (Effect arr) => Log -> arr (Int, x) ()
disposed noted = now (\(value, _) -> note noted ("disposed " <> show value))

-- | A disposal that notes the value, and the request that is its input.
disposedOf :: (Effect arr) => Log -> arr (Int, String) ()
disposedOf noted = now (\(value, request) -> note noted ("disposed " <> show value <> " of " <> request))
