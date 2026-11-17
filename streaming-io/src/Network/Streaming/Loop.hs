{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

-- | Every concurrent part on one thread: an arrow is a program as data, which
-- a scheduler runs. A part that waits is the rest of its program and the value
-- going into it, so nothing is captured when it waits. The simulation runs the
-- same programs in an order a seed chooses, with a virtual clock that jumps to
-- the earliest deadline when no part can go on.
--
-- What follows a program is data too, so a scope is a frame in it: a failure
-- unwinds the frames, and runs the release of each resource it finds, behind a
-- shield. A cancel scope's flag is an STM variable, which the watch of a part
-- that waits includes, so that cancelling a scope wakes the parts inside it
-- with the cancellation to raise, in the same way as an event. A part that
-- waits for a ticket is held by the ticket, and is not cancelled there.
module Network.Streaming.Loop
  ( Loop,
    Report (..),
    runLoop,
    Outcome (..),
    Simulated (..),
    runSimulation,
    awaitFired,
  )
where

import Control.Arrow (Arrow (..), ArrowChoice (..), returnA)
import Control.Category (Category (..))
import Control.Concurrent.STM (STM, TVar, atomically, check, newTVarIO, orElse, readTVar, readTVarIO, retry, writeTVar)
import Control.Exception (SomeException, bracket, mask, onException, throwIO, toException, try, uninterruptibleMask_)
import Control.Monad (filterM, when)
import Data.Bits (shiftR, xor)
import qualified Data.Function as Function
import Data.Functor (($>))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.List (partition, sort)
import Data.Maybe (isJust, isNothing)
import Data.Word (Word64)
import Network.Streaming.Classes hiding (Stream (..))
import Network.Streaming.Combinators (done)
import Network.Streaming.Concurrent (Concurrent (Group, group, spawn), Joiner)
import qualified Network.Streaming.Concurrent as Joiner
import Network.Streaming.Internal.Inboxes (Address (..), Looked (..), Shelf, changeHere, closeShelf, collectedWrongly, lookOrWait, newShelf, postNow, postOrQueue, takeNow)
import Network.Streaming.Internal.Tickets
import Network.Streaming.Mode (Mode (..), ModeOf)
import Network.Streaming.Watch (Watch (..), delayWatch, fdReadable, fdWritable)
import Prelude hiding (id, (.))

-- | A program as data: functions, sequences, choices and concurrent pairs, and
-- its primitives: an action that never waits, a block until an event fires,
-- and waiting for a ticket.
data Loop a b where
  Arr :: (a -> b) -> Loop a b
  Seq :: Loop a b -> Loop b c -> Loop a c
  First :: Loop a b -> Loop (a, c) (b, c)
  Choose :: Loop a b -> Loop (Either a c) (Either b c)
  Together :: [Loop a ()] -> Loop a ()
  Now :: (a -> IO b) -> Loop a b
  Clock :: Loop () Instant
  Block :: Loop Event ()
  Await :: Loop (IORef (Slot a)) a
  AwaitRegistering :: Loop (IORef (Slot a), IO (Maybe a)) a
  HandWaiting :: Loop (IORef (Slot a), a) (Maybe a)
  UntilParked :: Loop (IORef (Slot a)) ()
  Collect :: Loop (IORef (Shelf a)) a
  Post :: Loop (IORef (Shelf a), a) Bool
  Scope' :: Loop a r -> Loop (r, Exit) () -> Loop (r, a) b -> Loop a b
  Try :: Loop a b -> Loop a (Either SomeException b)
  Shield' :: Loop a b -> Loop a b
  Race' :: Loop a b -> Loop a c -> Loop a (Either b c)
  -- | Waits until a scope beyond the first shield that the part is in is cancelled, or reaches the part outside it: the watch of a grace.
  Behind :: Beyond -> Loop () ()
  -- | Raises the cancellation that reaches the part: what a grace does when its period ended and it cut its body.
  Reraise :: Loop () b
  Group' :: (Joiner j) => j -> Loop (Group Loop s j, a) b -> Loop a (b, Joiner.Joined j)
  Spawn' :: (Joiner j) => Loop x (Joiner.Part j) -> Loop (Group Loop s j, x) ()

instance Category Loop where
  id = Arr Function.id
  (.) = flip Seq

instance Arrow Loop where
  arr = Arr
  first = First

instance Functor (Loop a) where
  fmap function step = Seq step (Arr function)

instance Applicative (Loop a) where
  pure result = Arr (const result)
  one <*> two = Seq (Seq (Arr (\a -> (a, a))) (First one)) (Seq (Arr (\(f, a) -> (a, f))) (Seq (First two) (Arr (\(x, f) -> f x))))

instance ArrowApplicative Loop

instance ArrowChoice Loop where
  left = Choose

-- | Annotations are for diagrams: the program is run as it is.
instance Annotate Loop

instance Concurrent Loop where
  newtype Group Loop s j = LoopGroup (LoopGang j)
  group joiner body = Group' joiner body
  spawn = Spawn'
  together = \case
    [] -> done
    [one] -> one
    parts -> Together parts

instance Effect Loop where
  now = Now

-- | The time of the run: the clock of the monotonic time, or in a simulation, the virtual clock.
instance Time Loop where
  clock = Clock

instance Scope Loop where
  scope = Scope'
  tryA = Try
  shield = Shield'
  grace (Duration limit) = graced limit

-- | A grace is a shield around a race of its body and a watch, which waits until a cancellation from outside is there, waits for the
-- period, and then waits until the cancellation reaches the part outside the shield: it ends, which cancels the body at its next
-- wait. The body leaves its result where the grace finds it: if it ended, the result is the result, whichever half the race
-- ended with, and the cancellation stays for the scope outside to find; if it was cut, the cancellation that is there is raised
-- outside the shield. The periods of nested graces are waited for at once, as the cancellation of the scope outside all of them is,
-- and each grace waits for the ones outside it to let it in.
graced :: Word64 -> Loop a b -> Loop a b
graced limit body =
  Now (\input -> (,) input <$> newIORef Nothing)
    `Seq` Arr (\both@(_, result) -> (both, result))
    `Seq` First (Shield' (Race' (First body `Seq` Now (\(value, result) -> writeIORef result (Just value))) watch))
    `Seq` Now (\(_, result) -> readIORef result)
    `Seq` Arr (maybe (Left ()) Right)
    `Seq` (Reraise ||| Arr Function.id)
  where
    watch =
      Arr (const ())
        `Seq` Behind Somewhere
        `Seq` Clock
        `Seq` Arr (\(Instant at) -> Until (Instant (at + limit)))
        `Seq` Block
        `Seq` Behind Deliverable

-- | What a grace's watch waits for beyond its shield: a scope that is cancelled, or one whose cancellation reaches the part there.
data Beyond = Somewhere | Deliverable

instance Race Loop where
  race = Race'

-- | A deadline is a race of the body and a wait until the instant, so the part that waits is cancelled when it
-- passes.
instance Within Loop where
  within (Duration limit) body =
    Arr ((),)
      `Seq` First Clock
      `Seq` Race' (Arr snd `Seq` body) (Arr (\(Instant at, _) -> Until (Instant (at + limit))) `Seq` Block)
      `Seq` Arr (either Just (const Nothing))
  timeoutA body =
    Arr ((),)
      `Seq` First Clock
      `Seq` Race' (Arr (snd . snd) `Seq` body) (Arr (\(Instant at, (Duration limit, _)) -> Until (Instant (at + limit))) `Seq` Block)
      `Seq` Arr (either Just (const Nothing))

-- | Nothing: the interpreter counts a step for every action that a part runs,
-- not only at a link, and gives way after 64 of them (a simulation, after each,
-- to try the orders of the parts' actions that a seed gives).
instance GiveWay Loop where
  giveWay = returnA

-- | Nothing: its interpreter runs the parts round-robin, or picks one at random in a simulation, so that no lane means anything there.
instance FastLane Loop where
  fastLane = id

-- | Suspends the part: the scheduler goes on with it once the watch fires.
instance Block Loop where
  block = Block

-- | A program as data observes nothing: 'Loop' is built in 'Release'.
type instance ModeOf Loop = 'Release

-- | The region of the tickets of this arrow: there is one core, the thread that runs it.
data Core

-- | A ticket is a slot on this thread. A part that waits for it is parked in it, with no watch, and goes back among those that can go
-- on when a part hands it a value: a ticket keeps no value for a part that is not parked. Handing a value now, abandoning the ticket and
-- saying that a part will await it never wait, so they are actions; the await, the hand that waits and the watch are primitives.
type instance Local Loop = 'Here Core

instance Exchange Loop ('Here Core) where
  newtype Future Loop ('Here Core) a = LoopFuture (IORef (Slot a))
  ticket = Now (\() -> newSlot >>= \slot -> pure (LoopFuture slot, HerePromise slot))
  await = Seq (Arr (\(LoopFuture slot) -> slot)) Await
  fulfil = Now (\(promise, value) -> fulfilNow promise value)
  abandon = Now (\(LoopFuture slot) -> closeSlot changeHere slot)
  awaitRegistering = Seq (Arr (\(LoopFuture slot, register) -> (slot, register (HerePromise slot)))) AwaitRegistering
  willAwait = Now (\future@(LoopFuture slot) -> commitRegistering changeHere slot >>= \committed -> if committed then pure future else ioError tookWrongly)
  handWaiting = Seq (Arr (\(HerePromise slot, value) -> (slot, value))) HandWaiting
  untilParked = Seq (Arr (\(HerePromise slot) -> slot)) UntilParked

-- | An inbox is a shelf on this thread, and a part that waits at it is parked there, with no watch, as at a ticket, until a post
-- or a collect lets it go on. Closing never waits, so it is an action; the collect and the post are primitives, which raise a
-- cancellation that has reached the part before they act.
instance Inboxes Loop ('Here Core) where
  newtype Inbox Loop name ('Here Core) a = LoopInbox (IORef (Shelf a))
  inbox = Now (\() -> newShelf >>= \shelf -> pure (LoopInbox shelf, HereAddress shelf))
  collect = Seq (Arr (\(LoopInbox shelf) -> shelf)) Collect
  post = Seq (Arr (\(HereAddress shelf, value) -> (shelf, value))) Post
  closeInbox = Now (\(LoopInbox shelf) -> closeShelf changeHere shelf)

-- | A part of a running program: a program with its input, or a value for
-- what follows, and what follows; or a failure, which unwinds what follows,
-- or the end of a wait, where a cancellation that came is raised.
data Process r where
  Run :: Loop a b -> a -> Frames b r -> Process r
  Give :: b -> Frames b r -> Process r
  Fail :: SomeException -> Frames b r -> Process r
  Check :: Frames () r -> Process r

-- | What the parts of a run share: whether any scope has been cancelled, which is all that most runs ever
-- ask: a part looks at the scopes it is in only once that is so, since the frames it would walk can be long; and
-- the number of the next scope.
data Env = Env !(TVar Bool) !(IORef Int)

newEnv :: IO Env
newEnv = Env <$> newTVarIO False <*> newIORef 0

-- | A cancel scope: which it is, whether it was cancelled, which the watches of the parts inside it read, and
-- the run's flag that some scope was.
data LoopScope = LoopScope !ScopeId !(TVar Bool) !(TVar Bool)

newLoopScope :: Env -> IO LoopScope
newLoopScope (Env anyCancelled next) = do
  ident <- readIORef next
  writeIORef next (ident + 1)
  (\flag -> LoopScope (ScopeId ident) flag anyCancelled) <$> newTVarIO False

cancelLoopScope :: LoopScope -> IO ()
cancelLoopScope (LoopScope _ flag anyCancelled) = atomically (writeTVar flag True *> writeTVar anyCancelled True)

-- | The scope whose cancellation reaches a part with these frames, if any scope has been cancelled at all.
cancelledNow :: Env -> Frames b r -> IO (Maybe ScopeId)
cancelledNow (Env anyCancelled _) frames = readTVarIO anyCancelled >>= \cancelled -> if cancelled then atomically (cancelledSTM frames) else pure Nothing

-- | What follows a program, as data: the rest of a sequence, the other half of
-- a pair, a choice to wrap, or the join of a concurrent pair; and what a scope
-- needs when the program ends or fails: the release of what it acquired, which
-- runs behind a shield, a failure to catch, a shield.
data Frames b r where
  Return :: Frames r r
  Then :: Loop b c -> Frames c r -> Frames b r
  Beside :: c -> Frames (b, c) r -> Frames b r
  OnLeft :: Frames (Either b c) r -> Frames b r
  Joining :: Bunch -> Frames () r -> Frames () r
  RaceFirst :: Contest b d -> Frames (Either b d) r -> Frames b r
  RaceSecond :: Contest b d -> Frames (Either b d) r -> Frames d r
  -- | What was acquired is there: the body goes on with it.
  Acquired :: Loop (x, Exit) () -> Loop (x, a) b -> a -> Frames b r -> Frames x r
  -- | The body of a scope, with what it holds.
  Guarded :: x -> Loop (x, Exit) () -> Frames b r -> Frames b r
  -- | The release of a scope runs: how the body ended, and the result or failure that goes on once it has.
  Releasing :: Exit -> Either SomeException b -> Frames b r -> Frames () r
  Trying :: Frames (Either SomeException b) r -> Frames b r
  Shielding :: Frames b r -> Frames b r
  -- | The body of a group: the group, what wakes the part that made it, and what follows the group.
  Grouped :: (Joiner j) => LoopGang j -> (Process r -> IO ()) -> Frames (b, Joiner.Joined j) r -> Frames b r
  -- | A part that was spawned into a group, which has nothing after it.
  Spawned :: (Joiner j, p ~ Joiner.Part j) => LoopGang j -> Int -> Frames p r

-- | The parts of a 'together': the cancel scope of its parts, how many there are, and what it has of how they ended.
data Bunch = Bunch !LoopScope !Int !(IORef Joined)

-- | What a group has of its first failure, and how many of its parts have ended.
data Joined = Joined !(Maybe SomeException) !Int

-- | A group that parts are spawned into: the cancel scope of its parts, what it has of them, how a cancellation of the scopes
-- outside the group is found by a part, given the scope that is found already, and whether any scope outside it is cancelled.
data LoopGang j = LoopGang !LoopScope !(IORef (Standing j)) (Maybe ScopeId -> STM (Maybe ScopeId)) (STM Bool)

-- | What a group has of its parts: how many were spawned, how many members are yet to end (the body is one), the joiner's tally, and
-- what wakes the part that made the group, once the body has ended: the last member to end does it.
data Standing j = Standing !Int !Int !(Joiner.Tally j) (Maybe (Joiner.Tally j -> IO ()))

-- | A race: the cancel scope of its halves, and its winner, once there is one.
data Contest b d = Contest !LoopScope !(IORef (Maybe (Either SomeException (Either b d))))

-- | The scope whose cancellation reaches a part with these frames: the outermost cancelled scope above the
-- innermost shield.
cancelledSTM :: Frames b r -> STM (Maybe ScopeId)
cancelledSTM = cancelledFrom Nothing

-- | The same, going on from the scope that is found already.
cancelledFrom :: Maybe ScopeId -> Frames x y -> STM (Maybe ScopeId)
cancelledFrom found = \case
  Return -> pure found
  Then _ rest -> cancelledFrom found rest
  Beside _ rest -> cancelledFrom found rest
  OnLeft rest -> cancelledFrom found rest
  Joining (Bunch scope' _ _) rest -> inside scope' found rest
  RaceFirst (Contest scope' _) rest -> inside scope' found rest
  RaceSecond (Contest scope' _) rest -> inside scope' found rest
  Acquired _ _ _ rest -> cancelledFrom found rest
  Guarded _ _ rest -> cancelledFrom found rest
  Releasing{} -> pure found
  Trying rest -> cancelledFrom found rest
  Shielding _ -> pure found
  -- The body is not in the scope of the parts: the joiner's cancellation doesn't reach it.
  Grouped _ _ rest -> cancelledFrom found rest
  Spawned (LoopGang (LoopScope ident flag _) _ outside _) _ -> readTVar flag >>= \cancelled -> outside (if cancelled then Just ident else found)
  where
    inside (LoopScope ident flag _) known rest = readTVar flag >>= \cancelled -> cancelledFrom (if cancelled then Just ident else known) rest

-- | The frames outside the first shield of a part's frames.
data Outside r = forall y. Outside (Frames y r)

outsideShield :: Frames b r -> Maybe (Outside r)
outsideShield = \case
  Return -> Nothing
  Then _ rest -> outsideShield rest
  Beside _ rest -> outsideShield rest
  OnLeft rest -> outsideShield rest
  Joining _ rest -> outsideShield rest
  RaceFirst _ rest -> outsideShield rest
  RaceSecond _ rest -> outsideShield rest
  Acquired _ _ _ rest -> outsideShield rest
  Guarded _ _ rest -> outsideShield rest
  Releasing _ _ rest -> outsideShield rest
  Trying rest -> outsideShield rest
  Shielding rest -> Just (Outside rest)
  Grouped _ _ rest -> outsideShield rest
  Spawned{} -> Nothing

-- | Whether any scope in the frames, whatever lies between, is cancelled.
cancelledAnywhereSTM :: Frames b r -> STM Bool
cancelledAnywhereSTM = go
  where
    go :: Frames x y -> STM Bool
    go = \case
      Return -> pure False
      Then _ rest -> go rest
      Beside _ rest -> go rest
      OnLeft rest -> go rest
      Joining (Bunch scope' _ _) rest -> inside scope' rest
      RaceFirst (Contest scope' _) rest -> inside scope' rest
      RaceSecond (Contest scope' _) rest -> inside scope' rest
      Acquired _ _ _ rest -> go rest
      Guarded _ _ rest -> go rest
      Releasing _ _ rest -> go rest
      Trying rest -> go rest
      Shielding rest -> go rest
      -- The body is not in the scope of the parts of its group.
      Grouped _ _ rest -> go rest
      Spawned (LoopGang (LoopScope _ flag _) _ _ outside) _ -> readTVar flag >>= \cancelled -> if cancelled then pure True else outside
    inside (LoopScope _ flag _) rest = readTVar flag >>= \cancelled -> if cancelled then pure True else go rest

-- | What a grace's watch waits for, in the frames of the part: retries until it is there.
beyondSTM :: Beyond -> Frames b r -> STM ()
beyondSTM beyond frames = case outsideShield frames of
  Nothing -> retry
  Just (Outside rest) -> case beyond of
    Somewhere -> cancelledAnywhereSTM rest >>= check
    Deliverable -> cancelledSTM rest >>= maybe retry (const (pure ()))

-- | Whether a grace's watch has what it waits for, without waiting, if any scope has been cancelled at all.
beyondNow :: Env -> Beyond -> Frames b r -> IO Bool
beyondNow (Env anyCancelled _) beyond frames =
  readTVarIO anyCancelled >>= \cancelled -> if cancelled then atomically ((True <$ beyondSTM beyond frames) `orElse` pure False) else pure False

-- | The watch of a part that waits for an event: the event, or a cancellation that reaches the part.
watchFor :: Env -> Event -> Process r -> IO Watch
watchFor (Env anyCancelled _) event process = (\(Watch ready cancel) -> Watch (ready `orElse` cancelled) cancel) <$> watchOf event
  where
    cancelled = case process of
      Check frames -> readTVar anyCancelled >>= check >> cancelledSTM frames >>= maybe retry (const (pure ()))
      _ -> retry

-- | What a part came to.
data Advanced r
  = Finished r
  | -- | It ended at a join, which the other half goes on from.
    Ended
  | -- | It waits for a ticket, whose slot holds it.
    Parked
  | -- | It used its steps, and can go on.
    Yielded (Process r)
  | Forked [Process r]
  | -- | It started a group, whose body goes on, and waits until the group has ended, which wakes it.
    Grouping (Process r)
  | Waits Event (Process r)

-- | A part of a group has ended, with a failure or without: the first failure cancels the others, and the part that
-- ends last goes on with the parent, with the failure if there was one.
joinPart :: Bunch -> Frames () r -> Maybe SomeException -> IO (Maybe (Process r))
joinPart (Bunch scope' size ref) rest failure = do
  Joined failed count <- readIORef ref
  failed' <- case failure of
    Just thrown | isNothing failed -> cancelLoopScope scope' $> Just thrown
    _ -> pure failed
  writeIORef ref (Joined failed' (count + 1))
  pure $ if count + 1 < size then Nothing else Just (maybe (Give () rest) (`Fail` rest) failed')

-- | A half of a race has ended: the first is the winner, which cancels the other; the loser lets the race go on
-- with the winner's failure, or its own if it isn't its cancellation, or the winner's result.
raceHalf :: Contest b d -> Frames (Either b d) r -> Either SomeException (Either b d) -> IO (Maybe (Process r))
raceHalf (Contest scope' ref) rest outcome =
  readIORef ref >>= \case
    Nothing -> Nothing <$ (writeIORef ref (Just outcome) *> cancelLoopScope scope')
    Just won -> pure $ case (won, outcome) of
      (Left thrown, _) -> Just (Fail thrown rest)
      (Right _, Left thrown) | isNothing (cancelledBy thrown) -> Just (Fail thrown rest)
      (Right result, _) -> Just (Give result rest)

-- | The number of the next spawn into the group, or nothing if the parts of the group were cancelled: the spawn then counts as a
-- part that ended cancelled, and no part runs.
spawnNumber :: forall j. (Joiner j) => LoopGang j -> IO (Maybe Int)
spawnNumber (LoopGang (LoopScope ident flag _) standing _ _) = do
  Standing number live tally finish <- readIORef standing
  readTVarIO flag >>= \cancelled ->
    if cancelled
      then Nothing <$ (writeIORef standing $! Standing (number + 1) live (fst (Joiner.ended @j number (Left (toException (Cancelled ident))) tally)) finish)
      else Just number <$ (writeIORef standing $! Standing (number + 1) (live + 1) tally finish)

-- | The body of a group ended: the parts are cancelled if it failed, or was cancelled. If no part is left, the part that made the group
-- is woken, with the body's failure, or its result and what the joiner made of the parts, or the failure that the joiner gives; if
-- some are, the one that ends last wakes it.
groupBodyEnded :: forall j b r. (Joiner j) => LoopGang j -> (Process r -> IO ()) -> Frames (b, Joiner.Joined j) r -> Either SomeException b -> IO ()
groupBodyEnded (LoopGang scope' standing _ _) wake rest outcome = do
  either (\_ -> cancelLoopScope scope') (\_ -> pure ()) outcome
  Standing spawned live tally _ <- readIORef standing
  let finish tally' =
        wake $ case outcome of
          Left thrown -> Fail thrown rest
          Right result -> either (`Fail` rest) (\extra -> Give (result, extra) rest) (Joiner.joined @j tally')
  writeIORef standing $! Standing spawned (live - 1) tally (Just finish)
  when (live == 1) (finish tally)

-- | A part of the group ended in this way, which was spawned as this number: the joiner is told, and the parts are cancelled if it
-- says so. The part that ends last wakes the part that made the group.
groupPartEnded :: forall j. (Joiner j) => LoopGang j -> Int -> Either SomeException (Joiner.Part j) -> IO ()
groupPartEnded (LoopGang scope' standing _ _) number outcome = do
  Standing spawned live tally finish <- readIORef standing
  let (tally', cancelling) = Joiner.ended @j number outcome tally
  when cancelling (cancelLoopScope scope')
  writeIORef standing $! Standing spawned (live - 1) tally' finish
  when (live == 1) (maybe (pure ()) ($ tally') finish)

-- | Runs a part until it waits, forks, ends, or has taken this many steps of
-- its primitives: what it came to, and how many steps it took. A part it parks
-- on a ticket goes back with the given action when its promise is fulfilled.
advance :: forall r. Env -> IO Instant -> Int -> (Process r -> IO ()) -> Process r -> IO (Int, Advanced r)
advance env time fuel wake = go 0
  where
    go :: Int -> Process r -> IO (Int, Advanced r)
    go steps process
      | steps >= fuel = pure (steps, Yielded process)
      | otherwise = case process of
          Run program input frames -> case program of
            Arr function -> go steps (Give (function input) frames)
            Seq before after -> go steps (Run before input (Then after frames))
            First inner -> case input of
              (a, c) -> go steps (Run inner a (Beside c frames))
            Choose inner -> case input of
              Left a -> go steps (Run inner a (OnLeft frames))
              Right c -> go steps (Give (Right c) frames)
            Together parts -> do
              scope' <- newLoopScope env
              bunch <- Bunch scope' (length parts) <$> newIORef (Joined Nothing 0)
              pure (steps, Forked [Run part input (Joining bunch frames) | part <- parts])
            Group' joiner body -> do
              scope' <- newLoopScope env
              standing <- newIORef $! Standing 0 1 (Joiner.begin joiner) Nothing
              let gang = LoopGang scope' standing (\found -> cancelledFrom found frames) (cancelledAnywhereSTM frames)
              pure (steps, Grouping (Run body (LoopGroup gang, input) (Grouped gang wake frames)))
            Spawn' part -> case input of
              (LoopGroup gang, x) ->
                spawnNumber gang >>= \case
                  Nothing -> go steps (Give () frames)
                  Just number -> pure (steps + 1, Forked [Run part x (Spawned gang number), Give () frames])
            Race' one two -> do
              scope' <- newLoopScope env
              contest <- Contest scope' <$> newIORef Nothing
              pure (steps, Forked [Run one input (RaceFirst contest frames), Run two input (RaceSecond contest frames)])
            Now action -> try (action input) >>= either (\failure -> go steps (Fail failure frames)) (\output -> go (steps + 1) (Give output frames))
            Clock -> time >>= \instant -> go (steps + 1) (Give instant frames)
            Block ->
              cancelledNow env frames >>= \case
                Just ident -> go steps (Fail (toException (Cancelled ident)) frames)
                Nothing -> pure (steps + 1, Waits input (Check frames))
            Await ->
              valueWaits input >>= \there ->
                let parking =
                      -- The part parks, and goes back with the value when a part hands it one.
                      parkTaker changeHere input (\value -> True <$ wake (Give value frames)) >>= \case
                        Parks -> pure (steps + 1, Parked)
                        Took value -> go (steps + 1) (Give value frames)
                        Misused -> go steps (Fail (toException tookWrongly) frames)
                 in if there
                      then
                        takeWaiting changeHere input >>= \case
                          Took value -> go (steps + 1) (Give value frames)
                          Misused -> go steps (Fail (toException tookWrongly) frames)
                          Parks -> parking
                      else parking
            AwaitRegistering -> case input of
              (slot, register) ->
                beginRegistering changeHere slot >>= \case
                  Took value -> go (steps + 1) (Give value frames)
                  Misused -> go steps (Fail (toException tookWrongly) frames)
                  Parks ->
                    try register >>= \case
                      Left failure -> giveUpRegistering changeHere slot *> go steps (Fail failure frames)
                      Right (Just value) -> giveUpRegistering changeHere slot >>= \clean -> if clean then go (steps + 1) (Give value frames) else go steps (Fail (toException registeredAndGave) frames)
                      Right Nothing ->
                        endRegistering changeHere slot >>= \case
                          Took value -> go (steps + 1) (Give value frames)
                          Misused -> go steps (Fail (toException tookWrongly) frames)
                          Parks ->
                            parkTaker changeHere slot (\value -> True <$ wake (Give value frames)) >>= \case
                              Parks -> pure (steps + 1, Parked)
                              Took value -> go (steps + 1) (Give value frames)
                              Misused -> go steps (Fail (toException tookWrongly) frames)
            HandWaiting -> case input of
              (slot, value) ->
                handTry changeHere slot value >>= \case
                  Passed -> go (steps + 1) (Give Nothing frames)
                  GaveBack -> go (steps + 1) (Give (Just value) frames)
                  SecondSender -> go steps (Fail (toException twoSenders) frames)
                  _ ->
                    parkSender changeHere slot value 0 (\taken -> wake (Give (if taken then Nothing else Just value) frames)) >>= \case
                      LeftWaiting -> pure (steps + 1, Parked)
                      Passed -> go (steps + 1) (Give Nothing frames)
                      GaveBack -> go (steps + 1) (Give (Just value) frames)
                      _ -> go steps (Fail (toException twoSenders) frames)
            UntilParked ->
              lookAtWatch input >>= \case
                WatchDone -> go (steps + 1) (Give () frames)
                SecondWatcher -> go steps (Fail (toException twoWatchers) frames)
                WatchWaits ->
                  parkWatcher changeHere input 0 (wake (Give () frames)) >>= \case
                    WatchWaits -> pure (steps + 1, Parked)
                    WatchDone -> go (steps + 1) (Give () frames)
                    SecondWatcher -> go steps (Fail (toException twoWatchers) frames)
            Collect ->
              cancelledNow env frames >>= \case
                Just ident -> go steps (Fail (toException (Cancelled ident)) frames)
                Nothing ->
                  takeNow changeHere input >>= \case
                    Just value -> go (steps + 1) (Give value frames)
                    -- The part parks, and goes on by collecting again when a post wakes it.
                    Nothing ->
                      lookOrWait changeHere input (wake (Run Collect input frames)) >>= \case
                        Registered -> pure (steps + 1, Parked)
                        Found -> go steps process
                        Unusable -> collectedWrongly
            Post -> case input of
              (shelf, value) ->
                cancelledNow env frames >>= \case
                  Just ident -> go steps (Fail (toException (Cancelled ident)) frames)
                  Nothing ->
                    postNow changeHere shelf value >>= \case
                      Just taken -> go (steps + 1) (Give taken frames)
                      Nothing -> do
                        token <- newIORef ()
                        _ <- postOrQueue changeHere shelf token value (\outcome -> wake (Give outcome frames))
                        pure (steps + 1, Parked)
            Scope' acquire release body -> go steps (Run acquire input (Acquired release body input frames))
            Try inner -> go steps (Run inner input (Trying frames))
            Shield' inner -> go steps (Run inner input (Shielding frames))
            Behind beyond ->
              beyondNow env beyond frames >>= \case
                True -> go steps (Give () frames)
                False -> pure (steps + 1, Waits (Watched (pure (Watch (beyondSTM beyond frames) (pure ())))) (Check frames))
            Reraise ->
              cancelledNow env frames >>= \case
                Just ident -> go steps (Fail (toException (Cancelled ident)) frames)
                Nothing -> ioError (userError "a grace ended and found no cancellation to deliver")
          Give value frames -> case frames of
            Return -> pure (steps, Finished value)
            Then next rest -> go steps (Run next value rest)
            Beside c rest -> go steps (Give (value, c) rest)
            OnLeft rest -> go steps (Give (Left value) rest)
            Joining bunch rest -> joinPart bunch rest Nothing >>= endHalf steps
            RaceFirst contest rest -> raceHalf contest rest (Right (Left value)) >>= endHalf steps
            RaceSecond contest rest -> raceHalf contest rest (Right (Right value)) >>= endHalf steps
            Grouped gang waking rest -> groupBodyEnded gang waking rest (Right value) *> pure (steps, Ended)
            Spawned gang number -> groupPartEnded gang number (Right value) *> pure (steps, Ended)
            Acquired release body input rest -> go steps (Run body (value, input) (Guarded value release rest))
            Guarded held release rest -> go steps (Run release (held, Done) (Releasing Done (Right value) rest))
            Releasing _ outcome rest -> go steps (either (`Fail` rest) (`Give` rest) outcome)
            Trying rest -> go steps (Give (Right value) rest)
            Shielding rest -> go steps (Give value rest)
          Fail failure frames -> case frames of
            Return -> throwIO failure
            Then _ rest -> go steps (Fail failure rest)
            Beside _ rest -> go steps (Fail failure rest)
            OnLeft rest -> go steps (Fail failure rest)
            Joining bunch rest -> joinPart bunch rest (Just failure) >>= endHalf steps
            RaceFirst contest rest -> raceHalf contest rest (Left failure) >>= endHalf steps
            RaceSecond contest rest -> raceHalf contest rest (Left failure) >>= endHalf steps
            Grouped gang waking rest -> groupBodyEnded gang waking rest (Left failure) *> pure (steps, Ended)
            Spawned gang number -> groupPartEnded gang number (Left failure) *> pure (steps, Ended)
            Acquired _ _ _ rest -> go steps (Fail failure rest)
            Guarded held release rest ->
              let exit = exitOf (Left failure)
               in go steps (Run release (held, exit) (Releasing exit (Left failure) rest))
            Releasing exit _ rest -> go steps (Fail (toException (ReleaseFailed exit failure)) rest)
            Trying rest -> go steps (if isJust (cancelledBy failure) then Fail failure rest else Give (Left failure) rest)
            Shielding rest -> go steps (Fail failure rest)
          Check frames ->
            cancelledNow env frames >>= \case
              Just ident -> go steps (Fail (toException (Cancelled ident)) frames)
              Nothing -> go steps (Give () frames)
    -- A half has ended: the pair goes on if it was the last, and the half ends if not.
    endHalf :: Int -> Maybe (Process r) -> IO (Int, Advanced r)
    endHalf steps = maybe (pure (steps, Ended)) (go steps)

-- | An event's watch: the scheduler waits on all of its parts' at once.
watchOf :: Event -> IO Watch
watchOf = \case
  Readable fd -> fdReadable fd
  Writable fd -> fdWritable fd
  Watched register -> register
  Until (Instant at) -> do
    current <- monotonicNanoseconds
    delayWatch (fromIntegral (if at > current then (at - current) `div` 1000 else 0))

-- | What a run did: how many steps, and how many waits. In a run of 'Loop' or
-- a simulation a step is any primitive that a part ran. In a run of
-- "Network.Streaming.Compiled" or "Network.Streaming.Delimited" it is only
-- a point to give way at, which is a link's receive or send, a wait, or a
-- ticket fulfilled or awaited.
data Report = Report {reportSteps :: !Int, reportWaits :: !Int}
  deriving (Eq, Show)

-- | Runs the program and all of its concurrent parts on this thread. It runs
-- each part that can go on in turn, for up to 64 steps, and when none can,
-- waits once for all of their watches. Parts parked on tickets go on once the
-- tickets have their values.
runLoop :: Loop a b -> a -> IO (b, Report)
runLoop program input = do
  woken <- newIORef []
  env <- newEnv
  let
    -- The parts that tickets woke join those that can go on.
    go report parked ready waiting = do
      risen <- reverse <$> readIORef woken
      writeIORef woken []
      step report (parked - length risen) (ready <> risen) waiting
    step report parked ready waiting = case ready of
      process : others -> do
        (steps, advanced) <- advance env (Instant <$> monotonicNanoseconds) 64 (\woke -> modifyIORef' woken (woke :)) process
        let report' = report{reportSteps = reportSteps report + steps}
        case advanced of
          Finished value -> pure (value, report')
          Ended -> go report' parked others waiting
          Parked -> go report' (parked + 1) others waiting
          Yielded process' -> go report' parked (others <> [process']) waiting
          Forked parts -> go report' parked (others <> parts) waiting
          Grouping body -> go report' (parked + 1) (others <> [body]) waiting
          Waits event process' -> go report' parked others ((watchFor env event process', process') : waiting)
      []
        | null waiting && parked > (0 :: Int) -> ioError (userError "runLoop: parts wait for tickets that nothing will fulfil")
        | null waiting -> ioError (userError "runLoop: the program ended without a result")
        | otherwise -> do
            fired <- awaitFired (zip (map fst waiting) [0 :: Int ..])
            let (wake, still) = partition ((`elem` fired) . fst) (zip [0 ..] waiting)
            go report{reportWaits = reportWaits report + 1} parked (map (snd . snd) wake) (map snd still)
  go (Report 0 0) 0 [Run program input Return] []

-- | How a simulation ended.
data Outcome b
  = Completed b
  | -- | These parts wait for each other: none of their watches can fire, and
    -- no ticket they wait for will be fulfilled.
    Deadlocked [Int]
  | -- | It took the most steps it was allowed.
    GaveUp
  deriving (Eq, Show)

data Simulated b = Simulated
  { simulatedOutcome :: Outcome b,
    simulatedReport :: Report,
    -- | The parts, by number, in the order they stepped.
    simulatedTrace :: [Int]
  }

-- | Runs the program on this thread in an order the seed chooses: one step of
-- a primitive at a time, of a part picked at random among those that can go
-- on, up to the given number of steps. It never waits: when no part can go
-- on, it looks at their watches and goes on with those that have fired; if
-- none has, but a part waits for an instant, the virtual clock jumps to the
-- earliest, so that time costs nothing and a run is the same every time; if
-- none does, the parts wait for each other, or for tickets that nothing will
-- fulfil, which it reports. The clock starts at 0. Its links must be
-- simulated ones, whose watches depend only on what the parts did, so that a
-- seed always gives the same run.
runSimulation :: Word64 -> Int -> Loop a b -> a -> IO (Simulated b)
runSimulation seed limit program input = do
  woken <- newIORef []
  env <- newEnv
  virtual <- newIORef 0
  let
    -- The parts that tickets woke join those that can go on.
    go random next report trace parked ready waiting = do
      risen <- reverse <$> readIORef woken
      writeIORef woken []
      step random next report trace (filter (`notElem` map fst risen) parked) (ready <> risen) waiting
    step random next report trace parked ready waiting
      | reportSteps report >= limit = finish GaveUp
      | null ready && null waiting && null parked = ioError (userError "runSimulation: the program ended without a result")
      | null ready = do
          checked <- traverse (\entry@(_, (event, process')) -> (,) entry <$> fires env virtual event process') waiting
          let (wake, still) = partition snd checked
              deadlines = [at | ((_, (Until (Instant at), _)), _) <- still]
          case () of
            _
              | not (null wake) -> go random next report{reportWaits = reportWaits report + length wake} trace parked [(ident, process) | ((ident, (_, process)), _) <- wake] (map fst still)
              -- Nothing can go on before the first deadline: the clock goes there.
              | not (null deadlines) -> modifyIORef' virtual (max (minimum deadlines)) *> step random next report trace parked ready waiting
              | otherwise -> finish (Deadlocked (sort (map (fst . fst) still <> parked)))
      | otherwise = do
          let (random', pick) = splitMix random
              ((ident, process), others) = takeAt (fromIntegral (pick `mod` fromIntegral (length ready))) ready
              trace' = ident : trace
          (steps, advanced) <- advance env (Instant <$> readIORef virtual) 1 (\woke -> modifyIORef' woken ((ident, woke) :)) process
          let report' = report{reportSteps = reportSteps report + steps}
          case advanced of
            Finished value -> pure (Simulated (Completed value) report' (reverse trace'))
            Ended -> go random' next report' trace' parked others waiting
            Parked -> go random' next report' trace' (ident : parked) others waiting
            Yielded process' -> go random' next report' trace' parked ((ident, process') : others) waiting
            Forked parts -> go random' (next + length parts) report' trace' parked (zip [next ..] parts <> others) waiting
            Grouping body -> go random' (next + 1) report' trace' (ident : parked) ((next, body) : others) waiting
            Waits event process' -> go random' next report' trace' parked others ((ident, (event, process')) : waiting)
      where
        finish outcome = pure (Simulated outcome report (reverse trace))
  go seed 1 (Report 0 0) [] [] [(0, Run program input Return)] []

-- | Registers the watches, waits until one has fired, and gives the values of
-- all that have fired by then. It releases them all, also if it is
-- interrupted.
awaitFired :: [(IO Watch, a)] -> IO [a]
awaitFired entries = mask $ \restore -> do
  registered <- newIORef []
  let register (make, value) = do
        watch <- make
        modifyIORef' registered (watch :)
        pure (watch, value)
      release = uninterruptibleMask_ (readIORef registered >>= mapM_ watchCancel)
  fired <-
    ( do
        watches <- traverse register entries
        restore (atomically (foldr (orElse . watchReady . fst) retry watches))
        filterM (hasFired . fst) watches
    )
      `onException` release
  map snd fired <$ release

-- | Whether the event has fired, without waiting: an instant has, if the virtual clock is at or past it.
fires :: Env -> IORef Word64 -> Event -> Process r -> IO Bool
fires env virtual event process = case event of
  Until (Instant at) -> (||) . (>= at) <$> readIORef virtual <*> cancelled
  _ -> anyFired (watchFor env event process)
  where
    cancelled = case process of
      Check frames -> isJust <$> cancelledNow env frames
      _ -> pure False

-- | Whether the event has fired, without waiting.
anyFired :: IO Watch -> IO Bool
anyFired make = bracket make watchCancel hasFired

hasFired :: Watch -> IO Bool
hasFired watch = atomically ((True <$ watchReady watch) `orElse` pure False)

-- | The element at the index, and the others in their order.
takeAt :: Int -> [a] -> (a, [a])
takeAt index list = case splitAt index list of
  (before, chosen : after) -> (chosen, before <> after)
  (_, []) -> error "takeAt: the index is past the end"

-- | SplitMix64: the next state, and a random number.
splitMix :: Word64 -> (Word64, Word64)
splitMix state = (state', mixed `xor` (mixed `shiftR` 31))
  where
    state' = state + 0x9e3779b97f4a7c15
    z = (state' `xor` (state' `shiftR` 30)) * 0xbf58476d1ce4e5b9
    mixed = (z `xor` (z `shiftR` 27)) * 0x94d049bb133111eb
