{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | What a ticket is made of, for the instances of 'Network.Streaming.Classes.Exchange': the promise end, and the slot that both
-- ends share, with the changes that the arrows make to it. The package doesn't expose this module: a program that could make a
-- 'Promise' of its own would choose its reach.
--
-- A ticket is a rendezvous, used over and over, and the slot keeps no value for a receiver that is not there: a value is its sender's
-- until a receiver takes it, and then the receiver's. The slot is one reference, which is empty again after each exchange. A change is
-- a function of the whole slot, applied with plain reads and writes by the parts of one core, and by compare-and-swap by the parts of
-- any core, so that one set of changes serves both reaches and every arrow. A part that waits leaves in the slot what puts it back
-- among the parts that can go on: each arrow makes that, and the slot only calls it.
--
-- The one case where a value waits for a receiver is the receiver's own: a receiver that has said it is about to hand its promise out
-- and await (it is 'Registering') has committed to take, so a value handed to it then is its, held until its await, and is given back
-- by 'closeSlot' if it never awaits.
module Network.Streaming.Internal.Tickets
  ( Reach (..),
    Promise (..),
    Slot,
    newSlot,
    newAwaitedSlot,
    Fulfilling (..),
    Taking (..),
    valueWaits,
    takeWaiting,
    parkTaker,
    beginRegistering,
    commitRegistering,
    endRegistering,
    giveUpRegistering,
    stopTaking,
    Handed (..),
    handTry,
    parkSender,
    withdrawSender,
    Watcher (..),
    adapting,
    anywhereSlot,
    lookAtWatch,
    parkWatcher,
    unwatch,
    closeSlot,
    tookWrongly,
    twoSenders,
    twoWatchers,
    registeredAndGave,
  )
where

import Control.Exception (IOException)
import Data.IORef (IORef, newIORef, readIORef)
import Data.Kind (Type)
import Network.Streaming.Internal.Atomic (Change, changeHere, update)

-- | Where a ticket may be fulfilled: on its own core only, which is the region @s@ of the arrow that made it, or from
-- any core.
data Reach = Here Type | Anywhere

-- | The end of a ticket that hands a value over. A promise of reach @'Here s@ mentions the region @s@, so that it can't leave the core
-- that made the ticket; a promise of reach @'Anywhere@ mentions no core, and can be kept in structures that cores share.
data family Promise (r :: Reach) a

-- | A promise of a ticket of this core: its slot, which only the parts of this core read and change.
newtype instance Promise ('Here _s) a = HerePromise (IORef (Slot a))

-- | A promise of a ticket of any core: its slot, which the parts of every core change atomically; or one that adapts another, which
-- only fulfils.
data instance Promise 'Anywhere a
  = AnywherePromise !(IORef (Slot a))
  | AdaptedPromise !(a -> IO Bool)

-- | A promise that is fulfilled by the function, which says whether the value was taken: for a pool that keeps, for a part that waits, a
-- promise of the value that it holds, which adapts the promise of what the part awaits. It can only be fulfilled: a hand that waits, or a
-- watch, of one fails.
adapting :: (a -> IO Bool) -> Promise 'Anywhere a
adapting = AdaptedPromise

-- | The slot of a promise of any core, for a hand that waits or a watch, which an adapting promise can't have.
anywhereSlot :: Promise 'Anywhere a -> IO (IORef (Slot a))
anywhereSlot = \case
  AnywherePromise slot -> pure slot
  AdaptedPromise _ -> ioError (userError "a promise that adapts another can be fulfilled, and not handed to with a wait or watched")

-- | What the slot holds:
--
-- * 'Idle': nothing waits.
-- * 'Registering': the receiver is about to await, and a value handed to it now is its.
-- * 'Delivered': the receiver is about to await, and a value was handed to it, which its await takes.
-- * 'Awaiting': the receiver is parked. It is how to give it a value, which says whether it will take it: it won't if its core has
--   ended.
-- * 'Offered': a sender waits for the receiver, with its value, a token that names it, and what wakes it with whether the value was
--   taken.
-- * 'Closed': the ticket was abandoned.
-- * 'Watched': a part waits until the receiver is parked, which a token names, and what wakes it. It is over 'Idle' or 'Offered'.
--
-- A value is evaluated: a thunk would keep what it was made from alive as long as the slot.
data Slot a
  = Idle
  | Registering
  | Delivered !a
  | Awaiting !(a -> IO Bool)
  | Offered !a !Int !(Bool -> IO ())
  | Closed
  | Watched !Int !(IO ()) !(Slot a)

-- | A new ticket's slot: nothing waits.
newSlot :: IO (IORef (Slot a))
newSlot = newIORef Idle

-- | The receiver is committed before either end of the ticket is published.
newAwaitedSlot :: IO (IORef (Slot a))
newAwaitedSlot = newIORef Registering
{-# INLINE newAwaitedSlot #-}

-- | Promises that an action fulfils, as the arrow's own 'Network.Streaming.Classes.fulfil' does but without its point to give
-- way at, so that it is a step of an arrow that can't wait: for what hands a value over together with what must not be left
-- half done if a cancellation came between, and for an action of a structure that parts share, which has no arrow to fulfil in.
-- It says whether the receiver took the value: it did if it is parked, or has said it is about to await, and it didn't if no receiver
-- is, or the ticket was abandoned, and then the value is the caller's. A promise of reach @'Here s@ is fulfilled by an action that
-- runs on the core of its arrow, as the part that awaits it is put back among those that can go on there.
class Fulfilling (r :: Reach) where
  fulfilNow :: Promise r a -> a -> IO Bool

-- | A promise of any core is fulfilled by compare-and-swap.
instance Fulfilling 'Anywhere where
  fulfilNow = \case
    AnywherePromise slot -> fulfilSlot update slot
    AdaptedPromise fulfilling -> fulfilling
  {-# INLINE fulfilNow #-}

-- | A promise of one core is fulfilled by plain reads and writes.
instance Fulfilling ('Here s) where
  fulfilNow (HerePromise slot) = fulfilSlot changeHere slot
  {-# INLINE fulfilNow #-}

-- | A change of what is under a watcher: the watcher stays, unless the receiver is parked or committed to take after the change, or
-- the ticket is closed, which wake it.
beneath :: Int -> IO () -> (Slot a -> (Slot a, (r, IO ()))) -> Slot a -> (Slot a, (r, IO ()))
beneath token signal change inner = case change inner of
  (new@Awaiting {}, (result, wake)) -> (new, (result, signal *> wake))
  (new@Registering, (result, wake)) -> (new, (result, signal *> wake))
  (new@Closed, (result, wake)) -> (new, (result, signal *> wake))
  (new, outcome) -> (Watched token signal new, outcome)
{-# INLINE beneath #-}

-- | What a take found.
data Taking a
  = -- | A value was there, and is the receiver's.
    Took !a
  | -- | No value is there: the receiver may park.
    Parks
  | -- | Another part awaits, or the ticket was abandoned.
    Misused

-- | Whether a value waits for the receiver: a sender's, or one that was handed to the receiver while it registered. A look, which
-- changes nothing.
valueWaits :: IORef (Slot a) -> IO Bool
valueWaits slot =
  readIORef slot >>= \case
    Offered {} -> pure True
    Delivered _ -> pure True
    Watched _ _ Offered {} -> pure True
    _ -> pure False
{-# INLINE valueWaits #-}

-- | The receiver takes the value that waits, if one does, and wakes its sender: a sender's, or one that was handed to the receiver
-- while it registered. If none waits, nothing changes.
takeWaiting :: Change -> IORef (Slot a) -> IO (Taking a)
takeWaiting change slot = do
  (taking, wake) <- change slot takeStep
  taking <$ wake
{-# INLINE takeWaiting #-}

takeStep :: Slot a -> (Slot a, (Taking a, IO ()))
takeStep = \case
  Watched token signal inner -> beneath token signal takeInner inner
  other -> takeInner other
  where
    takeInner = \case
      Offered value _ woken -> (Idle, (Took value, woken True))
      Delivered value -> (Idle, (Took value, pure ()))
      idle@Idle -> (idle, (Parks, pure ()))
      registering@Registering -> (registering, (Parks, pure ()))
      other -> (other, (Misused, pure ()))
    {-# INLINE takeInner #-}
{-# INLINE takeStep #-}

-- | The receiver parks, with how to give it a value, which says whether it takes it. It takes a value that waits instead, if one does,
-- since a sender came since the look. A watcher is woken.
parkTaker :: Change -> IORef (Slot a) -> (a -> IO Bool) -> IO (Taking a)
parkTaker change slot deliver = do
  (taking, wake) <- change slot parkStep
  taking <$ wake
  where
    parkStep = \case
      Watched token signal inner -> beneath token signal parkInner inner
      other -> parkInner other
    -- No recursion, so that the change is inlined where the reach is known, and allocates no pair.
    parkInner = \case
      Idle -> (Awaiting deliver, (Parks, pure ()))
      Registering -> (Awaiting deliver, (Parks, pure ()))
      Offered value _ woken -> (Idle, (Took value, woken True))
      Delivered value -> (Idle, (Took value, pure ()))
      other -> (other, (Misused, pure ()))
    {-# INLINE parkInner #-}
{-# INLINE parkTaker #-}

-- | The receiver is about to run its registration action and await: a value handed to it from now on is its. A value that waits is
-- taken instead, and the registration doesn't run. A watcher is woken, as the receiver is committed to take.
beginRegistering :: Change -> IORef (Slot a) -> IO (Taking a)
beginRegistering change slot = do
  (taking, wake) <- change slot beginStep
  taking <$ wake
  where
    beginStep = \case
      Watched token signal inner -> beneath token signal beginInner inner
      other -> beginInner other
    beginInner = \case
      Idle -> (Registering, (Parks, pure ()))
      Offered value _ woken -> (Idle, (Took value, woken True))
      Delivered value -> (Idle, (Took value, pure ()))
      registering@Registering -> (registering, (Parks, pure ()))
      other -> (other, (Misused, pure ()))
    {-# INLINE beginInner #-}
{-# INLINE beginRegistering #-}

-- | The receiver is about to hand its promise out with steps that may wait, and await: as 'beginRegistering', but a sender that waits
-- is left to wait, for the await to take. It says whether the ticket can be used so.
commitRegistering :: Change -> IORef (Slot a) -> IO Bool
commitRegistering change slot = do
  (committed, wake) <- change slot commitStep
  committed <$ wake
  where
    commitStep = \case
      Idle -> (Registering, (True, pure ()))
      kept@Offered {} -> (kept, (True, pure ()))
      kept@Registering -> (kept, (True, pure ()))
      kept@(Delivered _) -> (kept, (True, pure ()))
      Watched token signal inner -> case inner of
        Idle -> (Registering, (True, signal))
        _ -> (Watched token signal inner, (True, pure ()))
      other -> (other, (False, pure ()))
{-# INLINE commitRegistering #-}

-- | The receiver's registration action ended with no value, and the receiver is about to park: a value that was handed to it
-- meanwhile is taken at once.
endRegistering :: Change -> IORef (Slot a) -> IO (Taking a)
endRegistering change slot = change slot $ \case
  Delivered value -> (Idle, Took value)
  registering@Registering -> (registering, Parks)
  other -> (other, Misused)
{-# INLINE endRegistering #-}

-- | The receiver's registration action gave a value, so the receiver doesn't park. It returns 'False' if a value was handed to the
-- receiver meanwhile too, which is the registration's fault: it gave a value and registered the promise.
giveUpRegistering :: Change -> IORef (Slot a) -> IO Bool
giveUpRegistering change slot = change slot $ \case
  Registering -> (Idle, True)
  Delivered _ -> (Idle, False)
  other -> (other, False)
{-# INLINE giveUpRegistering #-}

-- | The receiver stops waiting, and says whether it did: it didn't if a sender has woken it already, and then the value is its.
stopTaking :: Change -> IORef (Slot a) -> IO Bool
stopTaking change slot = change slot $ \case
  Awaiting _ -> (Idle, True)
  other -> (other, False)
{-# INLINE stopTaking #-}

-- | What a hand found.
data Handed
  = -- | The receiver has the value.
    Passed
  | -- | The value is the sender's still: no receiver takes it, or the ticket was abandoned.
    GaveBack
  | -- | No receiver is parked, and the sender didn't ask to wait.
    MustWait
  | -- | The sender waits in the slot.
    LeftWaiting
  | -- | Another sender waits.
    SecondSender

-- | What a hand does to the slot, and what it still has to do: give a parked receiver the value, or say how it ended.
data Outcome a = Give !(a -> IO Bool) | Settled !Handed

-- | The sender gives the value to the receiver if it is parked, or has said it is about to await, and never waits: if it is not, the
-- value is the sender's still.
handTry :: Change -> IORef (Slot a) -> a -> IO Handed
handTry change slot !value =
  -- A look first: with no receiver parked, as it is as a rule, nothing is changed.
  readIORef slot >>= \case
    Idle -> pure MustWait
    _ ->
      change slot tryStep >>= \case
        Give deliver -> (\taken -> if taken then Passed else GaveBack) <$> deliver value
        Settled handed -> pure handed
  where
    tryStep = \case
      Awaiting deliver -> (Idle, Give deliver)
      Registering -> (Delivered value, Settled Passed)
      Closed -> (Closed, Settled GaveBack)
      idle@Idle -> (idle, Settled MustWait)
      watched@(Watched _ _ Idle) -> (watched, Settled MustWait)
      other -> (other, Settled SecondSender)
{-# INLINE handTry #-}

-- | The sender gives the value to the receiver as 'handTry' does, or waits in the slot, with its token and what wakes it with whether
-- the value was taken.
parkSender :: Change -> IORef (Slot a) -> a -> Int -> (Bool -> IO ()) -> IO Handed
parkSender change slot !value token woken =
  change slot parkStep >>= \case
    Give deliver -> (\taken -> if taken then Passed else GaveBack) <$> deliver value
    Settled handed -> pure handed
  where
    parkStep = \case
      Awaiting deliver -> (Idle, Give deliver)
      Registering -> (Delivered value, Settled Passed)
      Closed -> (Closed, Settled GaveBack)
      Idle -> (Offered value token woken, Settled LeftWaiting)
      Watched watcher signal Idle -> (Watched watcher signal (Offered value token woken), Settled LeftWaiting)
      other -> (other, Settled SecondSender)
{-# INLINE parkSender #-}

-- | The sender stops waiting, and says whether it did: it didn't if the receiver took the value, or the ticket was abandoned.
withdrawSender :: Change -> IORef (Slot a) -> Int -> IO Bool
withdrawSender change slot token = change slot $ \case
  Offered _ other _ | other == token -> (Idle, True)
  Watched watcher signal (Offered _ other _) | other == token -> (Watched watcher signal Idle, True)
  other -> (other, False)
{-# INLINE withdrawSender #-}

-- | Fulfils the slot: it says whether the receiver took the value. The value is evaluated, whether it is taken or not.
fulfilSlot :: Change -> IORef (Slot a) -> a -> IO Bool
fulfilSlot change slot value = (\case Passed -> True; _ -> False) <$> handTry change slot value
{-# INLINE fulfilSlot #-}

-- | What a part that watches found.
data Watcher
  = -- | The receiver is parked or committed to take, or the ticket was abandoned: there is nothing to wait for.
    WatchDone
  | -- | The part may wait: as the watcher, which is called once the receiver is parked.
    WatchWaits
  | -- | Another part watches.
    SecondWatcher

-- | What a look at the slot finds for a part that is about to watch, which changes nothing: 'WatchDone' if the receiver is parked or
-- committed to take, or the ticket was abandoned, so that there is nothing to wait for, and 'SecondWatcher' if another part watches.
lookAtWatch :: IORef (Slot a) -> IO Watcher
lookAtWatch slot =
  readIORef slot >>= \case
    Awaiting _ -> pure WatchDone
    Registering -> pure WatchDone
    Delivered _ -> pure WatchDone
    Closed -> pure WatchDone
    Watched {} -> pure SecondWatcher
    _ -> pure WatchWaits

-- | The part waits until the receiver is parked: the signal wakes it then. The signal is not kept if there is nothing to wait for, or
-- another part watches.
parkWatcher :: Change -> IORef (Slot a) -> Int -> IO () -> IO Watcher
parkWatcher change slot token signal = change slot $ \case
  Idle -> (Watched token signal Idle, WatchWaits)
  offered@Offered {} -> (Watched token signal offered, WatchWaits)
  watched@Watched {} -> (watched, SecondWatcher)
  other -> (other, WatchDone)
{-# INLINE parkWatcher #-}

-- | The part stops waiting until the receiver is parked, and says whether it did: it didn't if the receiver was parked, or the ticket
-- abandoned, which woke it.
unwatch :: Change -> IORef (Slot a) -> Int -> IO Bool
unwatch change slot token = change slot $ \case
  Watched other _ inner | other == token -> (inner, True)
  other -> (other, False)
{-# INLINE unwatch #-}

-- | Abandons the ticket: a sender that waits is woken with its value given back, a watcher is let go, and every later hand gives its
-- value back at once. It gives the value that was handed to the receiver while it registered and that it never awaited: that value is
-- the receiver's.
closeSlot :: Change -> IORef (Slot a) -> IO (Maybe a)
closeSlot change slot = do
  (left, wake) <- change slot closeStep
  left <$ wake
  where
    closeStep = \case
      Delivered value -> (Closed, (Just value, pure ()))
      Offered _ _ woken -> (Closed, (Nothing, woken False))
      Watched _ signal inner -> case closeStep inner of (_, (left, wake)) -> (Closed, (left, signal *> wake))
      _ -> (Closed, (Nothing, pure ()))
{-# INLINE closeSlot #-}

-- | The failure of a part that awaits a ticket that another part awaits, or that was abandoned.
tookWrongly :: IOException
tookWrongly = userError "two parts wait for one ticket, or a ticket was waited for after it was abandoned"

-- | The failure of a sender that hands a value over while another sender waits.
twoSenders :: IOException
twoSenders = userError "a second part hands a value to a ticket while one waits: one sender at a time"

-- | The failure of a part that watches a ticket that another part watches.
twoWatchers :: IOException
twoWatchers = userError "a ticket was watched by two parts at once: one watcher at a time"

-- | The failure of a registration that gave a value and had registered the promise too, so that a value was handed to the receiver as
-- well.
registeredAndGave :: IOException
registeredAndGave = userError "a registration gave a value and had registered the promise, which a value was handed to meanwhile"
