{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | What the promise end of a ticket is made of, for the instances of 'Network.Streaming.Classes.Exchange'. The
-- package doesn't expose this module: a program that could make a 'Promise' of its own would choose its reach.
module Network.Streaming.Internal.Tickets
  ( Reach (..),
    Promise (..),
    Slot (..),
    fulfilHere,
    abandonHere,
    Fulfilling (..),
  )
where

import Data.IORef (IORef, readIORef, writeIORef)
import Data.Kind (Type)

-- | Where a ticket may be fulfilled: on its own core only, which is the region @s@ of the arrow that made it, or from
-- any core.
data Reach = Here Type | Anywhere

-- | The end of a ticket that fulfils it. A promise of reach @'Here s@ mentions the region @s@, so that it can't leave the core
-- that made it; a promise of reach @'Anywhere@ mentions no core, and can be kept in structures that cores share.
data family Promise (r :: Reach) a

-- | A promise of a ticket of this core: its slot, which only the parts of this core read and change.
newtype instance Promise ('Here _s) a = HerePromise (IORef (Slot a))

-- | A promise that says whether the value was taken, as the arrow of the ticket does it.
newtype instance Promise 'Anywhere a = AnywherePromise (a -> IO Bool)

-- | The slot of a ticket of one core: empty, fulfilled with its value, holding the part that waits for it, or abandoned by
-- its waiter. The waiter is what puts the part back among those that can go on, with the action that runs where it
-- waited: the value, or a cancellation to raise. The value is evaluated: a thunk would keep what it was made from
-- alive as long as the slot.
data Slot a = Empty | Fulfilled !a | Waiter (IO a -> IO ()) | Abandoned

-- | Promises that an action fulfils, as the arrow's own 'Network.Streaming.Classes.fulfil' does but without its point to give
-- way at, so that it is a step of an arrow that can't wait: for what hands a value over together with what must not be left
-- half done if a cancellation came between, and for an action of a structure that parts share, which has no arrow to fulfil in.
-- It says whether the part that awaits the future took the value. A promise of reach @'Here s@ is fulfilled by an action that
-- runs on the core of its arrow, as the part that awaits it is put back among those that can go on there.
class Fulfilling (r :: Reach) where
  fulfilNow :: Promise r a -> a -> IO Bool

-- | A promise of any core is fulfilled by the function that the arrow of its ticket made it with.
instance Fulfilling 'Anywhere where
  fulfilNow (AnywherePromise fulfilling) = fulfilling
  {-# INLINE fulfilNow #-}

-- | A promise of one core is fulfilled in its slot, as the arrow's own 'fulfil' does it.
instance Fulfilling ('Here s) where
  fulfilNow (HerePromise slot) = fulfilHere slot
  {-# INLINE fulfilNow #-}

-- | Fulfils the slot with its value, and puts the part that waits for it back among those that can go on. It says
-- whether the waiter takes it: it doesn't if the waiter has gone. The value is evaluated, whether it is
-- taken or not. No atomic operation: the slot is read and written by the parts of one core.
fulfilHere :: IORef (Slot a) -> a -> IO Bool
fulfilHere slot !value =
  readIORef slot >>= \case
    Empty -> True <$ writeIORef slot (Fulfilled value)
    Waiter wake -> True <$ (writeIORef slot (Fulfilled value) *> wake (pure value))
    Abandoned -> pure False
    Fulfilled _ -> ioError (userError "a promise was fulfilled twice")
{-# INLINE fulfilHere #-}

-- | The waiter stops wanting the value: a later fulfil says it isn't taken. A value fulfilled already is the waiter's,
-- and this returns it.
abandonHere :: IORef (Slot a) -> IO (Maybe a)
abandonHere slot =
  readIORef slot >>= \case
    Fulfilled value -> Just value <$ writeIORef slot Abandoned
    _ -> Nothing <$ writeIORef slot Abandoned
{-# INLINE abandonHere #-}
