{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}

-- | What the giving end of a ticket is made of, for the instances of 'Network.Directions.Classes.Exchange'. The
-- package doesn't expose this module: a program that could make a 'Giver' of its own would choose its reach.
module Network.Directions.Tickets
  ( Reach (..),
    Giver (..),
    Slot (..),
    giveHere,
    abandonHere,
  )
where

import Data.IORef (IORef, readIORef, writeIORef)
import Data.Kind (Type)

-- | Where a ticket may be given: on its own core only, which is the region @s@ of the arrow that made it, or from
-- any core.
data Reach = Here Type | Anywhere

-- | The giving end of a ticket. A giver of reach @'Here s@ mentions the region @s@, so that it can't leave the core
-- that made it; a giver of reach @'Anywhere@ mentions no core, and can be kept in structures that cores share.
data family Giver (r :: Reach) a

-- | A giver of a ticket of this core: its slot, which only the parts of this core read and change.
newtype instance Giver ('Here _s) a = HereGiver (IORef (Slot a))

-- | A giver that says whether the value was given, as the arrow of the ticket does it.
newtype instance Giver 'Anywhere a = AnywhereGiver (a -> IO Bool)

-- | The slot of a ticket of one core: empty, given its value, holding the part that waits for it, or abandoned by
-- its waiter. The waiter is what puts the part back among those that can go on, with the action that runs where it
-- waited: the value, or a cancellation to raise. The value is evaluated: a thunk would keep what it was made from
-- alive as long as the slot.
data Slot a = Empty | Given !a | Waiter (IO a -> IO ()) | Abandoned

-- | Gives the slot its value, and puts the part that waits for it back among those that can go on. It says
-- whether the waiter takes it: it doesn't if the waiter has gone. The giver evaluates the value, whether it is
-- taken or not. No atomic operation: the slot is read and written by the parts of one core.
giveHere :: IORef (Slot a) -> a -> IO Bool
giveHere slot !value =
  readIORef slot >>= \case
    Empty -> True <$ writeIORef slot (Given value)
    Waiter wake -> True <$ (writeIORef slot (Given value) *> wake (pure value))
    Abandoned -> pure False
    Given _ -> ioError (userError "a ticket was given a value twice")
{-# INLINE giveHere #-}

-- | The waiter stops wanting the value: a later give says it isn't taken. A value given already is the waiter's,
-- and this gives it back.
abandonHere :: IORef (Slot a) -> IO (Maybe a)
abandonHere slot =
  readIORef slot >>= \case
    Given value -> Just value <$ writeIORef slot Abandoned
    _ -> Nothing <$ writeIORef slot Abandoned
{-# INLINE abandonHere #-}
