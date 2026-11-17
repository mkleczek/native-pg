{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

-- | The arrows that run parts at once: the parts of a 'together', which are known when the program is built, and the parts
-- that a group's body spawns while it runs, with the 'Joiner' that says what the group does as each ends. The classes of
-- "Network.Streaming.Classes" have 'together' and nothing else of this module: the names of the group's methods and of the
-- joiner's are common ones, so a program that spawns parts imports them from here. The tally and the parts' type are type
-- families of the joiner, so 'ended' and 'joined' name it by a type application, which only the runtimes make: @ended \@j@.
module Network.Streaming.Concurrent
  ( Concurrent (..),
    Joiner (..),
    AwaitAll (..),
  )
where

import Control.Exception (SomeException)

-- | Arrows that run at once on the same input, as '&&&' runs two of them one after the other.
class Concurrent arr where
  -- | Runs the parts, each on the input, as the parts of one cancel scope of their own, and ends when all have ended.
  -- The first failure cancels the others where they wait, and is rethrown once they have ended; a later failure is
  -- dropped. No part is made while it runs: the parts are known when the program is built. @together []@ ends at
  -- once, and one part may run as it is.
  together :: [arr a ()] -> arr a ()

  -- | A handle to a group, which its body uses to spawn parts into it. The type @s@ is the group's own, so that the handle
  -- can't leave the body.
  data Group arr s j

  -- | Runs the body with a handle to a new group, waits for every part that is spawned into it, and returns the body's
  -- result with what the joiner made of the parts.
  --
  -- * The body runs as the first member of the group, and spawns parts, at any time while it runs.
  -- * When a part ends, with a value, a failure or a cancellation, the joiner's 'ended' updates its tally, and may say that the
  --   parts of the group are to be cancelled: they are, at their next waits. The body is not: it goes on, as the owner of a
  --   scope does when its joiner shuts the scope down in Java. A part that is spawned after that doesn't run, and counts as
  --   cancelled.
  -- * The group ends when the body and every part have ended. If the body failed, its failure cancels the parts at once, and is
  --   rethrown after they have ended. Otherwise the group returns what 'joined' gives, or rethrows the failure that it gives.
  -- * A cancellation from outside reaches the body and every part, as it reaches any part inside the scope that is cancelled,
  --   and the group waits for them to end.
  -- * A part that has ended leaves nothing behind: no frame, no entry in the members of a scope, no joiner waiting for it. A
  --   group that has spawned 100,000 short parts holds what a group that has spawned none holds.
  -- * Under 'Network.Streaming.Threads.Threads' parts end on several threads at once, and the tally is updated atomically.
  group :: (Joiner j) => j -> (forall s. arr (Group arr s j, a) b) -> arr a (b, Joined j)

  -- | Starts the arrow on the input as a part of the group, on the group's core, and returns at once. The part is numbered
  -- in the order of the spawns, from 0. It is not a wait: a part is not cancelled at a spawn. The arrow is fixed when the
  -- program is built, as the parts of 'together' are: only its input is a run-time value.
  spawn :: (Joiner j) => arr x (Part j) -> arr (Group arr s j, x) ()


-- | What a group does as each of its parts ends, and what it returns. It is pure: the runtime keeps the tally, and
-- updates it, atomically where parts end on several threads.
class Joiner j where
  -- | What a part gives.
  type Part j

  -- | What the group returns besides the body's result.
  type Joined j

  -- | What the group has of its parts that have ended.
  type Tally j

  -- | The tally of a new group.
  begin :: j -> Tally j

  -- | The tally after the part of this number (in the order of the spawns, from 0) ended in this way, and whether the parts of the
  -- group are to be cancelled.
  ended :: Int -> Either SomeException (Part j) -> Tally j -> (Tally j, Bool)

  -- | What the group returns when all have ended, or the failure that it rethrows.
  joined :: Tally j -> Either SomeException (Joined j)

-- | A group whose parts' ends, fine or failed, are their own: nothing is cancelled, and the group returns @()@.
data AwaitAll = AwaitAll

instance Joiner AwaitAll where
  type Part AwaitAll = ()
  type Joined AwaitAll = ()
  type Tally AwaitAll = ()
  begin _ = ()
  {-# INLINE begin #-}
  ended _ _ () = ((), False)
  {-# INLINE ended #-}
  joined () = Right ()
  {-# INLINE joined #-}
