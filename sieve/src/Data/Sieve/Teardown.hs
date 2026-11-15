{-# LANGUAGE DataKinds #-}

-- | The teardown of values that are evicted from a cache while requests may still use them: a queue of the values, which the
-- eviction listener fills without waiting, and a part of the program that retires each value's lease, and then runs the
-- owner's teardown arrow on it.
--
-- The queue is not an inbox: an inbox holds one value, and its @post@ is a step that waits while the inbox is full and that a
-- cancellation can stop, which an eviction listener, an action that never waits, can't be. The graveyard has the queue, which
-- any number of values wait in, and the promise of the teardown part, when the part waits for a value: 'bury' gives the part
-- the value that it waits for at once, or queues it.
module Data.Sieve.Teardown
  ( Graveyard,
    newGraveyard,
    newSharedGraveyard,
    bury,
    outstanding,
    teardown,
  )
where

import Control.Arrow (returnA, (>>>), (>>^), (^>>), (|||))
import Control.Monad (unless)
import Data.Foldable (for_)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Kind (Type)
import Data.Sequence (Seq, ViewL (..), viewl, (<|), (|>))
import qualified Data.Sequence as Seq
import Data.Sieve (ReachOf, Reaching, Sharing (..))
import Data.Sieve.Lease (Lease, Retirement (..), retire)
import Network.Streaming.Classes
import Network.Streaming.Combinators (alongside, consume, decide, foreverA, step, tap, tryTicket)
import Network.Streaming.Internal.Atomic (update)

-- | The values that wait to be torn down, for the cores of the sharing.
data Graveyard (c :: Sharing) (v :: Type) = Graveyard !Bool !(IORef (Graves v))

-- | The values that were buried and that no part has taken, the oldest first; the action that gives a value to the part that
-- waits for one, if a part does; and how many values were buried and are not torn down yet.
data Graves v = Graves
  { gravesQueue :: !(Seq v),
    gravesParked :: !(Maybe (v -> IO Bool)),
    gravesOutstanding :: !Int
  }

-- | A graveyard for the parts of one core, which switch only at waits.
newGraveyard :: IO (Graveyard 'OneCore v)
newGraveyard = Graveyard False <$> newIORef (Graves Seq.empty Nothing 0)

-- | A graveyard that the parts of every core share. The state is evaluated before it is kept: the reference is changed by
-- compare-and-swap, which needs it to be a value.
newSharedGraveyard :: IO (Graveyard 'AllCores v)
newSharedGraveyard = Graveyard True <$> (newIORef $! Graves Seq.empty Nothing 0)

-- | Changes the state, and gives what the change says.
change :: Graveyard c v -> (Graves v -> (Graves v, r)) -> IO r
change (Graveyard shared ref) f
  | shared = update ref f
  | otherwise = readIORef ref >>= \old -> case f old of (!new, !result) -> result <$ writeIORef ref new

-- | Queues the value to be torn down, and gives it to the teardown part at once if the part waits for one: it is what an eviction
-- listener does. Never waits.
bury :: Graveyard c v -> v -> IO ()
bury graveyard value = do
  waiting <- change graveyard $ \graves ->
    let counted = graves {gravesOutstanding = gravesOutstanding graves + 1}
     in case gravesParked graves of
          Just wake -> (counted {gravesParked = Nothing}, Just wake)
          Nothing -> (counted {gravesQueue = gravesQueue graves |> value}, Nothing)
  for_ waiting $ \wake -> wake value >>= \taken -> unless taken (putBack graveyard value)

-- | Puts a value back at the front of the queue: it was given to a part that had gone.
putBack :: Graveyard c v -> v -> IO ()
putBack graveyard value = change graveyard (\graves -> (graves {gravesQueue = value <| gravesQueue graves}, ()))

-- | How many values were buried and are not torn down yet, the one that the part works on included.
outstanding :: Graveyard c v -> IO Int
outstanding (Graveyard _ ref) = gravesOutstanding <$> readIORef ref

-- | The part that tears the values down, one after the other and for ever: it takes the next value, or waits for one, retires
-- the lease of the value, within the deadline, and then runs the owner's arrow on the value and how the retirement ended,
-- unless the lease was retired before, when whoever retired it tears the value down. There is one such part for a graveyard.
-- A failure of the owner's arrow is the part's.
teardown :: forall c arr v. (Runtime arr, Reaching c arr) => (v -> Lease c) -> Duration -> arr (v, Retirement) () -> arr (Graveyard c v) ()
teardown leaseOf limit owner =
  named (Label "teardown" "Takes the values that were buried, one after the other, retires their leases, and tears them down.") $
    foreverA (giveWay >>> tap one)
  where
    one :: arr (Graveyard c v) ()
    one =
      alongside nextValue
        >>> alongside ((leaseOf . fst) ^>> retire limit)
        >>> decide "retired before?" (\(how, (value, graveyard)) -> if how == AlreadyRetired then Left graveyard else Right ((value, how), graveyard))
        >>> (taking "yes" returnA ||| taking "no" (consume owner))
        >>> step "torn down" (\graveyard -> change graveyard (\graves -> (graves {gravesOutstanding = gravesOutstanding graves - 1}, ())))

    -- The next value: the oldest of the queue, or, if there is none, what the part waits at a ticket of its own for. A part that
    -- is cancelled while it waits gives up its place, and a value that was given to it meanwhile goes back in the queue.
    nextValue :: arr (Graveyard c v) v
    nextValue = tryTicket takeOrPark (step "put back" (\(value, graveyard) -> putBack graveyard value)) >>^ either id snd

    takeOrPark :: arr (Promise (ReachOf c arr) v, Graveyard c v) (Either v ())
    takeOrPark = step "take or wait" (\(promise, graveyard) -> change graveyard (takingOrParking (fulfilNow promise)))

    takingOrParking wake graves = case viewl (gravesQueue graves) of
      value :< rest -> (graves {gravesQueue = rest}, Left value)
      EmptyL -> (graves {gravesParked = Just wake}, Right ())
