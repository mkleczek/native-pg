{-# LANGUAGE DataKinds #-}

-- | A lease on a value that must not be destroyed while a request uses it: a count of the requests in flight, and a flag that
-- says the value is retired, which no request enters after.
--
-- A request 'enter's before it uses the value and 'leave's when it is done, and neither waits. 'retire' marks the lease
-- retired, and waits for the requests that entered to leave, so that a teardown that follows it never meets a request that
-- uses the value, and no request enters a value after its teardown began: it is the guarantee of a count kept in the same
-- transaction that finds the value, without composing with the cache.
--
-- * **A lease of one core** is a plain count and a plain flag, for the parts of a core that switch only at waits.
-- * **A lease that all cores share** has a pair of counters for each core, each pair on a cache line of its own, which only
--   ever go up: the requests that entered and the requests that left, so that a request that enters on one core and leaves on
--   another, as the parts of the threads of 'Network.Streaming.Threads.Threads' may, leaves the sums right. A request that
--   enters adds to its core's count and then reads the flag, and backs out if it is set; a retirement sets the flag and then
--   reads the counts, the leaves first and then the enters: each side writes and then reads what the other wrote, with a full
--   barrier between, so one of them sees the other. The retirement is drained when the sums are equal.
module Data.Sieve.Lease
  ( Lease,
    newLease,
    newSharedLease,
    enter,
    leave,
    Retirement (..),
    retire,
    inFlight,
    isRetired,
  )
where

import Control.Arrow (arr, (>>>), (|||))
import Control.Concurrent (myThreadId)
import Control.Monad (when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Sieve (ReachOf, Reaching, Sharing (..))
import Data.Sieve.Memory
import GHC.Conc (getNumCapabilities, threadCapability)
import Network.Streaming.Classes
import Network.Streaming.Combinators (alongside, done, step, tryTicket)
import Network.Streaming.Internal.Atomic (update)

-- | The lease of a value, for the cores of the sharing.
data Lease (c :: Sharing) = Lease
  { leaseShared :: !Bool,
    -- | How many cores have counters of their own.
    leaseSlots :: !Int,
    -- | A line of 16 words, 128 bytes, for each core: the count of the requests that entered, and of those that left.
    leaseCounts :: !Ints,
    -- | The flag that says the lease is retired: 0 or 1.
    leaseFlag :: !Ints,
    -- | What wakes the retirement that waits for the requests to leave, while one does.
    leaseWake :: !(IORef (Maybe (IO ())))
  }

-- | The words of a core's line: how many requests entered, and how many left.
entered, left :: Int
entered = 0
left = 1

-- | The words of a line, which is as many as hold 128 bytes, the larger of the sizes of a processor's cache line.
lineWords :: Int
lineWords = 16

make :: Bool -> Int -> IO (Lease c)
make shared slots = Lease shared slots <$> newAlignedInts (slots * lineWords) <*> newAlignedInts lineWords <*> newIORef Nothing

-- | A lease for the parts of one core, which switch only at waits.
newLease :: IO (Lease 'OneCore)
newLease = make False 1

-- | A lease that the parts of every core share.
newSharedLease :: IO (Lease 'AllCores)
newSharedLease = getNumCapabilities >>= make True . max 1

-- | The core that the calling thread is on, as the number of a line.
slotOf :: Lease c -> IO Int
slotOf lease
  | leaseSlots lease == 1 = pure 0
  | otherwise = (\(core, _) -> core `mod` leaseSlots lease) <$> (myThreadId >>= threadCapability)

-- | Adds a request to a count of a core's line.
count :: Lease c -> Int -> Int -> IO ()
count lease slot which
  | leaseShared lease = () <$ fetchAddInt (leaseCounts lease) at 1
  | otherwise = readInt (leaseCounts lease) at >>= writeInt (leaseCounts lease) at . (+ 1)
  where
    at = slot * lineWords + which

-- | The sum of a count over the cores' lines: read one after the other, which no write waits for.
total :: Lease c -> Int -> IO Int
total lease which = go 0 0
  where
    go !slot !sum'
      | slot >= leaseSlots lease = pure sum'
      | otherwise = reader (leaseCounts lease) (slot * lineWords + which) >>= \n -> go (slot + 1) (sum' + n)
    reader = if leaseShared lease then readIntAtomic else readInt

-- | Whether every request that entered has left: the leaves are read before the enters, so that a sum that comes out equal
-- was equal at some moment, and a request that enters and leaves in between can't make it so falsely.
drained :: Lease c -> IO Bool
drained lease = total lease left >>= \leaves -> (== leaves) <$> total lease entered

retired :: Lease c -> IO Bool
retired lease = (/= 0) <$> (if leaseShared lease then readIntAtomic else readInt) (leaseFlag lease) 0

-- | Marks the lease retired, and says whether this call did.
markRetired :: Lease c -> IO Bool
markRetired lease
  | leaseShared lease = (== 0) <$> casInt (leaseFlag lease) 0 0 1
  | otherwise = readInt (leaseFlag lease) 0 >>= \flag -> if flag /= 0 then pure False else True <$ writeInt (leaseFlag lease) 0 1

-- | Takes the retirement's wake-up, if it is there: at most one caller gets it.
takeWake :: Lease c -> IO (Maybe (IO ()))
takeWake lease
  | leaseShared lease = update (leaseWake lease) (\old -> (Nothing, old))
  | otherwise = readIORef (leaseWake lease) <* writeIORef (leaseWake lease) Nothing

publishWake :: Lease c -> IO () -> IO ()
publishWake lease wake
  | leaseShared lease = update (leaseWake lease) (const (Just wake, ()))
  | otherwise = writeIORef (leaseWake lease) (Just wake)

-- | Counts a request in flight, unless the lease is retired: then it is 'False', and nothing is counted. Never waits.
enter :: Lease c -> IO Bool
enter lease = do
  slot <- slotOf lease
  count lease slot entered
  refused <- retired lease
  if refused then False <$ leaving lease slot else pure True

-- | A request in flight is done. The last one to leave a retired lease wakes the 'retire' that waits for it. Never waits.
leave :: Lease c -> IO ()
leave lease = slotOf lease >>= leaving lease

-- | A request on the core has left, or has backed out. If the lease is retired, and a retirement waits, and every request has
-- left, it wakes the retirement. The wake-up is taken by one caller only, so it runs once, and a retirement that publishes it
-- after this looked finds the requests left itself.
leaving :: Lease c -> Int -> IO ()
leaving lease slot = do
  count lease slot left
  flagged <- retired lease
  when flagged $
    readIORef (leaseWake lease) >>= \waiting ->
      when (maybe False (const True) waiting) $
        drained lease >>= \done' -> when done' (takeWake lease >>= sequence_)

-- | How a retirement ended.
data Retirement
  = -- | The requests in flight have left.
    Drained
  | -- | Some were still in flight at the deadline.
    TimedOut
  | -- | The lease was retired before: the part that retired it first does the waiting.
    AlreadyRetired
  deriving (Eq, Show)

-- | Marks the lease retired, so that no request enters after it, and waits for the requests that did to leave, until the
-- deadline. A part that waits is woken by the last request to leave, and one that is cancelled while it waits leaves the lease
-- retired.
retire :: forall c arr. (Runtime arr, Reaching c arr) => Duration -> arr (Lease c) Retirement
retire limit =
  named (Label "retire" "Marks the lease retired, and waits for the requests in flight to leave, until the deadline.") $
    step "mark retired" begin `giving` "first to retire?"
      >>> (taking "no" (arr (const AlreadyRetired)) ||| taking "yes" waiting)
  where
    begin lease = (\first' -> if first' then Right lease else Left ()) <$> markRetired lease

    waiting :: arr (Lease c) Retirement
    waiting = alongside (within limit (tryTicket handOut done)) >>> step "ended" ended

    -- Publishes what wakes the part, and looks again, since a request may have left before it was there to be woken: if it is
    -- drained then, nobody wakes the part, and it needn't wait.
    handOut :: arr (Promise (ReachOf c arr) (), Lease c) (Either Retirement ())
    handOut = step "wait for the requests" hand

    hand (promise, lease) =
      drained lease >>= \already ->
        if already
          then pure (Left Drained)
          else do
            publishWake lease (() <$ fulfilNow promise ())
            drained lease >>= \now' ->
              if now'
                then maybe (Right ()) (const (Left Drained)) <$> takeWake lease
                else pure (Right ())

    ended (outcome, lease) = case outcome of
      Nothing -> TimedOut <$ takeWake lease
      Just (Left how) -> pure how
      Just (Right _) -> pure Drained

-- | How many requests are in flight.
inFlight :: Lease c -> IO Int
inFlight lease = total lease left >>= \leaves -> subtract leaves <$> total lease entered

-- | Whether the lease is retired.
isRetired :: Lease c -> IO Bool
isRetired = retired
