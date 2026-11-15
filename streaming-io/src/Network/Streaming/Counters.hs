{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

-- | What the runtime counts, for tests and for a server's metrics: the polls, the armings of descriptors, and the
-- calls on sockets that cost a system call. The counts are the process's: every run and every thread adds to them.
-- An addition is atomic and goes to the cells of the capability it is made on, so that cores that count at the same
-- time don't write the same cache line; reading adds the cells up.
module Network.Streaming.Counters
  ( Counter (..),
    counterName,
    increment,
    incrementBy,
    Counts,
    snapshot,
    countOf,
    since,
    toList,
  )
where

import GHC.Exts (Int (..), MutableByteArray#, RealWorld, fetchAddIntArray#, myThreadId#, newAlignedPinnedByteArray#, readIntArray#, remInt#, setByteArray#, threadStatus#, (*#), (+#))
import GHC.IO (IO (..))
import System.IO.Unsafe (unsafePerformIO)

-- | What is counted.
data Counter
  = -- | Polls of the poller that did not wait: @epoll_wait@ or @kevent@ with a zero timeout.
    PollsImmediate
  | -- | Polls that waited, for a descriptor, a timer or a part another core gave. One that finds something ready returns at once.
    PollsBlocking
  | -- | Waits on a descriptor that armed it with the poller, or, under "Network.Streaming.Threads", with GHC's IO manager.
    Armings
  | -- | System calls that arming made: @epoll_ctl@, a modification that found no registration and the addition that follows count each. None on kqueue, where the poll takes the armings.
    ArmCalls
  | -- | Waits on a descriptor, whether they armed it or found it armed.
    DescriptorWaits
  | -- | Armings of a descriptor that another run armed last: a socket that moved between cores.
    DescriptorMoves
  | -- | @recv@ calls of a socket's receive, those that found nothing too.
    Receives
  | -- | @recv@ calls that found nothing: @EAGAIN@.
    ReceivesEmpty
  | -- | Receives that waited without a @recv@, because the receive before them on the socket was short.
    ReceivesSkipped
  | -- | @writev@ calls of a socket's send.
    Sends
  | -- | @accept@ calls that gave a connection.
    Accepts
  | -- | @accept@ calls that found no connection.
    AcceptsEmpty
  | -- | @fcntl@ calls that accepting a connection made.
    Fcntls
  | -- | @recv@ calls with @MSG_PEEK@ that a pool made at a checkout.
    Probes
  | -- | Captures of a part's stack by "Network.Streaming.Delimited", each of which copies the frames between the part's wait and the
    -- scheduler's prompt to the heap: a wait, a yield, and the fork of a group or a race.
    Captures
  deriving (Eq, Show, Enum, Bounded)

-- | The counter's name in a metric: lower case, with underscores.
counterName :: Counter -> String
counterName counter = case counter of
  PollsImmediate -> "polls_immediate"
  PollsBlocking -> "polls_blocking"
  Armings -> "armings"
  ArmCalls -> "arm_calls"
  DescriptorWaits -> "descriptor_waits"
  DescriptorMoves -> "descriptor_moves"
  Receives -> "receives"
  ReceivesEmpty -> "receives_empty"
  ReceivesSkipped -> "receives_skipped"
  Sends -> "sends"
  Accepts -> "accepts"
  AcceptsEmpty -> "accepts_empty"
  Fcntls -> "fcntls"
  Probes -> "probes"
  Captures -> "captures"

-- | The cells of one capability: the counters, and room to the next cache line.
cellsPerStripe :: Int
cellsPerStripe = 16

-- | How many capabilities have cells of their own: a capability with a higher number shares the cells of one with the same remainder.
stripes :: Int
stripes = 64

-- | The cells of all stripes, in one pinned array that is zero at first.
data Cells = Cells (MutableByteArray# RealWorld)

cells :: Cells
cells = unsafePerformIO . IO $ \s0 ->
  let !(I# size) = stripes * cellsPerStripe * 8
   in case newAlignedPinnedByteArray# size 128# s0 of
        (# s1, array #) -> case setByteArray# array 0# size 0# s1 of
          s2 -> (# s2, Cells array #)
{-# NOINLINE cells #-}

-- | Adds one to the counter.
increment :: Counter -> IO ()
increment counter = incrementBy counter 1
{-# INLINE increment #-}

-- | Adds to the counter. The addition is atomic, and goes to the cells of the capability that makes it.
incrementBy :: Counter -> Int -> IO ()
incrementBy counter (I# amount) = case cells of
  Cells array -> IO $ \s0 -> case myThreadId# s0 of
    (# s1, thread #) -> case threadStatus# thread s1 of
      (# s2, _, capability, _ #) ->
        let !(I# perStripe) = cellsPerStripe
            !(I# count) = stripes
            !(I# index) = fromEnum counter
         in case fetchAddIntArray# array ((remInt# capability count *# perStripe) +# index) amount s2 of
              (# s3, _ #) -> (# s3, () #)
{-# INLINE incrementBy #-}

-- | The counts at some moment, by the counter's number.
newtype Counts = Counts [Int]
  deriving (Eq, Show)

-- | The counts now: the sum of every capability's cells. The cells are read one after the other, so a count that is being
-- added to may or may not be in.
snapshot :: IO Counts
snapshot = Counts <$> mapM total [minBound .. maxBound :: Counter]
  where
    total counter = sum <$> mapM (cell (fromEnum counter)) [0 .. stripes - 1]
    cell (I# index) (I# stripe) = case cells of
      Cells array -> IO $ \s ->
        let !(I# perStripe) = cellsPerStripe
         in case readIntArray# array (stripe *# perStripe +# index) s of
              (# s1, value #) -> (# s1, I# value #)

-- | The count of the counter.
countOf :: Counter -> Counts -> Int
countOf counter (Counts values) = values !! fromEnum counter

-- | What was counted between two snapshots: the later one, less the earlier.
since :: Counts -> Counts -> Counts
since (Counts after) (Counts before) = Counts (zipWith (-) after before)

-- | The counts, with their counters.
toList :: Counts -> [(Counter, Int)]
toList (Counts values) = zip [minBound ..] values
