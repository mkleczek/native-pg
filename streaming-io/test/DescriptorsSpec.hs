{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PatternSynonyms #-}

-- | The waits of the arrows that run every part on one thread, on descriptors that are reused, that are ready before
-- anything waits for them, that two parts wait on, or that move between cores: the same programs for every arrow that
-- has what they need, over socket pairs.
module DescriptorsSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, second, (&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Monad (forM_, when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Data.Maybe (isNothing)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes hiding (within)
import qualified Network.Streaming.Classes as Classes (within)
import Network.Streaming.Combinators (pattern Again, pattern Finish, alongside, concurrently, consume, done, iterateWith, keeping, sleep, tap, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Mode (Instrumented, ModeOf)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Time arr, Within arr, Exchange arr (Local arr), Instrumented (ModeOf arr))

type Program a = forall arr. (Rich arr) => arr () a

-- | A program that parts of several cores run, and that hands a socket from one to another.
type CoresProgram a = forall arr. (Rich arr, Exchange arr 'Anywhere) => Shared -> Int -> arr () a

data Runner = Runner String (forall a. Program a -> IO a)

runners :: [Runner]
runners =
  [ Runner "threads" onThreads,
    Runner "delimited" onDelimited,
    Runner "compiled" onCompiled
  ]

onThreads, onDelimited, onCompiled :: Program a -> IO a
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimited program ()
onCompiled program = fst <$> runCompiled program ()

spec :: Spec
spec = describe "descriptors" $ do
  forM_ runners $ \(Runner name run) -> describe name $ do
    it "are waited on when a closed socket's numbers are reused for another pair, in the same run" $ do
      within (run reusedNumber) `shouldReturn` Just (True, 1, 1)

    it "are waited on for input that arrived while nothing waited for it" $ do
      within (run arrivedBefore) `shouldReturn` Just 1

    it "are waited on for reading and for writing at once, by two parts" $ do
      within (run bothWays) `shouldReturn` Just (1, flood)

    it "are waited on for input that arrived after a wait was cancelled by a deadline" $ do
      within (run cancelledThenArrived) `shouldReturn` Just (True, 1)

    it "are waited on again when a wait was cancelled by a deadline, and the numbers are reused" $ do
      within (run cancelledThenReused) `shouldReturn` Just (True, True, 1)

  describe "on two cores" $ do
    it "are waited on when a socket that was waited on at one core is served at another and closed, and its number is reused at the first (delimited)" $ do
      ensureCores
      shared <- newShared
      within (runCores 2 (pure shared) handedOver) `shouldReturn` Just [(True, 1), (True, 1)]

    it "are waited on when a socket that was waited on at one core is served at another and closed, and its number is reused at the first (threads)" $ do
      ensureCores
      shared <- newShared
      within (runThreads (concurrently (handedOver shared 0) (handedOver shared 1)) ()) `shouldReturn` Just ((True, 1), (True, 1))

-- | A test that waits for ever fails instead: the scheduler stops where it waits for a descriptor.
within :: IO a -> IO (Maybe a)
within = timeout 3000000

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

-- * Sockets

-- | Two connected sockets: what is received on the first is sent on the second.
type Pair = (Socket, Socket)

newPair :: IO Pair
newPair = Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol

-- | The number of the socket's descriptor.
number :: Socket -> IO Int
number = fmap fromIntegral . Socket.unsafeFdSocket

-- | Closes both sockets, and gives the number of the first one's descriptor.
closing :: Pair -> IO Int
closing (one, other) = number one <* Socket.close one <* Socket.close other

-- | Waits for input on the first socket, and receives what is there: how many bytes.
receiveByte :: (Rich arr) => arr Pair Int
receiveByte = now (\(reader, _) -> (\buffer -> (reader, roomOf buffer)) <$> newBuffer (positiveInt @16)) >>> receive

-- | Sends a byte on the second socket.
sendByte :: (Rich arr) => arr Pair Pair
sendByte = tap ((,[BS.singleton 7]) . snd ^>> sendAll)

-- | A wait for input with a deadline: whether the deadline passed first.
timesOut :: (Rich arr) => arr Pair Bool
timesOut = Classes.within (millis 10) receiveByte >>^ isNothing

-- | Goes on with what came in after the time.
after :: (Rich arr) => Duration -> arr a a
after duration = keeping (sleep duration) >>^ snd

-- | One part waits for a byte, which another part sends a little later: how many bytes the first got.
exchange :: (Rich arr) => arr Pair Int
exchange = concurrently receiveByte (after (millis 5) >>> sendByte) >>^ fst

-- | One wait on the pair, which stays as it was.
exchangeThenKeep :: (Rich arr) => arr Pair Pair
exchangeThenKeep = tap (exchange >>> done)

-- | A pair is made, one wait is made on it, and it is closed: the number of its first descriptor and the bytes got.
used :: (Rich arr) => arr () (Int, Int)
used = now (const newPair) >>> alongside exchange >>> now (\(count, pair) -> (, count) <$> closing pair)

-- * Programs

-- | A pair is used and closed, and a second pair, which gets its numbers, is used the same: whether the numbers are the
-- same, and the bytes each wait got.
reusedNumber :: (Rich arr) => arr () (Bool, Int, Int)
reusedNumber = used >>> keeping used >>^ \((n2, c2), (n1, c1)) -> (n1 == n2, c1, c2)

-- | A byte arrives on a socket that a part waited on before, while nothing waits for it, and a few rounds pass; a part
-- that then waits for input finds it: the bytes it receives once it has.
arrivedBefore :: (Rich arr) => arr () Int
arrivedBefore =
  now (const newPair)
    >>> exchangeThenKeep
    >>> sendByte
    >>> after (millis 20)
    >>> awaitThenReceive

-- | Waits for input on the first socket, which may be there already, and receives it: how many bytes.
awaitThenReceive :: (Rich arr) => arr Pair Int
awaitThenReceive = tap (fst ^>> awaitInput) >>> receiveByte

-- | The bytes that one part of 'bothWays' sends: more than the buffers of a socket pair hold.
flood :: Int
flood = 4194304

-- | One part waits for input on a socket, and another sends more on it than the kernel holds, so that it waits for room
-- too, while a third drains the other end and then sends the byte that the first waits for: the bytes the first got and
-- the bytes drained.
bothWays :: (Rich arr) => arr () (Int, Int)
bothWays = now (const setup) >>> concurrently reading (concurrently writing draining) >>^ second snd
  where
    setup = Rig <$> newPair <*> newBuffer (positiveInt @16384) <*> newBuffer (positiveInt @16384)
    reading = rigPair ^>> receiveByte
    writing = flooding ^>> sendAll
    flooding rig = (fst (rigPair rig), replicate (flood `div` 16384) (view (rigFloodBuffer rig) 16384))
    draining = toDrain ^>> alongside drain >>> second (oneByte ^>> sendAll) >>^ fst
    toDrain rig = (snd (rigPair rig), rigDrainBuffer rig)
    oneByte (other, big) = (other, [view big 1])

-- | What the parts of 'bothWays' use: the pair of sockets, the buffer that the flood is sent from, and the buffer that
-- the drain receives into.
data Rig = Rig {rigPair :: Pair, rigFloodBuffer :: Buffer, rigDrainBuffer :: Buffer}

-- | Receives from the socket until the bytes of 'flood' have come: how many came.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Instrumented (ModeOf arr)) => arr (Socket, Buffer) Int
drain = started ^>> iterateWith (arr finished) (alongside (room ^>> receive) >>^ received)
  where
    started (end, buffer) = Draining end buffer 0
    finished draining = if drainGot draining >= flood then Finish (drainGot draining) else Again draining
    room draining = (drainEnd draining, roomOf (drainBuffer draining))
    received (count, draining) = draining {drainGot = drainGot draining + count}

-- | Where 'drain' is: the socket that it receives from, the buffer that it receives into, and how many bytes came.
data Draining = Draining {drainEnd :: Socket, drainBuffer :: Buffer, drainGot :: Int}

-- | A part waits for input with a deadline that passes first, and the pair is closed: a second pair, which gets its
-- numbers, is used for a wait: whether the numbers are the same, whether the first wait was cancelled, and the bytes
-- the second got.
cancelledThenReused :: (Rich arr) => arr () (Bool, Bool, Int)
cancelledThenReused = abandoned >>> keeping used >>^ \((n2, count), (n1, cancelled)) -> (n1 == n2, cancelled, count)
  where
    abandoned = now (const newPair) >>> alongside timesOut >>> now (\(cancelled, pair) -> (, cancelled) <$> closing pair)

-- | A part waits for input with a deadline that passes first, a byte arrives on the socket afterwards, and a few
-- rounds pass; a part that then waits for input finds it: whether the first wait was cancelled, and the bytes the
-- second got.
cancelledThenArrived :: (Rich arr) => arr () (Bool, Int)
cancelledThenArrived = now (const newPair) >>> alongside timesOut >>> second (sendByte >>> after (millis 20) >>> awaitThenReceive)

-- * Two cores

-- | What two cores share: where the second leaves the giver of the ticket that it waits for the socket at, and a flag
-- that it raises once it has closed the socket.
data Shared = Shared (IORef (Maybe (Giver 'Anywhere Socket))) (IORef Bool)

newShared :: IO Shared
newShared = Shared <$> newIORef Nothing <*> newIORef False

-- | The first core waits on a socket, hands it to the second through a ticket, and sends it a byte, which the second
-- waits for and receives; the second closes the socket, and the first makes another pair, which gets its numbers, and
-- waits on that: whether the numbers are the same and the bytes each got.
handedOver :: CoresProgram (Bool, Int)
handedOver (Shared shelf closed) core
  | core == 0 =
      now (const (newPair >>= \pair -> (,) pair <$> number (fst pair)))
        >>> consume (exchangeThenKeep >>> handOver >>> after (millis 20) >>> sendByte >>> closedByTheOther >>> closeOther >>> done)
        >>> now (\n1 -> newPair >>= \pair -> (\n2 -> (n1 == n2, pair)) <$> number (fst pair))
        >>> second (alongside exchange >>> now (\(count, pair) -> count <$ closing pair))
  | otherwise =
      ticket @_ @'Anywhere
        >>> now (\(awaiting, giver) -> awaiting <$ atomicWriteIORef shelf (Just giver))
        >>> await
        >>> now (\socket -> (\buffer -> (socket, roomOf buffer)) <$> newBuffer (positiveInt @16))
        >>> (id &&& fst) ^>> first receive
        >>> second (now (\socket -> Socket.close socket *> atomicWriteIORef closed True))
        >>^ (True,) . fst
  where
    -- Waits for the second core's ticket, and gives it the first socket.
    handOver = keeping (untilJust (readIORef shelf)) >>> tap (second fst ^>> give >>> done) >>^ snd
    closedByTheOther = keeping (untilJust (fmap (\raised -> if raised then Just () else Nothing) (readIORef closed))) >>^ snd
    closeOther = now (\pair@(_, other) -> pair <$ Socket.close other)
