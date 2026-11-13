{-# LANGUAGE DataKinds #-}

-- | The waits of the arrows that run every part on one thread, on descriptors that are reused, that are ready before
-- anything waits for them, that two parts wait on, or that move between cores: the same programs for every arrow that
-- has what they need, over socket pairs.
module DescriptorsSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, returnA, second, (>>>), (>>^), (^>>), (|||))
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
import Network.Streaming.Combinators (concurrently, keeping, sleep, untilJust)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads
import Skeletest
import System.Timeout (timeout)

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Time arr, Within arr, Exchange arr (Local arr))

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
sendByte = (\pair@(_, sender) -> ((sender, [BS.singleton 7]), pair)) ^>> first sendAll >>^ snd

-- | Goes on with what came in after the time.
after :: (Rich arr) => Duration -> arr a a
after duration = ((),) ^>> first (sleep duration) >>^ snd

-- | One part waits for a byte, which another part sends a little later: how many bytes the first got.
exchange :: (Rich arr) => arr Pair Int
exchange = concurrently receiveByte (after (millis 5) >>> sendByte) >>^ fst

-- | One wait on the pair, which stays as it was.
exchangeThenKeep :: (Rich arr) => arr Pair Pair
exchangeThenKeep = (\pair -> (pair, pair)) ^>> first exchange >>^ snd

-- | A pair is made, one wait is made on it, and it is closed: the number of its first descriptor and the bytes got.
used :: (Rich arr) => arr () (Int, Int)
used = now (const newPair) >>> (\pair -> (pair, pair)) ^>> first exchange >>> now (\(count, pair) -> (, count) <$> closing pair)

-- * Programs

-- | A pair is used and closed, and a second pair, which gets its numbers, is used the same: whether the numbers are the
-- same, and the bytes each wait got.
reusedNumber :: (Rich arr) => arr () (Bool, Int, Int)
reusedNumber = used >>> (,()) ^>> second used >>^ \((n1, c1), (n2, c2)) -> (n1 == n2, c1, c2)

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
awaitThenReceive = (\pair@(reader, _) -> (reader, pair)) ^>> first awaitInput >>> arr snd >>> receiveByte

-- | The bytes that one part of 'bothWays' sends: more than the buffers of a socket pair hold.
flood :: Int
flood = 4194304

-- | One part waits for input on a socket, and another sends more on it than the kernel holds, so that it waits for room
-- too, while a third drains the other end and then sends the byte that the first waits for: the bytes the first got and
-- the bytes drained.
bothWays :: (Rich arr) => arr () (Int, Int)
bothWays = now (const setup) >>> concurrently reading (concurrently writing draining) >>^ (\(got, (_, drained)) -> (got, drained))
  where
    setup = (,,) <$> newPair <*> newBuffer (positiveInt @16384) <*> newBuffer (positiveInt @16384)
    reading = now (\((reader, _), _, _) -> (\buffer -> (reader, roomOf buffer)) <$> newBuffer (positiveInt @16)) >>> receive
    writing = (\((writer, _), big, _) -> (writer, replicate (flood `div` 16384) (view big 16384))) ^>> sendAll
    draining =
      (\((_, other), _, big) -> ((other, big), (other, big)))
        ^>> first drain
        >>> (\(drained, (other, big)) -> ((other, [view big 1]), drained))
        ^>> first sendAll
        >>^ snd

-- | Receives from the socket until the bytes of 'flood' have come: how many came.
drain :: (ArrowChoice arr, Effect arr, GiveWay arr, Block arr) => arr (Socket, Buffer) Int
drain = (\(end, buffer) -> (end, buffer, 0)) ^>> loop
  where
    loop = (\state@(_, _, got) -> if got >= flood then Left got else Right state) ^>> (returnA ||| step)
    step = (\(end, buffer, got) -> ((end, roomOf buffer), (end, buffer, got))) ^>> first receive >>> (\(count, (end, buffer, got)) -> (end, buffer, got + count)) ^>> loop

-- | A part waits for input with a deadline that passes first, and the pair is closed: a second pair, which gets its
-- numbers, is used for a wait: whether the numbers are the same, whether the first wait was cancelled, and the bytes
-- the second got.
cancelledThenReused :: (Rich arr) => arr () (Bool, Bool, Int)
cancelledThenReused = abandoned >>> (,()) ^>> second used >>^ \((n1, cancelled), (n2, count)) -> (n1 == n2, cancelled, count)
  where
    abandoned = now (const newPair) >>> (\pair -> (pair, pair)) ^>> first (Classes.within (millis 10) receiveByte >>^ isNothing) >>> now (\(cancelled, pair) -> (, cancelled) <$> closing pair)

-- | A part waits for input with a deadline that passes first, a byte arrives on the socket afterwards, and a few
-- rounds pass; a part that then waits for input finds it: whether the first wait was cancelled, and the bytes the
-- second got.
cancelledThenArrived :: (Rich arr) => arr () (Bool, Int)
cancelledThenArrived = now (const newPair) >>> (\pair -> (pair, pair)) ^>> first (Classes.within (millis 10) receiveByte >>^ isNothing) >>> second (sendByte >>> after (millis 20) >>> awaitThenReceive)

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
        >>> first (exchangeThenKeep >>> handOver >>> after (millis 20) >>> sendByte >>> closedByTheOther >>> closeOther)
        >>> arr snd
        >>> now (\n1 -> newPair >>= \pair -> (\n2 -> (n1 == n2, pair)) <$> number (fst pair))
        >>> second ((\pair -> (pair, pair)) ^>> first exchange >>> now (\(count, pair) -> count <$ closing pair))
  | otherwise =
      ticket @_ @'Anywhere
        >>> now (\(awaiting, giver) -> awaiting <$ atomicWriteIORef shelf (Just giver))
        >>> await
        >>> now (\socket -> (\buffer -> (socket, roomOf buffer)) <$> newBuffer (positiveInt @16))
        >>> (\(socket, room) -> ((socket, room), socket)) ^>> first receive
        >>> second (now (\socket -> Socket.close socket *> atomicWriteIORef closed True))
        >>^ \(count, ()) -> (True, count)
  where
    -- Waits for the second core's ticket, and gives it the first socket.
    handOver = keeping (untilJust (readIORef shelf)) >>> (\(giver, pair@(reader, _)) -> ((giver, reader), pair)) ^>> first give >>^ snd
    closedByTheOther = keeping (untilJust (fmap (\raised -> if raised then Just () else Nothing) (readIORef closed))) >>^ snd
    closeOther = now (\pair@(_, other) -> pair <$ Socket.close other)
