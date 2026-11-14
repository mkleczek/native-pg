{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}

-- | What the arrow-level tests of the buffer pool share: the arrows that run a program, pools of tiny buffers, and small
-- steps to write a request's script with. A script is written once for any arrow that has what 'Runtime' bundles, and
-- run by each of them.
module LoansTestSupport
  ( Program,
    Runner (..),
    runners,
    runOn,
    newTestPool,
    newSizedPool,
    requestFree,
    setupFree,
    requestWaiting,
    setupWaiting,
    ensureCores,
    Slot,
    newSlot,
    borrowInto,
    giveBack,
    memoryOf,
    capacityOf,
    differentMemory,
    sameMemory,
    request,
    holding,
    at,
    steps,
    yields,
    failsWith,
    stillWaits,
    proceeds,
    Signal,
    newSignal,
    raise,
    awaitSignal,
    awaitRequestWaiting,
    awaitSetupWaiting,
    Journal,
    newJournal,
    note,
    readJournal,
    choose,
    blockForever,
  )
where

import Control.Arrow (Arrow, ArrowChoice, (&&&), (>>>), (^>>), (|||))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Exception (fromException)
import Control.Monad (when)
import Data.Bool (bool)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isNothing)
import Data.Word (Word8)
import Foreign.ForeignPtr (ForeignPtr)
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory)
import Network.Streaming.BufferPool (BufferPoolSettings (..), newBufferPool)
import Network.Streaming.Classes
import Network.Streaming.Combinators (pattern Again, pattern Finish, done, iterateWith, sleep)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Internal.BufferPool (BufferPool (..))
import Network.Streaming.Loans (Loan, Loans, borrow, loanBuffer, release, withRequestBuffers)
import Network.Streaming.Pool (free, waiters)
import Network.Streaming.PositiveInt (PositiveInt, mkPositiveInt)
import Network.Streaming.Threads (runThreads)
import Skeletest
import System.IO.Error (ioeGetErrorString)
import System.Timeout (timeout)

-- | A program for any arrow that has what 'Runtime' bundles.
type Program a = forall arr. (Runtime arr) => arr () a

-- | A way to run a program: an arrow's runner.
data Runner = Runner String (forall a. Program a -> IO a)

-- | Every arrow the cases run under.
runners :: [Runner]
runners = [Runner "threads" onThreads, Runner "delimited" onDelimited, Runner "compiled" onCompiled]

-- | Each arrow runs a program for the unit: a function of its own, since a program is for any arrow.
onThreads, onDelimited, onCompiled :: Program a -> IO a
onThreads program = runThreads program ()
onDelimited program = fst <$> runDelimited program ()
onCompiled program = fst <$> runCompiled program ()

-- | The program, which fails the case if it doesn't end in ten seconds.
runOn :: Runner -> Program a -> IO a
runOn (Runner _ run) program = timeout 10000000 (run program) >>= maybe (failTest "the program did not end in ten seconds") pure

-- * Pools

-- | A pool of buffers of 8 bytes: this many ordinary, reserve and setup buffers.
newTestPool :: Int -> Int -> Int -> IO BufferPool
newTestPool = newSizedPool 8

-- | A pool of buffers of this size, and this many ordinary, reserve and setup buffers.
newSizedPool :: Int -> Int -> Int -> Int -> IO BufferPool
newSizedPool size ordinary reserve setup =
  newBufferPool
    BufferPoolSettings
      { bufferSize = positive size,
        ordinaryBufferCount = positive ordinary,
        reserveBufferCount = positive reserve,
        setupBufferCount = positive setup
      }
  where
    positive :: Int -> PositiveInt
    positive = fromMaybe (error "a count of a pool is positive") . mkPositiveInt . toInteger

-- | The buffers that are free in the pool of the requests and in the pool of the setup, and how many parts wait for each:
-- what a case looks at once its programs have ended.
requestFree, setupFree :: BufferPool -> IO [Buffer]
requestFree = free . requestBuffers
setupFree = free . setupBuffers

requestWaiting, setupWaiting :: BufferPool -> IO Int
requestWaiting = waiters . requestBuffers
setupWaiting = waiters . setupBuffers

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

-- * Steps of a request's script

-- | Where a script keeps a loan between its steps.
newtype Slot = Slot (IORef (Maybe Loan))

newSlot :: IO Slot
newSlot = Slot <$> newIORef Nothing

-- | Borrows, and keeps the loan in the slot.
borrowInto :: (Runtime arr) => Slot -> arr Loans ()
borrowInto (Slot ref) = borrow >>> now (writeIORef ref . Just)

-- | Gives back the loan in the slot, whatever the input.
giveBack :: (Runtime arr) => Slot -> arr x ()
giveBack (Slot ref) = now (const (heldIn ref)) >>> release

heldIn :: IORef (Maybe Loan) -> IO Loan
heldIn ref = readIORef ref >>= maybe (failTest "nothing was borrowed into the slot") pure

-- | The memory of the buffer that the slot's loan lends.
memoryOf :: Slot -> IO (ForeignPtr Word8)
memoryOf (Slot ref) = bufferMemory . loanBuffer <$> heldIn ref

-- | The capacity of the buffer that the slot's loan lends.
capacityOf :: Slot -> IO Int
capacityOf (Slot ref) = bufferCapacity . loanBuffer <$> heldIn ref

-- | The loans of the two slots lend the same buffer, or different ones.
differentMemory, sameMemory :: (Effect arr) => Slot -> Slot -> arr x ()
differentMemory one other = now (const (uncurry shouldNotBe =<< ((,) <$> memoryOf one <*> memoryOf other)))
sameMemory one other = now (const (uncurry shouldBe =<< ((,) <$> memoryOf one <*> memoryOf other)))

-- | The script as one request of the pool: its input is the request's loans.
request :: (Runtime arr) => BufferPool -> arr Loans b -> arr () b
request pool script = withRequestBuffers pool (fst ^>> script)

-- | The script as this many requests at once, which are admitted one after the other: its input is their loans, in
-- the order they were admitted.
holding :: (Runtime arr) => Int -> BufferPool -> arr [Loans] b -> arr () b
holding count pool script = const [] ^>> admitted count
  where
    admitted n
      | n <= 0 = script
      | otherwise = withRequestBuffers pool (inAdmissionOrder ^>> admitted (n - 1))
    inAdmissionOrder (loans, others) = others ++ [loans]

-- | The script of the request that is at this place among the requests held.
at :: (Arrow arr) => Int -> arr Loans b -> arr [Loans] b
at index script = (!! index) ^>> script

-- | The steps one after the other, each on the same input.
steps :: (Arrow arr) => [arr x ()] -> arr x ()
steps = foldr (\step rest -> step &&& rest >>> done) done

-- | A step whose result must be the expected one.
yields :: (Runtime arr, Eq b, Show b) => arr x b -> b -> arr x ()
yields step expected = step >>> now (`shouldBe` expected)

-- | A step that must fail with a user error of this text, promptly. A cancellation isn't a failure: it passes through.
failsWith :: (Runtime arr) => String -> arr x b -> arr x ()
failsWith text step =
  tryA step
    >>> now
      ( \case
          Left failure -> fmap ioeGetErrorString (fromException failure) `shouldBe` Just text
          Right _ -> failTest ("the step did not fail with " <> text)
      )

-- | The step is still waiting when 20 milliseconds have passed, and is cancelled then: it gives up its place, and
-- whatever it was given meanwhile goes back.
stillWaits :: (Runtime arr) => arr x b -> arr x ()
stillWaits step = within (millis 20) step >>> now (\outcome -> isNothing outcome `shouldBe` True)

-- | The step ends within five seconds, which is as good as promptly for a step that waits for a part that is there.
proceeds :: (Runtime arr) => arr x b -> arr x b
proceeds step = within (secs 5) step >>> now (maybe (failTest "the step did not end in five seconds") pure)

-- * Waiting for each other

-- | What the parts of a script wait for each other with: a signal that one raises and another waits for.
newtype Signal = Signal (IORef Bool)

newSignal :: IO Signal
newSignal = Signal <$> newIORef False

raise :: (Effect arr) => Signal -> arr x ()
raise (Signal ref) = now (const (writeIORef ref True))

-- | Waits until the signal is raised.
awaitSignal :: (Runtime arr) => Signal -> arr x ()
awaitSignal (Signal ref) = pollWhen (readIORef ref)

-- | Looks until the action gives something, and between looks sleeps, which is a wait, so that the part is cancelled
-- there, and so that the parts that wait for it needn't be of the same core.
pollUntil :: (Runtime arr) => IO (Maybe a) -> arr x a
pollUntil look = done >>> iterateWith (now (const (maybe (Again ()) Finish <$> look))) (sleep (micros 50))

-- | Waits, as 'pollUntil' does, until the condition holds.
pollWhen :: (Runtime arr) => IO Bool -> arr x ()
pollWhen look = pollUntil (bool Nothing (Just ()) <$> look)

-- | Waits until this many parts wait for the pool of the requests, or of the setup.
awaitRequestWaiting, awaitSetupWaiting :: (Runtime arr) => BufferPool -> Int -> arr x ()
awaitRequestWaiting pool count = pollWhen ((>= count) <$> requestWaiting pool)
awaitSetupWaiting pool count = pollWhen ((>= count) <$> setupWaiting pool)

-- | What happened, in order.
newtype Journal = Journal (IORef [String])

newJournal :: IO Journal
newJournal = Journal <$> newIORef []

note :: (Effect arr) => Journal -> String -> arr x ()
note (Journal ref) text = now (const (atomicModifyIORef' ref (\noted -> (text : noted, ()))))

readJournal :: Journal -> IO [String]
readJournal (Journal ref) = reverse <$> readIORef ref

-- | The step at this place of the list, for an index that counts up for ever.
choose :: (ArrowChoice arr) => [arr () ()] -> arr Int ()
choose [] = done
choose [one] = done >>> one
choose (one : others) = countDown ^>> (one ||| choose others)
  where
    countDown index = if index <= 0 then Left () else Right (index - 1)

-- | A step that ends only when it is cancelled.
blockForever :: forall arr x. (Runtime arr) => arr x ()
blockForever = done >>> ticket @arr @'Anywhere >>> fst ^>> (await :: arr (Waiting arr 'Anywhere ()) ())
