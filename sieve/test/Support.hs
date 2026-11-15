{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

-- | What the specs share: the ways to run a program, which are an arrow and a sharing of the cache, the parts of the
-- sieve that the code has so far, and a few helpers.
module Support
  ( Part (..),
    notThere,
    needs,
    itNeeds,
    itCase,
    Setup (..),
    setups,
    sharedSetups,
    isShared,
    Log,
    newLog,
    noteIn,
    noteEvicted,
    readLog,
    ensureCores,
    promptly,
    ints,
    counter,
    bump,
    tenTimes,
    accept,
    valuesOf,
    spinUntil,
    pause,
    later,
    viewOf,
    withPart,
    Keys (..),
    intKeys,
    bytesKeys,
    textKeys,
    bytesKey,
    unBytesKey,
    textKey,
    unTextKey,
  )
where

import Control.Arrow (ArrowChoice, (>>>), (>>^), (^>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Monad (when)
import Data.Kind (Type)
import Data.Bool (bool)
import Data.Char (isDigit)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Sieve
import Data.Sieve.Lease (Lease, newLease, newSharedLease)
import Data.Sieve.Teardown (Graveyard, newGraveyard, newSharedGraveyard)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, sleep, tap, untilJust)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.Threads (runThreads)
import qualified Data.ByteString.Char8 as B8
import qualified Data.Text as T
import Data.Text (Text)
import Foreign.Ptr (minusPtr)
import Skeletest
import System.Timeout (timeout)

-- | A part of the sieve, which the cases that need it are expected to fail without.
data Part
  = -- | The table: the cache of one core, and the operations that don't wait.
    Table
  | -- | 'cached', its hook and its loader.
    Cached
  | -- | The loads that callers share.
    SingleFlight
  | -- | The cache that all cores share.
    Shared
  | -- | The keys that are strict byte strings and text.
    Strings
  | -- | The eviction listener is told of every value that leaves the cache or is not kept.
    Owned
  | -- | The lease.
    Lease
  | -- | The graveyard and the teardown part.
    Teardown
  deriving (Eq, Show)

-- | The parts that the code doesn't have yet. A change that adds one takes it off the list, and with it the marks of the cases
-- that wait for it.
notThere :: [Part]
notThere = []

-- | Marks every case of the spec as an expected failure while a part that it needs is missing. A case that then passes
-- unexpectedly fails the suite.
needs :: [Part] -> Spec -> Spec
needs parts = case filter (`elem` notThere) parts of
  [] -> id
  missing -> xfail ("sieve: " <> show missing <> " is not there yet")

-- | A case that needs the parts.
itNeeds :: (HasCallStack) => [Part] -> String -> IO () -> Spec
itNeeds parts name body = needs parts (it name body)

-- | A case that runs on the setup: it needs the parts, and the one that shares if the setup's cache is shared.
itCase :: (HasCallStack) => Setup -> [Part] -> String -> IO () -> Spec
itCase setup parts = itNeeds (parts ++ [Shared | isShared setup])

-- | A way to run a program of a cache: the arrow that runs it, with the sharing of the cache. The program is for any arrow
-- that has what the sharing needs.
data Setup = forall c. Setup
  { setupName :: String,
    setupSharing :: Sharing,
    setupCache :: forall k v. (Key k) => Config k v -> IO (Cache c k v),
    setupLease :: IO (Lease c),
    setupGraveyard :: forall (v :: Type). IO (Graveyard c v),
    setupRun :: forall a. (forall arr. (Runtime arr, Reaching c arr) => arr () a) -> IO a
  }

-- | Whether the setup's cache is shared by all cores, so that it needs the part of the code that shares.
isShared :: Setup -> Bool
isShared setup = case setupSharing setup of
  AllCores -> True
  OneCore -> False

-- | Every arrow with the cache that it can use: threads preempt their parts, so they have only the shared cache.
setups :: [Setup]
setups =
  [ Setup "threads, shared cache" AllCores newSharedCache newSharedLease newSharedGraveyard (\program -> runThreads program ()),
    Setup "delimited, shared cache" AllCores newSharedCache newSharedLease newSharedGraveyard (\program -> fst <$> runDelimited program ()),
    Setup "delimited, cache of one core" OneCore newCache newLease newGraveyard (\program -> fst <$> runDelimited program ()),
    Setup "compiled, shared cache" AllCores newSharedCache newSharedLease newSharedGraveyard (\program -> fst <$> runCompiled program ()),
    Setup "compiled, cache of one core" OneCore newCache newLease newGraveyard (\program -> fst <$> runCompiled program ())
  ]

-- | The setups of the shared cache.
sharedSetups :: [Setup]
sharedSetups = filter isShared setups

-- | A log of what happened, in order.
type Log = IORef [String]

newLog :: IO Log
newLog = newIORef []

noteIn :: Log -> String -> IO ()
noteIn logged line = atomicModifyIORef' logged (\earlier -> (line : earlier, ()))

readLog :: Log -> IO [String]
readLog logged = reverse <$> readIORef logged

-- | A test's cores need capabilities: it runs with @+RTS -N4@, and sets that if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

-- | Runs the action, which gives nothing if it takes more than a minute.
promptly :: IO a -> IO (Maybe a)
promptly = timeout 60000000

-- | A config of this capacity, for a cache of numbers.
ints :: Int -> Config Int Int
ints = config

-- | A counter that parts of any core count with.
counter :: IO (IORef Int)
counter = newIORef 0

-- | Adds one, and gives what the counter was.
bump :: IORef Int -> IO Int
bump ref = atomicModifyIORef' ref (\n -> (n + 1, n))

-- | The loader: ten times the key, counting each load.
tenTimes :: (Effect arr) => IORef Int -> arr Int Int
tenTimes loads = now (\k -> k * 10 <$ bump loads)

-- | A hook that accepts every value as it is.
accept :: Instant -> v -> IO (Use v)
accept _ v = pure (Use v)

-- | The value of each key in turn, as 'cached' gives it, with a hook that accepts it.
valuesOf :: (Runtime arr, Reaching c arr) => arr Int Int -> Cache c Int Int -> [Int] -> arr () [Int]
valuesOf loader cache = traverse (\k -> const (cache, k) ^>> cached loader accept)

-- | Waits, giving way, until the test says yes.
spinUntil :: (ArrowChoice arr, Effect arr, GiveWay arr, Annotate arr) => IO Bool -> arr () ()
spinUntil test = untilJust (bool Nothing (Just ()) <$> test)

-- | Waits this long, and goes on with what came in.
pause :: (Runtime arr) => Duration -> arr a a
pause duration = tap (done >>> sleep duration)

-- | The arrow, after a pause.
later :: (Runtime arr) => Duration -> arr a b -> arr a b
later duration body = pause duration >>> body

-- | Whether the second byte string is a view of the memory of the first, and not a copy.
viewOf :: ByteString -> ByteString -> IO Bool
viewOf buffer candidate =
  BSU.unsafeUseAsCString buffer $ \start ->
    BSU.unsafeUseAsCString candidate $ \at ->
      let offset = at `minusPtr` start in pure (offset >= 0 && offset < BS.length buffer)

-- | The key of a number as a slice of a wider byte string, as the key of a request is a slice of its buffer: the bytes of a JWT.
bytesKey :: Int -> ByteString
bytesKey k = BS.take (BS.length bytes) (BS.drop 4 ("xxxx" <> bytes <> "yyyy"))
  where
    bytes = B8.pack ("jwt." <> show k <> ".sig")

unBytesKey :: ByteString -> Int
unBytesKey = read . takeWhile isDigit . drop 4 . B8.unpack

-- | The key of a number as a slice of a wider text, as a tenant's name is read from a header.
textKey :: Int -> Text
textKey k = T.take (T.length name) (T.drop 4 ("xxxx" <> name <> "yyyy"))
  where
    name = T.pack ("tenant-" <> show k)

unTextKey :: Text -> Int
unTextKey = read . drop 7 . T.unpack

-- | How a number is a key, and which number a key is: for the specs that run with each kind of key.
data Keys k = Keys {keyOfNumber :: Int -> k, numberOfKey :: k -> Int}

intKeys :: Keys Int
intKeys = Keys id id

bytesKeys :: Keys ByteString
bytesKeys = Keys bytesKey unBytesKey

textKeys :: Keys Text
textKeys = Keys textKey unTextKey

-- | A part that never ends, beside a scenario: what ends is the scenario, which cancels the part. It says which.
withPart :: (Runtime arr) => arr () () -> arr () b -> arr () String
withPart part scenario = race part scenario >>^ either (const "the part ended") (const "the scenario ended")

-- | Notes an eviction of a cache of numbers.
noteEvicted :: Log -> Int -> Int -> IO ()
noteEvicted logged k v = noteIn logged (show (k, v))
