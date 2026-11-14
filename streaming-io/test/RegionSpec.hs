{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fdefer-type-errors -Wno-deferred-type-errors #-}

-- | What the region of a core in the type of its arrow keeps from happening. This module is compiled with
-- @-fdefer-type-errors@: each program that ought not to compile is a type error that GHC defers to the point where
-- the program runs, where it throws a 'TypeError', which the tests catch. The programs that ought to compile are
-- in the module too, and run: in a module where they failed for another reason they would throw 'TypeError' too.
-- Each program that doesn't compile has a twin that does, the same with a promise of reach @'Anywhere@.
module RegionSpec (spec) where

import Control.Arrow (ArrowChoice, second, (>>>), (>>^), (^>>))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Exception (TypeError (..), evaluate, try)
import Control.Monad (void, when)
import Data.Coerce (coerce)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Network.Streaming.Classes
import Network.Streaming.Combinators (done, keeping, tap, untilJust)
import Network.Streaming.Delimited
import Skeletest

spec :: Spec
spec = describe "the region of a core" $ do
  describe "what compiles" $ do
    it "lets a core's parts make tickets of reach 'Here, and fulfil them and wait for them" $
      fst <$> runDelimited ownTickets () `shouldReturn` (3 :: Int)

    it "lets a promise of reach 'Anywhere cross cores, which has no core in its type" $ do
      ensureCores
      box <- newIORef Nothing
      runCores 2 (pure box) promiseAcrossCores `shouldReturn` [4, 1]

    it "lets the shared setup be typed to hold a promise of reach 'Anywhere" $ do
      ensureCores
      box <- newIORef Nothing
      runCores 2 (pure box) promiseInSetup `shouldReturn` [4, 1]

    it "lets each core wait at its own future of a ticket of reach 'Anywhere, and fulfil the other core's promise" $ do
      ensureCores
      boxes <- (,) <$> newIORef Nothing <*> newIORef Nothing
      runCores 2 (pure boxes) crossedPromises `shouldReturn` [11, 10]

  describe "what doesn't compile" $ do
    it "stops a promise of reach 'Here that one core made from being used by the program of another core" $ do
      ensureCores
      throwsTypeError localPromiseAcrossCores

    it "stops the future of a ticket that one core made from being awaited on another core, of either reach" $ do
      ensureCores
      throwsTypeError localFutureAcrossCores
      throwsTypeError anywhereFutureAcrossCores

    it "stops a promise of reach 'Here from being kept in what the cores share, which was made before the run" $ do
      ensureCores
      throwsTypeError localPromiseInSetup

    it "stops a ticket's end from leaving the run that made it" $ do
      throwsTypeError localPromiseEscapes
      throwsTypeError futureEscapes

    it "stops a ticket's end, or an arrow, from being coerced to the type of another core's" $ do
      throwsTypeError (evaluate (retag undefined))
      throwsTypeError (evaluate (retagFuture undefined))
      throwsTypeError (evaluate (retagArrow undefined))

-- | A run needs @+RTS -N4@ for its cores to run at once; this sets it if the run was started without.
ensureCores :: IO ()
ensureCores = getNumCapabilities >>= \n -> when (n < 4) (setNumCapabilities 4)

throwsTypeError :: IO a -> IO ()
throwsTypeError action =
  try (void action) >>= \case
    Left (TypeError _) -> pure ()
    Right () -> failTest "the program ran: it compiled"

-- | Waits, giving way, until the reference holds a value.
found :: (ArrowChoice arr, Annotate arr, Effect arr, GiveWay arr) => IORef (Maybe a) -> arr () a
found box = untilJust (readIORef box)

-- | The ends of a ticket of the core of the program, and of one that any core can fulfil.
hereTicket :: Delimited s () (Future (Delimited s) ('Here s) a, Promise ('Here s) a)
hereTicket = ticket

anywhereTicket :: Delimited s () (Future (Delimited s) 'Anywhere a, Promise 'Anywhere a)
anywhereTicket = ticket

-- | A ticket made, fulfilled, and waited for, on one core.
ownTickets :: Delimited s () Int
ownTickets = hereTicket >>> tap ((,3) . snd ^>> fulfil >>> done) >>> fst ^>> await

-- | Core 0 makes a ticket, leaves its promise where core 1 finds it, and waits; core 1 fulfils it.
promiseAcrossCores :: IORef (Maybe (Promise 'Anywhere Int)) -> Int -> Delimited s () Int
promiseAcrossCores box core
  | core == 0 = anywhereTicket >>> second (now (writeIORef box . Just)) >>> fst ^>> await
  | otherwise = found box >>> (,4) ^>> fulfil >>^ const 1

-- | The same, which is the program that isn't well-typed with the promise of a ticket of reach @'Here@, with the type
-- of what the cores share given where it is made.
promiseInSetup :: IORef (Maybe (Promise 'Anywhere Int)) -> Int -> Delimited s () Int
promiseInSetup = promiseAcrossCores

-- | Each core leaves the promise of its ticket, takes the other's and fulfils it, and waits at its own future:
-- core 0 gets 11 and core 1 gets 10.
crossedPromises :: (IORef (Maybe (Promise 'Anywhere Int)), IORef (Maybe (Promise 'Anywhere Int))) -> Int -> Delimited s () Int
crossedPromises (box0, box1) core =
  anywhereTicket
    >>> second (now (writeIORef mine . Just))
    >>> fst ^>> keeping (found theirs >>> (,core + 10) ^>> fulfil)
    >>> snd ^>> await
  where
    (mine, theirs) = if core == 0 then (box0, box1) else (box1, box0)

-- | Core 0 makes a ticket of its core and leaves its promise for core 1, which fulfils it.
localPromiseAcrossCores :: IO [Int]
localPromiseAcrossCores = do
  shelf <- newIORef Nothing
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then hereTicket >>> second (now (writeIORef box . Just)) >>> fst ^>> await
      else found box >>> (,4) ^>> fulfil >>^ const 1

-- | And the future, of either reach, left for core 1.
localFutureAcrossCores :: IO [Int]
localFutureAcrossCores = do
  shelf <- newIORef Nothing
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then hereTicket >>> now (\(end, _) -> 0 <$ writeIORef box (Just end))
      else found box >>> await

anywhereFutureAcrossCores :: IO [Int]
anywhereFutureAcrossCores = do
  shelf <- newIORef Nothing
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then anywhereTicket >>> now (\(end, _) -> 0 <$ writeIORef box (Just end))
      else found box >>> await

-- | The shared setup holds a reference for a promise of reach @'Here@, as it can't for the region of any run.
localPromiseInSetup :: IO [Int]
localPromiseInSetup = do
  shelf <- newIORef Nothing :: IO (IORef (Maybe (Promise ('Here ()) Int)))
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then hereTicket >>> second (now (writeIORef box . Just)) >>> fst ^>> await
      else found box >>> (,4) ^>> fulfil >>^ const 1

-- | A run whose result is the promise of a ticket of reach @'Here@, and one whose result is the future.
localPromiseEscapes :: IO ()
localPromiseEscapes = void (runDelimited (hereTicket >>^ snd :: Delimited s () (Promise ('Here s) Int)) ())

futureEscapes :: IO ()
futureEscapes = void (runDelimited (hereTicket >>^ fst :: Delimited s () (Future (Delimited s) ('Here s) Int)) ())

retag :: Promise ('Here s1) a -> Promise ('Here s2) a
retag = coerce

retagFuture :: Future (Delimited s1) ('Here s1) a -> Future (Delimited s2) ('Here s2) a
retagFuture = coerce

retagArrow :: Delimited s1 a b -> Delimited s2 a b
retagArrow = coerce
