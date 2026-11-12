{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}

-- | A program written in 'With', whose environment is a run-time value that its text doesn't thread, run by every
-- arrow that has the capabilities it uses.
module WithSpec (spec) where

import Control.Arrow (ArrowChoice, arr, returnA, (&&&), (>>>))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Network.Streaming.Classes
import Network.Streaming.Combinators (keeping, sleep)
import Network.Streaming.Compiled
import Network.Streaming.Delimited
import Network.Streaming.Loop
import Network.Streaming.Threads
import Network.Streaming.With
import Skeletest

-- | An arrow that has everything the programs use.
type Rich arr = (ArrowChoice arr, Effect arr, GiveWay arr, Block arr, Concurrent arr, Exchange arr (Local arr), Scope arr, Race arr, Time arr, Within arr)

spec :: Spec
spec = describe "an arrow with an environment" $ do
  it "gives the program its environment wherever it asks, whatever the arrow that runs it" $ do
    results <- inEach exchanging 100 (21 :: Int)
    results `shouldBe` replicate 4 (142, ["acquired", "released Done"])

  it "has the arrow's time, deadlines, giving way and races, which don't look at the environment" $ do
    results <- inEach (const timed) 7 ()
    results `shouldBe` replicate 4 ((Just 7, Left 7), [])

-- | In a scope that notes what it does, a ticket whose giver gives the input doubled from one half of a pair while
-- the other half waits for it; then the environment is added to it.
exchanging :: forall arr. (Rich arr) => IORef [String] -> With Int arr Int Int
exchanging noted = scope (now (\_ -> note "acquired")) (now (\(_, exit) -> note ("released " <> show exit))) (arr snd >>> doubled) >>> (returnA &&& ask) >>> arr (uncurry (+))
  where
    note what = modifyIORef' noted (what :)
    doubled = keeping (ticket @(With Int arr) @(Local arr)) >>> concurrently giving waiting >>> arr snd
    giving = arr (\((_, giver), number) -> (giver, 2 * number)) >>> give >>> arr (const ())
    waiting = arr (\((waitingEnd, _), _) -> waitingEnd) >>> await

-- | A deadline that doesn't pass, and a race that the part that doesn't wait wins.
timed :: (Rich arr) => With Int arr () (Maybe Int, Either Int Int)
timed = within (secs 60) (giveWay >>> ask) &&& race ask (sleep (secs 60) >>> ask)

-- | Runs the program with each arrow: what it gave, and what it noted.
inEach :: forall a b. (forall arr. (Rich arr) => IORef [String] -> With Int arr a b) -> Int -> a -> IO [(b, [String])]
inEach program environment input =
  sequence
    [ noting (\noted -> runThreads (with (program noted)) (environment, input)),
      noting (\noted -> fst <$> runCompiled (with (program noted)) (environment, input)),
      noting (\noted -> fst <$> runDelimited (with (program noted)) (environment, input)),
      noting (\noted -> fst <$> runLoop (with (program noted)) (environment, input))
    ]
  where
    noting :: (IORef [String] -> IO b) -> IO (b, [String])
    noting run = do
      noted <- newIORef []
      result <- run noted
      (,) result . reverse <$> readIORef noted
