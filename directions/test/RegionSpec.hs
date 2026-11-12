{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fdefer-type-errors -Wno-deferred-type-errors #-}

-- | What the region of a core in the type of its arrow keeps from happening. This module is compiled with
-- @-fdefer-type-errors@: each program that ought not to compile is a type error that GHC defers to the point where
-- the program runs, where it throws a 'TypeError', which the tests catch. The programs that ought to compile are
-- in the module too, and run: in a module where they failed for another reason they would throw 'TypeError' too.
module RegionSpec (spec) where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (|||))
import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Exception (TypeError (..), evaluate, try)
import Control.Monad (void, when)
import Data.Coerce (coerce)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Network.Directions.Classes
import Network.Directions.Delimited
import Skeletest

spec :: Spec
spec = describe "the region of a core" $ do
  describe "what compiles" $ do
    it "lets a core's parts make tickets, and give them and wait for them" $
      fst <$> runDelimited ownTickets () `shouldReturn` (3 :: Int)

    it "lets a giver cross cores, which has no core in its type" $ do
      ensureCores
      box <- newIORef Nothing
      runCores 2 (pure box) giverAcrossCores `shouldReturn` [4, 1]

  describe "what doesn't compile" $ do
    it "stops a ticket that one core made from being given a value by another core" $ do
      ensureCores
      throwsTypeError ticketAcrossCores

    it "stops the end of a remote ticket that one core made from being waited at by another core" $ do
      ensureCores
      throwsTypeError awaitingAcrossCores

    it "stops a ticket from leaving the run that made it" $
      throwsTypeError ticketEscapes

    it "stops a ticket, or an arrow, from being coerced to the type of another core's" $ do
      throwsTypeError (evaluate (retag undefined))
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
found :: (ArrowChoice arr, Effect arr, GiveWay arr) => IORef (Maybe a) -> arr () a
found box = loop
  where
    loop = now (\() -> maybe (Left ()) Right <$> readIORef box) >>> ((giveWay >>> loop) ||| returnA)

-- | A ticket made, given a value, and waited for, on one core.
ownTickets :: Delimited s () Int
ownTickets = newTicket >>> arr (\ticket -> ((ticket, 3), ticket)) >>> first fulfil >>> arr snd >>> await

-- | Core 0 makes a remote ticket, leaves its giver where core 1 finds it, and waits; core 1 gives it a value.
giverAcrossCores :: IORef (Maybe (Giver Int)) -> Int -> Delimited s () Int
giverAcrossCores box core
  | core == 0 = newRemote >>> now (\(awaiting, giver) -> awaiting <$ writeIORef box (Just giver)) >>> awaitRemote
  | otherwise = found box >>> arr (,4) >>> give >>> arr (const 1)

-- | The same with the ticket itself left for core 1: it has the type of core 0's.
ticketAcrossCores :: IO [Int]
ticketAcrossCores = do
  shelf <- newIORef Nothing
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then newTicket >>> now (\ticket -> ticket <$ writeIORef box (Just ticket)) >>> await
      else found box >>> arr (,4) >>> fulfil >>> arr (const 1)

-- | And the end of a remote ticket that is waited at.
awaitingAcrossCores :: IO [Int]
awaitingAcrossCores = do
  shelf <- newIORef Nothing
  runCores 2 (pure shelf) $ \box core ->
    if core == 0
      then newRemote >>> now (\(awaiting, _) -> writeIORef box (Just awaiting)) >>> arr (const 0)
      else found box >>> awaitRemote

-- | A run whose result is a ticket.
ticketEscapes :: IO ()
ticketEscapes = void (runDelimited (newTicket :: Delimited s () (Ticket (Delimited s) Int)) ())

retag :: Ticket (Delimited s1) a -> Ticket (Delimited s2) a
retag = coerce

retagArrow :: Delimited s1 a b -> Delimited s2 a b
retagArrow = coerce
