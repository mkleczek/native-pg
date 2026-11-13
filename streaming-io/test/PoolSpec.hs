{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | What programs lend from the pool: an item for as long as a body runs, the pool's closing, a checkout that gives up,
-- and the setup buffers of the pool that a program's loans belong to. Every case is run by each arrow.
module PoolSpec (spec) where

import Control.Arrow (arr, (>>>), (^>>))
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Data.Tuple (Solo)
import LoansTestSupport
import Network.Streaming.Buffer (Buffer, bufferCapacity)
import Network.Streaming.Classes
import Network.Streaming.Combinators (keeping, together)
import Network.Streaming.Loans (withSetupBuffersOf)
import Network.Streaming.Pool
import Skeletest

spec :: Spec
spec = describe "the pool" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let run :: Program a -> IO a
      run = runOn runner

  describe "withLoan" $ do
    it "lends an item for as long as the body runs, and takes it back when it ends" $ do
      pool <- newPool [7 :: Int]
      run (withLoan pool (now (\(item, ()) -> (item,) <$> free pool))) `shouldReturn` Right (7, [])
      free pool `shouldReturn` [7]

    it "takes the item back when the body fails" $ do
      pool <- newPool [7 :: Int]
      run (tryA (withLoan pool (now (\_ -> throwIO (userError "boom") :: IO ())))) >>= either (const (pure ())) (const (failTest "the body did not fail"))
      free pool `shouldReturn` [7]

    it "is 'PoolClosed' once the pool is closed, and gives the items that were free to its closer" $ do
      pool <- newPool [7 :: Int, 8]
      closePool pool `shouldReturn` [7, 8]
      run (withLoan pool (arr (const ()))) `shouldReturn` Left PoolClosed

    it "gives up the checkout after the time, and has nobody waiting for the item" $ do
      pool <- newPool [7 :: Int]
      -- The item is held by the outer body, so the checkout of the inner one has to wait for it.
      run (withLoan pool (arr (const ()) >>> withLoanWithin (millis 20) pool (arr (const ())))) `shouldReturn` Right (Left CheckoutTimedOut)
      free pool `shouldReturn` [7]
      waiters pool `shouldReturn` 0

    it "lends an item that is free at once, within the time" $ do
      pool <- newPool [7 :: Int]
      run (withLoanWithin (millis 20) pool (arr fst)) `shouldReturn` Right 7

  describe "the setup buffers of the pool that the loans belong to" $ do
    it "grants them, whichever pool the request is of, and gives them back" $ do
      buffers <- newTestPool 1 1 2
      run (request buffers ((, ()) ^>> withSetupBuffersOf @(Buffer, Buffer) (arr (\((one, other), _) -> (bufferCapacity one, bufferCapacity other))))) `shouldReturn` (8, 8)
      length <$> setupFree buffers `shouldReturn` 2

    it "gives them back when the program fails" $ do
      buffers <- newTestPool 1 1 1
      run (request buffers (tryA ((, ()) ^>> withSetupBuffersOf @(Solo Buffer) (now (\_ -> throwIO (userError "boom") :: IO ()))))) >>= either (const (pure ())) (const (failTest "the body did not fail"))
      length <$> setupFree buffers `shouldReturn` 1

    it "refuses a grant that can never be made, at once" $ do
      buffers <- newTestPool 1 1 2
      run (request buffers (failsWith "buffer count must fit capacity" ((, ()) ^>> withSetupBuffersOf @(Buffer, Buffer, Buffer) (arr (const ())))))

    it "makes a grant wait for the buffers of another, and serves it once they are given back" $ do
      buffers <- newTestPool 1 1 1
      journal <- newJournal
      held <- newSignal
      release' <- newSignal
      run $
        request buffers $
          together
            [ (, ()) ^>> withSetupBuffersOf @(Solo Buffer) (raise held >>> awaitSignal release' >>> note journal "first gave back"),
              keeping (awaitSignal held) >>> arr snd >>> ((, ()) ^>> withSetupBuffersOf @(Solo Buffer) (note journal "second granted")),
              arr (const ()) >>> awaitSignal held >>> awaitSetupWaiting buffers 1 >>> raise release'
            ]
      readJournal journal `shouldReturn` ["first gave back", "second granted"]

