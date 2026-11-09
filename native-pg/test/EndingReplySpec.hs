{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The ending's reply, waited for in place, over a link that the spec feeds.
module EndingReplySpec (spec) where

import Control.Concurrent.STM (atomically)
import Control.Monad.IO.Class (liftIO)
import Control.Exception (AsyncException (ThreadKilled), IOException, throwIO, try)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (isJust, isNothing)
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import qualified Database.PostgreSQL.Diagnostics as PG
import Database.PostgreSQL.Internal.Context (Context)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import qualified Database.PostgreSQL.Transaction as Tx
import FeedSupport
import Network.Streaming.BufferPool (BufferAccess, borrowBuffer, tryBorrowBuffer)
import Network.Streaming.Scope (withScope)
import Network.Streaming.Source (WaitingSource (..))
import Network.Streaming.Watch (Watch (..), awaitWatch)
import Skeletest hiding (context)
import System.Timeout (timeout)

newtype Body tx = Body (Maybe (Tx.Field tx))

bodyStatement :: Tx.Statement () Body
bodyStatement = Tx.Statement "SELECT body" (const []) (Tx.streamingRow @'[] Body)

body :: BS.ByteString
body = BS.take 200 (BS.concat [BC.pack (show n) <> "," | n <- [0 :: Int .. 200]])

-- | Runs the action in a transaction of a scope, with a final pipeline that
-- ends in COMMIT and has already been sent: the body's field, pulled to its end.
afterBody :: Bool -> Context -> (forall s. Tx.Tx s -> IO a) -> IO a
afterBody final context action = withScope $ \scope -> do
  tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Write (Exchange.Session context)
  Tx.runIn tx $ do
    cursor <- (if final then Tx.finalPipeline else Tx.pipeline) (Tx.statement () bodyStatement)
    Tx.nextRow cursor >>= \case
      Just (Body (Just field)) -> liftIO (pullToEnd (Tx.fieldSource field))
      _ -> liftIO (failTest "the row has no field")
  action tx

-- | Pulls until the end, whatever the chunks, which are not waited for.
pullToEnd :: WaitingSource source => source -> IO ()
pullToEnd source =
  pullWaiting source noWait >>= \case
    Just _ -> pullToEnd source
    Nothing -> pure ()

-- | A wait that fails the spec: the call may not wait.
noWait :: IO Watch -> IO ()
noWait _ = failTest "the call waited"

-- | What the server sends for a final pipeline that commits.
committed :: BS.ByteString
committed = command "BEGIN" <> rows [[Just body]] <> command "COMMIT" <> ready

-- | What the server sends up to the end of the body, and after it.
throughBody, afterBodyBytes :: BS.ByteString
(throughBody, afterBodyBytes) =
  let (before, rest) = BS.breakSubstring body committed
   in (before <> body, BS.drop (BS.length body) rest)

-- | In a scope of a request that has the loan that is its own held elsewhere,
-- so that its input takes the pool's spare buffer: pulls the field of a final
-- pipeline to its end, which uses up the buffer, and queues the taker for the
-- spare, so that the input finds no buffer when it reads the ending's reply.
-- The action gets the transaction and the taker's registration, and the scope
-- ends after it, aborted.
starved :: BufferAccess -> FeedLink -> Context -> (forall s. Tx.Tx s -> Watch -> IO a) -> IO a
starved taker link context use = do
  feed link throughBody
  outcome <- newIORef Nothing
  ended <- try @AsyncException $ withScope $ \scope -> do
    tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Write (Exchange.Session context)
    Tx.runIn tx $ do
      cursor <- Tx.finalPipeline (Tx.statement () bodyStatement)
      Tx.nextRow cursor >>= \case
        Just (Body (Just field)) -> liftIO (pullToEnd (Tx.fieldSource field))
        _ -> liftIO (failTest "the row has no field")
    registration <- queueFor taker
    use tx registration >>= writeIORef outcome . Just
    throwIO ThreadKilled :: IO ()
  ended `shouldBe` Left ThreadKilled
  readIORef outcome >>= maybe (failTest "the scope ended early") pure

spec :: Spec
spec = describe "the reply to the transaction's ending" $ do
  it "gives success once COMMIT's reply is in the input, and sent COMMIT once" $
    withFeed 4096 $ \link context -> do
      feed link committed
      afterBody True context $ \tx -> do
        Tx.endingWaiting tx noWait >>= \case
          Right () -> pure ()
          Left failure -> failTest ("the ending failed: " <> show failure)
        Tx.endingWaiting tx noWait >>= \case
          Right () -> pure ()
          Left _ -> failTest "the answer changed when it was asked again"
      sent <- sentBytes link
      occurrences "COMMIT" sent `shouldBe` 1

  it "gives the failure of COMMIT, e.g. a deferred constraint, as a value" $
    withFeed 4096 $ \link context -> do
      feed link (command "BEGIN" <> rows [[Just body]] <> message 49 "" <> message 50 "" <> errorMessage "23503" <> ready)
      afterBody True context $ \tx ->
        Tx.endingWaiting tx noWait >>= \case
          Left (Tx.QueryError "COMMIT" (Tx.ServerFailed (PG.ServerError diagnostics))) ->
            PG.diagnosticCode diagnostics `shouldBe` "23503"
          Left other -> failTest ("an unexpected failure: " <> show other)
          Right () -> failTest "COMMIT's failure was lost"

  it "waits while the reply hasn't arrived, and takes it once the wait has fed it" $
    withFeed 4096 $ \link context -> do
      let (early, late) = BS.splitAt (BS.length committed - 3) committed
      feed link early
      afterBody True context $ \tx -> do
        waits <- newIORef (0 :: Int)
        -- Part of a message is not the reply
        let wait register = modifyIORef' waits (+ 1) *> feed link late *> awaitWatch register
        timeout 5_000_000 (Tx.endingWaiting tx wait) >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the reply that arrived was not taken"
        readIORef waits `shouldReturn` 1

  it "gives the watch of what it waits for, which fires when the server sends the rest" $
    withFeed 4096 $ \link context -> do
      let (early, late) = BS.splitAt (BS.length committed - 20) committed
      feed link early
      afterBody True context $ \tx -> do
        let wait register = do
              watch <- register
              timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
              feed link late
              timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
              watchCancel watch
        Tx.endingWaiting tx wait >>= \case
          Right () -> pure ()
          _ -> failTest "the reply that arrived was not taken"

  it "receives the reply in receives that don't wait, when the input is smaller than the rest" $
    withFeed 64 $ \link context -> do
      feed link committed
      afterBody True context $ \tx ->
        Tx.endingWaiting tx noWait >>= \case
          Right () -> pure ()
          _ -> failTest "the reply was not taken"

  it "queues an ending that wasn't queued, and waits until its reply arrives" $
    withFeed 4096 $ \link context -> do
      feed link (command "BEGIN" <> rows [[Just body]] <> readyTx)
      afterBody False context $ \tx -> do
        let wait register = do
              sent <- sentBytes link
              occurrences "COMMIT" sent `shouldBe` 1
              feed link (command "COMMIT" <> ready)
              awaitWatch register
        timeout 5_000_000 (Tx.endingWaiting tx wait) >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the reply that arrived was not taken"

  it "rejects replies that don't fit the input, while the field is unread" $
    withFeed 64 $ \link context -> do
      feed link committed
      withScope $ \scope -> do
        tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Write (Exchange.Session context)
        Tx.runIn tx $ do
          cursor <- Tx.finalPipeline (Tx.statement () bodyStatement)
          _ <- Tx.nextRow cursor
          pure ()
        outcome <- try @ConnectionError (Tx.endingWaiting tx noWait)
        case outcome of
          Left (RowMisuse _) -> pure ()
          other -> failTest ("unread bytes that don't fit were accepted: " <> either show (const "no failure") other)

  it "fails, and abandons the transaction, when the wait fails" $
    withFeed 4096 $ \link context -> do
      let (early, _) = BS.splitAt (BS.length committed - 3) committed
      feed link early
      outcome <- newIORef Nothing
      -- the scope that ends after the abandoned transaction fails too
      _ <- try @ConnectionError $ afterBody True context $ \tx -> do
        gone <- try @IOException (Tx.endingWaiting tx (\_ -> throwIO (userError "the client went away")))
        again <- try @ConnectionError (Tx.endingWaiting tx noWait)
        writeIORef outcome (Just (gone, again))
      readIORef outcome >>= \case
        Just (Left _, Left (SessionUnusable _)) -> pure ()
        _ -> failTest "the wait's failure was not the call's, or the transaction was not abandoned"

  describe "without a buffer to receive into" $ do
    it "waits for a buffer, and its watch fires when one is returned" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context ->
          starved taker link context $ \tx registration -> do
            let wait register = do
                  watch <- register
                  timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                  watchCancel registration
                  timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                  watchCancel watch
                  feed link afterBodyBytes
            Tx.endingWaiting tx wait >>= \case
              Right () -> pure ()
              _ -> failTest "the reply that arrived was not taken"

    it "withdraws its registration for a buffer when its scope ends" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        registration <- withRequestFeed reader $ \link context ->
          starved taker link context $ \tx registration -> do
            let wait register = do
                  watch <- register
                  timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                  watchCancel watch
                  throwIO ThreadKilled
            _ <- try @AsyncException (Tx.endingWaiting tx wait)
            pure registration
        -- a registration that was left would be a second loan, and fail this try
        isNothing <$> tryBorrowBuffer reader `shouldReturn` True
        watchCancel registration
        isJust <$> tryBorrowBuffer reader `shouldReturn` True

-- | How many times the needle occurs in the bytes.
occurrences :: BS.ByteString -> BS.ByteString -> Int
occurrences needle bytes = case BS.breakSubstring needle bytes of
  (_, rest) | BS.null rest -> 0
  (_, rest) -> 1 + occurrences needle (BS.drop (BS.length needle) rest)
