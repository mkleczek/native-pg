{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The ending's reply without waiting, over a link that the spec feeds.
module EndingReplySpec (spec) where

import Control.Concurrent.STM (atomically)
import Control.Monad.IO.Class (liftIO)
import Control.Exception (AsyncException (ThreadKilled), throwIO, try)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Maybe (isJust, isNothing)
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import qualified Database.PostgreSQL.Diagnostics as PG
import Database.PostgreSQL.Internal.Context (Context)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import qualified Database.PostgreSQL.Transaction as Tx
import FeedSupport
import Network.Streaming.BufferPool (BufferAccess, borrowBuffer, tryBorrowBuffer)
import Network.Streaming.Scope (withScope)
import Network.Streaming.Source (Pulled (..), Source (..))
import Network.Streaming.Watch (Watch (..))
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
pullToEnd :: Source source => source -> IO ()
pullToEnd source = pullReady source >>= \case
  Pulled _ -> pullToEnd source
  Pending -> failTest "the pull was pending"
  Ended -> pure ()

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
        Tx.endingReady tx >>= \case
          Just (Right ()) -> pure ()
          Just (Left failure) -> failTest ("the ending failed: " <> show failure)
          Nothing -> failTest "the reply had arrived"
        Tx.endingReady tx >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the answer changed when it was asked again"
      sent <- sentBytes link
      occurrences "COMMIT" sent `shouldBe` 1

  it "gives the failure of COMMIT, e.g. a deferred constraint, as a value" $
    withFeed 4096 $ \link context -> do
      feed link (command "BEGIN" <> rows [[Just body]] <> message 49 "" <> message 50 "" <> errorMessage "23503" <> ready)
      afterBody True context $ \tx ->
        Tx.endingReady tx >>= \case
          Just (Left (Tx.QueryError "COMMIT" (Tx.ServerFailed (PG.ServerError diagnostics)))) ->
            PG.diagnosticCode diagnostics `shouldBe` "23503"
          Just (Left other) -> failTest ("an unexpected failure: " <> show other)
          Just (Right ()) -> failTest "COMMIT's failure was lost"
          Nothing -> failTest "the reply had arrived"

  it "gives nothing while the reply hasn't arrived, and doesn't wait" $
    withFeed 4096 $ \link context -> do
      let (early, late) = BS.splitAt (BS.length committed - 3) committed
      feed link early
      afterBody True context $ \tx -> do
        -- Part of a message is not the reply
        timeout 1_000_000 (Tx.endingReady tx) >>= \case
          Just Nothing -> pure ()
          _ -> failTest "a reply that is cut was taken, or the call waited"
        feed link late
        Tx.endingReady tx >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the reply that arrived was not taken"

  it "fires its watch when the server sends the rest" $
    withFeed 4096 $ \link context -> do
      let (early, late) = BS.splitAt (BS.length committed - 20) committed
      feed link early
      afterBody True context $ \tx -> do
        notYet tx
        watch <- Tx.endingWatch tx
        timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
        feed link late
        timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
        watchCancel watch
        Tx.endingReady tx >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the reply that arrived was not taken"

  it "receives the reply in receives that don't wait, when the input is smaller than the rest" $
    withFeed 64 $ \link context -> do
      feed link committed
      afterBody True context $ \tx ->
        Tx.endingReady tx >>= \case
          Just (Right ()) -> pure ()
          _ -> failTest "the reply was not taken"

  it "queues an ending that wasn't queued, and gives nothing until its reply arrives" $
    withFeed 4096 $ \link context -> do
      feed link (command "BEGIN" <> rows [[Just body]] <> readyTx)
      afterBody False context $ \tx -> do
        notYet tx
        sent <- sentBytes link
        occurrences "COMMIT" sent `shouldBe` 1
        feed link (command "COMMIT" <> ready)
        Tx.endingReady tx >>= \case
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
        outcome <- try @ConnectionError (Tx.endingReady tx)
        case outcome of
          Left (RowMisuse _) -> pure ()
          other -> failTest ("unread bytes that don't fit were accepted: " <> either show (const "no failure") other)
  describe "without a buffer to receive into" $ do
    it "gives nothing without waiting, and its watch fires when a buffer is returned" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context ->
          starved taker link context $ \tx registration -> do
            nothingWithoutWaiting tx
            watch <- Tx.endingWatch tx
            timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
            -- a call while the registration is pending doesn't try, which would fail
            nothingWithoutWaiting tx
            watchCancel registration
            timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
            watchCancel watch
            feed link afterBodyBytes
            Tx.endingReady tx >>= \case
              Just (Right ()) -> pure ()
              _ -> failTest "the reply that arrived was not taken"

    it "withdraws its registration for a buffer when its scope ends" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        registration <- withRequestFeed reader $ \link context ->
          starved taker link context $ \tx registration -> do
            nothingWithoutWaiting tx
            watch <- Tx.endingWatch tx
            timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
            watchCancel watch
            pure registration
        -- a registration that was left would be a second loan, and fail this try
        isNothing <$> tryBorrowBuffer reader `shouldReturn` True
        watchCancel registration
        isJust <$> tryBorrowBuffer reader `shouldReturn` True
  where
    notYet tx =
      Tx.endingReady tx >>= \case
        Nothing -> pure ()
        Just _ -> failTest "the reply had not arrived"
    nothingWithoutWaiting tx =
      timeout 1_000_000 (Tx.endingReady tx) >>= \case
        Just Nothing -> pure ()
        _ -> failTest "a reply that can't be received was taken, or the call waited"

-- | How many times the needle occurs in the bytes.
occurrences :: BS.ByteString -> BS.ByteString -> Int
occurrences needle bytes = case BS.breakSubstring needle bytes of
  (_, rest) | BS.null rest -> 0
  (_, rest) -> 1 + occurrences needle (BS.drop (BS.length needle) rest)
