{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Fields as pull sources, over a link that the spec feeds.
module FieldSourceSpec (spec) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Exception (AsyncException (ThreadKilled), IOException, evaluate, throwIO, try)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (isJust, isNothing)
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import Database.PostgreSQL.Internal.Context (Connection (..), Context, SessionState (..), contextConnection)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import qualified Database.PostgreSQL.Transaction as Tx
import FeedSupport
import Network.Streaming.BufferPool (BufferAccess, borrowBuffer, tryBorrowBuffer)
import Network.Streaming.Scope (withScope)
import Network.Streaming.Source (Pulled (..), Source (..), WaitingSource (..))
import Network.Streaming.Watch (Watch (..), awaitWatch)
import Skeletest hiding (context)
import System.Timeout (timeout)

newtype Body tx = Body (Maybe (Tx.Field tx))

-- | A row of an owned column and a field.
data Tagged tx = Tagged BS.ByteString (Maybe (Tx.Field tx))

taggedStatement :: Tx.Statement () Tagged
taggedStatement = Tx.Statement "SELECT tag, body" (const []) (Tx.streamingRow @'[BS.ByteString] Tagged)

bodyStatement :: Tx.Statement () Body
bodyStatement = Tx.Statement "SELECT body" (const []) (Tx.streamingRow @'[] Body)

-- | A body of this many bytes, in which no two chunks of 16 bytes are alike.
bodyOf :: Int -> BS.ByteString
bodyOf size = BS.take size (BS.concat [BC.pack (show n) <> "," | n <- [0 .. size]])

-- | The answer to a transaction that begins, sends these rows' bodies, and
-- commits.
answer :: [BS.ByteString] -> BS.ByteString
answer bodies = command "BEGIN" <> rows [[Just body] | body <- bodies] <> readyTx <> command "COMMIT" <> ready

-- | Reads the first row's field, and runs the action with it.
withField :: Context -> (forall tx. Tx.Field tx -> Tx.Transaction tx a) -> IO a
withField context use =
  Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
    cursor <- Tx.pipeline (Tx.statement () bodyStatement)
    Tx.nextRow cursor >>= \case
      Just (Body (Just field)) -> use field <* Tx.finishTransaction
      _ -> liftIO (failTest "the row has no field")

-- | What a pull found, with the chunk copied.
data Step = Chunk BS.ByteString | Over
  deriving (Eq)

instance Show Step where
  show (Chunk chunk) = "Chunk of " <> show (BS.length chunk) <> " bytes"
  show Over = "Over"

-- | A wait that fails the spec: the pull may not wait.
noWait :: IO Watch -> IO ()
noWait _ = failTest "the pull waited"

-- | One pull, with the wait it is given.
stepWith :: WaitingSource source => (IO Watch -> IO ()) -> source -> IO Step
stepWith wait source =
  pullWaiting source wait >>= \case
    Nothing -> pure Over
    Just chunk -> Chunk <$> evaluate (BS.copy chunk)

-- | One pull that has to return without a wait, as the input has what it needs.
step :: WaitingSource source => source -> IO Step
step = stepWith noWait

-- | One pull that has to give a chunk, without a wait.
stepChunk :: WaitingSource source => source -> IO BS.ByteString
stepChunk source =
  step source >>= \case
    Chunk chunk -> pure chunk
    Over -> failTest "the field ended"

-- | The chunks until the end of the field, copied, with the wait that the pulls are given.
chunksWith :: WaitingSource source => (IO Watch -> IO ()) -> source -> IO [BS.ByteString]
chunksWith wait source = go []
  where
    go chunks =
      stepWith wait source >>= \case
        Chunk chunk -> go (chunk : chunks)
        Over -> pure (reverse chunks)

-- | The chunks until the end of the field, which the pulls have to give without a wait.
chunks :: WaitingSource source => source -> IO [BS.ByteString]
chunks = chunksWith noWait

-- | The sizes of the chunks that pulls give, until the end of the field.
pulledSizes :: WaitingSource source => source -> IO [Int]
pulledSizes source = map BS.length <$> chunks source

-- | The wait of an answer that arrives in pieces: it feeds the next piece to the
-- link, and waits for the watch of the pull, which has fired by then.
feeding :: FeedLink -> IORef [BS.ByteString] -> IO Watch -> IO ()
feeding link pieces register = do
  readIORef pieces >>= \case
    [] -> pure ()
    piece : rest -> writeIORef pieces rest *> feed link piece
  awaitWatch register

-- | The sizes of the chunks that 'Tx.nextChunk' gives, until the end of the field.
blockingSizes :: Tx.Field tx -> Tx.Transaction tx [Int]
blockingSizes field = go []
  where
    go sizes =
      Tx.nextChunk field >>= \chunk ->
        if BS.null chunk then pure (reverse sizes) else go (BS.length chunk : sizes)

-- | The sizes of the chunks that pulls give while the answer arrives in pieces
-- of this size.
piecewiseSizes :: Int -> FeedLink -> Context -> BS.ByteString -> IO [Int]
piecewiseSizes size link context body = do
  -- the row's header, and some of the field, which the first receive brings together
  let (header, rest) = BS.splitAt (fieldStart body + 500) (answer [body])
  feed link header
  pieces <- newIORef (slices size rest)
  withField context $ \field -> liftIO $ map BS.length <$> chunksWith (feeding link pieces) (Tx.fieldSource field)

-- | Where a body starts in the answer.
fieldStart :: BS.ByteString -> Int
fieldStart body = BS.length (fst (BS.breakSubstring body (answer [body])))

-- | Where a field is read from the first block of the buffers of a request
-- that has the loan that is its own held elsewhere, as Warp holds it for a
-- request body, so that the input takes the pool's spare buffer. The first pull
-- takes the whole block, which uses the buffer up, and the taker queues for
-- the spare buffer, so that the next pull finds none. The action gets the
-- source and the taker's registration, and the scope ends after it, aborted,
-- for the field is not read to its end.
starved :: BufferAccess -> FeedLink -> Context -> BS.ByteString -> (forall s. Tx.FieldSource s -> Watch -> IO a) -> IO a
starved = starvedWith (Tx.statement () bodyStatement)

-- | 'starved' for a pipeline whose first statement is the body's, and which
-- may queue more.
starvedWith :: (forall tx. Tx.Pipeline tx (Tx.Result tx Body)) -> BufferAccess -> FeedLink -> Context -> BS.ByteString -> (forall s. Tx.FieldSource s -> Watch -> IO a) -> IO a
starvedWith queued taker link context body use = do
  feed link (BS.take scarceSize (answer [body]))
  outcome <- newIORef Nothing
  ended <- try @AsyncException $ withScope $ \scope -> do
    tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Read (Exchange.Session context)
    field <- Tx.runIn tx $ do
      cursor <- Tx.pipeline queued
      Tx.nextRow cursor >>= \case
        Just (Body (Just found)) -> pure found
        _ -> liftIO (failTest "the row has no field")
    let source = Tx.fieldSource field
    step source `shouldReturn` Chunk (BS.take (scarceSize - fieldStart body) body)
    registration <- queueFor taker
    use source registration >>= writeIORef outcome . Just
    throwIO ThreadKilled :: IO ()
  ended `shouldBe` Left ThreadKilled
  readIORef outcome >>= maybe (failTest "the scope ended early") pure

spec :: Spec
spec = describe "a field as a source that is waited for in place" $ do
  it "gives the bytes that the input holds without receiving, and ends at the field's end" $
    withFeed 4096 $ \link context -> do
      let body = bodyOf 300
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        emptyBefore <- emptyReceives link
        received <- chunks (Tx.fieldSource field)
        BS.concat received `shouldBe` body
        -- Everything came from the one receive that read the row
        emptyReceives link `shouldReturn` emptyBefore

  it "waits when it holds less than a blocking read gives and nothing can be received" $
    withFeed 64 $ \link context -> do
      let body = bodyOf 300
          response = answer [body]
          (early, late) = BS.splitAt (BS.length response - 100) response
      feed link early
      withField context $ \field -> liftIO $ do
        waits <- newIORef (0 :: Int)
        let wait register = do
              -- It tried to receive, and found nothing, before it waited
              (> 0) <$> emptyReceives link `shouldReturn` True
              modifyIORef' waits (+ 1)
              feed link late
              awaitWatch register
        received <- chunksWith wait (Tx.fieldSource field)
        BS.concat received `shouldBe` body
        readIORef waits `shouldReturn` 1

  it "never waits while the link has the bytes, though the buffer is smaller than the field" $
    withFeed 64 $ \link context -> do
      let body = bodyOf 5000
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        received <- chunks (Tx.fieldSource field)
        BS.concat received `shouldBe` body

  it "pulls a field larger than the buffer to its end, exactly, as the bytes arrive" $
    withFeed 1024 $ \link context -> do
      let body = bodyOf 100000
          response = answer [body]
      _ <- forkIO $ mapM_ (\piece -> feed link piece *> threadDelay 50) (slices 700 response)
      withField context $ \field -> liftIO $ do
        received <- timeout 5_000_000 (chunksWith awaitWatch (Tx.fieldSource field))
        BS.concat <$> received `shouldBe` Just body

  it "gives a chunk that lasts until the next pull" $
    withFeed 128 $ \link context -> do
      let body = bodyOf 1000
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
        pullWaiting source noWait >>= \case
          Just first -> do
            first `shouldBe` BS.take (BS.length first) body
            let taken = BS.length first
            pullWaiting source noWait >>= \case
              Just second -> second `shouldBe` BS.take (BS.length second) (BS.drop taken body)
              Nothing -> failTest "the second pull gave no chunk"
          Nothing -> failTest "the first pull gave no chunk"

  it "gives the watch of what it waits for, which fires when the server sends, and is released" $
    withFeed 64 $ \link context -> do
      let response = answer [bodyOf 300]
          (early, late) = BS.splitAt (BS.length response - 100) response
      feed link early
      withField context $ \field -> liftIO $ do
        let wait register = do
              watch <- register
              timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
              feed link late
              timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
              watchCancel watch
        received <- chunksWith wait (Tx.fieldSource field)
        BS.concat received `shouldBe` bodyOf 300

  it "expires when its row advances" $
    withFeed 4096 $ \link context -> do
      feed link (answer [bodyOf 100, bodyOf 50])
      outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        cursor <- Tx.pipeline (Tx.statement () bodyStatement)
        Tx.nextRow cursor >>= \case
          Just (Body (Just field)) -> do
            _ <- Tx.nextRow cursor
            failure <- liftIO (try @ConnectionError (pullWaiting (Tx.fieldSource field) noWait))
            Tx.finishTransaction
            pure failure
          _ -> liftIO (failTest "the row has no field")
      case outcome of
        Left (RowMisuse _) -> pure ()
        Left other -> failTest ("unexpected failure: " <> show other)
        Right _ -> failTest "a field of a row that advanced was pulled"

  it "rejects a pull from another thread" $
    withFeed 4096 $ \link context -> do
      feed link (answer [bodyOf 100])
      withField context $ \field -> liftIO $ do
        pulled <- newEmptyMVar
        _ <- forkIO (try @ConnectionError (void (pullWaiting (Tx.fieldSource field) noWait)) >>= putMVar pulled)
        takeMVar pulled >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("another thread pulled: " <> show (either show (const "ok") other))

  describe "chunks" $ do
    it "are as large as a blocking read's, though the link delivers 8 KiB with each receive" $ do
      let body = bodyOf 60000
      pulled <- withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        feed link (answer [body])
        withField context $ \field -> liftIO (pulledSizes (Tx.fieldSource field))
      blocking <- withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        feed link (answer [body])
        withField context blockingSizes
      -- the buffer's room, from where the field starts, and then whole buffers
      pulled `shouldBe` [16384 - fieldStart body, 16384, 16384, 60000 - 16384 * 3 + fieldStart body]
      pulled `shouldBe` blocking

    it "are not given before the input holds as much as a blocking read would give, and the bytes are kept" $
      withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        let body = bodyOf 60000
            (early, late) = BS.splitAt 8192 (answer [body])
        feed link early
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
              full = 16384 - fieldStart body
              wait _ = do
                -- 8 KiB is all that the link has, and it is only half of a chunk
                (> 0) <$> emptyReceives link `shouldReturn` True
                feed link (BS.take 8192 late)
          stepWith wait source `shouldReturn` Chunk (BS.take full body)
          -- what the transaction's end drains
          feed link (BS.drop 8192 late)

    it "end with the field's tail, when all of it has arrived, and not before" $
      withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        let body = bodyOf 20000
            response = answer [body]
            full = 16384 - fieldStart body
            (early, late) = BS.splitAt (fieldStart body + full + 1000) response
        feed link early
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          step source `shouldReturn` Chunk (BS.take full body)
          -- a thousand bytes of the 3,658 that the field has left
          stepWith (\_ -> feed link late) source `shouldReturn` Chunk (BS.drop full body)
          step source `shouldReturn` Over

  describe "without a buffer to receive into" $ do
    it "waits for a buffer, and its watch fires when one is returned" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
          starved taker link context body $ \source registration -> do
            let wait register = do
                  watch <- register
                  timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                  watchCancel registration
                  timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                  watchCancel watch
                  feed link (BS.drop scarceSize response)
            stepWith wait source `shouldReturn` Chunk (BS.take scarceSize (BS.drop (scarceSize - fieldStart body) body))

    it "keeps its place in the queue and its buffer when its wait releases the watch after it fired" $
      withScarceRequests $ \reader taker rival -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
          starved taker link context body $ \source registration -> do
            let wait register = do
                  feed link (BS.drop scarceSize response)
                  -- the wait registers the watch, and releases it once it has fired
                  watch <- register
                  rivalRegistration <- queueFor rival
                  watchCancel registration
                  awaitWatch (pure watch)
                  -- a release that returned the buffer would have given it to the rival
                  timeout 50_000 (atomically (watchReady rivalRegistration)) `shouldReturn` Nothing
            stepWith wait source `shouldReturn` Chunk (BS.take scarceSize (BS.drop (scarceSize - fieldStart body) body))

    it "withdraws its registration for a buffer when its scope ends" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        registration <- withRequestFeed reader $ \link context ->
          starved taker link context (bodyOf 10000) $ \source registration -> do
            -- the wait registers for a buffer, and the pull is interrupted
            _ <- try @AsyncException (stepWith (\register -> void register *> throwIO ThreadKilled) source)
            pure registration
        -- a registration that was left would be a second loan, and fail this try
        isNothing <$> tryBorrowBuffer reader `shouldReturn` True
        watchCancel registration
        isJust <$> tryBorrowBuffer reader `shouldReturn` True

    it "gives back the buffer that was granted to its registration, when its scope ends without a pull taking it" $
      withScarceRequests $ \reader taker rival -> do
        _ <- borrowBuffer reader
        rivalRegistration <- withRequestFeed reader $ \link context ->
          starved taker link context (bodyOf 10000) $ \source registration -> do
            rivalRegistrations <- newIORef Nothing
            let wait register = do
                  watch <- register
                  rivalRegistration <- queueFor rival
                  writeIORef rivalRegistrations (Just rivalRegistration)
                  watchCancel registration
                  timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                  throwIO ThreadKilled
            _ <- try @AsyncException (stepWith wait source)
            readIORef rivalRegistrations >>= maybe (failTest "the pull did not wait") pure
        timeout 2_000_000 (atomically (watchReady rivalRegistration)) `shouldReturn` Just ()

    it "holds the request's primary loan across its waits, and the chunks" $
      withScarceRequests $ \reader taker _ -> do
        -- the pool's spare is taken, so that only the primary can serve another use of the request's buffers
        _ <- borrowBuffer taker
        _ <- borrowBuffer taker
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
          feed link (BS.take scarceSize response)
          ended <- try @AsyncException $ withScope $ \scope -> do
            tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Read (Exchange.Session context)
            field <- Tx.runIn tx $ do
              cursor <- Tx.pipeline (Tx.statement () bodyStatement)
              Tx.nextRow cursor >>= \case
                Just (Body (Just found)) -> pure found
                _ -> liftIO (failTest "the row has no field")
            let source = Tx.fieldSource field
                first = scarceSize - fieldStart body
            -- the first pull takes the whole block, and leaves bytes of the field
            step source `shouldReturn` Chunk (BS.take first body)
            borrowed <- newEmptyMVar
            _ <- forkIO (try @IOError (borrowBuffer reader) >>= putMVar borrowed)
            threadDelay 20_000
            -- the consumed buffer is not given back, and the pull waits with it
            let wait register = do
                  isNothing <$> timeout 50_000 (readMVar borrowed) `shouldReturn` True
                  feed link (BS.take scarceSize (BS.drop scarceSize response))
                  awaitWatch register
            pullWaiting source wait >>= \case
              Just chunk -> chunk `shouldBe` BS.take (BS.length chunk) (BS.drop first body)
              Nothing -> failTest "the field ended"
            isNothing <$> timeout 50_000 (readMVar borrowed) `shouldReturn` True
            throwIO ThreadKilled :: IO ()
          ended `shouldBe` Left ThreadKilled

    it "gives an additional loan back before it waits, and a queued request gets it" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
          feed link (BS.take scarceSize response)
          ended <- try @AsyncException $ withScope $ \scope -> do
            tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Read (Exchange.Session context)
            field <- Tx.runIn tx $ do
              cursor <- Tx.pipeline (Tx.statement () bodyStatement)
              Tx.nextRow cursor >>= \case
                Just (Body (Just found)) -> pure found
                _ -> liftIO (failTest "the row has no field")
            let source = Tx.fieldSource field
                first = scarceSize - fieldStart body
            -- the first pull takes the whole block, and leaves bytes of the field
            step source `shouldReturn` Chunk (BS.take first body)
            queued <- queueFor taker
            -- the loan goes back, the request that waits for it gets it, and the pull waits for a buffer until it is given back
            let wait register = do
                  timeout 2_000_000 (atomically (watchReady queued)) `shouldReturn` Just ()
                  watchCancel queued
                  feed link (BS.take scarceSize (BS.drop scarceSize response))
                  awaitWatch register
            pullWaiting source wait >>= \case
              Just chunk -> chunk `shouldBe` BS.take (BS.length chunk) (BS.drop first body)
              Nothing -> failTest "the field ended"
            throwIO ThreadKilled :: IO ()
          ended `shouldBe` Left ThreadKilled

  describe "with output that is unsent" $ do
    it "waits for what the output waits for as well while no buffer is free, so that it keeps moving" $ do
      flag <- newTVarIO False
      let uploading =
            Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just (ParameterBorrowedStream 4 (UploadSource flag)))]) Tx.noResult
          queued = fst <$> ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () uploading)
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        -- the upload is never finished, so the request ends with output unsent
        outcome <- try @ConnectionError $ withRequestFeed reader $ \link context ->
          starvedWith queued taker link context (bodyOf 10000) $ \source _ -> do
            let wait register = do
                  watch <- register
                  -- no buffer is free, and the upload has nothing ready
                  timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                  atomically (writeTVar flag True)
                  timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                  watchCancel watch
                  throwIO ThreadKilled
            void (try @AsyncException (stepWith wait source))
        case outcome of
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("the request ended differently: " <> either show (const "no failure") other)

    it "waits for the upload's source as well, so that its readiness is noticed" $ do
      ready' <- newTVarIO False
      let upload = UploadSource ready'
          uploading =
            Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just (ParameterBorrowedStream 4 upload))]) Tx.noResult
      -- The upload is never finished, so the scope can only end by a failure
      result <- try @IOException $ withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
        feed link (BS.take (BS.length response - 100) response)
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          (cursor, _) <- Tx.pipeline ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () uploading)
          Tx.nextRow cursor >>= \case
            Just (Body (Just field)) -> liftIO $ do
              let wait register = do
                    watch <- register
                    -- PostgreSQL has nothing, and the link takes what it is offered,
                    -- so it is the upload that the pull waits for
                    timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                    atomically (writeTVar ready' True)
                    timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                    watchCancel watch
                    ioError (userError "done")
              void (chunksWith wait (Tx.fieldSource field))
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True

    it "waits for the link to be writable while it takes nothing" $ do
      flag <- newTVarIO False
      let uploading =
            Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just (ParameterBorrowedStream 4 (UploadSource flag)))]) Tx.noResult
      result <- try @IOException $ withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
        feed link (BS.take (BS.length response - 100) response)
        setWritable link False
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          (cursor, _) <- Tx.pipeline ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () uploading)
          Tx.nextRow cursor >>= \case
            Just (Body (Just field)) -> liftIO $ do
              let wait register = do
                    watch <- register
                    timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
                    setWritable link True
                    timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
                    watchCancel watch
                    ioError (userError "done")
              void (chunksWith wait (Tx.fieldSource field))
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True

    it "keeps sending unsent output while it waits, and pulls the field to its end" $ do
      flag <- newTVarIO False
      let uploading =
            Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just (ParameterBorrowedStream 4 (UploadSource flag)))]) Tx.noResult
          body = bodyOf 300
          response = answer [body]
          (early, late) = BS.splitAt (BS.length response - 100) response
      -- The reply to the upload is never fed, so the scope can only end by a failure
      result <- try @IOException $ withFeed 64 $ \link context -> do
        feed link early
        setWritable link False
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          (cursor, _) <- Tx.pipeline ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () uploading)
          Tx.nextRow cursor >>= \case
            Just (Body (Just field)) -> liftIO $ do
              waits <- newIORef (0 :: Int)
              sentBefore <- newIORef 0
              let wait register = do
                    n <- readIORef waits
                    writeIORef waits (n + 1)
                    case n of
                      0 -> do
                        writeIORef sentBefore . BS.length =<< sentBytes link
                        setWritable link True
                      1 -> do
                        -- the pull sent what the link takes now, up to the upload
                        before <- readIORef sentBefore
                        after <- BS.length <$> sentBytes link
                        (after > before) `shouldBe` True
                        feed link late
                      _ -> failTest "the pull waited too often"
                    awaitWatch register
              received <- chunksWith wait (Tx.fieldSource field)
              BS.concat received `shouldBe` body
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True

  describe "with blocking reads of the field between its pulls" $ do
    it "keeps the bytes in order" $
      withFeed 1024 $ \link context -> do
        let body = bodyOf 5000
        feed link (answer [body])
        withField context $ \field -> do
          let source = Tx.fieldSource field
          first <- liftIO (stepChunk source)
          chunk <- Tx.nextChunk field
          owned <- liftIO (evaluate (BS.copy chunk))
          rest <- liftIO (chunks source)
          liftIO (BS.concat (first : owned : rest) `shouldBe` body)

    it "ends at the field's end when a blocking read took the rest of the field" $
      withFeed 1024 $ \link context -> do
        let body = bodyOf 5000
        feed link (answer [body])
        withField context $ \field -> do
          let source = Tx.fieldSource field
              drainField chunks' =
                Tx.nextChunk field >>= \chunk ->
                  if BS.null chunk then pure (reverse chunks') else liftIO (evaluate (BS.copy chunk)) >>= \owned -> drainField (owned : chunks')
          first <- liftIO (stepChunk source)
          rest <- drainField []
          liftIO $ do
            step source `shouldReturn` Over
            BS.concat (first : rest) `shouldBe` body

  describe "after its row advanced, or its scope ended" $ do
    it "rejects a pull of a field whose row advanced, though the field of a later row has chunks" $
      withFeed 1024 $ \link context -> do
        let rowOne = bodyOf 3000
            rowTwo = "second:" <> bodyOf 2900
        feed link (answer [rowOne, rowTwo])
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just earlier)) -> do
              liftIO (step (Tx.fieldSource earlier) >>= \pulled -> (pulled /= Over) `shouldBe` True)
              Tx.nextRow cursor >>= \case
                Just (Body (Just later)) -> do
                  liftIO (step (Tx.fieldSource later) >>= \pulled -> (pulled /= Over) `shouldBe` True)
                  stale <- liftIO (try @ConnectionError (pullWaiting (Tx.fieldSource earlier) noWait))
                  Tx.finishTransaction
                  pure stale
                _ -> liftIO (failTest "the second row has no field")
            _ -> liftIO (failTest "the first row has no field")
        case outcome of
          Left (RowMisuse _) -> pure ()
          Left other -> failTest ("unexpected failure: " <> show other)
          Right _ -> failTest "a field of a row that advanced was pulled"

    it "rejects a pull from another thread, and the owner pulls on" $
      withFeed 64 $ \link context -> do
        let body = bodyOf 300
        feed link (answer [body])
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          first <- stepChunk source
          pulled <- newEmptyMVar
          _ <- forkIO (try @ConnectionError (void (pullWaiting source noWait)) >>= putMVar pulled)
          takeMVar pulled >>= \case
            Left (SessionUnusable _) -> pure ()
            other -> failTest ("another thread pulled: " <> either show (const "a result") other)
          rest <- chunks source
          BS.concat (first : rest) `shouldBe` body

    it "rejects a pull after the scope ended" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
        feed link response
        kept <- newIORef (pure Nothing)
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          _ <- step source
          writeIORef kept (pullWaiting source noWait)
        late' <- readIORef kept
        try @ConnectionError late' >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("a pull after the scope ended: " <> either show (const "gave a result") other)

    it "rejects a pull after the scope of its transaction ended" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
        feed link response
        kept <- newIORef (pure Nothing)
        withScope $ \scope -> do
          tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Read (Exchange.Session context)
          field <- Tx.runIn tx $ do
            cursor <- Tx.pipeline (Tx.statement () bodyStatement)
            Tx.nextRow cursor >>= \case
              Just (Body (Just found)) -> pure found
              _ -> liftIO (failTest "the row has no field")
          let source = Tx.fieldSource field
          _ <- step source
          writeIORef kept (pullWaiting source noWait)
          -- the scope completes, which drains the field and commits
        readIORef kept >>= try @ConnectionError >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("a pull after the scope ended: " <> either show (const "gave a result") other)

    it "rejects a pull of a field whose row advanced, though nothing else has been pulled since" $
      withFeed 1024 $ \link context -> do
        let rowOne = bodyOf 3000
            rowTwo = "second:" <> bodyOf 2900
        feed link (answer [rowOne, rowTwo])
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just earlier)) -> do
              liftIO (step (Tx.fieldSource earlier) >>= \pulled -> (pulled /= Over) `shouldBe` True)
              _ <- Tx.nextRow cursor
              stale <- liftIO (try @ConnectionError (pullWaiting (Tx.fieldSource earlier) noWait))
              Tx.finishTransaction
              pure stale
            _ -> liftIO (failTest "the first row has no field")
        case outcome of
          Left (RowMisuse _) -> pure ()
          Left other -> failTest ("unexpected failure: " <> show other)
          Right _ -> failTest "a field of a row that advanced was pulled"

  describe "when the wait, or the server's connection, fails" $ do
    it "fails, and breaks the connection, when the server's connection is lost" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
            early = BS.take (BS.length response - 100) response
        feed link early
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just field)) -> liftIO $ do
              let source = Tx.fieldSource field
              lost <- try @ConnectionError (chunksWith (\_ -> endFeed link) source)
              again <- try @ConnectionError (pullWaiting source noWait)
              pure (lost, again)
            _ -> liftIO (failTest "the row has no field")
        case outcome of
          (Left (ProtocolError _), Left (SessionUnusable _)) -> pure ()
          (lost, again) -> failTest ("the pulls gave " <> either show (const "a result") lost <> " and " <> either show (const "a result") again)
        state <- readIORef (connectionState (contextConnection context))
        case state of
          Broken -> pure ()
          _ -> failTest "the connection was not broken"

    it "fails, and breaks the connection, when the wait fails" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
            early = BS.take (BS.length response - 100) response
        feed link early
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just field)) -> liftIO $ do
              let source = Tx.fieldSource field
              gone <- try @IOException (chunksWith (\_ -> throwIO (userError "the client went away")) source)
              again <- try @ConnectionError (pullWaiting source noWait)
              pure (gone, again)
            _ -> liftIO (failTest "the row has no field")
        case outcome of
          (Left _, Left (SessionUnusable _)) -> pure ()
          (gone, again) -> failTest ("the pulls gave " <> either show (const "a result") gone <> " and " <> either show (const "a result") again)
        state <- readIORef (connectionState (contextConnection context))
        case state of
          Broken -> pure ()
          _ -> failTest "the connection was not broken"

  describe "storage that allocates" $ do
    it "keeps the owned columns of its row, though the field is pulled on in storage that is replaced as it grows" $
      withOwnedFeed 1024 $ \link context -> do
        let body = bodyOf 20000
        feed link (command "BEGIN" <> rows [[Just "tag-one", Just body]] <> readyTx <> command "COMMIT" <> ready)
        Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () taggedStatement)
          Tx.nextRow cursor >>= \case
            Just (Tagged tag (Just field)) -> do
              liftIO $ do
                received <- chunks (Tx.fieldSource field)
                BS.concat received `shouldBe` body
                tag `shouldBe` "tag-one"
              Tx.finishTransaction
            _ -> liftIO (failTest "the row has no field")

  describe "chunks that arrive in pieces" $ do
    it "are as large as a blocking read's" $ do
      let body = bodyOf 60000
      pulled <- withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        piecewiseSizes 3000 link context body
      blocking <- withFeed 16384 $ \link context -> do
        setReceiveLimit link 8192
        feed link (answer [body])
        withField context blockingSizes
      pulled `shouldBe` blocking

    it "are as large as a blocking read's, in the buffers of a request" $ do
      let body = bodyOf 20000
      pulled <- withScarceRequests $ \_ taker _ -> withRequestFeed taker $ \link context -> do
        setReceiveLimit link 1500
        piecewiseSizes 1000 link context body
      blocking <- withScarceRequests $ \_ taker _ -> withRequestFeed taker $ \link context -> do
        setReceiveLimit link 1500
        feed link (answer [body])
        withField context blockingSizes
      pulled `shouldBe` blocking

    it "pull on to the field's end exactly once a buffer was granted, as the bytes arrive" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
              taken = BS.take (scarceSize - fieldStart body) body
          starved taker link context body $ \source registration -> do
            -- the spare buffer goes back to the pool, and the pull takes it
            pieces <- newIORef (slices 1000 (BS.drop scarceSize response))
            released <- newIORef False
            let wait register = do
                  readIORef released >>= \case
                    False -> watchCancel registration *> writeIORef released True
                    True -> pure ()
                  feeding link pieces register
            rest <- chunksWith wait source
            BS.concat (taken : rest) `shouldBe` body
  where
    isLeft' = either (const True) (const False)

-- | A source that has nothing ready until its flag is set, and then four bytes.
newtype UploadSource = UploadSource (TVar Bool)

instance Source UploadSource where
  pullReady (UploadSource flag) = readTVarIO flag >>= \open -> pure (if open then Pulled "abcd" else Pending)
  watchSource (UploadSource flag) = pure (Watch (readTVar flag >>= check) (pure ()))

slices :: Int -> BS.ByteString -> [BS.ByteString]
slices size bytes
  | BS.null bytes = []
  | otherwise = let (piece, rest) = BS.splitAt size bytes in piece : slices size rest
