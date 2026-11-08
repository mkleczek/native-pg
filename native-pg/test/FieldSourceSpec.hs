{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Fields as pull sources, over a link that the spec feeds.
module FieldSourceSpec (spec) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (TVar, atomically, check, newTVarIO, readTVar, readTVarIO, writeTVar)
import Control.Exception (AsyncException (ThreadKilled), IOException, evaluate, throwIO, try)
import Control.Monad (forM, forM_, replicateM_, void, when)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import Data.Int (Int64)
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (isJust, isNothing)
import Database.PostgreSQL.Diagnostics (ConnectionError (..))
import Database.PostgreSQL.Internal.Channel (Channel (..), newChannel)
import Database.PostgreSQL.Internal.Context (Connection (..), Context, SessionState (..), contextConnection)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Streaming.Types (Parameter (..))
import qualified Database.PostgreSQL.Transaction as Tx
import FeedSupport
import Foreign.Marshal.Alloc (allocaBytes)
import GHC.Conc (getAllocationCounter)
import Network.Streaming.BufferPool (BufferAccess, borrowBuffer, tryBorrowBuffer)
import Network.Streaming.Scope (withScope)
import Network.Streaming.Source (Pulled (..), Source (..), pull)
import Network.Streaming.Watch (Watch (..), awaitWatch)
import Skeletest hiding (context)
import System.Timeout (timeout)

newtype Body tx = Body (Maybe (Tx.Field tx))

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

-- | Pulls until the source is pending or ended, copying each chunk, and says
-- which.
pullAvailable :: Source source => source -> IO ([BS.ByteString], Bool)
pullAvailable source = go []
  where
    go chunks =
      pullReady source >>= \case
        Pulled chunk -> evaluate (BS.copy chunk) >>= \copied -> go (copied : chunks)
        Pending -> pure (reverse chunks, False)
        Ended -> pure (reverse chunks, True)

-- | What a pull found, with the chunk copied.
data Step = Chunk BS.ByteString | Waits | Over
  deriving (Eq)

instance Show Step where
  show (Chunk chunk) = "Chunk of " <> show (BS.length chunk) <> " bytes"
  show Waits = "Waits"
  show Over = "Over"

-- | One pull, which has to return at once, as a pull never waits.
step :: Source source => source -> IO Step
step source =
  timeout 1_000_000 (pullReady source) >>= \case
    Nothing -> failTest "the pull waited"
    Just (Pulled chunk) -> Chunk <$> evaluate (BS.copy chunk)
    Just Pending -> pure Waits
    Just Ended -> pure Over

-- | The sizes of the chunks that pulls give, until the end of the field.
pulledSizes :: Source source => source -> IO [Int]
pulledSizes source = go []
  where
    go sizes =
      step source >>= \case
        Chunk chunk -> go (BS.length chunk : sizes)
        Waits -> failTest "the pull was pending, though the link has the bytes"
        Over -> pure (reverse sizes)

-- | The sizes of the chunks that 'Tx.nextChunk' gives, until the end of the field.
blockingSizes :: Tx.Field tx -> Tx.Transaction tx [Int]
blockingSizes field = go []
  where
    go sizes =
      Tx.nextChunk field >>= \chunk ->
        if BS.null chunk then pure (reverse sizes) else go (BS.length chunk : sizes)

-- | The sizes of the chunks that pulls give while the answer arrives in pieces
-- of this size, with the pulls of a wake-up after each piece.
piecewiseSizes :: Int -> FeedLink -> Context -> BS.ByteString -> IO [Int]
piecewiseSizes size link context body = do
  -- the row's header, and some of the field, which the first receive brings together
  let (header, rest) = BS.splitAt (fieldStart body + 500) (answer [body])
  feed link header
  withField context $ \field -> liftIO $ do
    let source = Tx.fieldSource field
    sizes <- newIORef []
    forM_ (slices size rest) $ \piece -> do
      feed link piece
      (chunks, _) <- pullAvailable source
      modifyIORef' sizes (<> map BS.length chunks)
    (chunks, ended) <- pullAvailable source
    ended `shouldBe` True
    (<> map BS.length chunks) <$> readIORef sizes

-- | The bytes that the action allocated on this thread, on average over this
-- many runs.
allocatedBy :: Int -> IO () -> IO Int64
allocatedBy runs action = do
  before <- getAllocationCounter
  loop runs
  after <- getAllocationCounter
  pure ((before - after) `div` fromIntegral runs)
  where
    loop :: Int -> IO ()
    loop 0 = pure ()
    loop n = action *> loop (n - 1)

-- | The bytes that the action allocated on this thread.
allocatedOnce :: IO a -> IO Int64
allocatedOnce action = do
  before <- getAllocationCounter
  _ <- action
  after <- getAllocationCounter
  pure (before - after)

-- | What a receive on a link of its own allocates, when the link has nothing.
rawEmptyReceive :: IO Int64
rawEmptyReceive = do
  link <- newFeedLink
  let channel = newChannel link
  allocaBytes 128 $ \pointer -> allocatedBy 1000 (void (channelReceiveReady channel pointer 128))

-- | What a receive on a link of its own allocates, when it has this many bytes.
rawWakeup :: Int -> IO Int64
rawWakeup size = do
  link <- newFeedLink
  let channel = newChannel link
  allocaBytes size $ \pointer -> do
    costs <- forM [1 .. 20 :: Int] $ \_ -> do
      feed link (BS.replicate size 65)
      allocatedOnce (channelReceiveReady channel pointer size)
    pure (sum costs `div` 20)

-- | What a pull may allocate beyond its receives: the closures of the handler
-- it installs, the values that the receive's interface boxes and gives back,
-- and the buffer's new bounds when bytes arrived. About a tenth of what the
-- layers above the receive allocate when each of them runs again.
extraWhenNothing, extraWhenPartial :: Int64
extraWhenNothing = 256
extraWhenPartial = 256

checkExtra :: String -> Int64 -> Int64 -> IO ()
checkExtra label extra bound =
  when (extra > bound) $
    failTest (label <> ": a pull allocated " <> show extra <> " bytes more than its receives, which may be " <> show bound)

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
spec = describe "a field as a pull source" $ do
  it "gives the bytes that the input holds without receiving, and ends at the field's end" $
    withFeed 4096 $ \link context -> do
      let body = bodyOf 300
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        emptyBefore <- emptyReceives link
        (chunks, ended) <- pullAvailable (Tx.fieldSource field)
        BS.concat chunks `shouldBe` body
        ended `shouldBe` True
        -- Everything came from the one receive that read the row
        emptyReceives link `shouldReturn` emptyBefore

  it "is pending when it holds less than a blocking read gives and nothing can be received" $
    withFeed 64 $ \link context -> do
      let body = bodyOf 300
          response = answer [body]
          (early, late) = BS.splitAt (BS.length response - 100) response
      feed link early
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
        (first, endedEarly) <- pullAvailable source
        endedEarly `shouldBe` False
        -- It tried to receive, and found nothing, before it was pending
        (> 0) <$> emptyReceives link `shouldReturn` True
        feed link late
        (rest, ended) <- pullAvailable source
        ended `shouldBe` True
        BS.concat (first <> rest) `shouldBe` body

  it "is never pending while the link has the bytes, though the buffer is smaller than the field" $
    withFeed 64 $ \link context -> do
      let body = bodyOf 5000
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        pendings <- newIORef (0 :: Int)
        let go chunks = pullReady (Tx.fieldSource field) >>= \case
              Pulled chunk -> evaluate (BS.copy chunk) >>= \copied -> go (copied : chunks)
              Pending -> do
                modifyIORef' pendings (+ 1)
                count <- readIORef pendings
                if count > 100 then failTest "the pull stayed pending" else go chunks
              Ended -> pure (BS.concat (reverse chunks))
        go [] `shouldReturn` body
        readIORef pendings `shouldReturn` 0

  it "pulls a field larger than the buffer to its end, exactly, as the bytes arrive" $
    withFeed 1024 $ \link context -> do
      let body = bodyOf 100000
          response = answer [body]
      _ <- forkIO $ mapM_ (\piece -> feed link piece *> threadDelay 50) (slices 700 response)
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
            go chunks = pull source >>= \case
              Just chunk -> evaluate (BS.copy chunk) >>= \copied -> go (copied : chunks)
              Nothing -> pure (BS.concat (reverse chunks))
        timeout 5_000_000 (go []) `shouldReturn` Just body

  it "gives a chunk that lasts until the next pull" $
    withFeed 128 $ \link context -> do
      let body = bodyOf 1000
      feed link (answer [body])
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
        pullReady source >>= \case
          Pulled first -> do
            first `shouldBe` BS.take (BS.length first) body
            let taken = BS.length first
            pullReady source >>= \case
              Pulled second -> second `shouldBe` BS.take (BS.length second) (BS.drop taken body)
              _ -> failTest "the second pull gave no chunk"
          _ -> failTest "the first pull gave no chunk"

  it "fires the watch of a pending pull when the server sends, and releases it" $
    withFeed 64 $ \link context -> do
      let response = answer [bodyOf 300]
          (early, late) = BS.splitAt (BS.length response - 100) response
      feed link early
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
        _ <- pullAvailable source
        watch <- watchSource source
        timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
        feed link late
        timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
        watchCancel watch

  it "expires when its row advances" $
    withFeed 4096 $ \link context -> do
      feed link (answer [bodyOf 100, bodyOf 50])
      outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
        cursor <- Tx.pipeline (Tx.statement () bodyStatement)
        Tx.nextRow cursor >>= \case
          Just (Body (Just field)) -> do
            _ <- Tx.nextRow cursor
            failure <- liftIO (try @ConnectionError (pullReady (Tx.fieldSource field)))
            Tx.finishTransaction
            pure failure
          _ -> liftIO (failTest "the row has no field")
      case outcome of
        Left (RowMisuse _) -> pure ()
        Left other -> failTest ("unexpected failure: " <> show other)
        Right _ -> failTest "a field of a row that advanced was pulled"

  it "rejects a pull and a watch from another thread" $
    withFeed 4096 $ \link context -> do
      feed link (answer [bodyOf 100])
      withField context $ \field -> liftIO $ do
        let source = Tx.fieldSource field
        pulled <- newEmptyMVar
        watched <- newEmptyMVar
        _ <- forkIO (try @ConnectionError (void (pullReady source)) >>= putMVar pulled)
        _ <- forkIO (try @ConnectionError (void (watchSource source)) >>= putMVar watched)
        takeMVar pulled >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("another thread pulled: " <> show (either show (const "ok") other))
        takeMVar watched >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("another thread watched: " <> show (either show (const "ok") other))

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
          -- 8 KiB is all that the link has, and it is only half of a chunk
          emptyBefore <- emptyReceives link
          step source `shouldReturn` Waits
          (> emptyBefore) <$> emptyReceives link `shouldReturn` True
          feed link (BS.take 8192 late)
          step source `shouldReturn` Chunk (BS.take full body)
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
          step source `shouldReturn` Waits
          feed link late
          step source `shouldReturn` Chunk (BS.drop full body)
          step source `shouldReturn` Over

  describe "without a buffer to receive into" $ do
    it "is pending without waiting, and its watch fires when a buffer is returned" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
          starved taker link context body $ \source registration -> do
            step source `shouldReturn` Waits
            watch <- watchSource source
            timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
            -- a pull while the registration is pending doesn't try, which would fail
            step source `shouldReturn` Waits
            watchCancel registration
            timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
            watchCancel watch

    it "keeps its place in the queue and its buffer when a wait releases the watch after it fired" $
      withScarceRequests $ \reader taker rival -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
          starved taker link context body $ \source registration -> do
            step source `shouldReturn` Waits
            feed link (BS.drop scarceSize response)
            -- the wait registers the watch, and releases it once it has fired
            watch <- watchSource source
            rivalRegistration <- queueFor rival
            watchCancel registration
            awaitWatch (pure watch)
            -- a release that returned the buffer would have given it to the rival
            timeout 50_000 (atomically (watchReady rivalRegistration)) `shouldReturn` Nothing
            step source `shouldReturn` Chunk (BS.take scarceSize (BS.drop (scarceSize - fieldStart body) body))

    it "withdraws its registration for a buffer when its scope ends" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        registration <- withRequestFeed reader $ \link context ->
          starved taker link context (bodyOf 10000) $ \source registration -> do
            step source `shouldReturn` Waits
            _ <- watchSource source
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
            step source `shouldReturn` Waits
            watch <- watchSource source
            rivalRegistration <- queueFor rival
            watchCancel registration
            timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
            pure rivalRegistration
        timeout 2_000_000 (atomically (watchReady rivalRegistration)) `shouldReturn` Just ()

  describe "with output that is unsent" $ do
    it "watches what the output waits for as well while no buffer is free, so that it keeps moving" $ do
      flag <- newTVarIO False
      let uploading =
            Tx.Statement "SELECT $1" (const [Tx.Param Nothing Tx.BinaryFormat (Just (ParameterBorrowedStream 4 (UploadSource flag)))]) Tx.noResult
          queued = fst <$> ((,) <$> Tx.statement () bodyStatement <*> Tx.statement () uploading)
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        -- the upload is never finished, so the request ends with output unsent
        outcome <- try @ConnectionError $ withRequestFeed reader $ \link context ->
          starvedWith queued taker link context (bodyOf 10000) $ \source _ -> do
            step source `shouldReturn` Waits
            watch <- watchSource source
            -- no buffer is free, and the upload has nothing ready
            timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
            atomically (writeTVar flag True)
            timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
            watchCancel watch
        case outcome of
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("the request ended differently: " <> either show (const "no failure") other)

    it "watches the upload's source as well, so that its readiness is noticed" $ do
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
              let source = Tx.fieldSource field
              _ <- pullAvailable source
              watch <- watchSource source
              -- PostgreSQL has nothing, and the link takes what it is offered,
              -- so it is the upload that the pull waits for
              timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
              atomically (writeTVar ready' True)
              timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
              watchCancel watch
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True

    it "watches for the link to be writable while it takes nothing" $ do
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
              let source = Tx.fieldSource field
              _ <- pullAvailable source
              watch <- watchSource source
              timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
              setWritable link True
              timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
              watchCancel watch
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True
  describe "after a pull that was pending" $ do
    it "keeps the bytes in order when a blocking read of the field comes between pulls" $
      withFeed 1024 $ \link context -> do
        let body = bodyOf 5000
            (early, late) = BS.splitAt (fieldStart body + 2500) (answer [body])
        feed link early
        withField context $ \field -> do
          let source = Tx.fieldSource field
          (first, endedEarly) <- liftIO (pullAvailable source)
          liftIO (endedEarly `shouldBe` False)
          liftIO (feed link late)
          chunk <- Tx.nextChunk field
          owned <- liftIO (evaluate (BS.copy chunk))
          (rest, ended) <- liftIO (pullAvailable source)
          liftIO $ do
            ended `shouldBe` True
            BS.concat (first <> [owned] <> rest) `shouldBe` body

    it "ends at the field's end when a blocking read took the rest of the field" $
      withFeed 1024 $ \link context -> do
        let body = bodyOf 5000
            (early, late) = BS.splitAt (fieldStart body + 2500) (answer [body])
        feed link early
        withField context $ \field -> do
          let source = Tx.fieldSource field
              drainField chunks =
                Tx.nextChunk field >>= \chunk ->
                  if BS.null chunk then pure (reverse chunks) else liftIO (evaluate (BS.copy chunk)) >>= \owned -> drainField (owned : chunks)
          (first, _) <- liftIO (pullAvailable source)
          liftIO (feed link late)
          rest <- drainField []
          liftIO $ do
            step source `shouldReturn` Over
            BS.concat (first <> rest) `shouldBe` body

    it "rejects a pull of a field whose row advanced, though the field of a later row is pending" $
      withFeed 1024 $ \link context -> do
        let rowOne = bodyOf 3000
            rowTwo = "second:" <> bodyOf 2900
            response = answer [rowOne, rowTwo]
            inOne = fieldStart rowOne + 1500
            inTwo = BS.length (fst (BS.breakSubstring rowTwo response)) + 1500
            (start, afterStart) = BS.splitAt inOne response
            (middle, end) = BS.splitAt (inTwo - inOne) afterStart
        feed link start
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just earlier)) -> do
              liftIO (pullAvailable (Tx.fieldSource earlier) >>= \(_, ended) -> ended `shouldBe` False)
              liftIO (feed link middle)
              Tx.nextRow cursor >>= \case
                Just (Body (Just later)) -> do
                  liftIO (pullAvailable (Tx.fieldSource later) >>= \(_, ended) -> ended `shouldBe` False)
                  stale <- liftIO (try @ConnectionError (pullReady (Tx.fieldSource earlier)))
                  liftIO (feed link end)
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
            (early, late) = BS.splitAt (BS.length (answer [body]) - 100) (answer [body])
        feed link early
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          (first, _) <- pullAvailable source
          pulled <- newEmptyMVar
          _ <- forkIO (try @ConnectionError (void (pullReady source)) >>= putMVar pulled)
          takeMVar pulled >>= \case
            Left (SessionUnusable _) -> pure ()
            other -> failTest ("another thread pulled: " <> either show (const "a result") other)
          feed link late
          (rest, ended) <- pullAvailable source
          ended `shouldBe` True
          BS.concat (first <> rest) `shouldBe` body

    it "rejects a pull after the scope ended" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
            (early, late) = BS.splitAt (BS.length response - 100) response
        feed link early
        kept <- newIORef (pure Ended)
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          _ <- pullAvailable source
          writeIORef kept (pullReady source)
          feed link late
        late' <- readIORef kept
        try @ConnectionError late' >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("a pull after the scope ended: " <> either show (const "gave a result") other)

    it "rejects a pull after the scope of its transaction ended" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
            (early, late) = BS.splitAt (BS.length response - 100) response
        feed link early
        kept <- newIORef (pure Ended)
        withScope $ \scope -> do
          tx <- Tx.transactionIn scope Tx.ReadCommitted Tx.Read (Exchange.Session context)
          field <- Tx.runIn tx $ do
            cursor <- Tx.pipeline (Tx.statement () bodyStatement)
            Tx.nextRow cursor >>= \case
              Just (Body (Just found)) -> pure found
              _ -> liftIO (failTest "the row has no field")
          let source = Tx.fieldSource field
          _ <- pullAvailable source
          writeIORef kept (pullReady source)
          -- the scope completes, which drains the field and commits
          feed link late
        readIORef kept >>= try @ConnectionError >>= \case
          Left (SessionUnusable _) -> pure ()
          other -> failTest ("a pull after the scope ended: " <> either show (const "gave a result") other)

    it "rejects a pull of a field whose row advanced, though nothing else has been pulled since" $
      withFeed 1024 $ \link context -> do
        let rowOne = bodyOf 3000
            rowTwo = "second:" <> bodyOf 2900
            response = answer [rowOne, rowTwo]
            inOne = fieldStart rowOne + 1500
            (start, end) = BS.splitAt inOne response
        feed link start
        outcome <- Tx.runTransaction Tx.ReadCommitted Tx.Read (Exchange.Session context) $ do
          cursor <- Tx.pipeline (Tx.statement () bodyStatement)
          Tx.nextRow cursor >>= \case
            Just (Body (Just earlier)) -> do
              liftIO (pullAvailable (Tx.fieldSource earlier) >>= \(_, ended) -> ended `shouldBe` False)
              liftIO (feed link end)
              _ <- Tx.nextRow cursor
              stale <- liftIO (try @ConnectionError (pullReady (Tx.fieldSource earlier)))
              Tx.finishTransaction
              pure stale
            _ -> liftIO (failTest "the first row has no field")
        case outcome of
          Left (RowMisuse _) -> pure ()
          Left other -> failTest ("unexpected failure: " <> show other)
          Right _ -> failTest "a field of a row that advanced was pulled"

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
              _ <- pullAvailable source
              endFeed link
              lost <- try @ConnectionError (pullReady source)
              again <- try @ConnectionError (pullReady source)
              pure (lost, again)
            _ -> liftIO (failTest "the row has no field")
        case outcome of
          (Left (ProtocolError _), Left (SessionUnusable _)) -> pure ()
          (lost, again) -> failTest ("the pulls gave " <> either show (const "a result") lost <> " and " <> either show (const "a result") again)
        state <- readIORef (connectionState (contextConnection context))
        case state of
          Broken -> pure ()
          _ -> failTest "the connection was not broken"

    it "fires the watch when the server sends, however often it was pulled in vain" $
      withFeed 64 $ \link context -> do
        let response = answer [bodyOf 300]
            (early, late) = BS.splitAt (BS.length response - 100) response
        feed link early
        withField context $ \field -> liftIO $ do
          let source = Tx.fieldSource field
          _ <- pullAvailable source
          replicateM_ 5 (step source `shouldReturn` Waits)
          watch <- watchSource source
          timeout 50_000 (atomically (watchReady watch)) `shouldReturn` Nothing
          feed link late
          timeout 2_000_000 (atomically (watchReady watch)) `shouldReturn` Just ()
          watchCancel watch

  describe "chunks that arrive in pieces, with a pull after each" $ do
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

  describe "a retry after a wait" $ do
    it "keeps sending unsent output while pulls are pending, and pulls the field to its end" $ do
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
              let source = Tx.fieldSource field
              (first, _) <- pullAvailable source
              step source `shouldReturn` Waits
              sentBefore <- BS.length <$> sentBytes link
              setWritable link True
              -- the pull sends what the link takes now, up to the upload
              step source `shouldReturn` Waits
              sentAfter <- BS.length <$> sentBytes link
              (sentAfter > sentBefore) `shouldBe` True
              feed link late
              (rest, ended) <- pullAvailable source
              ended `shouldBe` True
              BS.concat (first <> rest) `shouldBe` body
            _ -> liftIO (failTest "the row has no field")
          liftIO (ioError (userError "done"))
      isLeft' result `shouldBe` True

    it "pulls on to the field's end exactly once a buffer was granted, as the bytes arrive in pieces" $
      withScarceRequests $ \reader taker _ -> do
        _ <- borrowBuffer reader
        withRequestFeed reader $ \link context -> do
          let body = bodyOf 10000
              response = answer [body]
              taken = BS.take (scarceSize - fieldStart body) body
          starved taker link context body $ \source registration -> do
            step source `shouldReturn` Waits
            -- the spare buffer goes back to the pool, and the next pull takes it
            watchCancel registration
            collected <- newIORef []
            forM_ (slices 1000 (BS.drop scarceSize response)) $ \piece -> do
              feed link piece
              (chunks, _) <- pullAvailable source
              modifyIORef' collected (<> chunks)
            (chunks, ended) <- pullAvailable source
            ended `shouldBe` True
            rest <- readIORef collected
            BS.concat (taken : rest <> chunks) `shouldBe` body

  describe "a pull after a wake-up" $ do
    -- A pull has to receive, and a receive has its own cost, which is the link's:
    -- what these specs bound is what a pull adds to it. Pulls that are pending
    -- are repeated at every wake-up of a response, so what they add is paid
    -- many times for a large body.
    xfail "a pull that is retried goes through every layer above the receive again, and allocates a kilobyte more than the receive" $
      it "that received nothing allocates little more than the receive does" $ do
        raw <- rawEmptyReceive
        forM_ [("an empty buffer", 0), ("the start of a chunk in the buffer", 2000)] $ \(label, inBuffer) ->
          withScarceRequests $ \_ taker _ -> withRequestFeed taker $ \link context -> do
            let body = bodyOf 20000
                (early, late) = BS.splitAt (fieldStart body + inBuffer) (answer [body])
            feed link early
            withField context $ \field -> liftIO $ do
              let source = Tx.fieldSource field
              (_, ended) <- pullAvailable source
              ended `shouldBe` False
              pulled <- allocatedBy 1000 (void (pullReady source))
              checkExtra label (pulled - raw) extraWhenNothing
              feed link late

    xfail "a pull that is retried goes through every layer above the receive again, and allocates half a kilobyte more than its receives" $
      it "that received bytes, but not a whole chunk, allocates little more than its receives do" $ do
        rawEmpty <- rawEmptyReceive
        rawBytes <- rawWakeup 64
        withScarceRequests $ \_ taker _ -> withRequestFeed taker $ \link context -> do
          let body = bodyOf 20000
              (early, late) = BS.splitAt (fieldStart body + 2000) (answer [body])
              pieces = take 20 (slices 64 late)
          feed link early
          withField context $ \field -> liftIO $ do
            let source = Tx.fieldSource field
            (_, ended) <- pullAvailable source
            ended `shouldBe` False
            pulled <- forM pieces $ \piece -> do
              feed link piece
              allocatedOnce (pullReady source)
            -- the receive of the bytes, and the one after it, that finds nothing
            checkExtra "bytes that make no chunk" (sum pulled `div` fromIntegral (length pulled) - rawBytes - rawEmpty) extraWhenPartial
            feed link (BS.drop (sum (map BS.length pieces)) late)
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
