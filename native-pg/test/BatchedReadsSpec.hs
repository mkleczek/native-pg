{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
module BatchedReadsSpec (spec) where

import AcquisitionTestSupport (getAcquisition)
import Control.Exception (SomeException, bracket, evaluate, throwIO, try)
import Control.Monad (forM_, replicateM, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef
import Data.Word (Word8)
import Database.PostgreSQL.Diagnostics (ServerError)
import Database.PostgreSQL.Internal.Channel (newChannel)
import Database.PostgreSQL.Internal.Context
import qualified Database.PostgreSQL.Internal.Execution as PG
import Database.PostgreSQL.Internal.Protocol
import Database.PostgreSQL.Internal.Protocol.Codec (Alternatives (..), parseChoice)
import Database.PostgreSQL.Internal.Protocol.Codec.Payload (Payload (..))
import qualified Database.PostgreSQL.Internal.Row as PG
import qualified Database.PostgreSQL.Session as PG
import Database.PostgreSQL.Transport (Link (..))
import FeedSupport (message, number)
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Network.Streaming.Buffer (Buffer, bufferCapacity, bufferMemory, newBuffer)
import Network.Streaming.BufferPool (PoolSettings (..))
import qualified Network.Streaming.BufferPool as BufferPool
import Network.Streaming.PositiveInt (PositiveInt, positiveInt)
import Network.Streaming.Watch (readyNow)
import Skeletest hiding (context)
import qualified Skeletest.Predicate as P

spec :: Spec
spec = describe "batched scoped reads" $ do
  it "reads a reply of many small messages with one read" $
    withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
      server <- newServer [reply]
      connection <- connectionTo server
      scopeRun scope connection run `shouldReturn` expected
      readSizes server `shouldReturn` [256]
      readsStayInBlock scope server
      -- A reply that ends the read leaves nothing to carry over.
      readIORef (connectionCarryOver connection) `shouldReturn` BS.empty

  it "reads a second reply with one more read" $
    withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
      server <- newServer [reply, reply]
      connection <- connectionTo server
      scopeRun scope connection $ \context ->
        replicateM 2 (run context) `shouldReturn` replicate 2 expected
      length <$> readSizes server `shouldReturn` 2

  it "streams a row larger than the block in block-sized reads" $
    withScopes (positiveInt @64) $ \scopes -> forM_ scopes $ \scope -> do
      let column = BS.replicate 1000 120
          wire = parseComplete <> bindComplete <> dataRow [column] <> commandComplete "SELECT 1" <> ready
      server <- newServer [wire]
      connection <- connectionTo server
      chunks <- newIORef []
      result <- scopeRun scope connection $ \context ->
        PG.executeWithContext context execution (\_ row -> do
          PG.nextColumn row `shouldReturn` Just (PG.Column 1000)
          PG.columnChunks row (\chunk -> evaluate (BS.copy chunk) >>= \owned -> modifyIORef' chunks (<> [owned]))) ()
      result `shouldBe` (Right ((), "SELECT 1"), TxIdle)
      BS.concat <$> readIORef chunks `shouldReturn` column
      sizes <- readSizes server
      -- Every byte of the reply is read once, a block at a time.
      (length sizes <= (BS.length wire + 63) `div` 64 + 1) `shouldBe` True
      readsStayInBlock scope server

  it "never reads more than the free space of the block" $
    withScopes (positiveInt @64) $ \scopes -> forM_ scopes $ \scope -> do
      let column = BS.replicate 1000 120
      server <- newServer [parseComplete <> bindComplete <> dataRow [column] <> commandComplete "SELECT 1" <> ready]
      connection <- connectionTo server
      _ <- scopeRun scope connection $ \context ->
        PG.executeWithContext context execution (\_ row -> do
          _ <- PG.nextColumn row
          PG.columnChunks row (const (pure ()))) ()
      sizes <- readSizes server
      (maximum sizes <= 64) `shouldBe` True
      readsStayInBlock scope server

  describe "bytes after the last ReadyForQuery" $ do
    forM_ [("whole", maxBound), ("cut inside its header", 3), ("cut after its header", 5), ("cut inside its body", 12)] $ \(label, cut) ->
      it ("go to the next operation when a notification is " <> label) $
        withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
          let (before, after) = BS.splitAt cut (notification "one")
          server <- newServer [reply <> before, after <> reply]
          connection <- connectionTo server
          scopeRun scope connection run `shouldReturn` expected
          readSizes server `shouldReturn` [256]
          -- The block goes back to its owner, who may reuse it at once.
          scopeRecycle scope
          scopeRun scope connection run `shouldReturn` expected
          kept <- readIORef (connectionNotifications connection)
          map PG.notificationPayload kept `shouldBe` ["one"]
          length <$> readSizes server `shouldReturn` 2
          readsStayInBlock scope server

    it "are served to awaitNotification without reading the link" $
      withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
        server <- newServer [reply <> notification "one"]
        connection <- connectionTo server
        scopeRun scope connection run `shouldReturn` expected
        scopeRecycle scope
        carried <- scopeRun scope connection PG.awaitNotificationWithContext
        PG.notificationPayload carried `shouldBe` "one"
        readSizes server `shouldReturn` [256]
        PG.isUsable connection `shouldReturn` True

    forM_ [("whole", maxBound), ("cut", 7)] $ \(label, cut) ->
      it ("count as arrived for checkIdle when a notification is " <> label) $
        withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
          server <- newServer [reply <> BS.take cut (notification "one")]
          connection <- connectionTo server
          scopeRun scope connection run `shouldReturn` expected
          scopeRecycle scope
          PG.checkIdle connection `shouldReturn` Left "the server sent a message to the idle session, as it does before ending it"
          PG.isUsable connection `shouldReturn` False
          -- They were enough to tell: the link wasn't read.
          readSizes server `shouldReturn` [256]

    it "keep their order across messages, one of them cut" $
      withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
        let (before, after) = BS.splitAt 9 (notification "two")
        server <- newServer [reply <> notification "one" <> before, after <> reply]
        connection <- connectionTo server
        scopeRun scope connection run `shouldReturn` expected
        scopeRecycle scope
        scopeRun scope connection run `shouldReturn` expected
        kept <- readIORef (connectionNotifications connection)
        -- Newest first.
        map PG.notificationPayload kept `shouldBe` ["two", "one"]
        length <$> readSizes server `shouldReturn` 2

    it "go to an owned context before the link" $ do
      input <- newBuffer (positiveInt @256)
      output <- newBuffer (positiveInt @256)
      let (before, after) = BS.splitAt 7 (notification "one")
      server <- newServer [reply <> before, after <> reply]
      connection <- connectionTo server
      withContext connection input output run `shouldReturn` expected
      readSizes server `shouldReturn` [256]
      owned <- newOwnedContext connection
      run owned `shouldReturn` expected
      kept <- readIORef (connectionNotifications connection)
      map PG.notificationPayload kept `shouldBe` ["one"]

    it "keep their order when a smaller block takes them in pieces" $ do
      large <- newBuffer (positiveInt @256)
      small <- newBuffer (positiveInt @16)
      output <- newBuffer (positiveInt @256)
      server <- newServer [reply <> notification "alpha-01" <> notification "bravo-02"]
      connection <- connectionTo server
      withContext connection large output run `shouldReturn` expected
      -- The first notification ends inside the small block's second refill,
      -- which also takes the start of the second one: the block's leftover
      -- must come before what the carried-over bytes still hold.
      first <- withContext connection small output PG.awaitNotificationWithContext
      PG.notificationPayload first `shouldBe` "alpha-01"
      second <- withContext connection small output PG.awaitNotificationWithContext
      PG.notificationPayload second `shouldBe` "bravo-02"
      readSizes server `shouldReturn` [256]

  describe "a lent chunk" $
    it "stays valid while its callback runs and the rest of the reply is already read" $
      withTestPool (positiveInt @256) $ \access -> do
        let payload = BS.replicate 31 65 <> BS.replicate 31 66
        server <- newServer [parseComplete <> bindComplete <> dataRow [payload] <> dataRow [payload] <> commandComplete "SELECT 2" <> ready]
        connection <- connectionTo server
        observed <- newIORef []
        result <- withRequestContext connection access $ \context ->
          PG.executeWithContext context execution (\count row -> do
            _ <- PG.nextColumn row
            PG.columnChunks row $ \chunk -> do
              entered <- evaluate (BS.copy chunk)
              readsBefore <- length <$> readSizes server
              -- A producer that borrows while the chunk is lent gets another
              -- block, and writes to it as it pleases.
              bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer $
                scribble . BufferPool.loanBuffer
              left <- evaluate (BS.copy chunk)
              readsAfter <- length <$> readSizes server
              modifyIORef' observed (<> [(entered == payload, left == payload, readsAfter == readsBefore)])
            pure (count + 1 :: Int)) 0
        result `shouldBe` (Right (2, "SELECT 2"), TxIdle)
        readIORef observed `shouldReturn` replicate 2 (True, True, True)
        readSizes server `shouldReturn` [256]

  describe "a failed operation" $ do
    it "leaves the connection broken, whatever was read ahead" $
      withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
        server <- newServer [reply <> notification "one"]
        connection <- connectionTo server
        result <- try @SomeException $ scopeRun scope connection $ \context ->
          PG.executeWithContext context execution (\_ _ -> throwIO (userError "step failed")) ()
        result `shouldSatisfy` P.left P.anything
        PG.isUsable connection `shouldReturn` False
        readIORef (connectionCarryOver connection) `shouldReturn` BS.empty
        requests <- readSizes server
        scopeRun scope connection (const (pure ()))
          `shouldSatisfy` P.throws (P.eq (SessionUnusable "the session broke during an earlier operation"))
        readSizes server `shouldReturn` requests

    it "leaves the connection broken when a completed scope broke it" $
      withScopes (positiveInt @256) $ \scopes -> forM_ scopes $ \scope -> do
        server <- newServer [reply <> notification "one"]
        connection <- connectionTo server
        scopeRun scope connection $ \context -> do
          run context `shouldReturn` expected
          breakConnection connection
        PG.isUsable connection `shouldReturn` False
        readIORef (connectionCarryOver connection) `shouldReturn` BS.empty
        scopeRun scope connection (const (pure ()))
          `shouldSatisfy` P.throws (P.eq (SessionUnusable "the session broke during an earlier operation"))

  describe "protocol lookahead beyond the block" $
    it "parses back-to-back messages through a block smaller than their selector" $
      withScopes (positiveInt @4) $ \scopes -> forM_ scopes $ \scope -> do
        server <- newServerReady (authenticationOk <> authenticationOk) []
        connection <- connectionTo server
        scopeRun scope connection $ \context ->
          replicateM 2 (runContextInput context maxBound selector) `shouldReturn` ["ok", "ok"]
        PG.isUsable connection `shouldReturn` True
  where
    selector =
      parseChoice @'[ 'PayloadField 'AuthenticationOkType, 'PayloadField 'AuthenticationSASLType]
        ((\AuthenticationOk -> "ok") :|: (\(_ :: AuthenticationSASL ByteString) -> "sasl" :: String) :|: NoAlternatives)

-- | A server whose replies become readable once the client sends something,
-- handed over as a socket would: whatever is ready, up to the size asked for.
data Server = Server
  { serverReplies :: IORef [ByteString],
    serverReady :: IORef ByteString,
    -- | Where each read was asked to put its bytes and how many, newest first.
    serverReads :: IORef [(Ptr Word8, Int)]
  }

newServer :: [ByteString] -> IO Server
newServer = newServerReady BS.empty

newServerReady :: ByteString -> [ByteString] -> IO Server
newServerReady ready' replies = Server <$> newIORef replies <*> newIORef ready' <*> newIORef []

instance Link Server where
  -- Nothing ready is the end of the stream, so a read nobody expects fails.
  receiveReady server pointer size = do
    modifyIORef' (serverReads server) ((pointer, size) :)
    available <- readIORef (serverReady server)
    let chunk = BS.take size available
    BS.useAsCString chunk $ \origin -> copyBytes pointer (castPtr origin) (BS.length chunk)
    writeIORef (serverReady server) (BS.drop (BS.length chunk) available)
    pure (Just (BS.length chunk))
  sendReady server pieces = do
    let sent = sum (map BS.length pieces)
    when (sent > 0) $ readIORef (serverReplies server) >>= \case
      next : later -> do
        writeIORef (serverReplies server) later
        modifyIORef' (serverReady server) (<> next)
      [] -> pure ()
    pure sent
  flushReady _ = pure True
  watchReadable _ = fail "unexpected read wait"
  watchWritable _ = pure readyNow
  closeLink _ = pure ()

connectionTo :: Server -> IO Connection
connectionTo server = newConnection (newChannel server) 16

readSizes :: Server -> IO [Int]
readSizes server = reverse . map snd <$> readIORef (serverReads server)

-- | Where a scope gets its input block from.
data Scope = Scope
  { scopeBlock :: Buffer,
    scopeRun :: forall a. Connection -> (Context -> IO a) -> IO a,
    -- | Overwrites the block, as the next user of its storage would.
    scopeRecycle :: IO ()
  }

-- | Scopes over caller-supplied storage, and over a request's pooled block.
withScopes :: PositiveInt -> ([Scope] -> IO a) -> IO a
withScopes size use = do
  input <- newBuffer size
  output <- newBuffer (positiveInt @256)
  withTestPool size $ \access -> do
    primary <- BufferPool.borrowBuffer access
    let block = BufferPool.loanBuffer primary
    BufferPool.releaseBuffer primary
    use
      [ Scope input (\connection -> withContext connection input output) (scribble input),
        Scope block (\connection -> withRequestContext connection access)
          (bracket (BufferPool.borrowBuffer access) BufferPool.releaseBuffer (scribble . BufferPool.loanBuffer))
      ]

withTestPool :: PositiveInt -> (BufferPool.BufferAccess -> IO a) -> IO a
withTestPool size use = do
  waiting <- getAcquisition
  resources <- BufferPool.newBufferPool waiting PoolSettings
    { bufferSize = size,
      ordinaryBufferCount = positiveInt @1,
      reserveBufferCount = positiveInt @1,
      setupBufferCount = positiveInt @2
    }
  BufferPool.withRequestBuffers resources use

scribble :: Buffer -> IO ()
scribble buffer = withForeignPtr (bufferMemory buffer) $ \pointer -> fillBytes pointer 0xEE (bufferCapacity buffer)

-- | Every read of the server went to the scope's block, within its bounds.
readsStayInBlock :: Scope -> Server -> IO ()
readsStayInBlock scope server = do
  requests <- readIORef (serverReads server)
  withForeignPtr (bufferMemory (scopeBlock scope)) $ \base ->
    forM_ requests $ \(pointer, size) ->
      (pointer >= base && pointer `plusPtr` size <= base `plusPtr` bufferCapacity (scopeBlock scope)) `shouldBe` True

execution :: PG.Execution
execution = PG.Execution (PG.Unprepared "SELECT 1" []) [] [] []

-- | Runs the statement, collecting its rows' only column.
run :: Context -> IO (Either ServerError ([ByteString], ByteString), TransactionStatus)
run context = PG.executeWithContext context execution collect []
  where
    collect values row = do
      PG.nextColumn row `shouldReturn` Just (PG.Column 3)
      value <- PG.columnBytes row
      pure (values <> [value])

expected :: (Either ServerError ([ByteString], ByteString), TransactionStatus)
expected = (Right (["abc", "abc", "abc"], "SELECT 3"), TxIdle)

-- | The reply to 'execution' with three rows: seven messages.
reply :: ByteString
reply = parseComplete <> bindComplete <> mconcat (replicate 3 (dataRow ["abc"])) <> commandComplete "SELECT 3" <> ready

parseComplete, bindComplete, ready, authenticationOk :: ByteString
parseComplete = message 49 ""
bindComplete = message 50 ""
ready = message 90 "I"
authenticationOk = message 82 (number 0)

dataRow :: [ByteString] -> ByteString
dataRow columns = message 68 (BS.pack [0, fromIntegral (length columns)] <> foldMap (\value -> number (BS.length value) <> value) columns)

commandComplete :: ByteString -> ByteString
commandComplete tag = message 67 (tag <> "\0")

-- | A notification on channel @ch@ from process 9.
notification :: ByteString -> ByteString
notification payload = message 65 (number 9 <> "ch\0" <> payload <> "\0")
