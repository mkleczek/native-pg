{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | TLS links against a server in the same process, over a pair of Unix-domain sockets. The server is rustls too,
-- through its server API, and its side of a connection is a link made by the same functions as the client's. The two ends
-- are the parts of one program, which runs under each of the arrows: they wait for each other by blocking on the sockets.
--
-- A case that the code can't pass yet is marked with what it needs ('needs'), and the mark comes off as the code lands.
module TlsSpec (spec) where

import Control.Arrow (Arrow, first, (>>>), (>>^), (^>>))
import Control.Concurrent (threadWaitRead)
import Control.Exception (IOException, bracket, displayException, evaluate, fromException, throwIO)
import Control.Monad (forM_, replicateM, replicateM_, unless)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS
import Data.Int (Int64)
import Data.Word (Word32, Word8)
import Database.PostgreSQL.Tls.Internal
import Database.PostgreSQL.Transport (RootCertificates (..), ServerVerification (..), TlsError (..), TlsLink (..), TlsSettings (..), shake)
import Foreign.C.Types (CSize (..))
import Foreign.Marshal.Alloc (alloca, allocaBytes)
import Foreign.Marshal.Array (withArray)
import Foreign.Marshal.Utils (fillBytes)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (peek)
import GHC.Conc (getAllocationCounter)
import Network.Socket (Socket)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as Socket
import Network.Streaming.Buffer (Buffer, newBuffer)
import Network.Streaming.Classes
import Network.Streaming.Combinators (alongside, concurrently, done, iterateA, sleep, tap, pattern Again, pattern Finish)
import Network.Streaming.Compiled (runCompiled)
import Network.Streaming.Delimited (runDelimited)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Threads (runThreads)
import Skeletest
import qualified Skeletest.Predicate as P
import System.Mem (performMajorGC)
import System.Posix.Types (Fd (..))
import System.Timeout (timeout)

-- | Marks every case of the spec as an expected failure, for what it needs and the code doesn't have yet.
needs :: String -> Spec -> Spec
needs what = xfail ("v2: " <> what <> " is not there yet")

-- | A program for any arrow that has what 'Runtime' bundles.
type Program i a = forall arr. (Runtime arr) => arr i a

-- | A way to run a program: an arrow's runner. A program that never ends fails the case after a minute.
data Runner = Runner String (forall i a. Program i a -> i -> IO a)

onThreads, onDelimited, onCompiled :: Runner
onThreads = Runner "threads" (\program input -> limited (runThreads program input))
onDelimited = Runner "delimited" (\program input -> limited (fst <$> runDelimited program input))
onCompiled = Runner "compiled" (\program input -> limited (fst <$> runCompiled program input))

limited :: IO a -> IO a
limited action = timeout 60000000 action >>= maybe (failTest "the program did not end in a minute") pure

runners :: [Runner]
runners = [onThreads, onDelimited, onCompiled]

spec :: Spec
spec = do
  forM_ runners $ \(Runner name run) -> describe name $ do
    describe "the handshake" $ do
      needs "TLS" $ it "verifies the server's certificate against the roots, and its name" $
        withEnds "server" (verifying "ca.crt") $ \ends ->
          run handshaking ends `shouldReturn` ()

      needs "TLS" $ it "connects to any server without verification" $
        withEnds "server-other-name" (settings NoVerification) $ \ends ->
          run handshaking ends `shouldReturn` ()

      needs "TLS" $ it "refuses a certificate that no root signed" $
        withEnds "server" (verifying "other-ca.crt") $ \ends ->
          run refusal ends `shouldReturn` "SSL error: invalid peer certificate: UnknownIssuer"

      needs "TLS" $ it "refuses a certificate for another name" $
        withEnds "server-other-name" (verifying "ca.crt") $ \ends ->
          run refusal ends `shouldReturn` "SSL error: invalid peer certificate: NotValidForName"

      needs "TLS" $ it "gives the hash of the server's certificate for SCRAM's channel binding, and the server none" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          let expected =
                BS.pack
                  [ 0xff, 0xc8, 0xc8, 0xc2, 0x06, 0xfe, 0xe8, 0x34,
                    0xbd, 0xe2, 0x7d, 0x21, 0x19, 0x94, 0xf9, 0xa8,
                    0xb5, 0xbc, 0x44, 0x9c, 0x0e, 0xcd, 0xdb, 0x35,
                    0xdf, 0x38, 0x75, 0x72, 0xb0, 0x29, 0x51, 0x88
                  ]
          run (concurrently (atClient (fst ^>> shake)) (atServer (fst ^>> shake))) ends
            `shouldReturn` (Just (Right expected), Nothing)

      needs "TLS" $ it "refuses a server that doesn't choose ALPN's postgresql, when the settings say it has to" $
        withEnds "server" ((verifying "ca.crt") {tlsAlpnRequired = True}) $ \ends ->
          run refusal ends `shouldReturn` "SSL error: direct SSL connection was established without ALPN protocol negotiation extension"

      -- The client has made its native connection and waits for the server, and the constructor's temporary storage
      -- is gone by the time that the server answers.
      needs "TLS" $ it "keeps the server name alive while it waits for the server" $
        withEnds "server" (verifying "ca.crt") $ \ends ->
          run (concurrently (atClient shaken) (atServer (after (millis 50) (reclaiming >>> shaken))) >>> done) ends `shouldReturn` ()

    describe "a link" $ do
      needs "TLS" $ it "carries more bytes both ways than the sockets hold" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          let payload = BS.pack (take 1000000 (cycle [0 .. 250]))
              size = BS.length payload
          (echoed, ()) <-
            run
              ( concurrently
                  (atClient (shaken >>> sending [BS.take 300000 payload, BS.drop 300000 payload] >>> receivingExactly size))
                  (atServer (shaken >>> echoing size >>> done))
              )
              ends
          echoed `shouldBe` payload

      needs "TLS" $ it "hands out a record bigger than the room in order, over several receives" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          let payload = BS.pack (take 1000 (cycle [0 .. 250]))
          (pieces, ()) <-
            run
              ( concurrently
                  (atClient (shaken >>> receivingPieces 100 (BS.length payload)))
                  (atServer (shaken >>> sending [payload] >>> done))
              )
              ends
          BS.concat pieces `shouldBe` payload
          (length pieces > 1, all ((<= 100) . BS.length) pieces) `shouldBe` (True, True)

      needs "TLS" $ it "ends the peer's stream when it shuts its output down" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          run (concurrently (atServer (receivingUpTo 16)) (atClient (fst ^>> shutdownSend))) ends `shouldReturn` (BS.empty, ())

      -- Both handshakes are over first: the server writes its session tickets as it finishes, and a client that closed
      -- by then would make that write fail.
      needs "TLS" $ it "ends the peer's stream when it closes" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          run (concurrently (atServer (receivingUpTo 16)) (atClient (fst ^>> close))) ends `shouldReturn` (BS.empty, ())

      needs "TLS" $ it "ends the stream when the peer's socket closes without a close_notify" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          Socket.close (serverSocketOf ends)
          run (atClient (receivingUpTo 16)) ends `shouldReturn` BS.empty

      needs "TLS" $ it "throws TlsError for a record that can't be decrypted, after sending the alert for it" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          -- An application data record of 16 zero bytes, which can be no more than the authentication tag of an empty
          -- message, and not the right one.
          Socket.sendAll (serverSocketOf ends) (BS.pack [23, 3, 3, 0, 16] <> BS.replicate 16 0)
          (client, server) <- run (concurrently (failing (atClient (receivingUpTo 16 >>> done))) (failing (atServer (receivingUpTo 16 >>> done)))) ends
          client `shouldSatisfy` P.just (P.hasInfix "cannot decrypt peer's message")
          server `shouldSatisfy` P.just (P.hasInfix "BadRecordMac")

    describe "awaiting input" $ do
      needs "TLS" $ it "waits for input and takes none of it" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          let payload = "five!" :: ByteString
          (received, ()) <-
            run
              ( concurrently
                  (atClient (shaken >>> tap (fst ^>> awaitInput) >>> receivingUpTo 16))
                  (atServer (shaken >>> after (millis 100) (sending [payload]) >>> done))
              )
              ends
          received `shouldBe` payload

      needs "TLS" $ it "doesn't return while the peer sends nothing" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          run (atClient (fst ^>> within (millis 200) awaitInput)) ends `shouldReturn` Nothing

      -- The first receive takes a hundred of the bytes of a record, and the rest of them are decrypted, in rustls,
      -- while the socket has nothing.
      needs "TLS" $ it "returns at once when the bytes are in hand" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          let payload = BS.pack (take 1000 (cycle [0 .. 250]))
          run (concurrently (atClient shaken) (atServer (shaken >>> sending [payload] >>> done)) >>> done) ends `shouldReturn` ()
          (one, (waited, other)) <-
            run (atClient ((,) <$> receivingUpTo 100 <*> ((,) <$> (fst ^>> within (secs 5) awaitInput) <*> receivingUpTo 1000))) ends
          (one, waited, other) `shouldBe` (BS.take 100 payload, Just (), BS.drop 100 payload)

      needs "TLS" $ it "returns at the end of the stream" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          run (concurrently (atClient (fst ^>> within (secs 5) awaitInput)) (atServer (fst ^>> close))) ends `shouldReturn` (Just (), ())
          run (atClient (receivingUpTo 16)) ends `shouldReturn` BS.empty

    describe "a link's lifetime" $ do
      -- TLS 1.3's session tickets precede these application bytes.
      needs "TLS" $ it "keeps the server name alive for the tickets, after the setup returns" $
        withEnds "server" (verifying "ca.crt") $ \ends -> do
          run handshaking ends `shouldReturn` ()
          reclaimTemporaryBuffers
          let payload = BS.replicate 32768 42
          (received, ()) <- run (concurrently (atClient (receivingExactly (BS.length payload))) (atServer (sending [payload] >>> done))) ends
          received `shouldBe` payload

  describe "the calls that never wait" $ do
    needs "TLS" $ it "give nothing while the peer sends nothing" $
      withEnds "server" (verifying "ca.crt") $ \ends -> do
        handshaken ends
        allocaBytes 16384 $ \buffer ->
          replicateM_ 2 (linkTryReceive (clientLinkOf ends) buffer 16384 `shouldReturn` Nothing)

    needs "TLS" $ it "throw the socket's error as an IOException" $
      withEnds "server" (verifying "ca.crt") $ \ends -> do
        handshaken ends
        Socket.close (clientSocketOf ends)
        allocaBytes 16384 $ \buffer ->
          linkTryReceive (clientLinkOf ends) buffer 16384
            `shouldSatisfy` P.throws (displayException @IOException P.>>> P.eq "recv: invalid argument (Bad file descriptor)")

    needs "TLS" $ it "bound the output they retain when the peer stops reading, and resume without loss" $
      withEnds "server" (verifying "ca.crt") $ \ends -> do
        Socket.setSocketOption (clientSocketOf ends) Socket.SendBuffer 4096
        handshaken ends
        let payload = BS.replicate 1048576 42
            client = clientLinkOf ends
        taken <- linkTrySend client [payload]
        -- The first encrypt pass stops at rustls's budget before the blocked socket can push the rest back into the caller.
        (taken > 0 && taken <= 65536) `shouldBe` True
        (accepted, stalled) <- fillUntilStalled client payload 16 taken
        (accepted <= 131072, stalled) `shouldBe` (True, True)
        -- The peer reads again: what was accepted arrives whole, and then the rest.
        let Runner _ run = onThreads
        (_, drained) <- run (concurrently (atClient (fst ^>> flushing)) (atServer (receivingExactly accepted))) ends
        drained `shouldBe` BS.take accepted payload
        (_, more) <- run (concurrently (atClient (sending [BS.drop accepted payload] >>> done)) (atServer (receivingExactly (BS.length payload - accepted)))) ends
        more `shouldBe` BS.drop accepted payload

    -- All that a receive needs to allocate is its result, a 'Just' and an 'Int', and the call's own arguments. The bound
    -- leaves room for what measuring it adds.
    needs "TLS" $ it "allocate next to nothing for bytes that are ready" $
      withEnds "server" (verifying "ca.crt") $ \ends -> do
        handshaken ends
        let client = clientLinkOf ends
            server = serverLinkOf ends
        allocaBytes 16384 $ \buffer -> do
          -- The session tickets come first, and the first receive takes them.
          linkTryReceive client buffer 16384 `shouldReturn` Nothing
          costs <- replicateM 5 $ do
            -- The socket takes less than the encrypted bytes of the record: the server writes the rest as the client reads.
            _ <- linkTrySend server [BS.replicate 8192 42]
            receiveCosts client server buffer 8192
          maximum (concat costs) `shouldSatisfy` P.lte 128

  describe "a configuration" $ do
    needs "TLS" $ it "keeps cached server names alive after a connection closes" $ do
      config <- newTlsConfig (verifying "ca.crt") "localhost"
      replicateM_ 2 $ do
        withEndsOf config "server" (verifying "ca.crt") $ \ends -> do
          let Runner _ run = onThreads
          (received, ()) <-
            run
              ( concurrently
                  (atClient (shaken >>> receivingExactly 1))
                  (atServer (shaken >>> sending [BS.singleton 42] >>> done))
              )
              ends
          received `shouldBe` BS.singleton 42
          closeTls (clientTls ends)
        reclaimTemporaryBuffers

    it "needs its root certificate file" $
      newTlsConfig (settings (VerifyFull (RootCertificateFile (certificate "missing.crt")))) "localhost"
        `shouldSatisfy` P.throws (P.con (TlsError (P.hasInfix "could not read root certificate file")))

-- * The parts of a case

-- | The two ends of a case, and the memory that each receives into.
data Ends = Ends
  { clientSocketOf :: !Socket,
    serverSocketOf :: !Socket,
    clientTls :: !TlsConnection,
    clientLinkOf :: !TlsLink,
    serverLinkOf :: !TlsLink,
    clientBuffer :: !Buffer,
    serverBuffer :: !Buffer
  }

-- | A part that is the client's, on its link and its buffer.
atClient :: (Arrow arr) => arr (TlsLink, Buffer) b -> arr Ends b
atClient part = (\ends -> (clientLinkOf ends, clientBuffer ends)) ^>> part

-- | A part that is the server's.
atServer :: (Arrow arr) => arr (TlsLink, Buffer) b -> arr Ends b
atServer part = (\ends -> (serverLinkOf ends, serverBuffer ends)) ^>> part

-- | The part after the handshake of its link, with the link and the buffer for the part's steps.
shaken :: (Runtime arr) => arr (TlsLink, Buffer) (TlsLink, Buffer)
shaken = tap (fst ^>> shake >>> done)

-- | The handshakes of both ends, at once.
handshaking :: (Runtime arr) => arr Ends ()
handshaking = concurrently (atClient shaken) (atServer shaken) >>> done

-- | The step after waiting as long as the duration.
after :: (Runtime arr) => Duration -> arr a b -> arr a b
after delay step = tap (done >>> sleep delay) >>> step

-- | Reclaims the temporary buffers, and passes the input on.
reclaiming :: (Runtime arr) => arr a a
reclaiming = tap (now (const reclaimTemporaryBuffers))

-- | Sends the pieces on the link, and passes the link and the buffer on.
sending :: (Runtime arr) => [ByteString] -> arr (TlsLink, Buffer) (TlsLink, Buffer)
sending pieces = tap ((\(link, _) -> (link, pieces)) ^>> sendAll)

-- | Writes what the link took and has not written, as the socket takes it.
flushing :: (Runtime arr) => arr TlsLink ()
flushing = (\link -> (link, [])) ^>> sendAll

-- | One receive into at most this much of the buffer: its bytes, copied out of the view, none at the end of the stream.
receivingUpTo :: (Runtime arr) => Int -> arr (TlsLink, Buffer) ByteString
receivingUpTo size =
  (\(link, buffer) -> ((link, Room buffer 0 size), buffer))
    ^>> first receive
    >>> now (\(count, buffer) -> evaluate (BS.copy (view buffer count)))

-- | The pieces of the receives into a room of this size, until they make up this many bytes or the stream ends.
receivingPieces :: (Runtime arr) => Int -> Int -> arr (TlsLink, Buffer) [ByteString]
receivingPieces size count = (\(link, buffer) -> (link, buffer, 0 :: Int, [])) ^>> iterateA stepping
  where
    stepping =
      alongside ((\(link, buffer, _, _) -> (link, buffer)) ^>> receivingUpTo size)
        >>^ \(piece, (link, buffer, got, pieces)) ->
          if BS.null piece || got + BS.length piece >= count
            then Finish (reverse (if BS.null piece then pieces else piece : pieces))
            else Again (link, buffer, got + BS.length piece, piece : pieces)

-- | The bytes of the receives, until there are this many or the stream ends.
receivingExactly :: (Runtime arr) => Int -> arr (TlsLink, Buffer) ByteString
receivingExactly count = receivingPieces 16384 count >>^ BS.concat

-- | Receives this many bytes and sends them back, and passes the link and the buffer on.
echoing :: (Runtime arr) => Int -> arr (TlsLink, Buffer) (TlsLink, Buffer)
echoing count = alongside (receivingExactly count) >>> tap ((\(bytes, (link, _)) -> (link, [bytes])) ^>> sendAll) >>^ snd

-- | How the client's handshake fails, in the words of its failure.
refusal :: (Runtime arr) => arr Ends String
refusal =
  concurrently (failing (atClient (fst ^>> shake >>> done))) (failing (atServer (fst ^>> shake >>> done)))
    >>^ \(client, _) -> maybe "the handshake succeeded" id client

-- | The step's failure in words, or nothing if it didn't fail.
failing :: (Runtime arr) => arr a () -> arr a (Maybe String)
failing step = tryA step >>^ either (Just . worded) (const Nothing)
  where
    worded failure = maybe (show failure) (displayException @TlsError) (fromException failure)

-- * What the cases are made of

-- | The settings with the verification, and rustls's defaults otherwise.
settings :: ServerVerification -> TlsSettings
settings verification = TlsSettings verification Nothing [] [minBound .. maxBound] True False

-- | Verification against the roots of the file.
verifying :: FilePath -> TlsSettings
verifying roots = settings (VerifyFull (RootCertificateFile (certificate roots)))

certificate :: FilePath -> FilePath
certificate name = "test/certificates/" <> name

-- | Reclaim temporary pinned buffers and exercise reuse of their blocks. Small allocations matter here: a single large
-- allocation uses different storage from the CString passed to rustls's connection constructor.
reclaimTemporaryBuffers :: IO ()
reclaimTemporaryBuffers = replicateM_ 4 $ do
  performMajorGC
  replicateM_ 1024 (allocaBytes 1024 $ \buffer -> fillBytes buffer 120 1024)

-- | The ends after both handshakes, which a case of the calls that never wait does before it calls them.
handshaken :: Ends -> IO ()
handshaken ends = let Runner _ run = onThreads in run handshaking ends

-- | Sends until the link takes nothing more, with a finite retry budget so that a broken link that takes everything
-- doesn't hang the suite while the peer deliberately withholds reads: how many bytes it took, and whether it stalled.
fillUntilStalled :: TlsLink -> ByteString -> Int -> Int -> IO (Int, Bool)
fillUntilStalled _ _ 0 accepted = pure (accepted, False)
fillUntilStalled link payload retries accepted = do
  taken <- linkTrySend link [BS.drop accepted payload]
  if taken == 0
    then pure (accepted, True)
    else fillUntilStalled link payload (retries - 1) (accepted + taken)

-- | The bytes that each receive allocated on this thread, while waiting for the count of bytes: a receive that gives
-- bytes, or nothing yet, and then the peer writes what it hasn't written of what it took.
receiveCosts :: TlsLink -> TlsLink -> Ptr Word8 -> Int -> IO [Int64]
receiveCosts link peer buffer = go
  where
    go count
      | count <= 0 = pure []
      | otherwise = do
          before <- getAllocationCounter
          received <- linkTryReceive link buffer 16384
          after' <- getAllocationCounter
          let cost = before - after'
          case received of
            Just got -> (cost :) <$> go (count - got)
            Nothing -> do
              _ <- linkFlush peer
              Socket.withFdSocket (linkSocket link) (threadWaitRead . Fd)
              (cost :) <$> go count

-- | A connected pair of sockets: the client's link to the test server, and the server's, before their handshakes.
withEnds :: String -> TlsSettings -> (Ends -> IO a) -> IO a
withEnds name clientSettings action = do
  config <- newTlsConfig clientSettings "localhost"
  withEndsOf config name clientSettings action

-- | The same, for a configuration that the client's connection is made from.
withEndsOf :: TlsConfig -> String -> TlsSettings -> (Ends -> IO a) -> IO a
withEndsOf config name clientSettings action =
  bracket (Socket.socketPair Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) closeBoth $ \(clientSocket, serverSocket) -> do
    chain <- BS.readFile (certificate (name <> ".crt"))
    key <- BS.readFile (certificate (name <> ".key"))
    server <- newTlsConnection serverSocket =<< serverConnection chain key
    client <- clientConnection config clientSocket
    clientBuffer' <- newBuffer (positiveInt @16384)
    serverBuffer' <- newBuffer (positiveInt @16384)
    action
      Ends
        { clientSocketOf = clientSocket,
          serverSocketOf = serverSocket,
          clientTls = client,
          clientLinkOf = tlsLinkOf clientSettings client,
          serverLinkOf = tlsLinkOf (settings NoVerification) server,
          clientBuffer = clientBuffer',
          serverBuffer = serverBuffer'
        }
  where
    closeBoth (a, b) = Socket.close a *> Socket.close b

-- | A server's connection with the certificate chain and key.
serverConnection :: ByteString -> ByteString -> IO (Ptr RustlsConnection)
serverConnection chain key = do
  certified :: Ptr CertifiedKey <-
    BS.unsafeUseAsCStringLen chain $ \(chainBytes, chainLength) ->
      BS.unsafeUseAsCStringLen key $ \(keyBytes, keyLength) ->
        built (c_certified_key_build (castPtr chainBytes) (fromIntegral chainLength) (castPtr keyBytes) (fromIntegral keyLength))
  builder <- c_server_config_builder_new
  withArray [certified] $ \keys -> succeeds (c_server_config_builder_set_certified_keys builder (castPtr keys) 1)
  config <- built (c_server_config_builder_build builder)
  built (c_server_connection_new config)
  where
    built :: (Ptr () -> IO Word32) -> IO (Ptr a)
    built function = alloca $ \out -> succeeds (function (castPtr out)) *> peek out
    succeeds function = do
      result <- function
      unless (result == 7000) (throwIO (TlsError ("the test server's configuration failed with " <> show result)))

data CertifiedKey

data ServerConfigBuilder

data ServerConfig

foreign import capi unsafe "rustls.h rustls_certified_key_build"
  c_certified_key_build :: Ptr Word8 -> CSize -> Ptr Word8 -> CSize -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_config_builder_new"
  c_server_config_builder_new :: IO (Ptr ServerConfigBuilder)

foreign import capi unsafe "rustls.h rustls_server_config_builder_set_certified_keys"
  c_server_config_builder_set_certified_keys :: Ptr ServerConfigBuilder -> Ptr () -> CSize -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_config_builder_build"
  c_server_config_builder_build :: Ptr ServerConfigBuilder -> Ptr () -> IO Word32

foreign import capi unsafe "rustls.h rustls_server_connection_new"
  c_server_connection_new :: Ptr ServerConfig -> Ptr () -> IO Word32
