{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Tests against a real PostgreSQL. A temporary cluster is created with the
-- @initdb@ and @pg_ctl@ found on the path, so the PostgreSQL version is the
-- one they come from.
module Main (main) where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Exception (ErrorCall (..), IOException, bracket, bracket_, displayException, finally, throwIO, try)
import Control.Monad (forever, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.Functor (($>))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.IORef (modifyIORef, newIORef, readIORef, writeIORef)
import Data.Int (Int16, Int32, Int64)
import Data.List (intercalate, isInfixOf, isPrefixOf)
import Data.Maybe (isJust)
import Data.Scientific (Scientific)
import Data.Text (Text)
import Data.Text.Encoding (encodeUtf8)
import Data.Time (Day, DiffTime, UTCTime (..), fromGregorian)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Database.PostgreSQL.ConnectionString
import Database.PostgreSQL.Decoding
import Database.PostgreSQL.Diagnostics
import qualified Database.PostgreSQL.Encoders as Encoders
import Database.PostgreSQL.Protocol
  ( FormatCode (..),
    Notification (..),
    Parameter (..),
    ProcessId,
    Scalar (..),
    TransactionStatus (..),
    protocolVersion3_0,
    protocolVersion3_2,
  )
import Database.PostgreSQL.Protocol.Codec (ByteStream (..))
import qualified Database.PostgreSQL.Pipeline as Pipeline
import Database.PostgreSQL.Session
import qualified Database.PostgreSQL.Snippet as Snippet
import qualified Database.PostgreSQL.Transaction as Tx
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Transport (Address (..), Channel (..), StreamLengthError, TlsChannel (..), TlsClient (..))
import GHC.Clock (getMonotonicTime)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import System.Directory (copyFile, getTemporaryDirectory, removeDirectoryRecursive)
import System.FilePath (takeDirectory, (</>))
import System.Mem (performMajorGC)
import System.Posix.Files (groupReadMode, otherReadMode, ownerReadMode, setFileMode, unionFileModes)
import System.Posix.Temp (mkdtemp)
import System.Posix.Types (FileMode)
import System.Timeout (timeout)
import TestCluster (withCluster, certificate)
import Test.Hspec

main :: IO ()
main = withCluster $ \socketPath port -> hspec $ do
  let settings user password =
        (connectSettings (UnixAddress socketPath) user)
          { connectPassword = password,
            connectDatabase = Just "postgres"
          }
      withSession = bracket (fst <$> connect (settings "postgres" Nothing)) close
      withSessionInfo action =
        bracket (connect (settings "postgres" Nothing)) (close . fst) (uncurry action)

  describe "connect" $ do
    it "connects a trusted user and reports the server's parameters" $ do
      (session, info) <- connect (settings "postgres" Nothing)
      lookup "server_version" (serverParameters info) `shouldSatisfy` isJust
      serverBackendKey info `shouldSatisfy` isJust
      serverProtocolVersion info `shouldBe` protocolVersion3_0
      close session
    it "authenticates with SCRAM-SHA-256" $
      connectAndClose (settings "native_pg_scram" (Just "scram secret"))
    it "authenticates with MD5" $
      connectAndClose (settings "native_pg_md5" (Just "md5 secret"))
    it "authenticates with a cleartext password" $
      connectAndClose (settings "native_pg_cleartext" (Just "cleartext secret"))
    it "reports a wrong password" $
      connect (settings "native_pg_scram" (Just "wrong")) `shouldThrow` rejectedWith "28P01"
    it "reports a missing password" $
      connect (settings "native_pg_scram" Nothing) `shouldThrow` authenticationError
    it "reports a missing database" $
      connect ((settings "postgres" Nothing) {connectDatabase = Just "missing"}) `shouldThrow` rejectedWith "3D000"
    it "uses protocol 3.2 where the server has it, and 3.0 otherwise" $ do
      (session, info) <- connect ((settings "postgres" Nothing) {connectProtocolVersion = protocolVersion3_2})
      close session
      let major = fmap fst . Char8.readInt =<< lookup "server_version" (serverParameters info)
      serverProtocolVersion info
        `shouldBe` (if maybe False (>= 18) major then protocolVersion3_2 else protocolVersion3_0)

  describe "close" $ do
    let encrypted =
          (connectSettings (TcpAddress "127.0.0.1" port) "postgres")
            { connectDatabase = Just "postgres",
              connectSslMode = SslRequire,
              connectTlsClient = Just tlsClient
            }
        closed = (== SessionUnusable "the session is closed")
    for_ [("without TLS", settings "postgres" Nothing), ("with TLS", encrypted)] $ \(label, connecting) ->
      describe label $ around (bracket (fst <$> connect connecting) close) $ do
        it "marks the session unusable and rejects operations after closing" $ \session -> do
          isUsable session `shouldReturn` True
          close session
          isUsable session `shouldReturn` False
          query session "SELECT 1" [] `shouldThrow` closed
          prepare session "closed" "SELECT 1" [] `shouldThrow` closed
          closeStatement session "closed" `shouldThrow` closed
          runBatch session id (pure ()) `shouldThrow` closed
          awaitNotification session `shouldThrow` closed
        it "can close repeatedly, including during bracket cleanup" $ \session -> do
          close session
          timeout 5000000 (close session *> close session) `shouldReturn` Just ()
          isUsable session `shouldReturn` False
        it "can close a broken session repeatedly" $ \session -> do
          let interrupted () _ = throwIO (ErrorCall "interrupted")
          execute session (statement "SELECT 1" []) interrupted () `shouldThrow` errorCall "interrupted"
          isUsable session `shouldReturn` False
          timeout 5000000 (close session *> close session) `shouldReturn` Just ()
          query session "SELECT 1" [] `shouldThrow` closed
        it "can close after an operation is interrupted asynchronously" $ \session -> do
          timeout 100000 (awaitNotification session) `shouldReturn` Nothing
          isUsable session `shouldReturn` False
          timeout 5000000 (close session *> close session) `shouldReturn` Just ()
          query session "SELECT 1" [] `shouldThrow` closed
        it "rejects closing during an operation without disrupting it" $ \session -> do
          let step () row = do
                close session `shouldThrow` (== SessionUnusable "the session is busy with another operation")
                void (columns row)
          execute session (statement "SELECT 1" []) step () `shouldReturn` (Right ((), "SELECT 1"), TxIdle)
          isUsable session `shouldReturn` True
          query session "SELECT 2" [] `shouldReturn` (Right ([[Just "2"]], "SELECT 1"), TxIdle)

    -- Observe cleanup through the public channel API, including failures
    -- before and after the TLS connection is freed.
    let withCloseHooks beforeSend afterClose check = do
          closing <- newIORef False
          closes <- newIORef (0 :: Int)
          let client = TlsClient $ \tls serverName sock -> do
                opened <- startTlsChannel tlsClient tls serverName sock
                pure opened
                  { tlsChannel = \pendingOutput size ->
                      let channel = tlsChannel opened pendingOutput size
                       in channel
                            { channelSend = \output -> do
                                ending <- readIORef closing
                                when ending beforeSend
                                channelSend channel output,
                              channelClose =
                                (modifyIORef closes (+ 1) *> channelClose channel)
                                  `finally` afterClose
                            }
                  }
          bracket (fst <$> connect (encrypted {connectTlsClient = Just client})) close $ \session -> do
            writeIORef closing True
            _ <- check session
            isUsable session `shouldReturn` False
            close session
            query session "SELECT 1" [] `shouldThrow` closed
          readIORef closes `shouldReturn` 1
    it "closes the channel once when sending Terminate throws" $
      withCloseHooks (throwIO (ErrorCall "terminate failed")) (pure ()) $ \session ->
        close session `shouldThrow` errorCall "terminate failed"
    it "does not retry channel cleanup if it throws" $
      withCloseHooks (pure ()) (throwIO (ErrorCall "cleanup failed")) $ \session ->
        close session `shouldThrow` errorCall "cleanup failed"
    it "closes the channel once when sending Terminate is interrupted" $
      withCloseHooks (threadDelay 10000000) (pure ()) $ \session ->
        timeout 100000 (close session) `shouldReturn` Nothing

  describe "channel-binding data from TLS setup" $ do
    let suppliedBinding supplied mode user =
          (connectSettings (TcpAddress "127.0.0.1" port) user)
            { connectDatabase = Just "postgres",
              connectPassword = Just "scram secret",
              connectSslMode = SslRequire,
              connectChannelBinding = mode,
              connectTlsClient = Just (TlsClient $ \tls serverName sock -> do
                opened <- startTlsChannel tlsClient tls serverName sock
                pure opened {tlsServerEndPoint = supplied})
            }
        withBinding supplied mode user =
          bracket (fst <$> connect (suppliedBinding supplied mode user)) close
        bindingFailure = Just (Left "unsupported certificate hash")
    it "uses the supplied binding bytes in SCRAM-PLUS" $
      connectAndClose (suppliedBinding (Just (Right "wrong binding")) ChannelBindingRequire "native_pg_scram")
        `shouldThrow` rejectedWith "28000"
    for_ [("preferred", ChannelBindingPrefer), ("required", ChannelBindingRequire)] $ \(label, mode) ->
      it ("reports a binding error when SCRAM-PLUS is " <> label) $
        connectAndClose (suppliedBinding bindingFailure mode "native_pg_scram")
          `shouldThrow` (== AuthenticationError "unsupported certificate hash")
    it "ignores a binding error when channel binding is disabled" $
      withBinding bindingFailure ChannelBindingDisable "native_pg_scram" $ \session ->
        query session "SELECT current_user" []
          `shouldReturn` (Right ([[Just "native_pg_scram"]], "SELECT 1"), TxIdle)
    it "does not require binding data for non-SCRAM authentication" $
      withBinding bindingFailure ChannelBindingPrefer "postgres" $ \session ->
        query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "allows unbound SCRAM when binding is unavailable and optional" $
      withBinding Nothing ChannelBindingPrefer "native_pg_scram" $ \session ->
        query session "SELECT current_user" []
          `shouldReturn` (Right ([[Just "native_pg_scram"]], "SELECT 1"), TxIdle)
    it "rejects required binding when the TLS client supplies none" $
      connectAndClose (suppliedBinding Nothing ChannelBindingRequire "native_pg_scram")
        `shouldThrow` (== AuthenticationError "channel binding is required, but server did not offer an authentication method that supports channel binding")
    it "ignores binding errors on a cancellation connection" $
      withSessionInfo $ \_ info -> do
        Just key <- pure (serverBackendKey info)
        cancel (suppliedBinding bindingFailure ChannelBindingRequire "postgres") key

  describe "connection strings" $ do
    let directory = takeDirectory socketPath
        -- Without the environment, so that the PG* variables of the shell
        -- running the tests don't change them.
        connectWith string =
          either fail (connectFirst (\connecting -> connecting {connectTlsClient = Just tlsClient})) (parseConnectionString noDefaults (const Nothing) (Just "postgres") string)
        withConnection string = bracket (connectWith string) (\(session, _, _) -> close session)
        connectFailure string = do
          result <- try (connectWith string)
          case result of
            Left failure -> pure (displayException (failure :: ConnectFailure))
            Right (session, _, _) -> close session *> fail "connected"
        local extra = "host=" <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres " <> extra
        value session sql = fst <$> query session sql []
    it "connects with a URI that names the socket directory" $
      withConnection ("postgresql:///postgres?host=" <> Char8.pack directory <> "&port=" <> Char8.pack (show port)) $ \(session, _, host) -> do
        hostName host `shouldBe` directory
        value session "SELECT 1" `shouldReturn` Right ([[Just "1"]], "SELECT 1")
    it "tries the hosts in order" $
      withConnection ("host=/nonexistent," <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres") $ \(_, _, host) ->
        hostName host `shouldBe` directory
    it "words each host's failure as libpq does" $
      connectFailure "host=/nonexistent-1,/nonexistent-2"
        `shouldReturn` intercalate
          "\n"
          [ "connection to server on socket \"" <> path <> "/.s.PGSQL.5432\" failed: No such file or directory\n\tIs the server running locally and accepting connections on that socket?"
          | path <- ["/nonexistent-1", "/nonexistent-2"]
          ]
    it "words a host name that doesn't resolve as libpq does" $ do
      failure <- connectFailure "host=no-such-host.invalid"
      failure `shouldSatisfy` isPrefixOf "could not translate host name \"no-such-host.invalid\" to address: "
    it "names the application, with the fallback name if none is given" $ do
      withConnection (local "fallback_application_name=fallback") $ \(session, _, _) ->
        value session "SELECT current_setting('application_name')" `shouldReturn` Right ([[Just "fallback"]], "SELECT 1")
      withConnection (local "fallback_application_name=fallback application_name=app") $ \(session, _, _) ->
        value session "SELECT current_setting('application_name')" `shouldReturn` Right ([[Just "app"]], "SELECT 1")
    it "connects only to a server with the session attributes asked for" $ do
      let readOnly = "options='-c default_transaction_read_only=on' "
      withConnection (local (readOnly <> "target_session_attrs=read-only")) (\_ -> pure ())
      connectFailure (local (readOnly <> "target_session_attrs=read-write"))
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: session is read-only")
      withConnection (local "target_session_attrs=primary") (\_ -> pure ())
      connectFailure (local "target_session_attrs=standby")
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: server is not in hot standby mode")
      withConnection (local "target_session_attrs=prefer-standby") (\_ -> pure ())
    describe "with TLS" $ do
      let tcp extra = "host=127.0.0.1 port=" <> Char8.pack (show port) <> " dbname=postgres " <> extra
          encrypted session = value session "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
      it "encrypts with sslmode=require, without verifying the server" $ do
        withConnection (tcp "sslmode=require") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
      it "sends a large batch while reading its responses over TLS" $
        withConnection (tcp "sslmode=require") $ \(session, _, _) ->
          batchWhileReading session
      it "doesn't encrypt over a Unix-domain socket, as libpq doesn't" $ do
        withConnection (local "sslmode=require") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "f"]], "SELECT 1")
      it "fails if the server doesn't take TLS" $ do
        withPlaintextServer $ \plaintext ->
          connectFailure ("host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require")
            `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show plaintext <> " failed: server does not support SSL, but SSL was required")
      it "fails as libpq without TLS does, without a TLS client" $ do
        result <- try (either fail (connectFirst id) (parseConnectionString noDefaults (const Nothing) (Just "postgres") (tcp "sslmode=require")))
        either (pure . displayException @ConnectFailure) (\(session, _, _) -> close session *> fail "connected") result
          `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: sslmode value \"require\" invalid when SSL support is not compiled in")
      it "encrypts by default, with sslmode=prefer, as libpq does" $ do
        withConnection (tcp "") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
      it "falls back to plaintext with sslmode=prefer when the server rejects TLS" $ do
        withConnection (tcp "user=native_pg_plaintext") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "f"]], "SELECT 1")
      it "tries plaintext first with sslmode=allow, then TLS" $ do
        withConnection (tcp "sslmode=allow user=native_pg_plaintext") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "f"]], "SELECT 1")
        withConnection (tcp "sslmode=allow") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
      it "connects in plaintext with sslmode=prefer without a TLS client, as libpq without TLS does" $ do
        connected <- either fail (connectFirst id) (parseConnectionString noDefaults (const Nothing) (Just "postgres") (tcp "user=native_pg_plaintext"))
        (\(session, _, _) -> encrypted session <* close session) connected `shouldReturn` Right ([[Just "f"]], "SELECT 1")
      it "verifies the server's certificate and name with sslmode=verify-full" $ do
        withConnection (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=localhost hostaddr=127.0.0.1")) $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
        withConnection (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt"))) $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
      it "refuses a server whose certificate no root signed, or that has another name" $ do
        let at host = "connection to server at \"" <> host <> "\" (127.0.0.1), port " <> show port <> " failed: "
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "other-ca.crt") <> " host=localhost hostaddr=127.0.0.1"))
          `shouldReturn` (at "localhost" <> "SSL error: invalid peer certificate: UnknownIssuer")
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=other.example hostaddr=127.0.0.1"))
          `shouldReturn` (at "other.example" <> "SSL error: invalid peer certificate: NotValidForName")
      it "verifies against the system's root certificates with sslrootcert=system" $ do
        failure <- connectFailure (tcp "sslmode=verify-full sslrootcert=system host=localhost hostaddr=127.0.0.1")
        -- The platforms' verifiers word it differently.
        failure `shouldSatisfy` isPrefixOf ("connection to server at \"localhost\" (127.0.0.1), port " <> show port <> " failed: SSL error: ")
      it "needs the root certificate file, as libpq does" $ do
        connectFailure (tcp "sslmode=verify-full sslrootcert=/nonexistent/root.crt host=localhost hostaddr=127.0.0.1")
          `shouldReturn` ( "connection to server at \"localhost\" (127.0.0.1), port "
                             <> show port
                             <> " failed: root certificate file \"/nonexistent/root.crt\" does not exist\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification."
                         )
      it "starts TLS directly with sslnegotiation=direct, as PostgreSQL 17 takes it" $ do
        withConnection (tcp "sslnegotiation=direct sslmode=require") $ \(session, _, _) ->
          encrypted session `shouldReturn` Right ([[Just "t"]], "SELECT 1")
      it "cancels over TLS, and asks for TLS to cancel, as libpq does since PostgreSQL 17" $ do
        let tlsSettings host connectionString =
              either fail (\parsed -> pure ((hostConnectSettings parsed host) {connectTlsClient = Just tlsClient})) $
                parseConnectionString noDefaults (const Nothing) (Just "postgres") connectionString
        withConnection (tcp "sslmode=require") $ \(session, info, host) -> do
          Just key <- pure (serverBackendKey info)
          cancelling <- tlsSettings host (tcp "sslmode=require")
          _ <- forkIO (threadDelay 300000 *> cancel cancelling key)
          (result, _) <- query session "SELECT pg_sleep(30)" []
          either errorCode (const Nothing) result `shouldBe` Just "57014"
          withPlaintextServer $ \plaintext -> do
            let refused = "host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require"
            refusing <- tlsSettings (Host "127.0.0.1" Nothing plaintext) refused
            cancel refusing key `shouldThrow` (== TlsFailure "server does not support SSL, but SSL was required")
      it "binds SCRAM to TLS with channel_binding=require" $ do
        withConnection (tcp "user=native_pg_scram password='scram secret' sslmode=require channel_binding=require") $ \(session, _, _) ->
          value session "SELECT current_user" `shouldReturn` Right ([[Just "native_pg_scram"]], "SELECT 1")
      it "fails as libpq does with channel_binding=require when the server doesn't bind SCRAM to TLS" $ do
        let failure at reason = "connection to server " <> at <> " failed: " <> reason
            socket = "on socket \"" <> socketPath <> "\""
            tlsAt = "at \"127.0.0.1\", port " <> show port
        connectFailure (local "user=native_pg_scram password='scram secret' channel_binding=require")
          `shouldReturn` failure socket "channel binding is required, but server did not offer an authentication method that supports channel binding"
        connectFailure (local "user=native_pg_md5 password='md5 secret' channel_binding=require")
          `shouldReturn` failure socket "channel binding required but not supported by server's authentication request"
        connectFailure (tcp "sslmode=require channel_binding=require")
          `shouldReturn` failure tlsAt "channel binding required, but server authenticated client without channel binding"
      it "negotiates a version between ssl_min_protocol_version and ssl_max_protocol_version" $ do
        let version extra = withConnection (tcp ("sslmode=require " <> extra)) $ \(session, _, _) ->
              value session "SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
        version "" `shouldReturn` Right ([[Just "TLSv1.3"]], "SELECT 1")
        version "ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ([[Just "TLSv1.2"]], "SELECT 1")
        version "ssl_min_protocol_version=TLSv1 ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ([[Just "TLSv1.2"]], "SELECT 1")
      it "checks revocation lists, from sslcrl or sslcrldir" $ do
        let verified extra = tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=localhost hostaddr=127.0.0.1 " <> extra)
            revoked = "connection to server at \"localhost\" (127.0.0.1), port " <> show port <> " failed: SSL error: invalid peer certificate: Revoked"
        withConnection (verified ("sslcrl=" <> Char8.pack (certificate "empty.crl"))) (\_ -> pure ())
        connectFailure (verified ("sslcrl=" <> Char8.pack (certificate "revoked.crl"))) `shouldReturn` revoked
        temporary <- getTemporaryDirectory
        bracket (mkdtemp (temporary </> "native-pg-crl-")) removeDirectoryRecursive $ \lists -> do
          -- As OpenSSL's c_rehash names them.
          copyFile (certificate "revoked.crl") (lists </> "0123abcd.r0")
          connectFailure (verified ("sslcrldir=" <> Char8.pack lists)) `shouldReturn` revoked
        -- As libpq, a missing file is no list.
        withConnection (verified "sslcrl=/nonexistent/root.crl") (\_ -> pure ())
      it "authenticates with a client certificate" $ do
        withKeyCopy ownerReadMode $ \key -> do
          let certified = "user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key
          withConnection (tcp certified) $ \(session, _, _) ->
            value session "SELECT current_user" `shouldReturn` Right ([[Just "native_pg_certificate"]], "SELECT 1")
        failure <- connectFailure (tcp "user=native_pg_certificate sslmode=require sslcert=/nonexistent/client.crt sslkey=/nonexistent/client.key")
        failure `shouldSatisfy` isInfixOf "connection requires a valid client certificate"
      it "needs a client certificate's key, readable only by its owner, as libpq does" $ do
        let certified key = tcp ("user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key)
            at = "connection to server at \"127.0.0.1\", port " <> show port <> " failed: "
        connectFailure (certified "/nonexistent/client.key")
          `shouldReturn` (at <> "certificate present, but not private key file \"/nonexistent/client.key\"")
        withKeyCopy (ownerReadMode `unionFileModes` groupReadMode `unionFileModes` otherReadMode) $ \key ->
          connectFailure (certified key)
            `shouldReturn` (at <> "private key file \"" <> key <> "\" has group or world access; file must have permissions u=rw (0600) or less if owned by the current user, or permissions u=rw,g=r (0640) or less if owned by root")
      it "sends a batch larger than the sockets hold while reading its results" $ do
        withConnection (tcp "sslmode=require") $ \(session, _, _) -> do
          let collect execution = batched id execution (\rows row -> (: rows) <$> columns row) []
              large = collect (statement "SELECT repeat('x', 1000000)" [])
              long = collect (statement "SELECT length($1::text)" [Just (Char8.replicate 2000000 'y')])
          outcome <- timeout 10000000 (runBatch session id ((\(rows, _) _ -> rows) <$> large <*> long))
          fmap fst outcome `shouldBe` Just (Right [[Just (Char8.replicate 1000000 'x')]])
    it "gives up on a host after connect_timeout" $
      withSilentServer $ \silent -> do
        started <- getMonotonicTime
        connectFailure ("host=" <> Char8.pack silent <> " connect_timeout=2")
          `shouldReturn` ("connection to server on socket \"" <> silent <> "/.s.PGSQL.5432\" failed: timeout expired")
        elapsed <- subtract started <$> getMonotonicTime
        elapsed `shouldSatisfy` (\seconds -> seconds >= 2 && seconds < 5)

  describe "execute" $ around withSession $ do
    it "runs an unprepared statement with parameters" $ \session ->
      query session "SELECT $1::int + 1, $2::text" [Just "41", Just "x"]
        `shouldReturn` (Right ([[Just "42", Just "x"]], "SELECT 1"), TxIdle)
    it "passes NULL parameters and returns NULL columns" $ \session ->
      query session "SELECT $1::text, 1" [Nothing]
        `shouldReturn` (Right ([[Nothing, Just "1"]], "SELECT 1"), TxIdle)
    it "folds many rows" $ \session -> do
      (result, _) <- execute session (statement "SELECT generate_series(1, 10000)" []) (\count row -> (count + 1 :: Int) <$ columns row) 0
      result `shouldBe` Right (10000, "SELECT 10000")
    it "runs a prepared statement until it's closed" $ \session -> do
      prepare session "add_one" "SELECT $1::int + 1" [] `shouldReturn` (Right (), TxIdle)
      prepared session "add_one" [Just "1"] `shouldReturn` (Right ([[Just "2"]], "SELECT 1"), TxIdle)
      prepared session "add_one" [Just "41"] `shouldReturn` (Right ([[Just "42"]], "SELECT 1"), TxIdle)
      closeStatement session "add_one" `shouldReturn` (Right (), TxIdle)
      (result, _) <- prepared session "add_one" [Just "1"]
      either errorCode (const Nothing) result `shouldBe` Just "26000"
    it "returns an error and stays usable" $ \session -> do
      (result, status) <- query session "SELECT 1/0" []
      either errorCode (const Nothing) result `shouldBe` Just "22012"
      status `shouldBe` TxIdle
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "reports the transaction status" $ \session -> do
      fmap snd (query session "BEGIN" []) `shouldReturn` TxInTransaction
      fmap snd (query session "SELECT 1/0" []) `shouldReturn` TxFailedTransaction
      fmap snd (query session "ROLLBACK" []) `shouldReturn` TxIdle
    it "streams a parameter from its source" $ \session -> do
      let pattern = BS.concat (replicate 4096 "0123456789abcdef")
          chunks = 128
      source <- chunkSource (replicate chunks pattern)
      (result, _) <-
        execute
          session
          (Execution (Unprepared "SELECT $1::text = repeat('0123456789abcdef', 524288)" []) [] [Just (ParameterStream (ByteStream (chunks * BS.length pattern) source))] [])
          (\rows row -> (: rows) <$> columns row)
          []
      result `shouldBe` Right ([[Just "t"]], "SELECT 1")
    it "sends a parameter in chunks, short and long, larger than the socket's buffers" $ \session -> do
      let pattern = BS.concat (replicate 4096 "0123456789abcdef")
          chunks = concatMap (\chunk -> [BS.take 100 chunk, BS.drop 100 chunk]) (replicate 128 pattern)
      (result, _) <-
        execute
          session
          (Execution (Unprepared "SELECT $1::text = repeat('0123456789abcdef', 524288)" []) [] [Just (ParameterChunks chunks)] [])
          (\rows row -> (: rows) <$> columns row)
          []
      result `shouldBe` Right ([[Just "t"]], "SELECT 1")
    it "streams a parameter to a prepared statement, whether it returns rows or not" $ \session -> do
      _ <- query session "CREATE TEMPORARY TABLE streamed (value text)" []
      prepare session "insert_streamed" "INSERT INTO streamed VALUES ($1)" [] `shouldReturn` (Right (), TxIdle)
      prepare session "length_of" "SELECT length($1::text)" [] `shouldReturn` (Right (), TxIdle)
      let streamed name = do
            source <- chunkSource ["abc", "def"]
            collectRows session (Execution (Prepared name) [] [Just (ParameterStream (ByteStream 6 source))] [])
      streamed "insert_streamed" `shouldReturn` (Right ([], "INSERT 0 1"), TxIdle)
      streamed "length_of" `shouldReturn` (Right ([[Just "6"]], "SELECT 1"), TxIdle)
    it "counts a slow parameter stream against statement_timeout, whether the statement is prepared or not" $ \session -> do
      _ <- query session "SET statement_timeout = '200ms'" []
      prepare session "slow_length" "SELECT length($1::text)" [] `shouldReturn` (Right (), TxIdle)
      let slowly target = do
            source <- chunkSource (replicate 4 (Char8.replicate 1000 'y'))
            let delayed = threadDelay 150000 *> source
            (result, _) <- execute session (Execution target [] [Just (ParameterStream (ByteStream 4000 delayed))] []) (\() _ -> pure ()) ()
            pure (either errorCode (const Nothing) result)
      slowly (Unprepared "SELECT length($1::text)" []) `shouldReturn` Just "57014"
      slowly (Prepared "slow_length") `shouldReturn` Just "57014"
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "sends a batch while reading its results, as the server answers before reading all of it" batchWhileReading
    it "closes a session broken while sending, without waiting for the server to read" $ \session -> do
      let slowly = batched id (statement "SELECT repeat('x', 1000000)" []) (\() _ -> threadDelay 60000000) ()
          long = batched id (statement "SELECT length($1::text)" [Just (Char8.replicate 2000000 'y')]) (\() _ -> pure ()) ()
      void $ timeout 1000000 (runBatch session id (slowly *> long))
      timeout 5000000 (close session) `shouldReturn` Just ()
    it "takes a value larger than the read buffer whole" $ \session -> do
      (result, _) <- query session "SELECT repeat('y', 100000)" []
      result `shouldBe` Right ([[Just (Char8.replicate 100000 'y')]], "SELECT 1")
    it "streams a column to a slow consumer without holding it in memory" $ \session -> do
      enabled <- getRTSStatsEnabled
      unless enabled $ pendingWith "needs +RTS -T"
      let size = 64 * 1024 * 1024
      (result, _) <-
        execute session (statement ("SELECT repeat('x', " <> Char8.pack (show size) <> ")") []) (\_ row -> Just <$> consumeSlowly row) Nothing
      fmap fst result `shouldBe` Right (Just (size, True, True))
    it "rejects a row used after its step" $ \session -> do
      saved <- newIORef Nothing
      _ <- execute session (statement "SELECT 1" []) (\() row -> writeIORef saved (Just row)) ()
      Just row <- readIORef saved
      nextColumn row `shouldThrow` rowMisuse
    it "is unusable while busy, and broken after a failed operation" $ \session -> do
      let nested () _ = void (execute session (statement "SELECT 2" []) (\() _ -> pure ()) ())
      execute session (statement "SELECT 1" []) nested () `shouldThrow` sessionUnusable
      query session "SELECT 1" [] `shouldThrow` sessionUnusable
    it "tells whether it's usable" $ \session -> do
      isUsable session `shouldReturn` True
      _ <- query session "SELECT 1" []
      isUsable session `shouldReturn` True
      let interrupted () _ = do
            usableMeanwhile <- isUsable session
            usableMeanwhile `shouldBe` False
            throwIO (ErrorCall "interrupted")
      execute session (statement "SELECT 1" []) interrupted () `shouldThrow` errorCall "interrupted"
      isUsable session `shouldReturn` False
  describe "decodeRow" $ around withSession $ do
    it "decodes integers, booleans, floats and numerics" $ \session ->
      decodeSingle @'[Int16, Int32, Int64, Bool, Float, Double, Scientific] session "SELECT 1::int2, -2::int4, 3::int8, true, 1.5::float4, 2.5::float8, 3.25::numeric" (\a b c d e f g -> pure (a, b, c, d, e, f, g))
        `shouldReturn` Right (1, -2, 3, True, 1.5, 2.5, 3.25)
    it "decodes text, bytea, char, uuid and json" $ \session ->
      decodeSingle @'[Text, ByteString, Char, UUID, Json, Jsonb] session "SELECT 'héllo'::text, '\\xdeadbeef'::bytea, 'x'::char, '00000000-0000-0000-0000-000000000001'::uuid, '{\"a\":1}'::json, '{\"a\":1}'::jsonb" (\a b c d e f -> pure (a, b, c, d, e, f))
        `shouldReturn` Right ("héllo", BS.pack [0xde, 0xad, 0xbe, 0xef], 'x', UUID.fromWords 0 0 0 1, Json "{\"a\":1}", Jsonb "{\"a\": 1}")
    it "decodes dates, timestamps and intervals" $ \session ->
      decodeSingle @'[Day, UTCTime, DiffTime] session "SELECT '2024-01-02'::date, '2024-01-02 03:04:05+00'::timestamptz, '2 hours'::interval" (\a b c -> pure (a, b, c))
        `shouldReturn` Right (fromGregorian 2024 1 2, UTCTime (fromGregorian 2024 1 2) 11045, 7200)
    it "decodes NULL into Maybe and rejects it otherwise" $ \session -> do
      decodeSingle @'[Maybe Int32, Maybe Int32] session "SELECT NULL::int4, 1::int4" (curry pure)
        `shouldReturn` Right (Nothing, Just 1)
      decodeSingle @'[Int32] session "SELECT NULL::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnDecodingFailed 1 "unexpected NULL")
    it "decodes arrays, with NULL elements into Maybe" $ \session ->
      decodeSingle @'[[Int32], [Maybe Text]] session "SELECT ARRAY[1, 2, 3]::int4[], ARRAY['a', NULL]::text[]" (curry pure)
        `shouldReturn` Right ([1, 2, 3], [Just "a", Nothing])
    it "decodes composites and arrays of them" $ \session ->
      decodeSingle @'[Pair, [Pair]] session "SELECT ROW(1, 'a'::text), ARRAY[ROW(2, 'b'::text), ROW(3, 'c'::text)]" (curry pure)
        `shouldReturn` Right (Pair 1 "a", [Pair 2 "b", Pair 3 "c"])
    it "reports a wrong column count, or a column of another type, and stays usable" $ \session -> do
      decodeSingle @'[Int32] session "SELECT 1::int4, 2::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnCountMismatch 1 2)
      decodeSingle @'[Int64] session "SELECT 1::int4" (\_ -> pure ())
        `shouldReturn` Left (ColumnDecodingFailed 1 "int8 needs 8 bytes, got 4")
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "streams a last column after decoding the others" $ \session ->
      decodeSingleStreaming @'[Int32] session "SELECT 200::int4, repeat('x', 1000000)" (\status body -> (status,) <$> streamedLength body)
        `shouldReturn` Right (200, 1000000)

  describe "errors" $ around withSession $ do
    it "reports a missing relation with its position in the query" $ \session -> do
      (result, _) <- query session "SELECT * FROM missing_table" []
      fmap (\d -> (diagnosticSeverity d, diagnosticCode d, diagnosticMessage d, diagnosticPosition d)) (failed result)
        `shouldBe` Just (SeverityError, "42P01", "relation \"missing_table\" does not exist", Just 15)
    it "reports a constraint violation's detail, table and constraint" $ \session -> do
      _ <- query session "CREATE TEMPORARY TABLE item (id int PRIMARY KEY)" []
      _ <- query session "INSERT INTO item VALUES (1)" []
      (result, _) <- query session "INSERT INTO item VALUES (1)" []
      fmap (\d -> (diagnosticCode d, diagnosticDetail d, diagnosticTable d, diagnosticConstraint d)) (failed result)
        `shouldBe` Just ("23505", Just "Key (id)=(1) already exists.", Just "item", Just "item_pkey")
    it "reports a raised error's own SQLSTATE, detail, hint and context" $ \session -> do
      (result, _) <- query session "DO $$ BEGIN RAISE EXCEPTION 'custom' USING ERRCODE = 'PGRST', DETAIL = 'the detail', HINT = 'the hint'; END $$" []
      fmap (\d -> (diagnosticCode d, diagnosticMessage d, diagnosticDetail d, diagnosticHint d, isJust (diagnosticContext d))) (failed result)
        `shouldBe` Just ("PGRST", "custom", Just "the detail", Just "the hint", True)
    it "throws when the server ends the session, which is then broken" $ \session -> do
      outcome <- try (query session "SELECT pg_terminate_backend(pg_backend_pid())" [])
      case outcome of
        Left (ConnectionTerminated (ServerError d)) -> (diagnosticSeverity d, diagnosticCode d) `shouldBe` (SeverityFatal, "57P01")
        Left other -> expectationFailure ("expected ConnectionTerminated, got " <> show other)
        Right _ -> expectationFailure "expected ConnectionTerminated"
      query session "SELECT 1" [] `shouldThrow` sessionUnusable

  describe "cancel" $
    it "cancels a running statement, and the session stays usable" $
      withSessionInfo $ \session info -> do
        Just key <- pure (serverBackendKey info)
        _ <- forkIO (threadDelay 300000 *> cancel (settings "postgres" Nothing) key)
        started <- getMonotonicTime
        (result, status) <- query session "SELECT pg_sleep(30)" []
        finished <- getMonotonicTime
        (either errorCode (const Nothing) result, status) `shouldBe` (Just "57014", TxIdle)
        finished - started `shouldSatisfy` (< 10)
        query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)

  describe "notifications" $ do
    it "delivers a notification from another session" $
      withSessionInfo $ \listener _ ->
        withSessionInfo $ \notifier notifierInfo -> do
          _ <- query listener "LISTEN events" []
          _ <- query notifier "NOTIFY events, 'hello'" []
          Notification sender channel payload <- awaitNotification listener
          (Just sender, channel, payload) `shouldBe` (processIdOf notifierInfo, "events", "hello")
    it "keeps notifications that arrive during statements, in order" $
      withSessionInfo $ \session _ -> do
        _ <- query session "LISTEN events" []
        _ <- query session "NOTIFY events, 'first'" []
        _ <- query session "NOTIFY events, 'second'" []
        payloads <- traverse (const (notificationPayload <$> awaitNotification session)) [1 :: Int, 2]
        payloads `shouldBe` ["first", "second"]
    it "throws when a listening session is terminated" $
      withSessionInfo $ \listener listenerInfo ->
        withSessionInfo $ \administrator _ -> do
          _ <- query listener "LISTEN events" []
          Just (Scalar pid) <- pure (processIdOf listenerInfo)
          _ <- query administrator "SELECT pg_terminate_backend($1::int)" [Just (Char8.pack (show pid))]
          awaitNotification listener `shouldThrow` connectionTerminated

  describe "transactions" $ around withSession $ do
    let createItems session = void (query session "CREATE TEMP TABLE tx_items (n int)" [])
        countItems = Tx.Statement "SELECT count(*) FROM tx_items" (const []) (Tx.singleRow (Tx.row @'[Int64] id)) False
        insertItem = Tx.Statement "INSERT INTO tx_items VALUES ($1)" (\value -> [Tx.Param Nothing TextFormat (Just (ParameterBytes value))]) Tx.noResult False
        items = Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () countItems)
        failsWith check = either (\(Tx.QueryError _ failure) -> check failure) (const False)
    it "commits" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem) session `shouldReturn` Right ()
      items session `shouldReturn` Right 1
    it "rolls back when condemned" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> Tx.condemn) session `shouldReturn` Right ()
      items session `shouldReturn` Right 0
    it "rolls back after a failing statement, and reports it with its SQL" $ \session -> do
      createItems session
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> Tx.sql "SELECT 1 / 0") session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELECT 1 / 0" && failure `servedCode` "22012") (const False)
      items session `shouldReturn` Right 0
    it "begins with the isolation level and the mode" $ \session ->
      Tx.transactionNoRetry
        Tx.RepeatableRead
        Tx.Read
        (Tx.statement () (Tx.Statement "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')" (const []) (Tx.singleRow (Tx.row @'[Text, Text] (,))) False))
        session
        `shouldReturn` Right ("repeatable read", "on")
    it "prepares a preparable statement as its Prepare says, and runs the others unnamed" $ \session -> do
      asked <- newIORef []
      parsed <- newIORef []
      let prepareAs name query' _ = do
            modifyIORef asked (query' :)
            pure (Tx.PrepareAs name (modifyIORef parsed (name :)))
          named = Tx.Statement "SELECT count(*) FROM pg_prepared_statements WHERE name = 'answer'" (const []) (Tx.singleRow (Tx.row @'[Int64] id)) False
          answer = Tx.Statement "SELECT $1::int4 + 1" (\value -> [Tx.Param Nothing TextFormat (Just (ParameterBytes value))]) (Tx.singleRow (Tx.row @'[Int32] id)) True
      Tx.transactionNoRetryWith (prepareAs "answer") Tx.ReadCommitted Tx.Read ((,) <$> Tx.statement "41" answer <*> Tx.statement () named) session
        `shouldReturn` Right (42, 1)
      readIORef asked `shouldReturn` ["SELECT $1::int4 + 1"]
      readIORef parsed `shouldReturn` ["answer"]
      Tx.transactionNoRetryWith (\_ _ -> pure (Tx.PreparedAs "answer")) Tx.ReadCommitted Tx.Read (Tx.statement "1" answer) session
        `shouldReturn` Right 2
      result <- Tx.transactionNoRetryWith (prepareAs "broken") Tx.ReadCommitted Tx.Read (Tx.statement () (Tx.Statement "SELEC 1" (const []) Tx.noResult True)) session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELEC 1" && failure `servedCode` "42601") (const False)
      readIORef parsed `shouldReturn` ["answer"]
      let divided = Tx.Statement "SELECT 1 / $1::int4" (\value -> [Tx.Param Nothing TextFormat (Just (ParameterBytes value))]) Tx.noResult True
      failing <- Tx.transactionNoRetryWith (prepareAs "divided") Tx.ReadCommitted Tx.Read (Tx.statement "0" divided) session
      failing `shouldSatisfy` either (\(Tx.QueryError _ failure) -> failure `servedCode` "22012") (const False)
      -- parsed, so prepared, even though it failed to run
      readIORef parsed `shouldReturn` ["divided", "answer"]
    it "runs a pipeline's statements in order, and gives their results" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.pipeline ((,) <$> Pipeline.statement "1" insertItem <*> Pipeline.statement () countItems)) session
        `shouldReturn` Right ((), 1)
    it "skips a pipeline's statements after one that fails on the server, and reports that one" $ \session -> do
      createItems session
      result <-
        Tx.transactionNoRetry
          Tx.ReadCommitted
          Tx.Write
          (Tx.pipeline (Pipeline.statement "1" insertItem *> Pipeline.statement () (Tx.Statement "SELECT 1 / 0" (const []) Tx.noResult False) *> Pipeline.statement "2" insertItem))
          session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELECT 1 / 0" && failure `servedCode` "22012") (const False)
      items session `shouldReturn` Right 0
    it "reads a pipeline's results after one that doesn't decode, and stays usable" $ \session -> do
      let number sql' = Pipeline.statement () (Tx.Statement sql' (const []) (Tx.singleRow (Tx.row @'[Int32] id)) False)
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.pipeline ((,) <$> number "SELECT 'abc'::text" <*> number "SELECT 2::int4")) session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELECT 'abc'::text" && (case failure of Tx.RowFailed _ -> True; _ -> False)) (const False)
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "sends nothing for a transaction without statements" $ \session ->
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (pure (5 :: Int)) session `shouldReturn` Right 5
    it "reports a failure at commit" $ \session -> do
      void (query session "CREATE TEMP TABLE tx_unique (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)" [])
      let insertTwice = Tx.sql "INSERT INTO tx_unique VALUES (1)" *> Tx.sql "INSERT INTO tx_unique VALUES (1)"
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write insertTwice session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "COMMIT" && failure `servedCode` "23505") (const False)
    it "commits with the final pipeline" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.finalPipeline (Pipeline.statement "1" insertItem)) session `shouldReturn` Right ()
      items session `shouldReturn` Right 1
    it "aborts with the final pipeline when condemned before it" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.condemn *> Tx.finalPipeline (Pipeline.statement "1" insertItem)) session `shouldReturn` Right ()
      items session `shouldReturn` Right 0
    it "rolls back after a statement in the final pipeline fails" $ \session -> do
      createItems session
      let failing = Pipeline.statement () (Tx.Statement "SELECT 1 / 0" (const []) Tx.noResult False)
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.finalPipeline (Pipeline.statement "1" insertItem *> failing)) session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "SELECT 1 / 0" && failure `servedCode` "22012") (const False)
      items session `shouldReturn` Right 0
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "reports a failure at commit in the final pipeline, after which the transaction is over" $ \session -> do
      void (query session "CREATE TEMP TABLE tx_unique (n int UNIQUE DEFERRABLE INITIALLY DEFERRED)" [])
      let insert = Pipeline.statement () (Tx.Statement "INSERT INTO tx_unique VALUES (1)" (const []) Tx.noResult False)
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.finalPipeline (insert *> insert)) session
      result `shouldSatisfy` either (\(Tx.QueryError query' failure) -> query' == "COMMIT" && failure `servedCode` "23505") (const False)
      query session "SELECT 1" [] `shouldReturn` (Right ([[Just "1"]], "SELECT 1"), TxIdle)
    it "rejects a statement after the final pipeline" $ \session ->
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.finalPipeline (pure ()) *> Tx.sql "SELECT 1") session
        `shouldThrow` errorCall "a statement after the transaction's final pipeline"
    it "checks how many rows a result has" $ \session -> do
      let numbers :: Tx.Result a -> Int -> Tx.Transaction a
          numbers shape count = Tx.statement () (Tx.Statement ("SELECT n FROM generate_series(1, " <> Char8.pack (show count) <> ") n") (const []) shape False)
          number = Tx.row @'[Int32] id
          run transaction = Tx.transactionNoRetry Tx.ReadCommitted Tx.Read transaction session
      run (numbers (Tx.singleRow number) 0) >>= (`shouldSatisfy` failsWith (== Tx.UnexpectedAmountOfRows 0))
      run (numbers (Tx.singleRow number) 2) >>= (`shouldSatisfy` failsWith (== Tx.UnexpectedAmountOfRows 2))
      run (numbers (Tx.rowMaybe number) 0) `shouldReturn` Right Nothing
      run (numbers (Tx.rowMaybe number) 1) `shouldReturn` Right (Just 1)
      run (numbers (Tx.rowList number) 3) `shouldReturn` Right [1, 2, 3]
      run (numbers Tx.noResult 3) `shouldReturn` Right ()
    it "reports a row that doesn't decode" $ \session -> do
      result <- Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () (Tx.Statement "SELECT 'abc'::text" (const []) (Tx.singleRow (Tx.row @'[Int32] id)) False)) session
      result `shouldSatisfy` failsWith (\case Tx.RowFailed _ -> True; _ -> False)
    it "streams a column larger than the read buffer within the transaction" $ \session -> do
      let streamed accumulated current =
            case accumulated of
              Nothing -> Just <$> Tx.decodeScopedStreaming @'[Int32] current (\number stream -> (number,Tx.streamLength stream,) <$> drain stream 0)
              Just _ -> pure accumulated
          drain stream received =
            Tx.streamChunk stream >>= \chunk ->
              if BS.null chunk then pure received else drain stream (received + BS.length chunk)
      Tx.transactionNoRetry
        Tx.ReadCommitted
        Tx.Read
        (Tx.statement () (Tx.Statement "SELECT 7::int4, repeat('x', 1000000)" (const []) (Tx.foldRows streamed Nothing) False))
        session
        `shouldReturn` Right (Just (Right (7, 1000000, 1000000)))
    it "streams a column that may be NULL within a single row's scope" $ \session -> do
      let drain stream received =
            Tx.streamChunk stream >>= \chunk ->
              if BS.null chunk then pure received else drain stream (received + BS.length chunk)
          lengths current =
            Tx.decodeScopedStreamingNullable @'[Int32] current $ \number ->
              maybe (pure (number, Nothing)) (\stream -> (number,) . Just <$> drain stream 0)
          streamed query' = Tx.statement () (Tx.Statement query' (const []) (Tx.singleRow (Tx.scopedRow lengths)) False)
      Tx.transactionNoRetry
        Tx.ReadCommitted
        Tx.Read
        ((,) <$> streamed "SELECT 7::int4, repeat('x', 1000000)" <*> streamed "SELECT 8::int4, NULL::text")
        session
        `shouldReturn` Right ((7, Just 1000000), (8, Nothing))
    it "rejects NULL in a column streamed as one that can't be NULL" $ \session ->
      decodeSingleStreaming @'[Int32] session "SELECT 7::int4, NULL::text" (\_ _ -> pure ())
        `shouldReturn` Left (ColumnDecodingFailed 2 "unexpected NULL in a streamed column")
    it "aborts when the transaction throws between statements, and leaves the session usable" $ \session -> do
      createItems session
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Write (Tx.statement "1" insertItem *> liftIO (throwIO (ErrorCall "boom"))) session
        `shouldThrow` errorCall "boom"
      items session `shouldReturn` Right 0

  describe "snippets" $ around withSession $ do
    it "sends parameters the server reads as their types" $ \session -> do
      let snippet =
            "SELECT "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.unknown) "41"
              <> "::int + 1, array_to_string("
              <> Snippet.encoderAndParam (Encoders.nonNullable (Encoders.foldableArray (Encoders.nonNullable Encoders.text))) ["a", "zażółć"]
              <> ", ','), "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.jsonbLazyBytes) "{\"a\": 1}"
              <> " ->> 'a', "
              <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.jsonLazyBytes) "[1, 2]"
              <> "::text, "
              <> Snippet.encoderAndParam (Encoders.nullable Encoders.bytea) Nothing
              <> " IS NULL"
          statement = Snippet.dynamicallyParameterized snippet (Tx.singleRow (Tx.row @'[Int32, Text, Text, Text, Bool] (,,,,))) False
      Tx.transactionNoRetry Tx.ReadCommitted Tx.Read (Tx.statement () statement) session
        `shouldReturn` Right (42, "a,zażółć", "1", "[1, 2]", True)

  describe "execute (failures)" $ around withSession $ do
    it "breaks the session when a parameter stream is shorter than declared" $ \session -> do
      source <- chunkSource ["short"]
      let short = Execution (Unprepared "SELECT $1::text" []) [] [Just (ParameterStream (ByteStream 10 source))] []
      try (execute session short (\() _ -> pure ()) ()) >>= \case
        Left (_ :: StreamLengthError) -> pure ()
        Right _ -> expectationFailure "expected a StreamLengthError"
      query session "SELECT 1" [] `shouldThrow` sessionUnusable
    it "throws a parameter stream's failure, and breaks the session" $ \session -> do
      sent <- newIORef False
      let source = readIORef sent >>= \done -> if done then throwIO (userError "the client went away") else writeIORef sent True $> Char8.replicate 1000000 'y'
          failing = Execution (Unprepared "SELECT length($1::text)" []) [] [Just (ParameterStream (ByteStream 2000000 source))] []
      try (execute session failing (\() _ -> pure ()) ()) >>= \case
        Left (_ :: IOException) -> pure ()
        Right _ -> expectationFailure "expected the stream's failure"
      query session "SELECT 1" [] `shouldThrow` sessionUnusable
  where
    -- Streams the single column slowly, checking its bytes and that no chunk
    -- is larger than the read buffer, while sampling the live heap. The
    -- accumulators are strict, so they don't keep the chunks alive.
    consumeSlowly row = do
      Just (Column size) <- nextColumn row
      source <- columnStream row
      let loop !count !received !small !valid !peak = do
            chunk <- byteStreamNext source
            if BS.null chunk
              then pure (received, small, valid && size == received, peak)
              else do
                peak' <-
                  if count `mod` 256 == 255
                    then threadDelay 2000 *> (max peak <$> liveBytes)
                    else pure peak
                loop
                  (count + 1 :: Int)
                  (received + BS.length chunk)
                  (small && BS.length chunk <= 16384)
                  (valid && Char8.all (== 'x') chunk)
                  peak'
      baseline <- liveBytes
      (received, small, valid, peak) <- loop 0 0 True True baseline
      peak - baseline `shouldSatisfy` (< 8 * 1024 * 1024)
      pure (received, small, valid)

data Pair = Pair Int32 Text
  deriving (Eq, Show)

instance DecodeValue Pair where
  valueDecoder = composite (Pair <$> field <*> field)

-- | Runs a query with binary results and decodes its single row.
decodeSingle :: forall columns r. DecodeRow columns => Session 'Ready -> Text -> RowFunction columns (IO r) -> IO (Either DecodingError r)
decodeSingle session sql function = single session sql (\row -> decodeRow @columns row function)

decodeSingleStreaming :: forall columns r. DecodeRow columns => Session 'Ready -> Text -> RowFunction columns (ByteStream -> IO r) -> IO (Either DecodingError r)
decodeSingleStreaming session sql function = single session sql (\row -> decodeRowStreaming @columns row function)

single :: Session 'Ready -> Text -> (Row -> IO (Either DecodingError r)) -> IO (Either DecodingError r)
single session sql decode = do
  (result, _) <-
    execute
      session
      (Execution (Unprepared (encodeUtf8 sql) []) [] [] [BinaryFormat])
      (\_ row -> Just <$> decode row)
      Nothing
  case result of
    Right (Just decoded, _) -> pure decoded
    Right (Nothing, _) -> fail "the query returned no row"
    Left failure -> fail (show failure)

streamedLength :: ByteStream -> IO Int
streamedLength source = go 0
  where
    go !count = do
      chunk <- byteStreamNext source
      if BS.null chunk then pure count else go (count + BS.length chunk)

-- | Both the first response and the remaining request exceed the socket
-- buffers, so the client must wait for either direction to make progress.
batchWhileReading :: Session 'Ready -> Expectation
batchWhileReading session = do
  let collect execution = batched id execution (\rows row -> (: rows) <$> columns row) []
      large = collect (statement "SELECT repeat('x', 1000000)" [])
      long = collect (statement "SELECT length($1::text)" [Just (Char8.replicate 2000000 'y')])
  outcome <- timeout 10000000 (runBatch session id ((\_ (rows, _) -> rows) <$> large <*> long))
  fmap fst outcome `shouldBe` Just (Right [[Just "2000000"]])

statement :: ByteString -> [Maybe ByteString] -> Execution
statement sql parameters = Execution (Unprepared sql []) [] (map (fmap ParameterBytes) parameters) []

query :: Session 'Ready -> ByteString -> [Maybe ByteString] -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
query session sql parameters = collectRows session (statement sql parameters)

prepared :: Session 'Ready -> ByteString -> [Maybe ByteString] -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
prepared session name parameters =
  collectRows session (Execution (Prepared name) [] (map (fmap ParameterBytes) parameters) [])

collectRows :: Session 'Ready -> Execution -> IO (Either ServerError ([[Maybe ByteString]], ByteString), TransactionStatus)
collectRows session execution = do
  (result, status) <- execute session execution (\rows row -> (: rows) <$> columns row) []
  pure (fmap (first reverse) result, status)

-- | All of a row's columns, copied out of the read buffer.
columns :: Row -> IO [Maybe ByteString]
columns row = do
  column <- nextColumn row
  case column of
    Nothing -> pure []
    Just NullColumn -> (Nothing :) <$> columns row
    Just (Column _) -> do
      value <- BS.copy <$> columnBytes row
      (Just value :) <$> columns row

-- | A source that gives the chunks, then the end.
chunkSource :: [ByteString] -> IO (IO ByteString)
chunkSource chunks = do
  remaining <- newIORef chunks
  pure $ do
    queued <- readIORef remaining
    case queued of
      [] -> pure BS.empty
      chunk : others -> chunk <$ writeIORef remaining others

-- | Live heap bytes after a major collection.
liveBytes :: IO Int
liveBytes = do
  performMajorGC
  fromIntegral . gcdetails_live_bytes . gc <$> getRTSStats

connectAndClose :: ConnectSettings -> Expectation
connectAndClose settings = do
  (session, _) <- connect settings
  close session

errorCode :: ServerError -> Maybe ByteString
errorCode (ServerError fields) = Just (diagnosticCode fields)

-- | Whether the statement failed on the server with the SQLSTATE.
servedCode :: Tx.StatementError -> ByteString -> Bool
servedCode (Tx.ServerFailed failure) code = errorCode failure == Just code
servedCode _ _ = False

-- | The diagnostics of a failed statement.
failed :: Either ServerError a -> Maybe Diagnostics
failed = either (\(ServerError fields) -> Just fields) (const Nothing)

rejectedWith :: ByteString -> ConnectionError -> Bool
rejectedWith code (ConnectionRejected failure) = errorCode failure == Just code
rejectedWith _ _ = False

authenticationError :: ConnectionError -> Bool
authenticationError (AuthenticationError _) = True
authenticationError _ = False

processIdOf :: ServerInfo -> Maybe ProcessId
processIdOf info = fst . unScalar <$> serverBackendKey info

connectionTerminated :: ConnectionError -> Bool
connectionTerminated (ConnectionTerminated _) = True
connectionTerminated _ = False

sessionUnusable :: ConnectionError -> Bool
sessionUnusable (SessionUnusable _) = True
sessionUnusable _ = False

-- | No default host or certificate directory: every test gives its host.
noDefaults :: Defaults
noDefaults = Defaults "/tmp" Nothing

rowMisuse :: ConnectionError -> Bool
rowMisuse (RowMisuse _) = True
rowMisuse _ = False

-- | Runs the action with the directory of a Unix-domain socket that accepts
-- connections but never answers.
withSilentServer :: (FilePath -> IO a) -> IO a
withSilentServer action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-silent-")) removeDirectoryRecursive $ \directory ->
    bracket (Socket.socket Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
      Socket.bind sock (Socket.SockAddrUnix (directory </> ".s.PGSQL.5432"))
      Socket.listen sock 4
      action directory

-- | Runs the action with a copy of the client certificate's key, with the
-- file mode given.
withKeyCopy :: FileMode -> (FilePath -> IO a) -> IO a
withKeyCopy mode action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-key-")) removeDirectoryRecursive $ \directory -> do
    let key = directory </> "client.key"
    copyFile (certificate "client.key") key
    setFileMode key mode
    action key

-- | Runs the action with a port at 127.0.0.1 where a server answers that it
-- doesn't take TLS.
withPlaintextServer :: (Socket.PortNumber -> IO a) -> IO a
withPlaintextServer action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
    Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen sock 4
    let answer = forever $ do
          (client, _) <- Socket.accept sock
          forkIO $ (`finally` Socket.close client) $ do
            _ <- SocketBytes.recv client 8
            SocketBytes.sendAll client "N"
            void (SocketBytes.recv client 1)
    bracket (forkIO answer) killThread $ \_ -> action =<< Socket.socketPort sock
