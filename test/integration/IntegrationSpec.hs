{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -F -pgmF=skeletest-preprocessor #-}

-- | Tests against a real PostgreSQL, ported to the v2 API: connect, close, TLS and channel binding, connection
-- strings, single statements and their parameters, decoding by cursors, errors, cancel, notifications and snippets.
-- The transactions and pipelines are in "TransactionSpec".
--
-- A temporary cluster is created with the @initdb@ and @pg_ctl@ found on the path, so the PostgreSQL version is the
-- one they come from. Every case is written once, as a program for any arrow that has the capabilities of
-- 'Runtime', and runs under each runner of "TestPrograms".
--
-- A case that the v2 code can't pass yet is marked with what it needs ('needs'): the marks come off as the part that
-- they wait for lands.
module IntegrationSpec (spec) where

import Control.Arrow (arr, second, (&&&), (>>>), (>>^), (^>>))
import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (IOException, SomeException, bracket, catch, displayException, finally)
import Control.Monad (forM_, forever, unless, void)
import Data.Foldable (traverse_)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Int (Int16, Int32, Int64)
import Data.List (intercalate)
import Data.Maybe (isJust, isNothing)
import Data.Scientific (Scientific)
import Data.Text (Text)
import Data.Time (Day, DiffTime, UTCTime (..), fromGregorian)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Database.PostgreSQL.Connection (ChannelBindingMode (..), ConnectSettings (..), Connection)
import qualified Database.PostgreSQL.Connection as PG
import qualified Database.PostgreSQL.Connection.Internal as PGInternal
import Database.PostgreSQL.ConnectionString (Defaults (..), Host (..), fromConnectionString, parseConnectionString)
import Database.PostgreSQL.Decoding (DecodeValue (..), DecodingError (..), InPlace, Json (..), Jsonb (..), composite)
import qualified Database.PostgreSQL.Decoding as Decoding
import qualified Database.PostgreSQL.Encoders as Encoders
import Database.PostgreSQL.Errors
import qualified Database.PostgreSQL.Notifications as Notify
import qualified Database.PostgreSQL.Snippet as Snippet
import Database.PostgreSQL.Tls (tlsClient)
import Database.PostgreSQL.Transaction (Rows (..))
import qualified Database.PostgreSQL.Transaction as Tx
import Database.PostgreSQL.Types (protocolVersion3_0, protocolVersion3_2)
import GHC.Clock (getMonotonicTime)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Skeletest
import qualified Skeletest.Predicate as P
import System.Directory (copyFile, getTemporaryDirectory, removeDirectoryRecursive)
import System.FilePath (takeDirectory, (</>))
import System.Mem (performMajorGC)
import System.Posix.Files (groupReadMode, otherReadMode, ownerReadMode, setFileMode, unionFileModes)
import System.Posix.Temp (mkdtemp)
import System.Posix.Types (FileMode)
import TestCluster (certificate)
import TestPrograms hiding (field)

spec :: FilePath -> Socket.PortNumber -> Spec
spec socketPath port = describe "integration" $ forM_ runners $ \runner@(Runner name _) -> describe name $ do
  let run :: Program a -> IO a
      run = ordinary runner
      unix = unixSettings socketPath
      encryptedAs = tlsSettings port

  describe "connect" $ do
    it "connects a trusted user and reports the server's parameters" $ do
      info <- run (session (unix "postgres" Nothing) (arr (PG.serverInfo . fst)))
      lookup "server_version" (PG.serverParameters info) `shouldSatisfy` P.just P.anything
      PG.serverProcessId info `shouldSatisfy` P.just P.anything
      PG.serverProtocolVersion info `shouldBe` protocolVersion3_0
    it "authenticates with SCRAM-SHA-256" $
      run (session (unix "native_pg_scram" (Just "scram secret")) currentUser) `shouldReturn` Right ["native_pg_scram"]
    it "authenticates with MD5" $
      run (session (unix "native_pg_md5" (Just "md5 secret")) currentUser) `shouldReturn` Right ["native_pg_md5"]
    it "authenticates with a cleartext password" $
      run (session (unix "native_pg_cleartext" (Just "cleartext secret")) currentUser) `shouldReturn` Right ["native_pg_cleartext"]
    it "reports a wrong password" $
      (rejection <$> run (connectedTo (unix "native_pg_scram" (Just "wrong")) done)) `shouldReturn` Just "28P01"
    it "reports a missing password" $ do
      result <- run (connectedTo (unix "native_pg_scram" Nothing) done)
      authenticationFailure result `shouldSatisfy` P.just P.anything
    it "reports a missing database" $
      (rejection <$> run (connectedTo ((unix "postgres" Nothing) {connectDatabase = Just "missing"}) done))
        `shouldReturn` Just "3D000"
    it "uses protocol 3.2 where the server has it, and 3.0 otherwise" $ do
      info <- run (session ((unix "postgres" Nothing) {connectProtocolVersion = protocolVersion3_2}) (arr (PG.serverInfo . fst)))
      let major = fmap fst . Char8.readInt =<< lookup "server_version" (PG.serverParameters info)
      PG.serverProtocolVersion info
        `shouldBe` (if maybe False (>= 18) major then protocolVersion3_2 else protocolVersion3_0)

  -- A connection is closed when its scope ends. Closing it earlier is allowed, and so is closing it again.
  describe "close" $
    forM_ [("without TLS", unix "postgres" Nothing), ("with TLS", encryptedAs "postgres" Nothing)] $ \(label, connecting) ->
      (if label == "with TLS" then needs "TLS" else id) $ describe label $ do
        -- What listens needs the notifications, and over TLS everything needs TLS.
        let listening = if label == "with TLS" then id else needs "notifications"
        listening $ it "rejects every operation after closing" $
          run $
            session connecting $
              steps
                [ closing,
                  selectOne `failsAs` unusable,
                  committed Tx.Read (Tx.prepare "closed" (plain "SELECT 1" (single @_ @Int32))) Tx.prepared `failsAs` unusable,
                  conn (Notify.listen ["events"]) `failsAs` unusable,
                  conn Notify.notification `failsAs` unusable
                ]
        it "can close repeatedly, also where its scope ends" $
          run $
            session connecting $
              steps
                [ closing,
                  withinFor (secs 5) (steps [closing, closing]) `yields` Just (),
                  selectOne `failsAs` unusable
                ]
        -- v1 broke the session when the step of a row threw. A failing reading program of v2 rolls back and rethrows,
        -- and the connection stays usable (see "TransactionSpec"). What breaks one is the cancellation rule: a statement
        -- that is cancelled while its answer is awaited leaves answers that nobody will read, and a connection is
        -- never drained for a client that has gone away. The case says only that closing works after it, however often.
        it "can close a connection that a cancelled statement broke, repeatedly" $
          run $
            session connecting $
              steps
                [ withinFor (millis 300) (query sleeping) `yields` Nothing,
                  closing,
                  withinFor (secs 5) (steps [closing, closing]) `yields` Just (),
                  selectOne `failsAs` unusable
                ]
        -- The cancellation rule: v1 left the session unusable after a cancelled wait for a notification, but the reader
        -- is at a message boundary then and nothing is half sent or expected, so v2 leaves the connection usable.
        listening $ it "keeps a connection usable when a wait for a notification is cancelled" $
          run $
            session connecting $
              steps
                [ conn (Notify.listen ["events"]),
                  withinFor (millis 100) (conn Notify.notification) `yields` Nothing,
                  selectOne `yields` Right [1]
                ]
        -- A connection has one user at a time, and a second one is turned away with 'ConnectionBusy', without disrupting
        -- the first. In v1 only 'close' was, and a nested statement broke the session.
        it "rejects closing while a statement runs, without disrupting it" $
          run $
            session connecting $
              steps
                [ concurrently (query (plain "SELECT 1 FROM pg_sleep(0.3)" (single @_ @Int32))) (after (millis 100) (closing `failsWith` ConnectionBusy))
                    `yields` (Right [1], ()),
                  selectOne `yields` Right [1]
                ]
        it "rejects a second statement while one runs, without disrupting it" $
          run $
            session connecting $
              steps
                [ concurrently (query (plain "SELECT 1 FROM pg_sleep(0.3)" (single @_ @Int32))) (after (millis 100) (selectOne `failsWith` ConnectionBusy))
                    `yields` (Right [1], ()),
                  selectOne `yields` Right [1]
                ]

  -- The binding data that SCRAM-PLUS signs comes from the TLS link. These cases replace it.
  needs "TLS" $ describe "channel-binding data from TLS setup" $ do
    let suppliedBinding supplied mode user =
          PGInternal.withEndPointOverride supplied ((encryptedAs user (Just "scram secret")) {connectChannelBinding = mode})
        bindingFailure = Just (Left "unsupported certificate hash")
        connectedWith settings = run (connectedTo settings done)
    it "uses the supplied binding bytes in SCRAM-PLUS" $
      (rejection <$> connectedWith (suppliedBinding (Just (Right "wrong binding")) ChannelBindingRequire "native_pg_scram"))
        `shouldReturn` Just "28000"
    forM_ [("preferred", ChannelBindingPrefer), ("required", ChannelBindingRequire)] $ \(label, mode) ->
      it ("reports a binding error when SCRAM-PLUS is " <> label) $
        (authenticationFailure <$> connectedWith (suppliedBinding bindingFailure mode "native_pg_scram"))
          `shouldReturn` Just "unsupported certificate hash"
    it "ignores a binding error when channel binding is disabled" $
      run (session (suppliedBinding bindingFailure ChannelBindingDisable "native_pg_scram") currentUser)
        `shouldReturn` Right ["native_pg_scram"]
    it "does not require binding data for non-SCRAM authentication" $
      run (session (suppliedBinding bindingFailure ChannelBindingPrefer "postgres") selectOne) `shouldReturn` Right [1]
    it "allows unbound SCRAM when binding is unavailable and optional" $
      run (session (suppliedBinding Nothing ChannelBindingPrefer "native_pg_scram") currentUser)
        `shouldReturn` Right ["native_pg_scram"]
    it "rejects required binding when the TLS link supplies none" $
      (authenticationFailure <$> connectedWith (suppliedBinding Nothing ChannelBindingRequire "native_pg_scram"))
        `shouldReturn` Just "channel binding is required, but server did not offer an authentication method that supports channel binding"
    it "ignores binding errors on a cancellation connection" $
      run $
        session (unix "postgres" Nothing) $
          cancelling (suppliedBinding bindingFailure ChannelBindingRequire "postgres")

  describe "connection strings" $ do
    let directory = takeDirectory socketPath
        local extra = "host=" <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres " <> extra
        tcp extra = "host=127.0.0.1 port=" <> Char8.pack (show port) <> " dbname=postgres " <> extra
        -- A role that takes plaintext over TCP, at the host and with the extra options.
        viaPlaintext host extra = "host=" <> host <> " port=" <> Char8.pack (show port) <> " dbname=postgres user=native_pg_plaintext sslmode=disable " <> extra
        withString :: ByteString -> (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withString = onString runner
        connectFailure = failureOf runner
    it "connects with a URI that names the socket directory" $
      withString
        ("postgresql:///postgres?host=" <> Char8.pack directory <> "&port=" <> Char8.pack (show port))
        (arr (hostName . PG.connectedHost . fst) &&& selectOne)
        `shouldReturn` (directory, Right [1])
    it "tries the hosts in order" $
      withString
        ("host=/nonexistent," <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres")
        (arr (hostName . PG.connectedHost . fst))
        `shouldReturn` directory
    it "words each host's failure as libpq does" $
      connectFailure "host=/nonexistent-1,/nonexistent-2"
        `shouldReturn` intercalate
          "\n"
          [ "connection to server on socket \"" <> path <> "/.s.PGSQL.5432\" failed: No such file or directory\n\tIs the server running locally and accepting connections on that socket?"
          | path <- ["/nonexistent-1", "/nonexistent-2"]
          ]
    -- connect fails with the same shape for one host as for many: a list with an entry for each host it tried.
    it "reports the one host that failed as a list of one" $ do
      settings <- settingsOf (local "user=native_pg_scram password=wrong")
      result <- run (connectedTo settings done)
      case result of
        Left (ConnectFailure [(host, ConnectionRejected _)]) -> hostName host `shouldBe` directory
        _ -> failTest "expected the rejection of the one host, in a list of one"
    it "reports the hosts that failed, in order" $ do
      settings <- settingsOf "host=/missing-native-pg-first,/missing-native-pg-second dbname=postgres"
      result <- run (connectedTo settings done)
      case result of
        Left (ConnectFailure failures) ->
          map (hostName . fst) failures `shouldBe` ["/missing-native-pg-first", "/missing-native-pg-second"]
        _ -> failTest "the missing hosts connected, or failed otherwise"
    -- Each host has its own connect_timeout, and the next is tried after one that timed out.
    it "gives each host its own connect_timeout" $
      withSilentServer $ \one -> withSilentServer $ \other -> do
        started <- getMonotonicTime
        connectFailure ("host=" <> Char8.pack one <> "," <> Char8.pack other <> " connect_timeout=1")
          `shouldReturn` intercalate
            "\n"
            [ "connection to server on socket \"" <> silent <> "/.s.PGSQL.5432\" failed: timeout expired"
            | silent <- [one, other]
            ]
        elapsed <- subtract started <$> getMonotonicTime
        -- The deadline is read from the runtime's clock, which can differ from this one by a little.
        elapsed `shouldSatisfy` (P.gte 1.9 P.&& P.lt 6)
    it "connects to the next host after one that timed out" $
      withSilentServer $ \silent ->
        withString
          ("host=" <> Char8.pack silent <> "," <> Char8.pack directory <> " port=5432," <> Char8.pack (show port) <> " dbname=postgres connect_timeout=1")
          (arr (hostName . PG.connectedHost . fst) &&& selectOne)
          `shouldReturn` (directory, Right [1])
    -- A server that doesn't have the protocol version that min_protocol_version asks for doesn't qualify, as with
    -- target_session_attrs.
    it "doesn't qualify a server below min_protocol_version, as libpq words it" $
      withScripted [\client -> receiveStartup client *> SocketBytes.sendAll client (frame 'v' (int32 0x00030000 <> int32 0) <> frame 'R' (int32 0) <> frame 'Z' "I") *> drain client] $ \scripted ->
        connectFailure ("host=127.0.0.1 user=postgres sslmode=disable max_protocol_version=3.2 min_protocol_version=3.2 port=" <> Char8.pack (show scripted))
          `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show scripted <> " failed: server only supports protocol version 3.0, but min_protocol_version was set to 3.2")
    it "qualifies a server that has the protocol version that min_protocol_version asks for" $ do
      settings <- settingsOf (local "min_protocol_version=3.2 max_protocol_version=3.2")
      result <- run (connectedTo settings (arr (PG.serverProtocolVersion . PG.serverInfo . fst)))
      info <- run (session (unix "postgres" Nothing) (arr (PG.serverInfo . fst)))
      let major = fmap fst . Char8.readInt =<< lookup "server_version" (PG.serverParameters info)
      case result of
        Right version -> (maybe False (>= 18) major, version) `shouldBe` (True, protocolVersion3_2)
        Left (ConnectFailure [(_, Unsuitable _)]) -> maybe False (>= 18) major `shouldBe` False
        Left failure -> failTest ("failed otherwise: " <> displayException failure)
    -- A name is looked up, and the addresses it resolves to are tried in order. The cluster listens on 127.0.0.1 only, so
    -- an address of another family that comes first is refused.
    it "connects to a host by its name" $ do
      settings <- settingsOf (viaPlaintext "localhost" "")
      run (session settings (arr (hostName . PG.connectedHost . fst) &&& selectOne)) `shouldReturn` ("localhost", Right [1])
    it "words the failure of each address that a host name resolves to as libpq does" $
      withUnusedPort $ \unused -> do
        failure <- connectFailure ("host=localhost sslmode=disable port=" <> Char8.pack (show unused))
        failure
          `shouldSatisfy` P.hasInfix
            ("connection to server at \"localhost\" (127.0.0.1), port " <> show unused <> " failed: Connection refused\n\tIs the server running on that host and accepting TCP/IP connections?")
    it "tries the next host after a name that does not resolve" $
      withString
        ("host=no-such-host.invalid," <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres")
        (arr (hostName . PG.connectedHost . fst))
        `shouldReturn` directory
    -- With hostaddr, the name is only for TLS and for the words of a failure: nothing is looked up.
    it "connects to the address of hostaddr, without looking up the host name" $ do
      settings <- settingsOf (viaPlaintext "no-such-host.invalid" "hostaddr=127.0.0.1")
      run (session settings (arr (hostName . PG.connectedHost . fst) &&& selectOne)) `shouldReturn` ("no-such-host.invalid", Right [1])
    it "words a host name that doesn't resolve as libpq does" $ do
      failure <- connectFailure "host=no-such-host.invalid"
      failure `shouldSatisfy` P.hasPrefix "could not translate host name \"no-such-host.invalid\" to address: "
    it "names the application, with the fallback name if none is given" $ do
      withString (local "fallback_application_name=fallback") applicationName `shouldReturn` Right ["fallback"]
      withString (local "fallback_application_name=fallback application_name=app") applicationName `shouldReturn` Right ["app"]
    it "connects only to a server with the session attributes asked for" $ do
      let readOnly = "options='-c default_transaction_read_only=on' "
      withString (local (readOnly <> "target_session_attrs=read-only")) done `shouldReturn` ()
      connectFailure (local (readOnly <> "target_session_attrs=read-write"))
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: session is read-only")
      withString (local "target_session_attrs=primary") done `shouldReturn` ()
      connectFailure (local "target_session_attrs=standby")
        `shouldReturn` ("connection to server on socket \"" <> socketPath <> "\" failed: server is not in hot standby mode")
      -- With no standby to prefer, the primaries are tried again.
      withString (local "target_session_attrs=prefer-standby") done `shouldReturn` ()
    describe "with TLS" $ do
      let encryptedBy string = withString (tcp string) encrypted
      needs "TLS" $ it "encrypts with sslmode=require, without verifying the server" $
        encryptedBy "sslmode=require" `shouldReturn` Right [True]
      needs "TLS" $ it "sends a large batch while reading its responses over TLS" $
        withString (tcp "sslmode=require") batchWhileReading `shouldReturn` Just (Right (1000000, 2000000))
      it "doesn't encrypt over a Unix-domain socket, as libpq doesn't" $
        withString (local "sslmode=require") encrypted `shouldReturn` Right [False]
      needs "TLS" $ it "fails if the server doesn't take TLS" $
        withPlaintextServer $ \plaintext ->
          connectFailure ("host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require")
            `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show plaintext <> " failed: server does not support SSL, but SSL was required")
      it "fails as libpq without TLS does, without a TLS client" $ do
        settings <- settingsOf (tcp "sslmode=require")
        result <- run (connectedTo settings {connectTlsClient = Nothing} done)
        either displayException (const "connected") result
          `shouldBe` ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: sslmode value \"require\" invalid when SSL support is not compiled in")
      needs "TLS" $ it "encrypts by default, with sslmode=prefer, as libpq does" $
        encryptedBy "" `shouldReturn` Right [True]
      it "falls back to plaintext with sslmode=prefer when the server rejects TLS" $
        encryptedBy "user=native_pg_plaintext" `shouldReturn` Right [False]
      needs "TLS" $ it "tries plaintext first with sslmode=allow, then TLS" $ do
        encryptedBy "sslmode=allow user=native_pg_plaintext" `shouldReturn` Right [False]
        encryptedBy "sslmode=allow" `shouldReturn` Right [True]
      it "connects in plaintext with sslmode=prefer without a TLS client, as libpq without TLS does" $ do
        settings <- settingsOf (tcp "user=native_pg_plaintext")
        run (session settings {connectTlsClient = Nothing} encrypted) `shouldReturn` Right [False]
      needs "TLS" $ it "verifies the server's certificate and name with sslmode=verify-full" $ do
        encryptedBy ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=localhost hostaddr=127.0.0.1")
          `shouldReturn` Right [True]
        encryptedBy ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt")) `shouldReturn` Right [True]
      needs "TLS" $ it "refuses a server whose certificate no root signed, or that has another name" $ do
        let at host = "connection to server at \"" <> host <> "\" (127.0.0.1), port " <> show port <> " failed: "
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "other-ca.crt") <> " host=localhost hostaddr=127.0.0.1"))
          `shouldReturn` (at "localhost" <> "SSL error: invalid peer certificate: UnknownIssuer")
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=other.example hostaddr=127.0.0.1"))
          `shouldReturn` (at "other.example" <> "SSL error: invalid peer certificate: NotValidForName")
      needs "TLS" $ it "verifies against the system's root certificates with sslrootcert=system" $ do
        failure <- connectFailure (tcp "sslmode=verify-full sslrootcert=system host=localhost hostaddr=127.0.0.1")
        -- The platforms' verifiers word it differently.
        failure `shouldSatisfy` P.hasPrefix ("connection to server at \"localhost\" (127.0.0.1), port " <> show port <> " failed: SSL error: ")
      needs "TLS" $ it "needs the root certificate file, as libpq does" $
        connectFailure (tcp "sslmode=verify-full sslrootcert=/nonexistent/root.crt host=localhost hostaddr=127.0.0.1")
          `shouldReturn` ( "connection to server at \"localhost\" (127.0.0.1), port "
                             <> show port
                             <> " failed: root certificate file \"/nonexistent/root.crt\" does not exist\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification."
                         )
      needs "TLS" $ it "starts TLS directly with sslnegotiation=direct, as PostgreSQL 17 takes it" $
        encryptedBy "sslnegotiation=direct sslmode=require" `shouldReturn` Right [True]
      needs "TLS" $ it "cancels over TLS, and asks for TLS to cancel, as libpq does since PostgreSQL 17" $ do
        settings <- settingsOf (tcp "sslmode=require")
        withPlaintextServer $ \plaintext -> do
          refusing <- settingsOf ("host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require")
          run $
            session settings $
              steps
                [ (concurrently (query sleeping) (after (millis 300) (cancelling settings)) >>^ serverCode . fst) `yields` Just "57014",
                  cancelling refusing
                    `failsAs` \case
                      TlsFailure reason -> reason `shouldBe` "server does not support SSL, but SSL was required"
                      other -> failTest ("expected TlsFailure, got " <> show other)
                ]
      needs "TLS" $ it "binds SCRAM to TLS with channel_binding=require" $
        withString (tcp "user=native_pg_scram password='scram secret' sslmode=require channel_binding=require") currentUser
          `shouldReturn` Right ["native_pg_scram"]
      needs "TLS" $ it "fails as libpq does with channel_binding=require when the server doesn't bind SCRAM to TLS" $ do
        let failure at reason = "connection to server " <> at <> " failed: " <> reason
            socket = "on socket \"" <> socketPath <> "\""
            tlsAt = "at \"127.0.0.1\", port " <> show port
        connectFailure (local "user=native_pg_scram password='scram secret' channel_binding=require")
          `shouldReturn` failure socket "channel binding is required, but server did not offer an authentication method that supports channel binding"
        connectFailure (local "user=native_pg_md5 password='md5 secret' channel_binding=require")
          `shouldReturn` failure socket "channel binding required but not supported by server's authentication request"
        connectFailure (tcp "sslmode=require channel_binding=require")
          `shouldReturn` failure tlsAt "channel binding required, but server authenticated client without channel binding"
      needs "TLS" $ it "negotiates a version between ssl_min_protocol_version and ssl_max_protocol_version" $ do
        let version extra = withString (tcp ("sslmode=require " <> extra)) tlsVersion
        version "" `shouldReturn` Right ["TLSv1.3"]
        version "ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ["TLSv1.2"]
        version "ssl_min_protocol_version=TLSv1 ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ["TLSv1.2"]
      needs "TLS" $ it "checks revocation lists, from sslcrl or sslcrldir" $ do
        let verified extra = tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=localhost hostaddr=127.0.0.1 " <> extra)
            revoked = "connection to server at \"localhost\" (127.0.0.1), port " <> show port <> " failed: SSL error: invalid peer certificate: Revoked"
        withString (verified ("sslcrl=" <> Char8.pack (certificate "empty.crl"))) done `shouldReturn` ()
        connectFailure (verified ("sslcrl=" <> Char8.pack (certificate "revoked.crl"))) `shouldReturn` revoked
        temporary <- getTemporaryDirectory
        bracket (mkdtemp (temporary </> "native-pg-crl-")) removeDirectoryRecursive $ \lists -> do
          -- As OpenSSL's c_rehash names them.
          copyFile (certificate "revoked.crl") (lists </> "0123abcd.r0")
          connectFailure (verified ("sslcrldir=" <> Char8.pack lists)) `shouldReturn` revoked
        -- As libpq, a missing file is no list.
        withString (verified "sslcrl=/nonexistent/root.crl") done `shouldReturn` ()
      needs "TLS" $ it "authenticates with a client certificate" $ do
        withKeyCopy ownerReadMode $ \key -> do
          let certified = "user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key
          withString (tcp certified) currentUser `shouldReturn` Right ["native_pg_certificate"]
        failure <- connectFailure (tcp "user=native_pg_certificate sslmode=require sslcert=/nonexistent/client.crt sslkey=/nonexistent/client.key")
        failure `shouldSatisfy` P.hasInfix "connection requires a valid client certificate"
      needs "TLS" $ it "needs a client certificate's key, readable only by its owner, as libpq does" $ do
        let certified key = tcp ("user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key)
            at = "connection to server at \"127.0.0.1\", port " <> show port <> " failed: "
        connectFailure (certified "/nonexistent/client.key")
          `shouldReturn` (at <> "certificate present, but not private key file \"/nonexistent/client.key\"")
        withKeyCopy (ownerReadMode `unionFileModes` groupReadMode `unionFileModes` otherReadMode) $ \key ->
          connectFailure (certified key)
            `shouldReturn` (at <> "private key file \"" <> key <> "\" has group or world access; file must have permissions u=rw (0600) or less if owned by the current user, or permissions u=rw,g=r (0640) or less if owned by root")
    -- connect_timeout is a deadline: 'within' around the setup, as the plan says.
    it "gives up on a host after connect_timeout" $
      withSilentServer $ \silent -> do
        started <- getMonotonicTime
        connectFailure ("host=" <> Char8.pack silent <> " connect_timeout=2")
          `shouldReturn` ("connection to server on socket \"" <> silent <> "/.s.PGSQL.5432\" failed: timeout expired")
        elapsed <- subtract started <$> getMonotonicTime
        -- As above: the deadline can fire a little before this clock says that it has passed.
        elapsed `shouldSatisfy` (P.gte 1.9 P.&& P.lt 5)

  describe "execute" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "runs an unprepared statement with parameters" $
      withSession (query (valued "SELECT $1::int + 1, $2::text" [Just "41", Just "x"] (pair @_ @Int32 @Text)))
        `shouldReturn` Right [(42, "x")]
    -- A pipeline takes its parameters from the input of the program, and 'Tx.from' says from which part of it.
    it "takes a pipeline's parameters from the input, from the part that 'from' selects" $
      withSession
        ( withInput
            (("40", "2"), "unused" :: ByteString)
            (transactOn Tx.ReadCommitted Tx.Read Tx.Final (Tx.from fst (Tx.statement (Tx.Statement "SELECT $1::int + $2::int" (\(a, b) -> [textual (Just a), textual (Just b)]) (single @_ @Int32)))) readAll)
        )
        `shouldReturn` Right [42]
    it "passes NULL parameters and returns NULL columns" $
      withSession (query (valued "SELECT $1::text, 1" [Nothing] (nullableThen @_ @Text @Int32)))
        `shouldReturn` Right [(Nothing, 1)]
    it "reads many rows, one at a time" $
      withSession (transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT generate_series(1, 10000)" (single @_ @Int32))) countRows)
        `shouldReturn` Right 10000
    needs "prepared statements" $ it "runs a prepared statement until it's closed" $
      withSession
        ( steps
            [ preparing "add_one" (addOneWith "0") `yields` Right (),
              preparedAnswer "add_one" "1" `yields` Right 2,
              preparedAnswer "add_one" "41" `yields` Right 42,
              closed "add_one" `yields` Right (),
              (preparedAnswer "add_one" "1" >>^ serverCode) `yields` Just "26000"
            ]
        )
        `shouldReturn` ()
    it "returns an error and stays usable" $
      withSession
        ( steps
            [ (query (plain "SELECT 1/0" (single @_ @Int32)) >>^ serverCode) `yields` Just "22012",
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    needs "streamed parameters" $ it "streams a parameter from its source" $ do
      let payload = BS.concat (replicate 4096 "0123456789abcdef")
          chunks = 128
      withSession
        ( streaming
            (newChunks (replicate chunks payload))
            (query' (Tx.Statement "SELECT $1::text = repeat('0123456789abcdef', 524288)" (\source -> [streamedText (chunks * BS.length payload) source]) (single @_ @Bool)))
        )
        `shouldReturn` Right [True]
    it "sends a parameter in pieces, short and long, larger than the socket's buffers" $ do
      let payload = BS.concat (replicate 4096 "0123456789abcdef")
          pieces = concatMap (\piece -> [BS.take 100 piece, BS.drop 100 piece]) (replicate 128 payload)
      withSession (query (Tx.Statement "SELECT $1::text = repeat('0123456789abcdef', 524288)" (const [piecesText pieces]) (single @_ @Bool)))
        `shouldReturn` Right [True]
    needs "prepared statements" $ it "streams a parameter to a prepared statement, whether it returns rows or not" $
      withSession
        ( steps
            [ ddl "CREATE TEMPORARY TABLE streamed (value text)" `yields` Right (),
              preparing "insert_streamed" (insertStreamed (const [])) `yields` Right (),
              preparing "length_of" (lengthWith (const [])) `yields` Right (),
              streaming (newChunks ["abc", "def"]) (preparedStreamed "insert_streamed" (insertStreamed (\source -> [streamedText 6 source]))) `yields` Right [],
              streaming (newChunks ["abc", "def"]) (preparedStreamed "length_of" (lengthWith (\source -> [streamedText 6 source]))) `yields` Right [6]
            ]
        )
        `shouldReturn` ()
    needs "prepared statements" $ it "counts a slow parameter stream against statement_timeout, whether the statement is prepared or not" $
      withSession
        ( steps
            [ ddl "SET statement_timeout = '200ms'" `yields` Right (),
              preparing "slow_length" (lengthWith (const [])) `yields` Right (),
              (streaming slowSource (query' (lengthWith (\source -> [streamedText 4000 source]))) >>^ serverCode) `yields` Just "57014",
              (streaming slowSource (preparedStreamed "slow_length" (lengthWith (\source -> [streamedText 4000 source]))) >>^ serverCode) `yields` Just "57014",
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    needs "a connection that sends and reads at once" $ it "sends a batch while reading its results, as the server answers before reading all of it" $
      withSession batchWhileReading `shouldReturn` Just (Right (1000000, 2000000))
    -- The cancellation rule: the reading program is cancelled at its wait while the request side has not finished sending
    -- the 2 MB parameter, so something is half sent, and the connection is broken and closed rather than drained.
    it "closes a connection broken while sending, without waiting for the server to read" $
      withSession
        ( steps
            [ withinFor (secs 1) (committed Tx.Read largeThenLong (\(large, _) -> Tx.single large >>> sleepFor (secs 60))) `yields` Nothing,
              selectOne `failsAs` unusable,
              withinFor (secs 5) closing `yields` Just ()
            ]
        )
        `shouldReturn` ()
    -- A value larger than the buffer can only be read as a field: this is deliberate, and the cases below pin both sides.
    it "takes a value larger than the buffer as a field, whole" $
      withSession (transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT repeat('y', 100000)" (Tx.any Tx.field))) (\result -> Tx.single result >>> copyOut))
        `shouldReturn` Right (Char8.replicate 100000 'y')
    -- (v1: the value was copied out whole.)
    it "fails to decode a value larger than the buffer in place, which only a field can take" $
      withSession
        ( query (plain "SELECT repeat('y', 100000)" (single @_ @Text))
            `failsAs` \case
              ValueTooLarge _ -> pure ()
              other -> failTest ("expected ValueTooLarge, got " <> show other)
        )
        `shouldReturn` ()
    it "streams a column to a slow consumer without holding it in memory" $ do
      enabled <- getRTSStatsEnabled
      unless enabled $ skipTest "needs +RTS -T"
      let size = 64 * 1024 * 1024
      -- The field says how long it is, and its pieces add up to that.
      withSession (transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain ("SELECT repeat('x', " <> Char8.pack (show size) <> ")") (Tx.any Tx.field))) (\result -> Tx.single result >>> (arr Tx.fieldLength &&& consumeSlowly)))
        `shouldReturn` Right (size, (size, True, True))
    -- v1: RowMisuse for a row used after its step. A row of v2 can't leave its step, but a field can: it fails with
    -- 'FieldExpired', once the cursor has moved on, and once its transaction has ended.
    it "rejects a field used after the cursor moved on" $
      withSession
        ( transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT s::text FROM generate_series(1, 2) s" (Tx.any Tx.field))) $ \result ->
            Tx.single result
              >>> tap (done >>> Tx.nextRow result >>> done)
              >>> (next `failsWith` FieldExpired)
        )
        `shouldReturn` Right ()
    it "rejects a field used after its transaction ended" $
      withSession
        ( transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT 'x'::text" (Tx.any Tx.field))) Tx.single
            >>> now (either (\failure -> failTest ("the statement failed: " <> show failure)) pure)
            >>> (next `failsWith` FieldExpired)
        )
        `shouldReturn` ()

  describe "decoding" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
        decoded :: (forall arr. (Runtime arr) => Rows arr row) -> ByteString -> IO (Either QueryError [row])
        decoded rows sql = withSession (query (plain sql rows))
    it "decodes integers, booleans, floats and numerics" $
      decoded numbersRow "SELECT 1::int2, -2::int4, 3::int8, true, 1.5::float4, 2.5::float8, 3.25::numeric"
        `shouldReturn` Right [(1, -2, 3, True, 1.5, 2.5, 3.25)]
    it "decodes text, bytea, char, uuid and json" $
      decoded textsRow "SELECT 'h\195\169llo'::text, '\\xdeadbeef'::bytea, 'x'::char, '00000000-0000-0000-0000-000000000001'::uuid, '{\"a\":1}'::json, '{\"a\":1}'::jsonb"
        `shouldReturn` Right [("héllo", BS.pack [0xde, 0xad, 0xbe, 0xef], 'x', UUID.fromWords 0 0 0 1, Json "{\"a\":1}", Jsonb "{\"a\": 1}")]
    it "decodes dates, timestamps and intervals" $
      decoded (triple @_ @Day @UTCTime @DiffTime) "SELECT '2024-01-02'::date, '2024-01-02 03:04:05+00'::timestamptz, '2 hours'::interval"
        `shouldReturn` Right [(fromGregorian 2024 1 2, UTCTime (fromGregorian 2024 1 2) 11045, 7200)]
    it "decodes NULL into Maybe and rejects it otherwise" $ do
      decoded (pair @_ @(Maybe Int32) @(Maybe Int32)) "SELECT NULL::int4, 1::int4"
        `shouldReturn` Right [(Nothing, Just 1)]
      decoded (single @_ @Int32) "SELECT NULL::int4"
        `shouldReturn` Left (QueryError "SELECT NULL::int4" (RowFailed (ColumnDecodingFailed 1 "unexpected NULL")))
      -- A nullable column of v2 is read with 'nullable', and a column that isn't, with 'column'.
      decoded (nullableThen @_ @Int32 @Int32) "SELECT NULL::int4, 1::int4"
        `shouldReturn` Right [(Nothing, 1)]
    it "decodes arrays, with NULL elements into Maybe" $
      decoded (pair @_ @[Int32] @[Maybe Text]) "SELECT ARRAY[1, 2, 3]::int4[], ARRAY['a', NULL]::text[]"
        `shouldReturn` Right [([1, 2, 3], [Just "a", Nothing])]
    it "decodes composites and arrays of them" $
      decoded (pair @_ @Pair @[Pair]) "SELECT ROW(1, 'a'::text), ARRAY[ROW(2, 'b'::text), ROW(3, 'c'::text)]"
        `shouldReturn` Right [(Pair 1 "a", [Pair 2 "b", Pair 3 "c"])]
    it "reports a wrong column count, or a column of another type, and stays usable" $
      withSession
        ( steps
            [ query (plain "SELECT 1::int4, 2::int4" (single @_ @Int32))
                `yields` Left (QueryError "SELECT 1::int4, 2::int4" (RowFailed (ColumnCountMismatch 1 2))),
              query (plain "SELECT 1::int4" (single @_ @Int64))
                `yields` Left (QueryError "SELECT 1::int4" (RowFailed (ColumnDecodingFailed 1 "int8 needs 8 bytes, got 4"))),
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "streams a last column after decoding the others" $
      withSession
        ( transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT 200::int4, repeat('x', 1000000)" statusAndBody)) $ \result ->
            Tx.single result >>> second measure >>^ second fst
        )
        `shouldReturn` Right (200, 1000000)

  describe "errors" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "reports a missing relation with its position in the query" $ do
      result <- withSession (query (plain "SELECT * FROM missing_table" (single @_ @Int32)))
      fmap (\d -> (diagnosticSeverity d, diagnosticCode d, diagnosticMessage d, diagnosticPosition d)) (diagnosticsOf result)
        `shouldBe` Just (SeverityError, "42P01", "relation \"missing_table\" does not exist", Just 15)
    it "reports a constraint violation's detail, table and constraint" $ do
      result <-
        withSession
          ( steps
              [ ddl "CREATE TEMPORARY TABLE item (id int PRIMARY KEY)" `yields` Right (),
                ddl "INSERT INTO item VALUES (1)" `yields` Right ()
              ]
              &&& ddl "INSERT INTO item VALUES (1)"
              >>^ snd
          )
      fmap (\d -> (diagnosticCode d, diagnosticDetail d, diagnosticTable d, diagnosticConstraint d)) (diagnosticsOf result)
        `shouldBe` Just ("23505", Just "Key (id)=(1) already exists.", Just "item", Just "item_pkey")
    it "reports a raised error's own SQLSTATE, detail, hint and context" $ do
      result <- withSession (ddl "DO $$ BEGIN RAISE EXCEPTION 'custom' USING ERRCODE = 'PGRST', DETAIL = 'the detail', HINT = 'the hint'; END $$")
      fmap (\d -> (diagnosticCode d, diagnosticMessage d, diagnosticDetail d, diagnosticHint d, isJust (diagnosticContext d))) (diagnosticsOf result)
        `shouldBe` Just ("PGRST", "custom", Just "the detail", Just "the hint", True)
    -- A FATAL error ends the session: it is a failure, not the value of a statement.
    it "fails when the server ends the session, which is then broken" $
      withSession
        ( steps
            [ query (plain "SELECT pg_terminate_backend(pg_backend_pid())" (single @_ @Bool))
                `failsAs` \case
                  ConnectionTerminated (ServerError d) -> (diagnosticSeverity d, diagnosticCode d) `shouldBe` (SeverityFatal, "57P01")
                  other -> failTest ("expected ConnectionTerminated, got " <> show other),
              selectOne `failsAs` unusable
            ]
        )
        `shouldReturn` ()

  describe "cancel" $ do
    let missing = unixSettings "/nonexistent/.s.PGSQL.5432" "postgres" Nothing
        refused = \case
          UnreachableSocket path _ -> path `shouldBe` "/nonexistent/.s.PGSQL.5432"
          other -> failTest ("expected UnreachableSocket, got " <> show other)
    it "cancels a running statement, and the connection stays usable" $
      run
        ( session (unix "postgres" Nothing) $
            steps
              [ ( concurrently (timed (query sleeping)) (after (millis 300) (cancelling (unix "postgres" Nothing)))
                    >>^ \((result, elapsed), ()) -> (serverCode result, elapsed < 10 * 1000000000)
                )
                  `yields` (Just "57014", True),
                selectOne `yields` Right [1]
              ]
        )
        `shouldReturn` ()
    it "does nothing to a connection that runs no statement" $
      run (session (unix "postgres" Nothing) (steps [cancelling (unix "postgres" Nothing), selectOne `yields` Right [1]])) `shouldReturn` ()
    -- The attempt is one: a failure to connect is its ConnectError, not a list of them, and the other hosts of the settings
    -- are not tried: another server would be cancelling another session.
    it "fails with the ConnectError of its one attempt" $
      run (session (unix "postgres" Nothing) (cancelling missing `failsAs` refused)) `shouldReturn` ()
    it "makes one attempt, to the host of its settings, whatever other hosts they have" $
      run (session (unix "postgres" Nothing) (cancelling missing {connectFallbacks = [unix "postgres" Nothing]} `failsAs` refused)) `shouldReturn` ()
    it "gives up after connect_timeout when the server doesn't close the connection" $
      withSilentServer $ \silent -> do
        started <- getMonotonicTime
        run
          ( session (unix "postgres" Nothing) $
              cancelling ((unixSettings (silent </> ".s.PGSQL.5432") "postgres" Nothing) {connectTimeout = Just 1})
                `failsAs` \case
                  TimedOut -> pure ()
                  other -> failTest ("expected TimedOut, got " <> show other)
          )
          `shouldReturn` ()
        elapsed <- subtract started <$> getMonotonicTime
        elapsed `shouldSatisfy` (P.gte 0.9 P.&& P.lt 5)
    -- A pooler may not send the key. There is nothing to cancel with then, so nothing is asked of any server.
    it "does nothing for a connection whose server sent no key" $
      withScripted [\client -> receiveStartup client *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'Z' "I") *> drain client] $ \scripted -> do
        settings <- settingsOf ("host=127.0.0.1 user=postgres sslmode=disable port=" <> Char8.pack (show scripted))
        run (session settings (cancelling missing)) `shouldReturn` ()
    it "sends the request of the protocol over a connection of its own, and waits for the server to close it" $ do
      request <- newEmptyMVar
      let startup client =
            receiveStartup client
              *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'K' (int32 1234 <> "abcd") <> frame 'Z' "I")
              *> drain client
          cancelRequest client = receiving client 16 >>= putMVar request >> threadDelay 200000
      withScripted [startup, cancelRequest] $ \scripted -> do
        settings <- settingsOf ("host=127.0.0.1 user=postgres sslmode=disable port=" <> Char8.pack (show scripted))
        started <- getMonotonicTime
        run (session settings (cancelling settings)) `shouldReturn` ()
        elapsed <- subtract started <$> getMonotonicTime
        -- The cancel returned after the server closed the connection, which it did 200 ms after the request.
        elapsed `shouldSatisfy` P.gte 0.19
        takeMVar request `shouldReturn` (int32 16 <> int32 80877102 <> int32 1234 <> "abcd")

  -- A program that is cancelled leaves its connection usable if and only if nothing is owed on it in either direction: no
  -- reply is still expected, and nothing is half sent.
  describe "the cancellation rule" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "breaks the connection when a statement in flight is cancelled, and doesn't drain its answer" $
      withSession
        ( steps
            [ (timed (withinFor (millis 300) (query sleeping)) >>^ \(result, elapsed) -> (isNothing result, elapsed < 10 * 1000000000)) `yields` (True, True),
              brokenConnection
            ]
        )
        `shouldReturn` ()
    needs "notifications" $ it "keeps the connection usable when a wait for a notification is cancelled, and delivers a later one" $
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events"])),
                  onFirst (withinFor (millis 100) (conn Notify.notification)) `yields` Nothing,
                  onFirst selectOne `yields` Right [1],
                  onSecond (ddl "NOTIFY events, 'later'") `yields` Right (),
                  (onFirst (conn Notify.notification) >>^ Notify.notificationPayload) `yields` "later"
                ]
        )
        `shouldReturn` ()
    it "keeps the connection usable when a program is cancelled between statements, where nothing is in flight" $
      withSession
        ( steps
            [ withinFor (millis 100) (after (secs 30) selectOne) `yields` Nothing,
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()

  needs "notifications" $ describe "notifications" $ do
    it "delivers a notification from another connection" $
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events"])),
                  onSecond (ddl "NOTIFY events, 'hello'") `yields` Right (),
                  ( onFirst (conn Notify.notification)
                      &&& arr (PG.serverProcessId . PG.serverInfo . snd . fst)
                      >>^ \(delivered, notifierPid) ->
                        (Just (Notify.notificationProcessId delivered) == notifierPid, Notify.notificationChannel delivered, Notify.notificationPayload delivered)
                  )
                    `yields` (True, "events", "hello")
                ]
        )
        `shouldReturn` ()
    it "keeps notifications that arrive during statements, in order" $
      run
        ( session (unix "postgres" Nothing) $
            steps
              [ conn (Notify.listen ["events"]),
                ddl "NOTIFY events, 'first'" `yields` Right (),
                ddl "NOTIFY events, 'second'" `yields` Right (),
                (conn Notify.notification >>^ Notify.notificationPayload) `yields` "first",
                (conn Notify.notification >>^ Notify.notificationPayload) `yields` "second"
              ]
        )
        `shouldReturn` ()
    it "fails when a listening connection is terminated" $
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events"])),
                  terminatingTheFirst `yields` Right [True],
                  onFirst (conn Notify.notification)
                    `failsAs` \case
                      ConnectionTerminated _ -> pure ()
                      other -> failTest ("expected ConnectionTerminated, got " <> show other)
                ]
        )
        `shouldReturn` ()

  describe "snippets" $
    it "sends parameters the server reads as their types" $ do
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
      run
        ( session (unix "postgres" Nothing) $
            query (Snippet.fromSnippet snippet snippetRow)
        )
        `shouldReturn` Right [(42, "a,zażółć", "1", "[1, 2]", True)]

  -- A parameter stream that breaks its promise leaves the protocol in a state nobody can continue from.
  needs "streamed parameters" $ describe "execute (failures)" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "breaks the connection when a parameter stream is shorter than declared" $
      withSession
        ( steps
            [ streaming (newChunks ["short"]) (query' (Tx.Statement "SELECT $1::text" (\source -> [streamedText 10 source]) (single @_ @Text)))
                `failsAs` \case
                  StreamLengthError {} -> pure ()
                  other -> failTest ("expected StreamLengthError, got " <> show other),
              brokenConnection
            ]
        )
        `shouldReturn` ()
    it "throws a parameter stream's failure, and breaks the connection" $
      withSession
        ( steps
            [ streaming (newFailingChunks [Char8.replicate 1000000 'y']) (query' (Tx.Statement "SELECT length($1::text)" (\source -> [streamedText 2000000 source]) (single @_ @Int32)))
                `failsAs` \(_ :: IOException) -> pure (),
              brokenConnection
            ]
        )
        `shouldReturn` ()

-- * Statements and programs of the cases

-- | The answer to one statement that has no parameters, or a failure to connect.
selectOne :: (Runtime arr) => OnConn arr (Either QueryError [Int32])
selectOne = query (plain "SELECT 1" single)

currentUser :: (Runtime arr) => OnConn arr (Either QueryError [Text])
currentUser = query (plain "SELECT current_user" single)

applicationName :: (Runtime arr) => OnConn arr (Either QueryError [Text])
applicationName = query (plain "SELECT current_setting('application_name')" single)

tlsVersion :: (Runtime arr) => OnConn arr (Either QueryError [Text])
tlsVersion = query (plain "SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid()" single)

-- | Whether the connection of the step is encrypted, as the server sees it.
encrypted :: (Runtime arr) => OnConn arr (Either QueryError [Bool])
encrypted = query (plain "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()" single)

-- | A statement that waits 30 seconds.
sleeping :: (Runtime arr) => Tx.Statement arr () Int32
sleeping = plain "SELECT 1 FROM pg_sleep(30)" single

closing :: (Runtime arr) => OnConn arr ()
closing = conn PG.close

-- | What every use of a connection that was closed, or broken, fails with.
unusable :: ConnectionError -> IO ()
unusable = \case
  ConnectionBroken _ -> pure ()
  other -> failTest ("expected ConnectionBroken, got " <> show other)

-- | Every use of the connection fails: it is broken.
brokenConnection :: (Runtime arr) => OnConn arr ()
brokenConnection = selectOne `failsAs` unusable

-- | Waits as long as the duration, in a reading program.
sleepFor :: (Runtime arr) => Duration -> With Tx.Tx arr a ()
sleepFor duration = done >>> sleep duration

-- | Cancels the statement that runs on the connection, through the key it has, over a connection of its own that is
-- made to the settings: those of the connection, or others, for the cases of TLS and channel binding on the cancel
-- connection.
cancelling :: (Runtime arr) => ConnectSettings -> OnConn arr ()
cancelling settings = PG.cancelKey . fst ^>> PG.cancel settings

-- | With two connections, the second ends the backend of the first. The process id is the text of a parameter.
terminatingTheFirst :: (Runtime arr) => arr ((Connection, Connection), Loans) (Either QueryError [Bool])
terminatingTheFirst =
  now
    ( \((listener, administrator), loans) -> case PG.serverProcessId (PG.serverInfo listener) of
        Nothing -> failTest "the server sent no process id"
        Just pid -> pure (administrator, (loans, Char8.pack (show pid)))
    )
    >>> query' (Tx.Statement "SELECT pg_terminate_backend($1::int)" (\pid -> [textual (Just pid)]) single)

-- | The SQLSTATE of the rejection that the one host tried gave.
rejection :: Either ConnectFailure a -> Maybe ByteString
rejection = \case
  Left (ConnectFailure [(_, ConnectionRejected (ServerError diagnostics))]) -> Just (diagnosticCode diagnostics)
  _ -> Nothing

-- | The reason that authentication failed on the client's side, at the one host tried.
authenticationFailure :: Either ConnectFailure a -> Maybe String
authenticationFailure = \case
  Left (ConnectFailure [(_, AuthenticationError reason)]) -> Just reason
  _ -> Nothing

-- | The settings of a connection string, with the TLS client of native-pg-tls.
settingsOf :: ByteString -> IO ConnectSettings
settingsOf string = case parseConnectionString noDefaults (const Nothing) (Just "postgres") string of
  Left problem -> fail problem
  Right parsed -> either fail (\settings -> pure settings {connectTlsClient = Just tlsClient}) (fromConnectionString parsed)

-- | No default host or certificate directory: every case gives its host.
noDefaults :: Defaults
noDefaults = Defaults "/tmp" Nothing

-- | The body on a connection to the string's settings.
onString :: Runner -> ByteString -> (forall arr. (Runtime arr) => OnConn arr b) -> IO b
onString runner string body = settingsOf string >>= \settings -> ordinary runner (session settings body)

-- | What failing to connect says, as libpq words it.
failureOf :: Runner -> ByteString -> IO String
failureOf runner string = do
  settings <- settingsOf string
  result <- ordinary runner (connectedTo settings done)
  either (pure . displayException) (const (fail "connected")) result

-- | A pair of an Int and a text, as a composite.
data Pair = Pair Int32 Text
  deriving (Eq, Show)

instance DecodeValue Pair where
  valueDecoder = composite (Pair <$> Decoding.field <*> Decoding.field)

-- | Adds one to its parameter, which is the text.
addOneWith :: (Runtime arr) => ByteString -> Tx.Statement arr () Int32
addOneWith value = valued "SELECT $1::int + 1" [Just value] single

-- | Prepares a name, and awaits the server's acknowledgement.
preparing :: (Runtime arr) => ByteString -> Tx.Statement arr () row -> OnConn arr (Either QueryError ())
preparing name statement = committed Tx.Write (Tx.prepare name statement) Tx.prepared

-- | The answer of a statement that is prepared on the connection, for the text as its parameter. The statement's
-- SQL is never evaluated: the connection has it.
preparedAnswer :: (Runtime arr) => ByteString -> ByteString -> OnConn arr (Either QueryError Int32)
preparedAnswer name value =
  committed Tx.Write (Tx.executePrepared name ((addOneWith value) {Tx.sql = error "SQL forced"})) Tx.single

-- | The statement that inserts its parameter, which the function makes of the input.
insertStreamed :: (Runtime arr) => (p -> [Tx.Param arr]) -> Tx.Statement arr p ()
insertStreamed parameters = Tx.Statement "INSERT INTO streamed VALUES ($1)" parameters Tx.none

-- | The statement that takes the length of its parameter, which the function makes of the input.
lengthWith :: (Runtime arr) => (p -> [Tx.Param arr]) -> Tx.Statement arr p Int32
lengthWith parameters = Tx.Statement "SELECT length($1::text)" parameters single

-- | A source of four slow pieces of a thousand bytes.
slowSource :: IO SlowChunks
slowSource = newSlowChunks (millis 150) (replicate 4 (Char8.replicate 1000 'y'))

-- | The step, given this input with the connection and the loans.
withInput :: (Runtime arr) => p -> arr (Connection, (Loans, p)) b -> OnConn arr b
withInput input step = second (,input) ^>> step

-- | A statement run in a transaction of its own that takes its parameter stream as its input.
query' :: (Runtime arr) => Tx.Statement arr p row -> arr (Connection, (Loans, p)) (Either QueryError [row])
query' statement = transactOn Tx.ReadCommitted Tx.Write Tx.Final (Tx.statement statement) readAll

-- | A prepared statement run with its parameter stream as its input.
preparedStreamed :: (Runtime arr) => ByteString -> Tx.Statement arr p row -> arr (Connection, (Loans, p)) (Either QueryError [row])
preparedStreamed name statement = transactOn Tx.ReadCommitted Tx.Write Tx.Open (Tx.executePrepared name statement) readAll

-- | Both the first response and the remaining request exceed the socket buffers, so the connection must go on
-- reading while it sends. The first result is a field of a million bytes.
batchWhileReading :: (Runtime arr) => OnConn arr (Maybe (Either QueryError (Int, Int32)))
batchWhileReading =
  withinFor (secs 10) $
    committed Tx.Read largeThenLong $ \(large, long) ->
      (Tx.single large >>> measure >>^ fst) &&& Tx.single long

-- | A result with a large field, and a statement whose parameter is large.
largeThenLong :: (Runtime arr) => Tx.Pipeline arr () (Tx.Result arr Tx.Field, Tx.Result arr Int32)
largeThenLong =
  (,)
    <$> Tx.statement (plain "SELECT repeat('x', 1000000)" (Tx.any Tx.field))
    <*> Tx.statement (valued "SELECT length($1::text)" [Just (Char8.replicate 2000000 'y')] single)

-- | Streams the single field slowly, checking its bytes and that no piece is larger than the buffer, while
-- sampling the live heap. The accumulators are strict, so they don't keep the pieces alive.
consumeSlowly :: (Runtime arr) => With Tx.Tx arr Tx.Field (Int, Bool, Bool)
consumeSlowly =
  now (\field -> (\baseline -> (field, consumedFrom baseline)) <$> liveBytes)
    >>> foldPieces step
    >>> now (\Consumed {consumedBytes, consumedSmall, consumedValid, consumedPeak, consumedBaseline} -> do
          consumedPeak - consumedBaseline `shouldSatisfy` P.lt (8 * 1024 * 1024)
          pure (consumedBytes, consumedSmall, consumedValid))
  where
    consumedFrom baseline =
      Consumed
        { consumedPieces = 0,
          consumedBytes = 0,
          consumedSmall = True,
          consumedValid = True,
          consumedPeak = baseline,
          consumedBaseline = baseline
        }
    -- A blocking delay every 256 pieces, on purpose: the consumer is slow without a wait of the arrow, so that only
    -- the socket pushes back on the server. The pieces are views, no larger than the pool's buffers of 1 KiB.
    step consumed@Consumed {consumedPieces, consumedBytes, consumedSmall, consumedValid, consumedPeak} piece = do
      peak' <-
        if consumedPieces `mod` 256 == 255
          then threadDelay 2000 *> (max consumedPeak <$> liveBytes)
          else pure consumedPeak
      -- The piece is looked at now: it is a view, and the bytes it is of are the next piece's once it is received.
      let !small' = consumedSmall && BS.length piece <= 1024
          !valid' = consumedValid && BS.all (== 120) piece
      pure
        consumed
          { consumedPieces = consumedPieces + 1,
            consumedBytes = consumedBytes + BS.length piece,
            consumedSmall = small',
            consumedValid = valid',
            consumedPeak = peak'
          }

-- | What 'consumeSlowly' has seen of the pieces: how many there were, how many bytes they had, whether all were small
-- and valid, and the live heap bytes at the start and at the highest sample.
data Consumed = Consumed
  { consumedPieces :: Int,
    consumedBytes :: Int,
    consumedSmall :: Bool,
    consumedValid :: Bool,
    consumedPeak :: Int,
    consumedBaseline :: Int
  }

-- | Live heap bytes after a major collection.
liveBytes :: IO Int
liveBytes = do
  performMajorGC
  fromIntegral . gcdetails_live_bytes . gc <$> getRTSStats

-- | Runs the action with the directory of a Unix-domain socket that accepts connections but never answers.
withSilentServer :: (FilePath -> IO a) -> IO a
withSilentServer action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-silent-")) removeDirectoryRecursive $ \directory ->
    bracket (Socket.socket Socket.AF_UNIX Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
      Socket.bind sock (Socket.SockAddrUnix (directory </> ".s.PGSQL.5432"))
      Socket.listen sock 4
      action directory

-- | Runs the action with a port at 127.0.0.1 that nothing listens on.
withUnusedPort :: (Socket.PortNumber -> IO a) -> IO a
withUnusedPort action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close
    (\sock -> Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1))) *> Socket.socketPort sock)
    >>= action

-- | Runs the action with the port at 127.0.0.1 of a server that serves the connections that come, one by each script in
-- turn, which is given its socket. A connection is closed when its script ends, and what goes wrong in it shows in the client.
withScripted :: [Socket.Socket -> IO ()] -> (Socket.PortNumber -> IO a) -> IO a
withScripted scripts action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \listener -> do
    Socket.bind listener (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    Socket.listen listener 4
    -- Each connection is served on a thread of its own, since the first is still open while the second is made.
    let serve script = do
          (client, _) <- Socket.accept listener
          void (forkIO (script client `finally` Socket.close client `catch` \(_ :: SomeException) -> pure ()))
    bracket (forkIO (traverse_ serve scripts `catch` \(_ :: SomeException) -> pure ())) killThread $ \_ -> action =<< Socket.socketPort listener

-- | A message of the server: its tag, its length, and its payload.
frame :: Char -> ByteString -> ByteString
frame kind payload = Char8.singleton kind <> int32 (BS.length payload + 4) <> payload

-- | The four bytes of a number, the most significant first.
int32 :: Int -> ByteString
int32 number = BS.pack [fromIntegral (number `shiftR` shift) | shift <- [24, 16, 8, 0]]

-- | Takes the client's startup message, which has no tag: its length, and the rest.
receiveStartup :: Socket.Socket -> IO ByteString
receiveStartup client = do
  size <- BS.foldl' (\total byte -> total * 256 + fromIntegral byte) 0 <$> SocketBytes.recv client 4
  SocketBytes.recv client (size - 4)

-- | Takes this many bytes from the client.
receiving :: Socket.Socket -> Int -> IO ByteString
receiving client size
  | size <= 0 = pure BS.empty
  | otherwise = SocketBytes.recv client size >>= \piece -> if BS.null piece then pure piece else (piece <>) <$> receiving client (size - BS.length piece)

-- | Takes what the client sends until it closes the connection.
drain :: Socket.Socket -> IO ()
drain client = SocketBytes.recv client 4096 >>= \received -> unless (BS.null received) (drain client)

-- | Runs the action with a copy of the client certificate's key, with the file mode given.
withKeyCopy :: FileMode -> (FilePath -> IO a) -> IO a
withKeyCopy mode action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-key-")) removeDirectoryRecursive $ \directory -> do
    let key = directory </> "client.key"
    copyFile (certificate "client.key") key
    setFileMode key mode
    action key

-- | Runs the action with a port at 127.0.0.1 where a server answers that it doesn't take TLS.
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

-- * Rows of the cases

numbersRow :: (Runtime arr) => Rows arr (Int16, Int32, Int64, Bool, Float, Double, Scientific)
numbersRow =
  Tx.any ((,,,,,,) <$> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column)

textsRow :: (Runtime arr) => Rows arr (Text, ByteString, Char, UUID, Json, Jsonb)
textsRow =
  Tx.any ((,,,,,) <$> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column)

-- | A column that may be NULL, and one that may not.
nullableThen :: (Runtime arr, InPlace a, InPlace b) => Rows arr (Maybe a, b)
nullableThen = Tx.any (Tx.nullable &&& Tx.column)

-- | A number, and a field that follows it.
statusAndBody :: (Runtime arr) => Rows arr (Int32, Tx.Field)
statusAndBody = Tx.any (Tx.column &&& Tx.field)

snippetRow :: (Runtime arr) => Rows arr (Int32, Text, Text, Text, Bool)
snippetRow =
  Tx.any ((,,,,) <$> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column <*> Tx.column)
