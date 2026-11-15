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

import Control.Arrow (arr, second, (&&&), (>>>), (>>^), (^>>), (|||))
import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (IOException, bracket, displayException, finally)
import Control.Monad (forM_, forever, unless, void)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import Data.Int (Int16, Int32, Int64)
import Data.List (intercalate, isPrefixOf, nub)
import Data.Maybe (isJust, isNothing)
import Data.Scientific (Scientific)
import Data.Text (Text)
import Data.Time (Day, DiffTime, UTCTime (..), fromGregorian)
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Database.PostgreSQL.Connection (ChannelBindingMode (..), ConnectSettings (..), Connection, SslMode (..), prepareTls)
import qualified Database.PostgreSQL.Connection as PG
import qualified Database.PostgreSQL.Connection.Internal as PGInternal
import Database.PostgreSQL.ConnectionString (Defaults (..), Host (..), emptyDefaults, fromConnectionString, parseConnectionString)
import Database.PostgreSQL.Decoding (DecodeValue (..), DecodingError (..), InPlace, Json (..), Jsonb (..), composite)
import qualified Database.PostgreSQL.Decoding as Decoding
import qualified Database.PostgreSQL.Encoders as Encoders
import Database.PostgreSQL.Errors
import Database.PostgreSQL.Internal.Cares (Resolver, ResolverOptions (..), defaultResolverOptions, newResolverWith)
import qualified Database.PostgreSQL.Notifications as Notify
import Database.PostgreSQL.Resolver (newResolver)
import qualified Database.PostgreSQL.Snippet as Snippet
import Database.PostgreSQL.Tls (newTlsClient)
import Database.PostgreSQL.Transaction (Rows (..))
import qualified Database.PostgreSQL.Transaction as Tx
import Database.PostgreSQL.Transport (RootCertificates (..))
import Database.PostgreSQL.Types (protocolVersion3_0, protocolVersion3_2)
import GHC.Clock (getMonotonicTime)
import GHC.Stats (GCDetails (..), RTSStats (..), getRTSStats, getRTSStatsEnabled)
import qualified Network.Socket as Socket
import qualified Network.Socket.ByteString as SocketBytes
import Skeletest
import qualified Skeletest.Predicate as P
import System.Directory (copyFile, getTemporaryDirectory, removeDirectoryRecursive, removeFile, setModificationTime)
import System.FilePath (takeDirectory, (</>))
import System.Mem (performMajorGC)
import System.Posix.Files (groupReadMode, otherReadMode, ownerReadMode, setFileMode, unionFileModes)
import System.Posix.Temp (mkdtemp)
import System.Posix.Types (FileMode)
import DnsServer (DnsServer (..), Reply (..), withDnsServer)
import TestCluster (certificate)
import TestPrograms hiding (field)
import TestServers

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

  -- What the client can't do in the authentication is worded as libpq words it, in the failure of the host. The cluster
  -- has no server that asks for what the client doesn't have, so a scripted server asks.
  describe "authentication wording" $ do
    let failureAt settings = either displayException (const "connected") <$> run (connectedTo settings done)
        onSocket reason = "connection to server on socket \"" <> socketPath <> "\" failed: " <> reason
        startupThen reply client = receiveStartup client *> SocketBytes.sendAll client reply *> drain client
        -- The reason that the one host gave, which the scripts make.
        reasonOf :: [Socket.Socket -> IO ()] -> IO String
        reasonOf scripts = withScripted scripts $ \scriptedPort -> do
          settings <- settingsOf ("host=127.0.0.1 user=postgres password=secret sslmode=disable port=" <> Char8.pack (show scriptedPort))
          failure <- failureAt settings
          pure (drop (length ("connection to server at \"127.0.0.1\", port " <> show scriptedPort <> " failed: ")) failure)
        scripted reply = reasonOf [startupThen reply]
    forM_ [("SCRAM-SHA-256", "native_pg_scram"), ("MD5", "native_pg_md5"), ("a cleartext password", "native_pg_cleartext")] $ \(method, user) ->
      it ("says that no password was supplied, when the server asks for one for " <> method) $
        failureAt (unix user Nothing) `shouldReturn` onSocket "fe_sendauth: no password supplied"
    it "says that GSSAPI isn't supported" $ scripted (frame 'R' (int32 7)) `shouldReturn` "GSSAPI authentication not supported"
    it "says that SSPI isn't supported" $ scripted (frame 'R' (int32 9)) `shouldReturn` "SSPI authentication not supported"
    -- Checked against libpq's own words, from a psql on the same scripted server.
    it "says that Kerberos 5 isn't supported" $
      scripted (frame 'R' (int32 2)) `shouldReturn` "Kerberos 5 authentication not supported"
    it "says that an authentication method that no client has isn't supported" $
      scripted (frame 'R' (int32 99)) `shouldReturn` "authentication method 99 not supported"
    it "says that none of the SASL mechanisms is supported" $
      scripted (frame 'R' (int32 10 <> "SCRAM-SHA-512\0\0")) `shouldReturn` "none of the server's SASL authentication mechanisms are supported"
    -- The server's side of SCRAM, up to a message that the client doesn't accept.
    let scramThen serverFirst final = \client -> do
          _ <- receiveStartup client
          SocketBytes.sendAll client (frame 'R' (int32 10 <> "SCRAM-SHA-256\0\0"))
          (_, clientFirst) <- receiveMessage client
          let nonce = BS.drop 2 (snd (BS.breakSubstring "r=" clientFirst))
          SocketBytes.sendAll client (frame 'R' (int32 11 <> serverFirst nonce))
          maybe (pure ()) (\finalMessage -> receiveMessage client *> SocketBytes.sendAll client (frame 'R' (int32 12 <> finalMessage))) final
          drain client
        scramFailure serverFirst final = reasonOf [scramThen serverFirst final]
        agreeing nonce = "r=" <> nonce <> "server,s=c2FsdA==,i=4096"
    it "says that the server's nonce isn't the client's" $
      scramFailure (const "r=anothernonce,s=c2FsdA==,i=4096") Nothing `shouldReturn` "invalid SCRAM response (nonce mismatch)"
    it "says that the server's signature is incorrect" $
      scramFailure agreeing (Just ("v=" <> BS.replicate 43 65 <> "=")) `shouldReturn` "incorrect server signature"
    it "says what the server reports of its failing the proof" $
      scramFailure agreeing (Just "e=invalid-proof") `shouldReturn` "error received from server in SCRAM exchange: invalid-proof"

  -- A connection is closed when its scope ends. Closing it earlier is allowed, and so is closing it again.
  describe "close" $
    forM_ [("without TLS", pure (unix "postgres" Nothing)), ("with TLS", encryptedAs "postgres" Nothing)] $ \(label, connecting) ->
      describe label $ do
        it "rejects every operation after closing" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ closing,
                  selectOne `failsAs` unusable,
                  committed Tx.Read (Tx.prepare "closed" (plain "SELECT 1" (single @_ @Int32))) Tx.prepared `failsAs` unusable,
                  conn (Notify.listen ["events"]) `failsAs` unusable,
                  conn Notify.notification `failsAs` unusable
                ]
        it "can close repeatedly, also where its scope ends" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ closing,
                  withinFor (secs 5) (steps [closing, closing]) `yields` Just (),
                  selectOne `failsAs` unusable
                ]
        -- v1 broke the session when the step of a row threw. A failing reading program of v2 rolls back and rethrows,
        -- and the connection stays usable (see "TransactionSpec"). What breaks one is the cancellation rule: a statement
        -- that is cancelled while its answer is awaited leaves answers that nobody will read, and a connection is
        -- never drained for a client that has gone away. The case says only that closing works after it, however often.
        it "can close a connection that a cancelled statement broke, repeatedly" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ withinFor (millis 300) (query sleeping) `yields` Nothing,
                  closing,
                  withinFor (secs 5) (steps [closing, closing]) `yields` Just (),
                  selectOne `failsAs` unusable
                ]
        -- The cancellation rule: v1 left the session unusable after a cancelled wait for a notification, but the reader
        -- is at a message boundary then and nothing is half sent or expected, so v2 leaves the connection usable.
        it "keeps a connection usable when a wait for a notification is cancelled" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ conn (Notify.listen ["events"]),
                  withinFor (millis 100) (conn Notify.notification) `yields` Nothing,
                  selectOne `yields` Right [1]
                ]
        -- A connection has one user at a time, and a second one is turned away with 'ConnectionBusy', without disrupting
        -- the first. In v1 only 'close' was, and a nested statement broke the session.
        it "rejects closing while a statement runs, without disrupting it" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ concurrently (query (plain "SELECT 1 FROM pg_sleep(0.3)" (single @_ @Int32))) (after (millis 100) (closing `failsWith` ConnectionBusy))
                    `yields` (Right [1], ()),
                  selectOne `yields` Right [1]
                ]
        it "rejects a second statement while one runs, without disrupting it" $ do
          settings <- connecting
          run $
            session settings $
              steps
                [ concurrently (query (plain "SELECT 1 FROM pg_sleep(0.3)" (single @_ @Int32))) (after (millis 100) (selectOne `failsWith` ConnectionBusy))
                    `yields` (Right [1], ()),
                  selectOne `yields` Right [1]
                ]

  -- The binding data that SCRAM-PLUS signs comes from the TLS link. These cases replace it.
  describe "channel-binding data from TLS setup" $ do
    let suppliedBinding supplied mode user =
          (\settings -> PGInternal.withEndPointOverride supplied (settings {connectChannelBinding = mode})) <$> encryptedAs user (Just "scram secret")
        bindingFailure = Just (Left "unsupported certificate hash")
        connectedWith settings = run (connectedTo settings done)
    it "uses the supplied binding bytes in SCRAM-PLUS" $
      (rejection <$> (suppliedBinding (Just (Right "wrong binding")) ChannelBindingRequire "native_pg_scram" >>= connectedWith))
        `shouldReturn` Just "28000"
    forM_ [("preferred", ChannelBindingPrefer), ("required", ChannelBindingRequire)] $ \(label, mode) ->
      it ("reports a binding error when SCRAM-PLUS is " <> label) $
        (authenticationFailure <$> (suppliedBinding bindingFailure mode "native_pg_scram" >>= connectedWith))
          `shouldReturn` Just "unsupported certificate hash"
    it "ignores a binding error when channel binding is disabled" $
      (suppliedBinding bindingFailure ChannelBindingDisable "native_pg_scram" >>= \settings -> run (session settings currentUser))
        `shouldReturn` Right ["native_pg_scram"]
    it "does not require binding data for non-SCRAM authentication" $
      (suppliedBinding bindingFailure ChannelBindingPrefer "postgres" >>= \settings -> run (session settings selectOne)) `shouldReturn` Right [1]
    it "allows unbound SCRAM when binding is unavailable and optional" $
      (suppliedBinding Nothing ChannelBindingPrefer "native_pg_scram" >>= \settings -> run (session settings currentUser))
        `shouldReturn` Right ["native_pg_scram"]
    it "rejects required binding when the TLS link supplies none" $
      (authenticationFailure <$> (suppliedBinding Nothing ChannelBindingRequire "native_pg_scram" >>= connectedWith))
        `shouldReturn` Just "channel binding is required, but server did not offer an authentication method that supports channel binding"
    it "ignores binding errors on a cancellation connection" $ do
      supplied <- suppliedBinding bindingFailure ChannelBindingRequire "postgres"
      run $
        session (unix "postgres" Nothing) $
          cancelling supplied

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
    -- A scripted DNS server stands for the system's, so that a lookup can be answered, answered late or not answered. The
    -- resolver asks it only, and looks up no search domains.
    describe "host names, looked up by c-ares" $ do
      let loopback = Addresses [(127, 0, 0, 1)]
          viaName resolver host extra = settingsVia resolver (viaPlaintext host extra)
          connectedBy settings = run (session settings (arr (hostName . PG.connectedHost . fst) &&& selectOne))
          failureOf' settings = either displayException (const "connected") <$> run (connectedTo settings done)
          asked dns = nub . map fst <$> dnsQuestions dns
          secondsOf start = subtract start <$> getMonotonicTime
      it "connects to the address that a name has, and asks the server for it" $
        withDnsServer (const loopback) $ \dns -> do
          resolver <- resolverFor dns 2000 1
          settings <- viaName resolver "scripted.test" ""
          connectedBy settings `shouldReturn` ("scripted.test", Right [1])
          asked dns `shouldReturn` ["scripted.test"]
      it "waits for an answer that comes late" $
        withDnsServer (const (Later 600000 loopback)) $ \dns -> do
          resolver <- resolverFor dns 3000 1
          settings <- viaName resolver "slow.test" ""
          started <- getMonotonicTime
          connectedBy settings `shouldReturn` ("slow.test", Right [1])
          secondsOf started >>= (`shouldSatisfy` P.gte 0.5)
      -- The part that looks the name up waits for the answer where the other parts can go on: here the other part ends
      -- while the answer is still to come, which it couldn't if the lookup held up its core.
      it "leaves the core running other parts while it waits for the answer" $
        withDnsServer (const (Later 1500000 loopback)) $ \dns -> do
          resolver <- resolverFor dns 5000 1
          settings <- viaName resolver "slow.test" ""
          ((connected, lookupTime), ((), tickTime)) <-
            run (concurrently (timed (connectedTo settings done)) (timed (done >>> times 10 (sleep (millis 20)))))
          connected `shouldSatisfy` P.right P.anything
          (tickTime < lookupTime, lookupTime >= 1400000000) `shouldBe` (True, True)
      it "gives up on a lookup after connect_timeout" $
        withDnsServer (const Silent) $ \dns -> do
          resolver <- resolverFor dns 30000 1
          settings <- viaName resolver "silent.test" "connect_timeout=2"
          started <- getMonotonicTime
          failureOf' settings `shouldReturn` ("connection to server at \"silent.test\", port " <> show port <> " failed: timeout expired")
          secondsOf started >>= (`shouldSatisfy` (P.gte 1.9 P.&& P.lt 6))
      it "stops a lookup that is cancelled, and looks the next name up with the same resolver" $
        withDnsServer (\queried -> if queried == "silent.test" then Silent else loopback) $ \dns -> do
          resolver <- resolverFor dns 30000 1
          silent <- viaName resolver "silent.test" ""
          answered <- viaName resolver "scripted.test" ""
          started <- getMonotonicTime
          run (withinFor (millis 300) (connectedTo silent done)) >>= (`shouldSatisfy` P.nothing)
          secondsOf started >>= (`shouldSatisfy` P.lt 2)
          connectedBy answered `shouldReturn` ("scripted.test", Right [1])
      -- The cancelled lookup cancels what the resolver is asking for, and the other lookup goes on.
      it "lets a lookup go on when another one that the resolver serves is cancelled" $
        withDnsServer (\queried -> if queried == "silent.test" then Silent else Later 1000000 loopback) $ \dns -> do
          resolver <- resolverFor dns 30000 1
          silent <- viaName resolver "silent.test" ""
          slow <- viaName resolver "slow.test" ""
          (slowAnswer, silentAnswer) <-
            run (concurrently (session slow (arr (hostName . PG.connectedHost . fst) &&& selectOne)) (withinFor (millis 300) (connectedTo silent done)))
          (slowAnswer, isNothing silentAnswer) `shouldBe` (("slow.test", Right [1]), True)
      -- The resolver leaves the addresses in the order that the server gives them here. Both fail at once, as no address
      -- of the loopback does on every system: a broadcast and a multicast address, which no connection can be made to.
      it "tries the addresses of a name in order, and words the failure of each" $
        withDnsServer (const (Addresses [(255, 255, 255, 255), (224, 0, 0, 1)])) $ \dns -> do
          resolver <- newResolverWith (resolverOptionsFor dns 2000 1) {resolverSort = False}
          settings <- settingsVia resolver ("host=ordered.test sslmode=disable port=" <> Char8.pack (show port))
          failure <- failureOf' settings
          let addressOf line = takeWhile (/= ')') (drop 1 (dropWhile (/= '(') line))
          map addressOf (filter ("connection to server at \"ordered.test\"" `isPrefixOf`) (lines failure)) `shouldBe` ["255.255.255.255", "224.0.0.1"]
      it "connects to a later address of a name after one that fails" $
        withDnsServer (const (Addresses [(255, 255, 255, 255), (127, 0, 0, 1)])) $ \dns -> do
          resolver <- newResolverWith (resolverOptionsFor dns 2000 1) {resolverSort = False}
          settings <- viaName resolver "ordered.test" ""
          connectedBy settings `shouldReturn` ("ordered.test", Right [1])
      -- Over TCP the resolver waits for the socket's being readable and writable at once while it connects, which is one wait
      -- for several events.
      it "looks a name up over TCP" $
        withDnsServer (const (Later 300000 loopback)) $ \dns -> do
          resolver <- newResolverWith (resolverOptionsFor dns 3000 1) {resolverTcp = True}
          settings <- viaName resolver "stream.test" ""
          connectedBy settings `shouldReturn` ("stream.test", Right [1])
          asked dns `shouldReturn` ["stream.test"]
      -- Two servers, and a query that goes to either, make the resolver wait for the sockets of both at once: one wait for
      -- several events. The names are looked up together, so that both servers are asked.
      it "looks names up while it waits for the sockets of two servers" $
        withDnsServer (const (Later 300000 loopback)) $ \primary -> withDnsServer (const (Later 300000 loopback)) $ \backup -> do
          resolver <- newResolverWith (resolverOptionsFor primary 3000 1) {resolverServers = Just ("127.0.0.1:" <> show (dnsPort primary) <> ",127.0.0.1:" <> show (dnsPort backup)), resolverRotate = True}
          settings <- traverse (\n -> viaName resolver ("many" <> Char8.pack (show n) <> ".test") "") [1 .. 8 :: Int]
          results <- run (concurrentlyAll [connectedTo each done | each <- settings])
          length (filter (either (const False) (const True)) results) `shouldBe` 8
          (,) <$> (not . null <$> dnsQuestions primary) <*> (not . null <$> dnsQuestions backup) `shouldReturn` (True, True)
      it "words a name that the server says doesn't exist as libpq does" $
        withDnsServer (const NoSuchName) $ \dns -> do
          resolver <- resolverFor dns 2000 1
          settings <- settingsVia resolver "host=nope.test"
          failure <- failureOf' settings
          failure `shouldSatisfy` P.hasPrefix "could not translate host name \"nope.test\" to address: "
          asked dns `shouldReturn` ["nope.test"]
      -- A name in the hosts file is never asked for. The file is read when the resolver is made, and not by the first
      -- lookup: the second content has the file's old modification time, which the resolver takes for a file it has
      -- read, so only what the first content said is found.
      it "reads the hosts file when it is made, and not when a name is looked up" $
        withDnsServer (const NoSuchName) $ \dns -> withTemporaryHosts "127.0.0.1 hosted.test\n" $ \hosts -> do
          resolver <- newResolverWith (resolverOptionsFor dns 2000 1) {resolverHostsFile = Just hosts}
          writeHosts hosts "127.0.0.1 changed.test\n"
          hosted <- viaName resolver "hosted.test" ""
          changed <- viaName resolver "changed.test" ""
          connectedBy hosted `shouldReturn` ("hosted.test", Right [1])
          failureOf' changed >>= (`shouldSatisfy` P.hasPrefix "could not translate host name \"changed.test\" to address: ")
          asked dns `shouldReturn` ["changed.test"]
      -- An address and a Unix-domain socket need no lookup, and neither does a name with hostaddr.
      it "doesn't ask the server for an address, or for a name with hostaddr" $
        withDnsServer (const Silent) $ \dns -> do
          resolver <- resolverFor dns 2000 1
          numeric <- settingsVia resolver (viaPlaintext "127.0.0.1" "")
          hostaddr <- viaName resolver "never.test" "hostaddr=127.0.0.1"
          connectedBy numeric `shouldReturn` ("127.0.0.1", Right [1])
          connectedBy hostaddr `shouldReturn` ("never.test", Right [1])
          asked dns `shouldReturn` []
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
    -- connect and cancel take their settings as input: one arrow, built once, serves the settings of each run.
    describe "settings as input" $ do
      let connectingWith :: [ConnectSettings] -> (forall arr. (Runtime arr) => OnConn arr b) -> IO [Either ConnectFailure b]
          connectingWith settingsList body = run (across (onSettings body) settingsList)
          reasons :: [Either ConnectFailure b] -> [Either String b]
          reasons = map (either (Left . displayException) Right)
      it "connects with the settings of each run, one after the other, and a failure of one doesn't disturb the next" $ do
        one <- settingsOf (local "application_name=one")
        two <- settingsOf (local "application_name=two")
        wrong <- settingsOf (local "user=native_pg_scram password=wrong")
        results <- connectingWith [one, two, wrong, one] (arr (lookup "application_name" . PG.serverParameters . PG.serverInfo . fst))
        map (\result -> (rejection result, either (const Nothing) id result)) results
          `shouldBe` [(Nothing, Just "one"), (Nothing, Just "two"), (Just "28P01", Nothing), (Nothing, Just "one")]
      it "takes the TLS mode and the TLS client from the settings of each run" $ do
        secured <- settingsOf (tcp "sslmode=require")
        unencrypted <- settingsOf (local "")
        results <- connectingWith [secured, unencrypted, secured {connectTlsClient = Nothing}, secured] encrypted
        reasons results
          `shouldBe` [ Right (Right [True]),
                       Right (Right [False]),
                       Left ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: sslmode value \"require\" invalid when SSL support is not compiled in"),
                       Right (Right [True])
                     ]
      it "takes connect_timeout from the settings of each run, and has none for settings that have none" $
        withSilentServer $ \silent -> do
          slow <- settingsOf ("host=" <> Char8.pack silent <> " connect_timeout=1")
          unlimited <- settingsOf (local "")
          started <- getMonotonicTime
          results <- connectingWith [slow, unlimited] done
          elapsed <- subtract started <$> getMonotonicTime
          reasons results
            `shouldBe` [Left ("connection to server on socket \"" <> silent <> "/.s.PGSQL.5432\" failed: timeout expired"), Right ()]
          elapsed `shouldSatisfy` (P.gte 0.9 P.&& P.lt 5)
      it "takes the hosts from the settings of each run" $ do
        missing <- settingsOf "host=/nonexistent-1,/nonexistent-2"
        fallback <- settingsOf ("host=/nonexistent," <> Char8.pack directory <> " port=" <> Char8.pack (show port) <> " dbname=postgres")
        results <- connectingWith [missing, fallback, missing] (arr (hostName . PG.connectedHost . fst))
        map (either (\(ConnectFailure failures) -> Left (length failures)) Right) results `shouldBe` [Left 2, Right directory, Left 2]
      it "cancels with the settings of each run" $ do
        settings <- settingsOf (tcp "sslmode=require")
        withPlaintextServer $ \plaintext -> do
          refusing <- settingsOf ("host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require")
          run $
            session (unix "postgres" Nothing) $
              steps
                [ (concurrently (query sleeping) (after (millis 300) (cancelling settings)) >>^ serverCode . fst) `yields` Just "57014",
                  cancelling refusing
                    `failsAs` \case
                      TlsFailure reason -> reason `shouldBe` "server does not support SSL, but SSL was required"
                      other -> failTest ("expected TlsFailure, got " <> show other)
                ]
    describe "with TLS" $ do
      let encryptedBy string = withString (tcp string) encrypted
      it "encrypts with sslmode=require, without verifying the server" $
        encryptedBy "sslmode=require" `shouldReturn` Right [True]
      -- The batch is larger than the buffers that the two ends of a TCP connection can grow to, so that the server stops
      -- writing a result that nobody reads, and stops reading the request, for every platform and every run.
      it "sends a large batch while reading its responses over TLS" $
        withString (tcp "sslmode=require") (batchWhileReading 32000000 32000000) `shouldReturn` Just (Right (32000000, 32000000))
      it "doesn't encrypt over a Unix-domain socket, as libpq doesn't" $
        withString (local "sslmode=require") encrypted `shouldReturn` Right [False]
      it "fails if the server doesn't take TLS" $
        withPlaintextServer $ \plaintext ->
          connectFailure ("host=127.0.0.1 port=" <> Char8.pack (show plaintext) <> " sslmode=require")
            `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show plaintext <> " failed: server does not support SSL, but SSL was required")
      it "fails as libpq without TLS does, without a TLS client" $ do
        settings <- settingsOf (tcp "sslmode=require")
        result <- run (connectedTo settings {connectTlsClient = Nothing} done)
        either displayException (const "connected") result
          `shouldBe` ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: sslmode value \"require\" invalid when SSL support is not compiled in")
      it "encrypts by default, with sslmode=prefer, as libpq does" $
        encryptedBy "" `shouldReturn` Right [True]
      it "falls back to plaintext with sslmode=prefer when the server rejects TLS" $
        encryptedBy "user=native_pg_plaintext" `shouldReturn` Right [False]
      -- libpq tries again without TLS only after the server's rejection and a failure of TLS. A host that doesn't qualify, and
      -- a failure of the client's own, are reported as they are: the cluster takes these users over TLS only, so a second
      -- attempt in plaintext would report a rejection by pg_hba.conf in their place.
      it "reports a host that doesn't qualify over TLS with sslmode=prefer, and doesn't try it in plaintext" $
        connectFailure (tcp "options='-c default_transaction_read_only=on' target_session_attrs=read-write")
          `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: session is read-only")
      it "reports the client's own authentication failure over TLS with sslmode=prefer, and doesn't try plaintext" $
        connectFailure (tcp "user=native_pg_scram")
          `shouldReturn` ("connection to server at \"127.0.0.1\", port " <> show port <> " failed: fe_sendauth: no password supplied")
      it "tries plaintext after a failure of TLS with sslmode=prefer" $
        withScripted
          [ \client -> receiveStartup client *> SocketBytes.sendAll client "S" *> SocketBytes.recv client 4096 *> SocketBytes.sendAll client "this is no TLS record" *> drain client,
            \client -> receiveStartup client *> SocketBytes.sendAll client (frame 'R' (int32 0) <> frame 'Z' "I") *> drain client
          ]
          $ \scripted -> withString ("host=127.0.0.1 user=postgres sslmode=prefer port=" <> Char8.pack (show scripted)) done `shouldReturn` ()
      it "tries plaintext first with sslmode=allow, then TLS" $ do
        encryptedBy "sslmode=allow user=native_pg_plaintext" `shouldReturn` Right [False]
        encryptedBy "sslmode=allow" `shouldReturn` Right [True]
      it "connects in plaintext with sslmode=prefer without a TLS client, as libpq without TLS does" $ do
        settings <- settingsOf (tcp "user=native_pg_plaintext")
        run (session settings {connectTlsClient = Nothing} encrypted) `shouldReturn` Right [False]
      it "verifies the server's certificate and name with sslmode=verify-full" $ do
        encryptedBy ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=localhost hostaddr=127.0.0.1")
          `shouldReturn` Right [True]
        encryptedBy ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt")) `shouldReturn` Right [True]
      it "refuses a server whose certificate no root signed, or that has another name" $ do
        let at host = "connection to server at \"" <> host <> "\" (127.0.0.1), port " <> show port <> " failed: "
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "other-ca.crt") <> " host=localhost hostaddr=127.0.0.1"))
          `shouldReturn` (at "localhost" <> "SSL error: invalid peer certificate: UnknownIssuer")
        connectFailure (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt") <> " host=other.example hostaddr=127.0.0.1"))
          `shouldReturn` (at "other.example" <> "SSL error: invalid peer certificate: NotValidForName")
      it "verifies against the system's root certificates with sslrootcert=system" $ do
        failure <- connectFailure (tcp "sslmode=verify-full sslrootcert=system host=localhost hostaddr=127.0.0.1")
        -- The platforms' verifiers word it differently.
        failure `shouldSatisfy` P.hasPrefix ("connection to server at \"localhost\" (127.0.0.1), port " <> show port <> " failed: SSL error: ")
      it "needs the root certificate file, as libpq does" $
        connectFailure (tcp "sslmode=verify-full sslrootcert=/nonexistent/root.crt host=localhost hostaddr=127.0.0.1")
          `shouldReturn` ( "connection to server at \"localhost\" (127.0.0.1), port "
                             <> show port
                             <> " failed: root certificate file \"/nonexistent/root.crt\" does not exist\nEither provide the file, use the system's trusted roots with sslrootcert=system, or change sslmode to disable server certificate verification."
                         )
      it "starts TLS directly with sslnegotiation=direct, as PostgreSQL 17 takes it" $
        encryptedBy "sslnegotiation=direct sslmode=require" `shouldReturn` Right [True]
      it "cancels over TLS, and asks for TLS to cancel, as libpq does since PostgreSQL 17" $ do
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
      it "binds SCRAM to TLS with channel_binding=require" $
        withString (tcp "user=native_pg_scram password='scram secret' sslmode=require channel_binding=require") currentUser
          `shouldReturn` Right ["native_pg_scram"]
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
        let version extra = withString (tcp ("sslmode=require " <> extra)) tlsVersion
        version "" `shouldReturn` Right ["TLSv1.3"]
        version "ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ["TLSv1.2"]
        version "ssl_min_protocol_version=TLSv1 ssl_max_protocol_version=TLSv1.2" `shouldReturn` Right ["TLSv1.2"]
      it "checks revocation lists, from sslcrl or sslcrldir" $ do
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
      it "authenticates with a client certificate" $ do
        withKeyCopy ownerReadMode $ \key -> do
          let certified = "user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key
          withString (tcp certified) currentUser `shouldReturn` Right ["native_pg_certificate"]
        failure <- connectFailure (tcp "user=native_pg_certificate sslmode=require sslcert=/nonexistent/client.crt sslkey=/nonexistent/client.key")
        failure `shouldSatisfy` P.hasInfix "connection requires a valid client certificate"
      it "needs a client certificate's key, readable only by its owner, as libpq does" $ do
        let certified key = tcp ("user=native_pg_certificate sslmode=require sslcert=" <> Char8.pack (certificate "client.crt") <> " sslkey=" <> Char8.pack key)
            at = "connection to server at \"127.0.0.1\", port " <> show port <> " failed: "
        connectFailure (certified "/nonexistent/client.key")
          `shouldReturn` (at <> "certificate present, but not private key file \"/nonexistent/client.key\"")
        withKeyCopy (ownerReadMode `unionFileModes` groupReadMode `unionFileModes` otherReadMode) $ \key ->
          connectFailure (certified key)
            `shouldReturn` (at <> "private key file \"" <> key <> "\" has group or world access; file must have permissions u=rw (0600) or less if owned by the current user, or permissions u=rw,g=r (0640) or less if owned by root")
      -- The TLS configuration of settings is built in IO, when the settings are made, from the files, once. A connect
      -- makes a connection from it, and reads no file.
      it "connects with a configuration that was built from files that are gone" $
        withCertificateCopy "ca.crt" $ \roots -> do
          settings <- settingsOf (tcp ("sslmode=verify-full sslrootcert=" <> Char8.pack roots <> " host=localhost hostaddr=127.0.0.1"))
          removeFile roots
          run (session settings encrypted) `shouldReturn` Right [True]
          run (session settings encrypted) `shouldReturn` Right [True]
      it "connects again and again with the configuration that was built once" $ do
        settings <- settingsOf (tcp "sslmode=require")
        run (session settings encrypted) `shouldReturn` Right [True]
        run (session settings (alsoOn settings (onSecond encrypted))) `shouldReturn` Right [True]
      -- Settings whose configuration wasn't built fail, also with sslmode=prefer, which would otherwise connect without TLS.
      it "fails with TlsFailure, and says why, when the TLS configuration of the settings was never built" $
        forM_ ["sslmode=require", "sslmode=prefer", "sslmode=allow", "sslmode=verify-full sslrootcert=" <> Char8.pack (certificate "ca.crt")] $ \mode -> do
          settings <- unpreparedSettingsOf (tcp mode)
          result <- run (connectedTo settings done)
          case result of
            Left (ConnectFailure [(_, TlsFailure reason)]) -> reason `shouldSatisfy` P.hasInfix "was not built"
            _ -> failTest ("expected a TlsFailure for " <> show mode)
      -- What a configuration was built for is part of it: settings that changed since don't use it.
      it "fails with TlsFailure when the settings changed after their TLS configuration was built" $ do
        settings <- settingsOf (tcp "sslmode=require")
        let verifying = settings {connectSslMode = SslVerifyFull (RootCertificateFile (certificate "ca.crt"))}
        result <- run (connectedTo verifying done)
        case result of
          Left (ConnectFailure [(_, TlsFailure reason)]) -> reason `shouldSatisfy` P.hasInfix "was not built"
          _ -> failTest "expected a TlsFailure"
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
            (transactOn Tx.ReadCommitted Tx.Read Tx.Final (Tx.from fst (Tx.statement (Tx.Statement "SELECT $1::int + $2::int" (\(a, b) -> [textual (Just a), textual (Just b)]) Nothing (single @_ @Int32)))) readAll)
        )
        `shouldReturn` Right [42]
    it "passes NULL parameters and returns NULL columns" $
      withSession (query (valued "SELECT $1::text, 1" [Nothing] (nullableThen @_ @Text @Int32)))
        `shouldReturn` Right [(Nothing, 1)]
    it "reads many rows, one at a time" $
      withSession (transact Tx.ReadCommitted Tx.Read Tx.Final (Tx.statement (plain "SELECT generate_series(1, 10000)" (single @_ @Int32))) countRows)
        `shouldReturn` Right 10000
    it "runs a prepared statement until it's closed" $
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
    it "streams a parameter from its source" $ do
      let payload = BS.concat (replicate 4096 "0123456789abcdef")
          chunks = 128
      withSession
        ( streaming
            (newChunks (replicate chunks payload))
            (query' (Tx.streamFrom id (Tx.Statement "SELECT $1::text = repeat('0123456789abcdef', 524288)" (const [streamedText (chunks * BS.length payload)]) Nothing (single @_ @Bool))))
        )
        `shouldReturn` Right [True]
    it "streams an empty parameter, and one in a statement that follows a streamed one" $
      withSession
        ( steps
            [ streaming (newChunks []) (query' (Tx.streamFrom id (lengthWith (const [streamedText 0])))) `yields` Right [0],
              streaming (newChunks ["ab", "", "c"]) (query' (Tx.streamFrom id (Tx.Statement "SELECT length($1::text), length($2::text)" (const [streamedText 3, textual (Just "four")]) Nothing pair))) `yields` Right [(3 :: Int32, 4 :: Int32)]
            ]
        )
        `shouldReturn` ()
    it "takes the parameters of a statement from consecutive parts of its stream, wherever the pieces end" $
      withSession
        ( streaming (newChunks ["ab", "cdefg", "hi"]) (query' (Tx.streamFrom id (Tx.Statement "SELECT $1::text, $2::text" (const [streamedText 4, streamedText 5]) Nothing pair)))
            `yields` Right [("abcd" :: Text, "efghi" :: Text)]
        )
        `shouldReturn` ()
    it "gives each statement of a pipeline the stream that it declares" $
      withSession
        ( streaming
            ((,) <$> newChunks ["ab", "c"] <*> newChunks ["defg", "h"])
            ( transactOn
                Tx.ReadCommitted
                Tx.Write
                Tx.Final
                ( (,)
                    <$> Tx.from fst (Tx.statement (Tx.streamFrom id (lengthWith (const [streamedText 3]))))
                    <*> Tx.from snd (Tx.statement (Tx.streamFrom id (lengthWith (const [streamedText 5]))))
                )
                (\(shorter, longer) -> (,) <$> Tx.single shorter <*> Tx.single longer)
            )
            `yields` Right (3 :: Int32, 5 :: Int32)
        )
        `shouldReturn` ()
    it "fails a streamed parameter of a statement that has no stream before anything is sent, and the connection stays usable" $
      withSession
        ( steps
            [ query (Tx.Statement "SELECT length($1::text)" (const [streamedText 3]) Nothing (single @_ @Int32))
                `failsAs` \case
                  StreamLengthError 3 0 -> pure ()
                  other -> failTest ("expected StreamLengthError 3 0, got " <> show other),
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "sends a parameter in pieces, short and long, larger than the socket's buffers" $ do
      let payload = BS.concat (replicate 4096 "0123456789abcdef")
          pieces = concatMap (\piece -> [BS.take 100 piece, BS.drop 100 piece]) (replicate 128 payload)
      withSession (query (Tx.Statement "SELECT $1::text = repeat('0123456789abcdef', 524288)" (const [piecesText pieces]) Nothing (single @_ @Bool)))
        `shouldReturn` Right [True]
    it "streams a parameter to a prepared statement, whether it returns rows or not" $
      withSession
        ( steps
            [ ddl "CREATE TEMPORARY TABLE streamed (value text)" `yields` Right (),
              preparing "insert_streamed" (insertStreamed (const [])) `yields` Right (),
              preparing "length_of" (lengthWith (const [])) `yields` Right (),
              streaming (newChunks ["abc", "def"]) (preparedStreamed "insert_streamed" (Tx.streamFrom id (insertStreamed (const [streamedText 6])))) `yields` Right [],
              streaming (newChunks ["abc", "def"]) (preparedStreamed "length_of" (Tx.streamFrom id (lengthWith (const [streamedText 6])))) `yields` Right [6]
            ]
        )
        `shouldReturn` ()
    it "counts a slow parameter stream against statement_timeout, whether the statement is prepared or not" $
      withSession
        ( steps
            [ ddl "SET statement_timeout = '200ms'" `yields` Right (),
              preparing "slow_length" (lengthWith (const [])) `yields` Right (),
              (streaming slowSource (query' (Tx.streamFrom id (lengthWith (const [streamedText 4000])))) >>^ serverCode) `yields` Just "57014",
              (streaming slowSource (preparedStreamed "slow_length" (Tx.streamFrom id (lengthWith (const [streamedText 4000])))) >>^ serverCode) `yields` Just "57014",
              selectOne `yields` Right [1]
            ]
        )
        `shouldReturn` ()
    it "sends a batch while reading its results, as the server answers before reading all of it" $
      withSession (batchWhileReading 1000000 2000000) `shouldReturn` Just (Right (1000000, 2000000))
    it "streams a parameter while it reads the large result of a statement before it" $
      withSession
        ( withinFor (secs 10) $
            streaming
              (newChunks (replicate 64 (Char8.replicate 32768 'y')))
              ( transactOn
                  Tx.ReadCommitted
                  Tx.Read
                  Tx.Final
                  ( (,)
                      <$> Tx.from (const ()) (Tx.statement (plain "SELECT repeat('x', 1000000)" (Tx.any Tx.field)))
                      <*> Tx.statement (Tx.streamFrom id (lengthWith (const [streamedText 2097152])))
                  )
                  (\(large, long) -> (Tx.single large >>> measure >>^ fst) &&& Tx.single long)
              )
        )
        `shouldReturn` Just (Right (1000000, 2097152))
    -- The cancellation rule: the reading program is cancelled at its wait while the request side has not finished sending
    -- the 2 MB parameter, so something is half sent, and the connection is broken and closed rather than drained.
    it "closes a connection broken while sending, without waiting for the server to read" $
      withSession
        ( steps
            [ withinFor (secs 1) (committed Tx.Read (largeThenLong 1000000 2000000) (\(large, _) -> Tx.single large >>> sleepFor (secs 60))) `yields` Nothing,
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

  -- A program is built once, and a statement made from its input has the SQL, and the parameters, of each run.
  describe "a statement made from the input" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "sends the SQL and the parameters that the input makes, for each run of one program" $
      withSession (overInputs (query' offsetBy) [(1, "41"), (10, "41"), (100, "41")])
        `shouldReturn` [Right [42], Right [51], Right [141]]
    it "names the SQL that it was sent with in the failure of its statement, and the connection goes on" $
      withSession (overInputs (query' dividing >>^ failedSql) [2, 0, 5])
        `shouldReturn` [Right [5], Left "SELECT 10 / 0", Right [2]]
    it "takes the parameter from its stream, with the SQL that the input makes" $ do
      inputs <- traverse (\(offset, size, pieces) -> (offset,size,) <$> newChunks pieces) [(0, 5, ["ab", "cde"]), (100, 5, ["fghij"]), (1000, 7, ["", "k", "lmnopq"])]
      withSession (overInputs (query' lengthPlus) inputs) `shouldReturn` [Right [5], Right [105], Right [1007]]
    it "has the parameters that the input makes, a stream and one more or not" $ do
      inputs <- traverse (\extra -> (extra,) <$> newChunks ["abc"]) [False, True, False]
      withSession (overInputs (query' lengthOrBoth) inputs) `shouldReturn` [Right [3], Right [7], Right [3]]
    it "is prepared, executed and closed under one name in one pipeline, for SQL that differs from run to run" $
      withSession ((,) <$> overInputs (preparedOnce "made" offsetBy) [(1, "41"), (10, "41")] <*> query (plain "SELECT name FROM pg_prepared_statements" (single @_ @Text)))
        `shouldReturn` ([Right 42, Right 51], Right [])
    it "takes the parameter from its stream when it is prepared, executed and closed under one name" $ do
      inputs <- traverse (\(offset, size, pieces) -> (offset,size,) <$> newChunks pieces) [(0, 5, ["ab", "cde"]), (100, 5, ["fghij"])]
      withSession (overInputs (preparedOnce "made_streamed" lengthPlus) inputs) `shouldReturn` [Right 5, Right 105]
    it "executes a statement that the connection has prepared without evaluating the SQL that the input makes" $
      withSession
        ( steps
            [ preparing "made_earlier" (addOneWith "0") `yields` Right (),
              overInputs (transactOn Tx.ReadCommitted Tx.Write Tx.Open (Tx.executePrepared "made_earlier" withoutSql) Tx.single) ["1", "41"] `yields` [Right 2, Right 42]
            ]
        )
        `shouldReturn` ()
    it "is one statement of a pipeline among others, whose results are where they were built" $
      withSession
        ( overInputs
            ( transactOn
                Tx.ReadCommitted
                Tx.Write
                Tx.Final
                ((,,) <$> Tx.from fst (Tx.statement offsetBy) <*> Tx.from (const ()) (Tx.statement (plain "SELECT 7::int" (single @_ @Int32))) <*> Tx.from snd (Tx.statement offsetBy))
                (\(left, middle, right) -> Tx.single left &&& Tx.single middle &&& Tx.single right)
            )
            [((1, "41"), (10, "41")), ((100, "1"), (1000, "1"))]
        )
        `shouldReturn` [Right (42, (7, 51)), Right (101, (7, 1001))]

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
    it "keeps the connection usable when a wait for a notification is cancelled, and delivers a later one" $
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

  describe "notifications" $ do
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
    -- The server sends a notification to a session that is idle, as soon as it has it. It is read by the statement that
    -- runs next, among that statement's replies, and kept.
    it "keeps a notification that arrived while the connection was idle, when a statement runs before it is taken" $
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events"])),
                  onSecond (ddl "NOTIFY events, 'idle'") `yields` Right (),
                  onFirst selectOne `yields` Right [1],
                  onFirst selectOne `yields` Right [1],
                  (onFirst (conn Notify.notification) >>^ Notify.notificationPayload) `yields` "idle",
                  withinFor (millis 100) (onFirst (conn Notify.notification)) `yields` Nothing
                ]
        )
        `shouldReturn` ()
    it "delivers each notification once, in the order they were sent, whether a statement or a wait reads them" $
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events", "more"])),
                  onSecond (ddl "NOTIFY events, 'one'") `yields` Right (),
                  onFirst selectOne `yields` Right [1],
                  onSecond (ddl "NOTIFY more, 'two'") `yields` Right (),
                  onSecond (ddl "NOTIFY events, 'three'") `yields` Right (),
                  (onFirst (conn Notify.notification) >>^ payloadAndChannel) `yields` ("one", "events"),
                  (onFirst (conn Notify.notification) >>^ payloadAndChannel) `yields` ("two", "more"),
                  onFirst selectOne `yields` Right [1],
                  (onFirst (conn Notify.notification) >>^ payloadAndChannel) `yields` ("three", "events"),
                  withinFor (millis 100) (onFirst (conn Notify.notification)) `yields` Nothing
                ]
        )
        `shouldReturn` ()
    -- The buffers of the case are 1 KiB: a payload of 7000 bytes is read by pieces, wherever it arrives.
    it "delivers a payload larger than the buffer, whether a statement or a wait reads it" $ do
      let payload = Char8.replicate 7000 'p'
      run
        ( session (unix "postgres" Nothing) $
            alsoOn (unix "postgres" Nothing) $
              steps
                [ onFirst (conn (Notify.listen ["events"])),
                  onSecond (notifying payload) `yields` Right [True],
                  onFirst selectOne `yields` Right [1],
                  (onFirst (conn Notify.notification) >>^ Notify.notificationPayload) `yields` payload,
                  onSecond (notifying payload) `yields` Right [True],
                  (onFirst (conn Notify.notification) >>^ Notify.notificationPayload) `yields` payload
                ]
        )
        `shouldReturn` ()
    it "reports a channel the server rejects, and stays usable" $
      run
        ( session (unix "postgres" Nothing) $
            steps
              [ conn (Notify.listen [""])
                  `failsAs` \case
                    QueryError _ (ServerFailed (ServerError diagnostics)) -> diagnosticCode diagnostics `shouldBe` "42601"
                    other -> failTest ("expected a QueryError, got " <> show other),
                selectOne `yields` Right [1],
                conn (Notify.listen []) `yields` ()
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

  -- A snippet marks the parameter that its statement streams, so that SQL that is generated can have a streamed parameter: the
  -- statement from the snippet takes its bytes from the stream that the statement declares, as 'Tx.FromStream' does.
  describe "snippets that stream a parameter" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "takes the parameter that the snippet marks from the stream of its statement" $
      withSession (streaming (newChunks ["abc", "def"]) (query' (Tx.streamFrom id (Snippet.fromSnippet (lengthOfStreamed 6) single))))
        `shouldReturn` Right [6 :: Int32]
    it "takes it among parameters that are bytes, in the order that the snippet has them" $
      withSession (streaming (newChunks ["ab", "cd"]) (query' (Tx.streamFrom id (Snippet.fromSnippet (afterBytes 4) single))))
        `shouldReturn` Right ["xyabcd" :: Text]
    it "takes it from the stream of a statement made from the input, whose snippet the input makes" $ do
      inputs <- traverse (\(size, pieces) -> (size,) <$> newChunks pieces) [(5, ["ab", "cde"]), (3, ["xyz"]), (0, [])]
      withSession (overInputs (query' madeFromSnippet) inputs) `shouldReturn` [Right [5], Right [3], Right [0]]

  -- A parameter stream that breaks its promise leaves the protocol in a state nobody can continue from.
  describe "execute (failures)" $ do
    let withSession :: (forall arr. (Runtime arr) => OnConn arr b) -> IO b
        withSession body = run (session (unix "postgres" Nothing) body)
    it "breaks the connection when a parameter stream is shorter than declared" $
      withSession
        ( steps
            [ streaming (newChunks ["short"]) (query' (Tx.streamFrom id (Tx.Statement "SELECT $1::text" (const [streamedText 10]) Nothing (single @_ @Text))))
                `failsAs` \case
                  StreamLengthError {} -> pure ()
                  other -> failTest ("expected StreamLengthError, got " <> show other),
              brokenConnection
            ]
        )
        `shouldReturn` ()
    it "breaks the connection when a parameter stream is longer than declared" $
      withSession
        ( steps
            [ streaming (newChunks ["more than", " declared"]) (query' (Tx.streamFrom id (Tx.Statement "SELECT $1::text" (const [streamedText 5]) Nothing (single @_ @Text))))
                `failsAs` \case
                  StreamLengthError {} -> pure ()
                  other -> failTest ("expected StreamLengthError, got " <> show other),
              brokenConnection
            ]
        )
        `shouldReturn` ()
    -- The cancellation rule: a message that is half sent owes the server its rest, wherever the sender waits.
    it "breaks the connection when the wait for a slow parameter stream is cancelled" $
      withSession
        ( steps
            [ withinFor (millis 300) (streaming slowSource (query' (Tx.streamFrom id (lengthWith (const [streamedText 4000]))))) `yields` Nothing,
              brokenConnection
            ]
        )
        `shouldReturn` ()
    it "throws a parameter stream's failure, and breaks the connection" $
      withSession
        ( steps
            [ streaming (newFailingChunks [Char8.replicate 1000000 'y']) (query' (Tx.streamFrom id (Tx.Statement "SELECT length($1::text)" (const [streamedText 2000000]) Nothing (single @_ @Int32))))
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
cancelling settings = (\(connection, _) -> (settings, connection)) ^>> cancelWith

-- | Cancels the statement that runs on the connection, with the settings that the input gives: one arrow for any settings.
cancelWith :: (Runtime arr) => arr (ConnectSettings, Connection) ()
cancelWith = (\(settings, connection) -> (settings, PG.cancelKey connection)) ^>> PG.cancel

-- | The body on a connection made to the settings that the input gives, with the loans after them, closed when the body
-- ends: the connection that 'session' makes, for settings that come at run time. A failure to connect is a value.
onSettings :: (Runtime arr) => OnConn arr b -> arr (ConnectSettings, Loans) (Either ConnectFailure b)
onSettings body = scope PG.connect (fst ^>> (done ||| PG.close)) ((\(connected, (_, loans)) -> fmap (,loans) connected) ^>> (arr Left ||| (body >>^ Right)))

-- | With two connections, the second ends the backend of the first. The process id is the text of a parameter.
terminatingTheFirst :: (Runtime arr) => arr ((Connection, Connection), Loans) (Either QueryError [Bool])
terminatingTheFirst =
  now
    ( \((listener, administrator), loans) -> case PG.serverProcessId (PG.serverInfo listener) of
        Nothing -> failTest "the server sent no process id"
        Just pid -> pure (administrator, (loans, Char8.pack (show pid)))
    )
    >>> query' (Tx.Statement "SELECT pg_terminate_backend($1::int)" (\pid -> [textual (Just pid)]) Nothing single)

-- | Notifies the channel with the payload, from a function of the server, so that the payload is a parameter.
notifying :: (Runtime arr) => ByteString -> OnConn arr (Either QueryError [Bool])
notifying payload = query (valued "SELECT pg_notify('events', $1::text) IS NOT NULL" [Just payload] single)

-- | What a notification says: its payload and its channel.
payloadAndChannel :: Notify.Notification -> (ByteString, ByteString)
payloadAndChannel delivered = (Notify.notificationPayload delivered, Notify.notificationChannel delivered)

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

-- | The settings of a connection string, with the TLS client of native-pg-tls and its configuration built, and a resolver
-- that has the system's configuration.
settingsOf :: ByteString -> IO ConnectSettings
settingsOf string = case parseConnectionString noDefaults string of
  Left problem -> fail problem
  Right parsed -> do
    resolver <- newResolver
    client <- newTlsClient
    either fail (\settings -> prepareTls settings {connectTlsClient = Just client, connectResolver = Just resolver}) (fromConnectionString parsed)

-- | The programs at once, each on the same input, and what each gave.
concurrentlyAll :: (Runtime arr) => [arr a b] -> arr a [b]
concurrentlyAll = foldr (\one others -> concurrently one others >>^ uncurry (:)) (arr (const []))

-- | The settings of a connection string, as 'settingsOf' makes them, whose host names are looked up by the resolver.
settingsVia :: Resolver -> ByteString -> IO ConnectSettings
settingsVia resolver string = (\settings -> settings {connectResolver = Just resolver}) <$> settingsOf string

-- | The options of a resolver that asks the scripted server only, which waits this many milliseconds for an answer, as
-- many times as the query is tried, and looks up no search domains.
resolverOptionsFor :: DnsServer -> Int -> Int -> ResolverOptions
resolverOptionsFor dns timeoutMilliseconds tries =
  defaultResolverOptions
    { resolverServers = Just ("127.0.0.1:" <> show (dnsPort dns)),
      resolverTimeout = Just timeoutMilliseconds,
      resolverTries = Just tries,
      resolverSearchDomains = False
    }

-- | A resolver that asks the scripted server only.
resolverFor :: DnsServer -> Int -> Int -> IO Resolver
resolverFor dns timeoutMilliseconds tries = newResolverWith (resolverOptionsFor dns timeoutMilliseconds tries)

-- | Runs the action with a hosts file of this content, whose modification time is long past.
withTemporaryHosts :: ByteString -> (FilePath -> IO a) -> IO a
withTemporaryHosts content action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-hosts-")) removeDirectoryRecursive $ \directory -> do
    let hosts = directory </> "hosts"
    writeHosts hosts content
    action hosts

-- | Writes the hosts file, and sets its modification time to a past one, so that a resolver takes it for a file that
-- hasn't changed since it read it.
writeHosts :: FilePath -> ByteString -> IO ()
writeHosts hosts content = BS.writeFile hosts content *> setModificationTime hosts (UTCTime (fromGregorian 2001 1 1) 0)

-- | The settings of a connection string, with the TLS client of native-pg-tls but no TLS configuration built.
unpreparedSettingsOf :: ByteString -> IO ConnectSettings
unpreparedSettingsOf string = case parseConnectionString noDefaults string of
  Left problem -> fail problem
  Right parsed -> do
    client <- newTlsClient
    either fail (\settings -> pure settings {connectTlsClient = Just client}) (fromConnectionString parsed)

-- | Runs the action with a copy of the test certificate, which it may remove.
withCertificateCopy :: FilePath -> (FilePath -> IO a) -> IO a
withCertificateCopy name action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-certificate-")) removeDirectoryRecursive $ \directory -> do
    let copy = directory </> name
    copyFile (certificate name) copy
    action copy

-- | No default host or certificate directory, nothing from the process but the user "postgres": every case gives its host.
noDefaults :: Defaults
noDefaults = (emptyDefaults "/tmp" Nothing) {defaultUser = Just "postgres"}

-- | The body on a connection to the string's settings.
onString :: Runner -> ByteString -> (forall arr. (Runtime arr) => OnConn arr b) -> IO b
onString runner string body = settingsOf string >>= \settings -> ordinary runner (session settings body)

-- | What failing to connect says, as libpq words it.
failureOf :: Runner -> ByteString -> IO String
failureOf runner string = do
  settings <- settingsOf string
  result <- ordinary runner (connectedTo settings done)
  either (pure . displayException) (const (fail "connected")) result

-- | The snippet whose parameter is this many bytes of the stream of its statement, as text.
lengthOfStreamed :: Int -> Snippet.Snippet
lengthOfStreamed size = "SELECT length(" <> streamedParam size <> "::text)"

-- | The snippet whose parameters are two bytes and then this many bytes of the stream, concatenated.
afterBytes :: Int -> Snippet.Snippet
afterBytes size = "SELECT " <> Snippet.encoderAndParam (Encoders.nonNullable Encoders.unknown) "xy" <> "::text || " <> streamedParam size <> "::text"

-- | The parameter that is this many bytes of the stream of the statement, of a type that the server infers.
streamedParam :: Int -> Snippet.Snippet
streamedParam size = Snippet.param (Encoders.rawParam Encoders.rawUnknown (Just (Encoders.ParameterStream size)))

-- | The statement that a snippet makes of the input, which is the size of the stream, and the stream: its SQL and parameters
-- are those of 'Snippet.fromSnippet'.
madeFromSnippet :: forall arr. (Runtime arr) => Tx.Statement arr (Int, Chunks) Int32
madeFromSnippet = Tx.streamFrom snd (Tx.statementFrom (\(size, _) -> let given = Snippet.fromSnippet (lengthOfStreamed size) (Tx.none :: Rows arr ()) in (Tx.sql given, Tx.params given ())) single)

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

-- | The statement whose SQL adds a number to its parameter: the number and the parameter come from the input.
offsetBy :: (Runtime arr) => Tx.Statement arr (Int, ByteString) Int32
offsetBy = Tx.statementFrom (\(offset, value) -> ("SELECT $1::int + " <> Char8.pack (show offset), [textual (Just value)])) single

-- | The statement whose SQL divides ten by the number that is its input.
dividing :: (Runtime arr) => Tx.Statement arr Int Int32
dividing = Tx.statementFrom (\divisor -> ("SELECT 10 / " <> Char8.pack (show divisor), [])) single

-- | The statement that adds a number to the length of the parameter that the stream gives: the number, the length and the stream
-- are the input.
lengthPlus :: (Runtime arr) => Tx.Statement arr (Int, Int, Chunks) Int32
lengthPlus =
  Tx.streamFrom (\(_, _, chunks) -> chunks) (Tx.statementFrom (\(offset, size, _) -> ("SELECT length($1::text) + " <> Char8.pack (show offset), [streamedText size])) single)

-- | The statement whose parameters are the stream, of three bytes, and one more if the input says so, which its SQL then uses.
lengthOrBoth :: (Runtime arr) => Tx.Statement arr (Bool, Chunks) Int32
lengthOrBoth = Tx.streamFrom snd (Tx.statementFrom made single)
  where
    made (True, _) = ("SELECT length($1::text) + length($2::text)", [streamedText 3, textual (Just "four")])
    made (False, _) = ("SELECT length($1::text)", [streamedText 3])

-- | A statement that the connection has prepared under a name, whose SQL the input doesn't make: it has none.
withoutSql :: (Runtime arr) => Tx.Statement arr ByteString Int32
withoutSql = Tx.statementFrom (\value -> (error "SQL forced", [textual (Just value)])) single

-- | The statement prepared, executed and closed under the name, in one pipeline that commits, and its one row.
preparedOnce :: (Runtime arr) => ByteString -> Tx.Statement arr p Int32 -> arr (Connection, (Loans, p)) (Either QueryError Int32)
preparedOnce name statement =
  transactOn
    Tx.ReadCommitted
    Tx.Write
    Tx.Final
    ((,,) <$> Tx.prepare name statement <*> Tx.executePrepared name statement <*> Tx.closeStatement name)
    (\(preparation, result, deallocation) -> Tx.prepared preparation >>> Tx.single result >>> tap (done >>> Tx.discard deallocation))

-- | What failed a statement, if it was the server: the SQL that it was sent with.
failedSql :: Either QueryError a -> Either ByteString a
failedSql = \case
  Left (QueryError text _) -> Left text
  Right value -> Right value

-- | The statement that inserts its parameter, which the function makes of the input.
insertStreamed :: (Runtime arr) => (p -> [Tx.Param]) -> Tx.Statement arr p ()
insertStreamed parameters = Tx.Statement "INSERT INTO streamed VALUES ($1)" parameters Nothing Tx.none

-- | The statement that takes the length of its parameter, which the function makes of the input.
lengthWith :: (Runtime arr) => (p -> [Tx.Param]) -> Tx.Statement arr p Int32
lengthWith parameters = Tx.Statement "SELECT length($1::text)" parameters Nothing single

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
batchWhileReading :: (Runtime arr) => Int -> Int -> OnConn arr (Maybe (Either QueryError (Int, Int32)))
batchWhileReading resultSize parameterSize =
  withinFor (secs 10) $
    committed Tx.Read (largeThenLong resultSize parameterSize) $ \(large, long) ->
      (Tx.single large >>> measure >>^ fst) &&& Tx.single long

-- | A result with a field of the first size, and a statement whose parameter is of the second.
largeThenLong :: (Runtime arr) => Int -> Int -> Tx.Pipeline arr () (Tx.Result arr Tx.Field, Tx.Result arr Int32)
largeThenLong resultSize parameterSize =
  (,)
    <$> Tx.statement (plain ("SELECT repeat('x', " <> Char8.pack (show resultSize) <> ")") (Tx.any Tx.field))
    <*> Tx.statement (valued "SELECT length($1::text)" [Just (Char8.replicate parameterSize 'y')] single)

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

-- | Runs the action with a port at 127.0.0.1 that nothing listens on.
withUnusedPort :: (Socket.PortNumber -> IO a) -> IO a
withUnusedPort action =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close
    (\sock -> Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1))) *> Socket.socketPort sock)
    >>= action

-- | Takes a message of the client, which has a tag: the tag, and the payload after the length.
receiveMessage :: Socket.Socket -> IO (Char, ByteString)
receiveMessage client = do
  tag <- receiving client 1
  size <- BS.foldl' (\total byte -> total * 256 + fromIntegral byte) 0 <$> receiving client 4
  payload <- receiving client (size - 4)
  pure (Char8.head tag, payload)

-- | Takes this many bytes from the client.
receiving :: Socket.Socket -> Int -> IO ByteString
receiving client size
  | size <= 0 = pure BS.empty
  | otherwise = SocketBytes.recv client size >>= \piece -> if BS.null piece then pure piece else (piece <>) <$> receiving client (size - BS.length piece)

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
