module TestCluster (withCluster, certificate) where

import Control.Exception (bracket, bracket_)
import Data.Foldable (for_)
import qualified Network.Socket as Socket
import System.Directory (copyFile, getTemporaryDirectory, removeDirectoryRecursive)
import System.Exit (ExitCode (..))
import System.FilePath ((</>))
import System.Posix.Files (ownerReadMode, setFileMode)
import System.Posix.Temp (mkdtemp)
import System.Process (readProcessWithExitCode)

-- | One of the test certificates and keys, which native-pg-tls's tests share.
certificate :: FilePath -> FilePath
certificate name = "native-pg-tls/test/certificates/" <> name

-- | Runs the action with the socket path and the port of a new cluster, which
-- has a role for each password method. On its port, at 127.0.0.1, it takes
-- TLS with the test certificates: a client certificate for one role, plaintext
-- only for another, and TLS only for the others.
withCluster :: (FilePath -> Socket.PortNumber -> IO a) -> IO a
withCluster action = do
  temporary <- getTemporaryDirectory
  bracket (mkdtemp (temporary </> "native-pg-")) removeDirectoryRecursive $ \directory -> do
    let dataDirectory = directory </> "data"
    run "initdb" ["--pgdata", dataDirectory, "--username", "postgres", "--auth", "trust", "--no-sync", "--no-locale", "--encoding", "UTF8"]
    writeFile (dataDirectory </> "pg_hba.conf") hbaRules
    for_ ["server.crt", "server.key", "ca.crt"] $ \file -> do
      copyFile (certificate file) (dataDirectory </> file)
      setFileMode (dataDirectory </> file) ownerReadMode
    port <- freePort
    appendFile (dataDirectory </> "postgresql.conf") $
      unlines
        [ "listen_addresses = '127.0.0.1'",
          "port = " <> show port,
          "unix_socket_directories = '" <> directory <> "'",
          "ssl = on",
          "ssl_cert_file = 'server.crt'",
          "ssl_key_file = 'server.key'",
          "ssl_ca_file = 'ca.crt'"
        ]
    bracket_
      (run "pg_ctl" ["--pgdata", dataDirectory, "--log", directory </> "postgres.log", "--wait", "start"])
      (run "pg_ctl" ["--pgdata", dataDirectory, "--mode", "immediate", "stop"])
      $ do
        run "psql" ["--host", directory, "--port", show port, "--username", "postgres", "--dbname", "postgres", "--quiet", "--set", "ON_ERROR_STOP=1", "--command", rolesSql]
        action (directory </> ".s.PGSQL." <> show port) port
  where
    hbaRules =
      unlines
        [ "local all native_pg_scram scram-sha-256",
          "local all native_pg_md5 md5",
          "local all native_pg_cleartext password",
          "local all all trust",
          "hostssl all native_pg_certificate 127.0.0.1/32 cert",
          "hostssl all native_pg_scram 127.0.0.1/32 scram-sha-256",
          "hostssl all native_pg_plaintext 127.0.0.1/32 reject",
          "hostnossl all native_pg_plaintext 127.0.0.1/32 trust",
          "hostssl all all 127.0.0.1/32 trust"
        ]
    -- A port nothing listens on now, at least.
    freePort =
      bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \sock -> do
        Socket.bind sock (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
        Socket.socketPort sock
    rolesSql =
      unlines
        [ "SET password_encryption = 'scram-sha-256';",
          "CREATE ROLE native_pg_scram LOGIN PASSWORD 'scram secret';",
          "CREATE ROLE native_pg_cleartext LOGIN PASSWORD 'cleartext secret';",
          "CREATE ROLE native_pg_certificate LOGIN;",
          "CREATE ROLE native_pg_plaintext LOGIN;",
          "SET client_min_messages = error;",
          "SET password_encryption = 'md5';",
          "CREATE ROLE native_pg_md5 LOGIN PASSWORD 'md5 secret';"
        ]
    run command arguments = do
      (exitCode, out, err) <- readProcessWithExitCode command arguments ""
      case exitCode of
        ExitSuccess -> pure ()
        ExitFailure _ -> fail (unwords (command : arguments) <> " failed:\n" <> out <> err)
