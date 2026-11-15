-- | TLS for native-pg through rustls-ffi: the configuration of a client, and the client that a connection starts TLS with.
module Database.PostgreSQL.Tls
  ( SystemRoots,
    loadSystemRoots,
    TlsConfig,
    newTlsConfig,
    newTlsClient,
  )
where

import Database.PostgreSQL.Tls.Internal (SystemRoots, TlsConfig, clientConnection, loadSystemRoots, newTlsConfig, tlsLinkOf)
import Database.PostgreSQL.Transport (TlsClient (..), TlsStarter (..))

-- | The client that starts TLS with rustls, which a program makes once, in IO, and puts in the settings of its connections.
-- It loads the system's root certificates, which every configuration that verifies against them shares. It builds a
-- configuration for the settings of a host, and for its server's name, reading the files that it needs: the program does
-- that where it makes the settings, with @prepareTls@. A connection is made from the configuration, and has not shaken
-- hands: it does, waiting for the socket as it goes.
newTlsClient :: IO TlsClient
newTlsClient = do
  roots <- loadSystemRoots
  pure . TlsClient $ \settings serverName -> do
    config <- newTlsConfig roots settings serverName
    pure (TlsStarter (\sock -> tlsLinkOf settings <$> clientConnection config sock))
