-- | TLS for native-pg through rustls-ffi: the configuration of a client, and the
-- client that a connection starts TLS with.
module Database.PostgreSQL.Tls
  ( TlsConfig,
    newTlsConfig,
    newTlsClient,
    tlsClient,
  )
where

import Database.PostgreSQL.Tls.Internal (TlsConfig, clientConnection, newTlsConfig, tlsLinkOf)
import Database.PostgreSQL.Transport (TlsClient (..))

-- | The client that starts TLS with rustls, which a connection's settings take: it is the same for every connection.
-- Each connection has a configuration of its own, so that changed certificate files count, as with libpq. What it makes
-- has not shaken hands: the connection does, waiting for the socket as it goes.
tlsClient :: TlsClient
tlsClient = TlsClient $ \settings serverName sock -> do
  config <- newTlsConfig settings serverName
  tlsLinkOf settings <$> clientConnection config sock

-- | The client that starts TLS with rustls, made in IO. (A stub: it is 'tlsClient' still.)
newTlsClient :: IO TlsClient
newTlsClient = pure tlsClient
