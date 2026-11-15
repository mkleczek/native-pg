-- | TLS for native-pg through rustls-ffi: the configuration of a client, and the
-- client that a connection starts TLS with.
module Database.PostgreSQL.Tls
  ( TlsConfig,
    newTlsConfig,
    tlsClient,
  )
where

import Database.PostgreSQL.Tls.Internal (TlsConfig, newTlsConfig)
import Database.PostgreSQL.Transport (TlsClient (..))
import Network.Streaming.NotImplemented (notImplementedIO)

-- | The client that starts TLS with rustls, which a connection's settings take.
-- A stub, until a connection can use one: it is the same for every connection.
tlsClient :: TlsClient
tlsClient = TlsClient (\_ _ _ -> notImplementedIO "starting TLS")
