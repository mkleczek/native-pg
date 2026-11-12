-- | TLS for native-pg through rustls-ffi: the configuration of a client.
module Database.PostgreSQL.Tls
  ( TlsConfig,
    newTlsConfig,
  )
where

import Database.PostgreSQL.Tls.Internal (TlsConfig, newTlsConfig)
