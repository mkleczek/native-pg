-- | TLS for native-pg through rustls-ffi. Supply 'tlsClient' in the session's
-- @connectTlsClient@ setting.
module Database.PostgreSQL.Tls
  ( tlsClient,
  )
where

import Database.PostgreSQL.Tls.Internal (tlsClient)
