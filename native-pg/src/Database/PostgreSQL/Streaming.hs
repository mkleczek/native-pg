-- | Operation-scoped byte storage. Connections keep only transport and
-- protocol state between calls; Resources can be shared with an HTTP server.
module Database.PostgreSQL.Streaming
  ( withOperation,
    connect,
    connectFirst,
  ) where

import Database.PostgreSQL.ConnectionString (ConnectionString, Host)
import Database.PostgreSQL.Session (Connection, Context, ConnectSettings, ServerInfo)
import Network.Streaming.Resources (Resources)

-- | Borrow an initialized input/output pair for one exclusive connection
-- scope. Borrowed rows and streams expire before the pair is returned.
withOperation :: Resources -> Connection -> (Context -> IO a) -> IO a
withOperation _ _ _ = ioError (userError "pooled operations are not implemented")

-- | Establish a connection using the separate setup budget. The returned
-- connection retains no payload workspace.
connect :: Resources -> ConnectSettings -> IO (Connection, ServerInfo)
connect _ _ = ioError (userError "pooled setup is not implemented")

-- | Try hosts and qualify the connection using temporary setup storage.
connectFirst :: Resources -> (ConnectSettings -> ConnectSettings) -> ConnectionString -> IO (Connection, ServerInfo, Host)
connectFirst _ _ _ = ioError (userError "pooled host selection is not implemented")
