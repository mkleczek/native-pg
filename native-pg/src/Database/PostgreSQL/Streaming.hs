-- | Operation-scoped byte storage. Connections keep only transport and
-- protocol state between calls; Resources can be shared with an HTTP server.
module Database.PostgreSQL.Streaming
  ( withOperation,
    withBuffers,
    ByteStream (..),
    connect,
    connectFirst,
  ) where

import Database.PostgreSQL.ConnectionString (ConnectionString, Host)
import qualified Database.PostgreSQL.Internal.Startup as Startup
import qualified Database.PostgreSQL.Internal.HostSelection as HostSelection
import qualified Database.PostgreSQL.Internal.Context as Context
import qualified Database.PostgreSQL.Session as Session
import Database.PostgreSQL.Session (Connection, Session)
import qualified Database.PostgreSQL.Internal.Exchange as Exchange
import Database.PostgreSQL.Session.Settings (ConnectSettings, ServerInfo)
import Database.PostgreSQL.Streaming.Types (ByteStream (..))
import Network.Streaming.Resources (Resources)
import qualified Network.Streaming.Resources as Resources
import Network.Streaming.Buffer (Buffer)

-- | Borrow an initialized input/output pair for one exclusive connection
-- scope. Borrowed rows and streams expire before the pair is returned.
withOperation :: Resources -> Connection -> (Session -> IO a) -> IO a
withOperation resources connection action = Resources.withBuffers @(Buffer, Buffer) resources $ \(input, output) ->
  withBuffers input output connection action

-- | Use disjoint, initialized shared storage for one exclusive operation.
-- Both buffers must remain exclusively owned until the callback returns;
-- borrowed slices expire before that ownership is released.
withBuffers :: Buffer -> Buffer -> Connection -> (Session -> IO a) -> IO a
withBuffers input output connection action =
  Context.withContext connection input output (action . Exchange.Session)

-- | Establish a connection using the separate setup budget. The returned
-- connection retains no payload workspace.
connect :: Resources -> ConnectSettings -> IO (Connection, ServerInfo)
connect resources settings = Resources.withSetupBuffers @(Buffer, Buffer) resources $ \(input, output) ->
  Startup.connectWithBuffers input output settings

-- | Try hosts and qualify the connection using temporary setup storage.
connectFirst :: Resources -> (ConnectSettings -> ConnectSettings) -> ConnectionString -> IO (Connection, ServerInfo, Host)
connectFirst resources adjust string = Resources.withSetupBuffers @(Buffer, Buffer) resources $ \(input, output) ->
  HostSelection.connectFirstUsing (Startup.connectWithBuffers input output) Session.close
    (\connection query -> Context.withContext connection input output $ \context ->
      HostSelection.queryValue (Exchange.Session context) query) adjust string
