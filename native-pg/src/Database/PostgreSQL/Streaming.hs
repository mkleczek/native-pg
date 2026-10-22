{-# LANGUAGE DataKinds #-}
-- | Operation-scoped byte storage. Connections keep only transport and
-- protocol state between calls; Resources can be shared with an HTTP server.
module Database.PostgreSQL.Streaming
  ( withOperation,
    connect,
    connectFirst,
  ) where

import Database.PostgreSQL.ConnectionString (ConnectionString, Host)
import qualified Database.PostgreSQL.ConnectionString as ConnectionString
import qualified Database.PostgreSQL.Buffer as Buffer
import qualified Database.PostgreSQL.Session as Session
import Database.PostgreSQL.Session (Connection, Context, ConnectSettings, ServerInfo)
import Network.Streaming.PositiveInt (positiveInt)
import Network.Streaming.Resources (Resources)
import qualified Network.Streaming.Resources.Internal as Resources

-- | Borrow an initialized input/output pair for one exclusive connection
-- scope. Borrowed rows and streams expire before the pair is returned.
withOperation :: Resources -> Connection -> (Context -> IO a) -> IO a
withOperation _ _ _ = ioError (userError "pooled operations are not implemented")

-- | Establish a connection using the separate setup budget. The returned
-- connection retains no payload workspace.
connect :: Resources -> ConnectSettings -> IO (Connection, ServerInfo)
connect resources settings = withSetupPair resources $ \input output ->
  Session.connectWithBuffers input output settings

-- | Try hosts and qualify the connection using temporary setup storage.
connectFirst :: Resources -> (ConnectSettings -> ConnectSettings) -> ConnectionString -> IO (Connection, ServerInfo, Host)
connectFirst resources adjust string = withSetupPair resources $ \input output ->
  ConnectionString.connectFirstWithBuffers input output adjust string

withSetupPair :: Resources -> (Buffer.Buffer -> Buffer.Buffer -> IO a) -> IO a
withSetupPair resources action = Resources.withSetupBuffers resources (positiveInt @2) $ \buffers ->
  case buffers of
    [input, output] -> do
      input' <- adapt input
      output' <- adapt output
      action input' output'
    _ -> ioError (userError "invalid setup workspace grant")
  where
    adapt descriptor = Buffer.bufferFromInitializedMemory
      (Resources.bufferMemory descriptor) (Resources.bufferCapacity descriptor)
