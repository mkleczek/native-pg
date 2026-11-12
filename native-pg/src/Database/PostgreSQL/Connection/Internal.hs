-- | What a connection is made of, and what only tests reach: the constructors of
-- the types that "Database.PostgreSQL.Connection" exports without them, and the
-- override of the TLS end-point data that SCRAM-PLUS binds to.
module Database.PostgreSQL.Connection.Internal
  ( Connection (..),
    CancelKey (..),
    withEndPointOverride,
  )
where

import Data.ByteString (ByteString)
import Database.PostgreSQL.ConnectionString.Types (Host)
import Database.PostgreSQL.Internal.Protocol.Messages.Types (BackendKey)
import Database.PostgreSQL.Session.Settings (ConnectSettings (..), ServerInfo)

-- | A connection: what it learned of its server and where it is, and, to come,
-- its link, its reader and its session state.
data Connection = Connection ServerInfo Host CancelKey

-- | What cancels the statement that a connection runs, if the server sent it:
-- some connection poolers don't.
newtype CancelKey = CancelKey (Maybe (BackendKey ByteString))

-- | The settings, with the data that channel binding takes from the TLS link
-- replaced: by the bytes, or by why there are none, or by none.
withEndPointOverride :: Maybe (Either String ByteString) -> ConnectSettings -> ConnectSettings
withEndPointOverride override settings = settings {connectEndPointOverride = Just override}
