-- | LISTEN and NOTIFY: a connection that listens, and the notifications it is
-- sent. The steps are stubs, which fail with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the code is there.
module Database.PostgreSQL.Notifications
  ( Notification (..),
    listen,
    notification,
  )
where

import Data.ByteString (ByteString)
import Data.Int (Int32)
import Database.PostgreSQL.Connection (Connection)
import Network.Streaming.Classes (Effect)
import Network.Streaming.NotImplemented (notImplemented)

-- | A notification: the backend that sent it, the channel, and the payload.
data Notification = Notification
  { notificationProcessId :: Int32,
    notificationChannel :: ByteString,
    notificationPayload :: ByteString
  }
  deriving (Eq, Show)

-- | Listens on the channels. The connection is one of its own, not a pooled
-- one, though it can still run transactions, for example to NOTIFY.
listen :: (Effect arr) => [ByteString] -> arr Connection ()
listen _ = notImplemented "listen"

-- | The next notification, waiting for it: the ones that came while statements
-- ran are kept, and come in order.
notification :: (Effect arr) => arr Connection Notification
notification = notImplemented "notification"
