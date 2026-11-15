{-# LANGUAGE LambdaCase #-}

-- | What a connection is made of, and what only tests reach: the constructors of
-- the types that "Database.PostgreSQL.Connection" exports without them, and the
-- override of the TLS end-point data that SCRAM-PLUS binds to.
--
-- A connection is a socket and what the server said of itself, and whether it is
-- idle, used by one part, or broken. It holds nothing that belongs to the arrow it
-- was made under, so that a connection made under one runtime works under another.
module Database.PostgreSQL.Connection.Internal
  ( Connection (..),
    Session (..),
    Status (..),
    CancelKey (..),
    newConnection,
    leased,
    claim,
    settle,
    breakSession,
    withEndPointOverride,
  )
where

import Control.Exception (throwIO)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.IORef (IORef, newIORef, readIORef)
import Database.PostgreSQL.ConnectionString.Types (Host)
import Database.PostgreSQL.Errors (ConnectionError (..), UsageError (..))
import Database.PostgreSQL.Internal.Carrier (Carrier, closeCarrier)
import Database.PostgreSQL.Internal.Protocol.Messages.Types (BackendKey)
import Database.PostgreSQL.Session.Settings (ConnectSettings (..), ServerInfo)
import Network.Streaming.Internal.Atomic (update)

-- | A connection: what it learned of its server and where it is, the key that
-- cancels its statements, its socket and whether it can be used, and whether the
-- handle that a body was given of it is still its own.
data Connection = Connection
  { connectionInfo :: ServerInfo,
    connectionHost :: Host,
    connectionKey :: CancelKey,
    connectionSession :: !Session,
    -- | Whether the handle is live: a pool gives each body a handle of its own,
    -- which is dead once the body ended, whoever has the connection by then.
    connectionLease :: !(IORef Bool)
  }

-- | The part of a connection that every handle of it shares.
data Session = Session
  { sessionCarrier :: !Carrier,
    sessionStatus :: !(IORef Status)
  }

-- | Whether the connection waits for a part, is used by one, or is broken, and
-- why.
data Status = Idle | Busy | Broken String
  deriving (Eq, Show)

-- | What cancels the statement that a connection runs, if the server sent it:
-- some connection poolers don't.
newtype CancelKey = CancelKey (Maybe (BackendKey ByteString))

-- | A connection on the carrier, which nobody uses yet, and the handle of it.
newConnection :: ServerInfo -> Host -> CancelKey -> Carrier -> IO Connection
newConnection info host key carrier = (Connection info host key . Session carrier <$> newIORef Idle) <*> newIORef True

-- | The connection through a new handle, which is live until it is given up.
leased :: Connection -> IO Connection
leased connection = (\lease -> connection {connectionLease = lease}) <$> newIORef True

-- | Takes the connection for the one part that uses it. It fails if the handle is
-- dead, or the connection is used by another part or is broken.
claim :: Connection -> IO ()
claim connection = do
  live <- readIORef (connectionLease connection)
  unless live (throwIO ConnectionReleased)
  taken <- update (sessionStatus (connectionSession connection)) (\case Idle -> (Busy, Nothing); other -> (other, Just other))
  case taken of
    Nothing -> pure ()
    Just (Broken reason) -> throwIO (ConnectionBroken reason)
    Just _ -> throwIO ConnectionBusy

-- | Gives the connection back after a use: it is idle again if it can go on, and
-- broken, with its socket closed, for the reason if it can't.
settle :: Session -> Maybe String -> IO ()
settle session = \case
  Nothing -> update (sessionStatus session) (\case Busy -> (Idle, ()); other -> (other, ()))
  Just reason -> breakSession session reason

-- | Breaks the session for the reason, and closes its socket: it can't be used
-- again, and breaking it again changes nothing.
breakSession :: Session -> String -> IO ()
breakSession session reason = do
  first <- update (sessionStatus session) (\status -> case status of Broken _ -> (status, False); _ -> (Broken reason, True))
  when first (closeCarrier (sessionCarrier session))

-- | The settings, with the data that channel binding takes from the TLS link
-- replaced: by the bytes, or by why there are none, or by none.
withEndPointOverride :: Maybe (Either String ByteString) -> ConnectSettings -> ConnectSettings
withEndPointOverride override settings = settings {connectEndPointOverride = Just override}
