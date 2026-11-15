{-# LANGUAGE LambdaCase #-}

-- | What a connection is made of, and what only tests reach: the constructors of
-- the types that "Database.PostgreSQL.Connection" exports without them, and the
-- override of the TLS end-point data that SCRAM-PLUS binds to.
--
-- A connection is a socket and what the server said of itself, and whether it is
-- idle, used by one part, or broken, and what it keeps between its uses: the bytes
-- that a use read past its last message, the notifications that came while nobody
-- waited for them, and the buffer that listening reads in; and the cells that a
-- program keeps for it, which nothing in it reads. It holds nothing that
-- belongs to the arrow it was made under, so that a connection made under one runtime
-- works under another.
module Database.PostgreSQL.Connection.Internal
  ( Connection (..),
    Session (..),
    Status (..),
    Kept (..),
    CancelKey (..),
    newConnection,
    leased,
    claim,
    settle,
    breakSession,
    keepNotification,
    takeNotification,
    leaveCarry,
    takeCarry,
    seedReader,
    pastLastMessage,
    midMessage,
    withEndPointOverride,
  )
where

import Control.Exception (throwIO)
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Foreign.ForeignPtr (plusForeignPtr)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (castPtr)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Dynamic (Dynamic)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Database.PostgreSQL.ConnectionString.Types (Host)
import Database.PostgreSQL.Errors (ConnectionError (..), UsageError (..))
import Database.PostgreSQL.Internal.Carrier (Carrier, closeCarrier)
import Database.PostgreSQL.Internal.Protocol.Messages.Types (BackendKey)
import Database.PostgreSQL.Session.Settings (ConnectSettings (..), ServerInfo)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Reader (Reader, mark, markEnd, markLimit, markStart, readerCapacity, readerMemory, setMark)
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
    sessionStatus :: !(IORef Status),
    -- | What the session keeps between uses, which only the part that uses it changes.
    sessionKept :: !(IORef Kept),
    -- | The cells that a program keeps for the connection, one for each type of value. No use of the connection reads them.
    sessionSlots :: !(IORef [Dynamic])
  }

-- | What a session keeps between its uses: the bytes that the server sent after the last message that a use read, which
-- the next use starts with; the notifications that arrived while no 'Database.PostgreSQL.Notifications.notification'
-- waited, as their bodies, the oldest first; and the buffer that listening reads in, once a connection has listened.
data Kept = Kept
  { keptCarry :: !ByteString,
    keptNotifications :: !Backlog,
    keptBuffer :: !(Maybe Buffer)
  }

-- | A queue: what is taken is the first of the front, and the back, which is newest first, takes its place when the front
-- is empty.
data Backlog = Backlog [ByteString] [ByteString]

-- | Whether the connection waits for a part, is used by one, or is broken, and
-- why.
data Status = Idle | Busy | Broken String
  deriving (Eq, Show)

-- | What cancels the statement that a connection runs, if the server sent it:
-- some connection poolers don't.
newtype CancelKey = CancelKey (Maybe (BackendKey ByteString))

-- | A connection on the carrier, which nobody uses yet, and the handle of it.
newConnection :: ServerInfo -> Host -> CancelKey -> Carrier -> IO Connection
newConnection info host key carrier = do
  status <- newIORef Idle
  kept <- newIORef (Kept BS.empty (Backlog [] []) Nothing)
  slots <- newIORef []
  Connection info host key (Session carrier status kept slots) <$> newIORef True

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

-- | Keeps the body of a notification, after the ones that are kept.
keepNotification :: Session -> ByteString -> IO ()
keepNotification session body = modifyKept session (\kept@Kept {keptNotifications = Backlog front back} -> kept {keptNotifications = Backlog front (body : back)})

-- | Takes the oldest notification that is kept, if there is one.
takeNotification :: Session -> IO (Maybe ByteString)
takeNotification session = do
  Kept {keptNotifications = Backlog front back} <- readIORef (sessionKept session)
  case front of
    body : others -> Just body <$ modifyKept session (\kept -> kept {keptNotifications = Backlog others back})
    []
      | null back -> pure Nothing
      | otherwise -> case reverse back of
          body : others -> Just body <$ modifyKept session (\kept -> kept {keptNotifications = Backlog others []})
          [] -> pure Nothing

-- | Keeps the bytes that the server sent after the last message that a use read, for the next use to start with, after
-- those that are kept already.
leaveCarry :: Session -> ByteString -> IO ()
leaveCarry session carry = modifyKept session (\kept -> kept {keptCarry = keptCarry kept <> carry})

-- | Takes the bytes that were left, if there are any.
takeCarry :: Session -> IO ByteString
takeCarry session = do
  kept <- readIORef (sessionKept session)
  if BS.null (keptCarry kept)
    then pure BS.empty
    else keptCarry kept <$ writeIORef (sessionKept session) kept {keptCarry = BS.empty}

-- | Starts the reader with the bytes that a session kept, as the first it has in hand, after what it held, which was
-- taken out before. They are what the server sent after the last message of a use, so they fit the buffer of that use,
-- and the failure is for a buffer that is smaller. The buffer is not one that anything is rendered into meanwhile.
seedReader :: Reader Carrier -> ByteString -> IO ()
seedReader reader carried = do
  when (BS.length carried > readerCapacity reader) (throwIO (ConnectionBroken "the server sent more, after the last message, than the buffer of the next use holds"))
  BSU.unsafeUseAsCStringLen carried (\(source, size) -> unsafeWithForeignPtr (readerMemory reader) (\memory -> copyBytes memory (castPtr source) size))
  setMark reader markStart 0
  setMark reader markLimit 0
  setMark reader markEnd (BS.length carried)

-- | The bytes that the reader holds beyond the last message that it read, copied: those that the server sent after it,
-- of a message that is not whole too.
pastLastMessage :: Reader Carrier -> IO ByteString
pastLastMessage reader = do
  start <- mark reader markStart
  limit <- mark reader markLimit
  end <- mark reader markEnd
  let from = max start limit
  pure (if end > from then BS.copy (BSI.BS (readerMemory reader `plusForeignPtr` from) (end - from)) else BS.empty)

-- | Whether the reader is in the middle of a message, with the rest of its body to come: what was read of it is gone, so
-- a connection that is left then can't be read from again.
midMessage :: Reader Carrier -> IO Bool
midMessage reader = (>) <$> mark reader markLimit <*> mark reader markEnd

modifyKept :: Session -> (Kept -> Kept) -> IO ()
modifyKept session change = readIORef (sessionKept session) >>= \kept -> writeIORef (sessionKept session) $! change kept

-- | The settings, with the data that channel binding takes from the TLS link
-- replaced: by the bytes, or by why there are none, or by none.
withEndPointOverride :: Maybe (Either String ByteString) -> ConnectSettings -> ConnectSettings
withEndPointOverride override settings = settings {connectEndPointOverride = Just override}
