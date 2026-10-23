-- | Events in the lives of a pool's connections, for logs and metrics.
module Database.PostgreSQL.Pool.Observation
  ( Observation (..)
  , ConnectionStatus (..)
  , ConnectionReadyForUseReason (..)
  , ConnectionTerminationReason (..)
  )
where

import Control.Exception (SomeException)
import Data.Text (Text)
import Data.UUID (UUID)

-- | A connection entered a status.
data Observation
  = ConnectionObservation
      UUID
      -- ^ The connection's ID, generated when it's opened.
      ConnectionStatus
  deriving (Show)

data ConnectionStatus
  = -- | Being opened. Every connection starts here.
    ConnectingConnectionStatus
  | -- | Open, and idle in the pool.
    ReadyForUseConnectionStatus ConnectionReadyForUseReason
  | -- | Taken by a use, after which it's ready again or terminated.
    InUseConnectionStatus
  | -- | Closed.
    TerminatedConnectionStatus ConnectionTerminationReason
  deriving (Show)

data ConnectionReadyForUseReason
  = -- | It was just opened.
    EstablishedConnectionReadyForUseReason
  | -- | A use failed, or threw, without breaking it.
    SessionFailedConnectionReadyForUseReason SomeException
  | -- | A use succeeded.
    SessionSucceededConnectionReadyForUseReason
  deriving (Show)

data ConnectionTerminationReason
  = -- | It reached the maximum lifetime.
    AgingConnectionTerminationReason
  | -- | It was idle for the maximum idle time.
    IdlenessConnectionTerminationReason
  | -- | Opening it failed, or a use broke it, with details if there are any.
    NetworkErrorConnectionTerminationReason (Maybe Text)
  | -- | The pool was released.
    ReleaseConnectionTerminationReason
  deriving (Show)
