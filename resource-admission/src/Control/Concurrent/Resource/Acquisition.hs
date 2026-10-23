module Control.Concurrent.Resource.Acquisition
  ( Context, Request, Deadline(..)
  , newContext, deadlineAfter, deadlineExpired
  , acquire, tryGrant
  ) where
import Control.Concurrent.STM
import Data.Time.Clock (DiffTime)
import Data.Word (Word64)
import GHC.Event qualified as Timer
newtype Context = Context Timer.TimerManager
data Request a = Request
data Deadline = NoDeadline | At Word64 deriving (Eq, Show)
newContext :: Timer.TimerManager -> Context
newContext = Context
deadlineAfter :: DiffTime -> IO Deadline
deadlineAfter _ = error "pending deadline implementation"
deadlineExpired :: Deadline -> IO Bool
deadlineExpired _ = error "pending deadline implementation"
acquire :: Context -> Deadline -> STM a -> (Request a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> IO (Maybe a)
acquire _ _ _ _ _ _ = error "pending private acquisition implementation"
tryGrant :: Request a -> STM a -> STM Bool
tryGrant _ _ = error "pending reservation implementation"
