module Control.Concurrent.Resource.Waiting
  ( Waiting, Waiter, Deadline(..), WaitError(..)
  , newWaiting, closeWaiting, withWaiting, deadlineAfter, deadlineExpired
  , withWait, tryGrant, reject
  ) where
import Control.Concurrent.STM
import Data.Time.Clock (DiffTime)
import Data.Word (Word64)
data Waiting = Waiting
data Waiter a = Waiter
data Deadline = NoDeadline | At Word64 deriving (Eq, Show)
data WaitError = TimedOut | Closed deriving (Eq, Show)
newWaiting :: IO Waiting
newWaiting = pure Waiting
closeWaiting :: Waiting -> IO ()
closeWaiting _ = pure ()
withWaiting :: (Waiting -> IO a) -> IO a
withWaiting f = newWaiting >>= f
deadlineAfter :: DiffTime -> IO Deadline
deadlineAfter _ = error "pending deadline implementation"
deadlineExpired :: Deadline -> IO Bool
deadlineExpired _ = error "pending deadline implementation"
withWait :: Waiting -> Deadline -> (Waiter a -> STM handle) -> (handle -> STM ()) -> (a -> IO ()) -> (a -> IO r) -> IO (Either WaitError r)
withWait _ _ _ _ _ _ = error "pending private delivery implementation"
tryGrant :: Waiter a -> STM a -> STM Bool
tryGrant _ _ = error "pending reservation implementation"
reject :: Waiter a -> WaitError -> STM Bool
reject _ _ = error "pending rejection implementation"
