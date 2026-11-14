-- | What the stubs of the v2 APIs fail with: every operation that would do I/O
-- or wait, in native-pg and fulmen, until the code is there. They share this
-- one exception, so that a test that is expected to fail can be told to fail
-- for that reason and no other.
module Network.Streaming.NotImplemented
  ( NotImplemented (..),
    notImplemented,
    notImplementedIO,
  )
where

import Control.Exception (Exception (..), throwIO)
import Network.Streaming.Classes (Effect (..))

-- | The operation, as the API names it, has no code yet.
newtype NotImplemented = NotImplemented String
  deriving (Eq, Show)

instance Exception NotImplemented where
  displayException (NotImplemented operation) = operation <> " is not implemented yet"

-- | A step that fails with 'NotImplemented', raised through 'now'.
notImplemented :: (Effect arr) => String -> arr a b
notImplemented operation = now (const (notImplementedIO operation))

-- | The same, for an operation in IO.
notImplementedIO :: String -> IO a
notImplementedIO = throwIO . NotImplemented
