{-# LANGUAGE RankNTypes #-}

-- | The API of a scope, without its behaviour yet: nothing is registered, so no
-- release ever runs.
module Network.Streaming.Scope
  ( Scope,
    withScope,
    acquireIn,
    Owned,
    owned,
    Ending (..),
    ScopeClosed (..),
  )
where

import Control.Exception (Exception, SomeException)

data Scope s = Scope

data Ending
  = Completed
  | Aborted SomeException

newtype Owned s a = Owned a

owned :: Owned s a -> a
owned (Owned resource) = resource

data ScopeClosed = ScopeClosed deriving (Show)

instance Exception ScopeClosed

withScope :: (forall s. Scope s -> IO a) -> IO a
withScope body = body Scope

acquireIn :: Scope s -> IO a -> (Ending -> a -> IO ()) -> IO (Owned s a)
acquireIn _ acquire _ = Owned <$> acquire
