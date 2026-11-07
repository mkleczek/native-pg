{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}

-- | A region that owns what is acquired in it, and releases it when it closes,
-- whichever way it ends.
--
-- 'withScope' is a bracket for any number of resources. An acquisition
-- registers its release in the scope, and the scope runs the releases last in,
-- first out, once each, when its body is done: on success, on an exception and
-- on an asynchronous one. A release learns how the scope ended, so that it can
-- return what it holds when all went well and discard it when it didn't.
--
-- A scope belongs to the thread that opened it, as an 'Data.STRef.STRef' does:
-- its releases are registered and run without synchronization, which keeps
-- a scope with two resources as cheap as two nested brackets.
--
-- The scope's type parameter is rank-2, as the state thread's is in
-- 'Control.Monad.ST.runST': a @'Scope' s@ and an @'Owned' s a@ can't be
-- returned from 'withScope', and a value whose type mentions @s@ can't either.
-- A library that hands out resources from a scope therefore gives them types
-- that mention @s@, and keeps what they wrap private. The tag doesn't reach
-- inside the @a@ that 'owned' takes out of an 'Owned', which is as plain as
-- the library made it: the library checks at run time that such a value is
-- still live, as native-pg's sessions do once their operation has ended.
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

import Control.Exception (Exception, SomeException, mask, mask_, throwIO, toException, try)
import Control.Monad (unless)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)

-- | The releases of what was acquired in a region, the latest first, until
-- the region closes.
newtype Scope s = Scope (IORef Releases)

data Releases
  = Open [Ending -> IO ()]
  | Closed

-- | How a scope ended, as each release learns it. It is the scope's own type:
-- IO has no abort case of its own, and so no dependency is needed for one.
data Ending
  = -- | The body returned, and no release has failed yet.
    Completed
  | -- | The body threw, or an earlier release did, with the exception that
    -- 'withScope' throws unless a later release replaces it.
    Aborted SomeException

-- | A resource acquired in a scope, which is released when the scope closes.
-- It is tagged with the scope, so it can't be returned from 'withScope'.
newtype Owned s a = Owned a

-- | The resource. Once taken out of its 'Owned', its type no longer says which
-- scope it belongs to: see the module's header.
owned :: Owned s a -> a
owned (Owned resource) = resource
{-# INLINE owned #-}

-- | Something was acquired in a scope that had closed already.
data ScopeClosed = ScopeClosed deriving (Show)

instance Exception ScopeClosed

-- | Runs the body with a new scope, and closes the scope when the body is done,
-- also if it throws, by running the releases of what the body acquired, last in,
-- first out, each once.
--
-- Every release runs, even if one of them throws. As with nested brackets,
-- the exception of the last release to throw replaces any before it, and the
-- body's if it threw, and a release that follows learns of it as 'Aborted'. If
-- none throws, the body's exception, if any, is rethrown, and its result is
-- returned otherwise. The releases run masked, as a bracket's does.
withScope :: (forall s. Scope s -> IO a) -> IO a
withScope body = mask $ \restore -> do
  releases <- newIORef (Open [])
  result <- try (restore (body (Scope releases)))
  closeScope releases (either Aborted (const Completed) result) >>= \case
    Completed -> either (\failure -> throwIO (failure :: SomeException)) pure result
    Aborted failure -> throwIO failure
{-# INLINE withScope #-}

-- | Closes the scope, and gives the ending that the scope had after its
-- releases, which is the last failure of one, if there was one.
closeScope :: IORef Releases -> Ending -> IO Ending
closeScope releases ending = do
  pending <- readIORef releases
  writeIORef releases Closed
  runReleases ending (case pending of Open registered -> registered; Closed -> [])
{-# NOINLINE closeScope #-}

runReleases :: Ending -> [Ending -> IO ()] -> IO Ending
runReleases ending = \case
  [] -> pure ending
  release : earlier ->
    try (release ending) >>= \case
      Right () -> runReleases ending earlier
      Left failure -> runReleases (Aborted failure) earlier

-- | Acquires a resource, and registers its release in the scope: the release
-- runs when the scope closes, learning how it ended. The acquisition runs
-- masked, so that no asynchronous exception comes between it and the
-- registration, but it can still be interrupted where it blocks. An acquisition
-- that throws registers nothing: it releases what it took itself. The release
-- is run once, and last in, first out with the scope's others.
--
-- Acquiring in a scope that has closed throws 'ScopeClosed', before the
-- acquisition if it's known by then. The scope can only be reached from a
-- release that runs while it closes.
acquireIn :: Scope s -> IO a -> (Ending -> a -> IO ()) -> IO (Owned s a)
acquireIn (Scope releases) acquire release = mask_ $ do
  readIORef releases >>= \case
    Closed -> throwIO ScopeClosed
    Open _ -> pure ()
  resource <- acquire
  registered <- register releases (`release` resource)
  unless registered $ do
    release (Aborted (toException ScopeClosed)) resource
    throwIO ScopeClosed
  pure (Owned resource)
{-# INLINE acquireIn #-}

register :: IORef Releases -> (Ending -> IO ()) -> IO Bool
register releases release = readIORef releases >>= \case
  Open registered -> True <$ writeIORef releases (Open (release : registered))
  Closed -> pure False
{-# NOINLINE register #-}
