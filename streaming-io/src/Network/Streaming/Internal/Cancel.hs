{-# LANGUAGE BangPatterns #-}

-- | Cancel scopes, which every arrow keeps its cancellation in: a scope is a flag and the parts inside it, and a
-- part has a stack of the scopes it is in, with the shields between them. Cancelling a scope sets its flag and
-- tells the parts inside it; each arrow tells them in its own way. A part looks at the flags of its scopes at its
-- waits, and a cancellation is for it only if its scope lies above the innermost shield.
--
-- A part looks at its scopes only when its own flag says that a cancellation may have reached it, so that the cost
-- of a point that nothing was cancelled for doesn't depend on how many scopes the part is in. The flag is set for
-- every part inside a scope that is cancelled, whether or not a shield holds it, and for a part that begins after
-- that; it is cleared when none of the part's scopes, shields or not, is cancelled. A part that finds a flag looks
-- at its scopes as it always did: the flag only says when to.
--
-- What a scope keeps of the parts inside it is the arrow's, and its type. "Network.Streaming.Threads", whose parts
-- run on several threads, keeps them in a map that every thread that enters or leaves the scope changes atomically.
-- The arrows that run every part of a core on one thread ("Network.Streaming.Internal.Scheduler") keep nothing of them
-- when a part enters or leaves a scope: a scope begins with the parts that made it, and the rest are found
-- through those when it is cancelled. The scope of the parts that a group spawns while it runs is the one that
-- holds them, each added as it is spawned and taken out as it ends.
module Network.Streaming.Internal.Cancel
  ( CancelScope (..),
    Frame (..),
    newCancelScope,
    cancelledIn,
    cancelledAnywhere,
    cancelledFlagged,
    reachedBy,
    deliverable,
  )
where

import Control.Monad (when)
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import Network.Streaming.Classes (Exit, ScopeId (..))

-- | A scope: which it is, whether it was cancelled, and what the arrow keeps of the parts inside it, in a reference.
data CancelScope m = CancelScope
  { scopeIdent :: !ScopeId,
    scopeCancelled :: !(IORef Bool),
    scopeMembers :: !(IORef m)
  }

-- | What a part is in, innermost first: a cancel scope, a shield, a resource's scope, a release that is due, or the mark of a
-- catcher. A cancellation passes through the last three: a part in a resource's scope or with a release due is in the run's
-- root scope, which a shutdown cancels. A release that is due is what a scope that has no frame of its own on the part's stack
-- leaves for a failure to find: it is run with how the body ended, by the catcher that the failure comes to, and the mark is the
-- lowest frame that catcher runs the releases of.
data Frame m = Inside !(CancelScope m) | Shielded | Held | Owes !(Exit -> IO ()) | Marked

-- | A scope with this number, which the arrow takes care that no other scope that its parts can be in has, and
-- what it keeps of the parts inside it to begin with.
newCancelScope :: Int -> m -> IO (CancelScope m)
newCancelScope ident members = CancelScope (ScopeId ident) <$> newIORef False <*> newIORef members

-- | The scope whose cancellation reaches a part with these frames: the outermost cancelled scope above the
-- innermost shield.
cancelledIn :: [Frame m] -> IO (Maybe ScopeId)
cancelledIn = go Nothing
  where
    go !found [] = pure found
    go !found (Shielded : _) = pure found
    go !found (Inside s : outer) =
      readIORef (scopeCancelled s) >>= \cancelled -> if cancelled then go (Just (scopeIdent s)) outer else go found outer
    go !found (_ : outer) = go found outer

-- | Whether any of these frames is a cancelled scope, shields or not.
cancelledAnywhere :: [Frame m] -> IO Bool
cancelledAnywhere = go
  where
    go [] = pure False
    go (Inside s : outer) = readIORef (scopeCancelled s) >>= \cancelled -> if cancelled then pure True else go outer
    go (_ : outer) = go outer

-- | 'cancelledIn' for a part that has a flag, which says whether a cancellation may have reached it: the frames are looked
-- at only if it is set.
cancelledFlagged :: IORef Bool -> [Frame m] -> IO (Maybe ScopeId)
cancelledFlagged flag frames = readIORef flag >>= \flagged -> if flagged then reachedBy flag frames else pure Nothing
{-# INLINE cancelledFlagged #-}

-- | The scope whose cancellation reaches a part with these frames and a flag that is set, as 'cancelledIn' says. The flag
-- is cleared when no frame at all is a cancelled scope: if one lies behind a shield, it stays, so that the part
-- finds it at its first point after the shield. The flag is cleared before the frames are looked at, with a barrier, so
-- that a thread that cancels a scope meanwhile, which sets the scope's flag and then the part's, is not lost: the
-- part sees the scope's, or the thread's sets it again after.
reachedBy :: IORef Bool -> [Frame m] -> IO (Maybe ScopeId)
reachedBy flag frames = do
  atomicWriteIORef flag False
  found <- cancelledIn frames
  anywhere <- maybe (cancelledAnywhere frames) (const (pure True)) found
  when anywhere (atomicWriteIORef flag True)
  pure found
{-# NOINLINE reachedBy #-}

-- | Whether a cancellation of the scope is for a part with these frames: it lies above the innermost shield.
deliverable :: ScopeId -> [Frame m] -> Bool
deliverable _ [] = False
deliverable _ (Shielded : _) = False
deliverable ident (Inside s : outer) = scopeIdent s == ident || deliverable ident outer
deliverable ident (_ : outer) = deliverable ident outer
