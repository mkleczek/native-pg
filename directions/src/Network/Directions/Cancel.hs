{-# LANGUAGE BangPatterns #-}

-- | Cancel scopes, which every arrow keeps its cancellation in: a scope is a flag and the parts inside it, and a
-- part has a stack of the scopes it is in, with the shields between them. Cancelling a scope sets its flag and
-- tells the parts inside it; each arrow tells them in its own way. A part looks at the flags of its scopes at its
-- waits, and a cancellation is for it only if its scope lies above the innermost shield.
--
-- What a scope keeps of the parts inside it is the arrow's, and its type. "Network.Directions.Threads", whose parts
-- run on several threads, keeps them in a map that every thread that enters or leaves the scope changes atomically.
-- The arrows that run every part of a core on one thread ("Network.Directions.Scheduler") keep nothing of them
-- when a part enters or leaves a scope: a scope begins with the parts that made it, and the rest are found
-- through those when it is cancelled.
module Network.Directions.Cancel
  ( CancelScope (..),
    Frame (..),
    newCancelScope,
    cancelledIn,
    deliverable,
  )
where

import Data.IORef (IORef, newIORef, readIORef)
import Network.Directions.Classes (ScopeId (..))

-- | A scope: which it is, whether it was cancelled, and what the arrow keeps of the parts inside it, in a reference.
data CancelScope m = CancelScope
  { scopeIdent :: !ScopeId,
    scopeCancelled :: !(IORef Bool),
    scopeMembers :: !(IORef m)
  }

-- | What a part is in, innermost first: a cancel scope, a shield, or a resource's scope, which a cancellation
-- passes through: a part in one is in the run's root scope, which a shutdown cancels.
data Frame m = Inside !(CancelScope m) | Shielded | Held

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
    go !found (Held : outer) = go found outer
    go !found (Inside s : outer) =
      readIORef (scopeCancelled s) >>= \cancelled -> if cancelled then go (Just (scopeIdent s)) outer else go found outer

-- | Whether a cancellation of the scope is for a part with these frames: it lies above the innermost shield.
deliverable :: ScopeId -> [Frame m] -> Bool
deliverable _ [] = False
deliverable _ (Shielded : _) = False
deliverable ident (Held : outer) = deliverable ident outer
deliverable ident (Inside s : outer) = scopeIdent s == ident || deliverable ident outer
