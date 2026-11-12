{-# LANGUAGE BangPatterns #-}

-- | Cancel scopes, which every arrow keeps its cancellation in: a scope is a flag and the parts inside it, and a
-- part has a stack of the scopes it is in, with the shields between them. Cancelling a scope sets its flag and
-- tells the parts inside it; each arrow tells them in its own way. A part looks at the flags of its scopes at its
-- waits, and a cancellation is for it only if its scope lies above the innermost shield.
module Network.Directions.Cancel
  ( CancelScope (..),
    Frame (..),
    Identified (..),
    nextIdent,
    newCancelScope,
    cancelledIn,
    deliverable,
    enter,
    leave,
    markCancelled,
  )
where

import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef)
import qualified Data.IntMap.Strict as IntMap
import Network.Directions.Atomic (update)
import Network.Directions.Classes (ScopeId (..))
import System.IO.Unsafe (unsafePerformIO)

-- | A scope: which it is, whether it was cancelled, and the parts inside it, of the arrow's part type, by their
-- numbers.
data CancelScope p = CancelScope
  { scopeIdent :: !ScopeId,
    scopeCancelled :: !(IORef Bool),
    scopeMembers :: !(IORef (IntMap.IntMap p))
  }

-- | What a part is in, innermost first: a cancel scope, a shield, or a resource's scope, which a cancellation
-- passes through: a part in one is in the run's root scope, which a shutdown cancels.
data Frame p = Inside !(CancelScope p) | Shielded | Held

-- | A part has a number, which no other has.
class Identified p where
  identOf :: p -> Int

newCancelScope :: IO (CancelScope p)
newCancelScope = CancelScope . ScopeId <$> nextIdent <*> newIORef False <*> newIORef IntMap.empty

counter :: IORef Int
counter = unsafePerformIO (newIORef 0)
{-# NOINLINE counter #-}

-- | A number that no other has, from any thread.
nextIdent :: IO Int
nextIdent = update counter (\n -> (n + 1, n))

-- | The scope whose cancellation reaches a part with these frames: the outermost cancelled scope above the
-- innermost shield.
cancelledIn :: [Frame p] -> IO (Maybe ScopeId)
cancelledIn = go Nothing
  where
    go !found [] = pure found
    go !found (Shielded : _) = pure found
    go !found (Held : outer) = go found outer
    go !found (Inside s : outer) =
      readIORef (scopeCancelled s) >>= \cancelled -> if cancelled then go (Just (scopeIdent s)) outer else go found outer

-- | Whether a cancellation of the scope is for a part with these frames: it lies above the innermost shield.
deliverable :: ScopeId -> [Frame p] -> Bool
deliverable _ [] = False
deliverable _ (Shielded : _) = False
deliverable ident (Held : outer) = deliverable ident outer
deliverable ident (Inside s : outer) = scopeIdent s == ident || deliverable ident outer

-- | The part is inside the scope.
enter :: (Identified p) => CancelScope p -> p -> IO ()
enter s part = update (scopeMembers s) (\parts -> (IntMap.insert (identOf part) part parts, ()))

-- | The part is not inside the scope any more.
leave :: (Identified p) => CancelScope p -> p -> IO ()
leave s part = update (scopeMembers s) (\parts -> (IntMap.delete (identOf part) parts, ()))

-- | Sets the scope's flag, and gives the parts that were inside it. The flag is set with a barrier, so that a
-- part that enters the scope after the parts are read sees it.
markCancelled :: CancelScope p -> IO [p]
markCancelled s = atomicWriteIORef (scopeCancelled s) True *> (IntMap.elems <$> readIORef (scopeMembers s))
