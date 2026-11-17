{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

-- | Cancel scopes, which every arrow keeps its cancellation in: a scope is a flag and the parts inside it, and a
-- part has a stack of the scopes it is in, with the shields and graces between them. Cancelling a scope sets its flag and
-- tells the parts inside it; each arrow tells them in its own way. A part looks at the flags of its scopes at its
-- waits, and a cancellation is for it only if its scope lies above the innermost shield, and the periods of the graces between
-- it and the part have passed.
--
-- A part looks at its scopes only when its own flag says that a cancellation may have reached it, so that the cost
-- of a point that nothing was cancelled for doesn't depend on how many scopes the part is in. The flag is set for
-- every part inside a scope that is cancelled, whether or not a shield holds it, and for a part that begins after
-- that; it is cleared when none of the part's scopes, shields or not, is cancelled. A part that finds a flag looks
-- at its scopes as it always did: the flag only says when to.
--
-- A scope's flag says when it was cancelled, which only a grace asks for, and for which periods of graces a timer is armed already.
-- A grace is one frame of the part's stack, with its period, and nothing else: it lets the cancellation of a scope outside it in once
-- its period has passed since the scope was cancelled, so that the state of a region is in the scope that was cancelled, and a region
-- that is entered costs what a shield does.
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
    isCancelled,
    setCancelled,
    setCancelledAtomic,
    arrivalOf,
    armOnce,
    gracedFrame,
    Hold (..),
    holdOf,
    heldIn,
    heldUntil,
    cancelledIn,
    cancelledAnywhere,
    cancelledFlagged,
    reachedBy,
    deliverable,
  )
where

import Control.Monad (when)
import Data.IORef (IORef, atomicWriteIORef, newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import Network.Streaming.Classes (Exit, ScopeId (..), monotonicNanoseconds)
import Network.Streaming.Internal.Atomic (update)

-- | Whether a scope was cancelled, and if it was, the instant, which is 0 until a grace asks for it, and the periods that a timer is
-- armed for already.
data Flag = NotCancelled | CancelledAt !Word64 [Word64]

-- | A scope: which it is, whether and when it was cancelled, and what the arrow keeps of the parts inside it, in a reference.
data CancelScope m = CancelScope
  { scopeIdent :: !ScopeId,
    scopeCancelled :: !(IORef Flag),
    scopeMembers :: !(IORef m)
  }

-- | What a part is in, innermost first: a cancel scope, a shield, a grace with its period in nanoseconds, a resource's scope, a release
-- that is due, or the mark of a catcher. A cancellation passes through the last three: a part in a resource's scope or with a release
-- due is in the run's root scope, which a shutdown cancels. A release that is due is what a scope that has no frame of its own on the
-- part's stack leaves for a failure to find: it is run with how the body ended, by the catcher that the failure comes to, and the mark
-- is the lowest frame that catcher runs the releases of.
data Frame m = Inside !(CancelScope m) | Shielded | Graced !Word64 | Held | Owes !(Exit -> IO ()) | Marked

-- | The frame of a grace of this period, which the arrow makes once, when it builds the region: not inlined, so that an entry doesn't
-- make it again.
gracedFrame :: Word64 -> Frame m
gracedFrame = Graced
{-# NOINLINE gracedFrame #-}

-- | A scope with this number, which the arrow takes care that no other scope that its parts can be in has, and
-- what it keeps of the parts inside it to begin with.
newCancelScope :: Int -> m -> IO (CancelScope m)
newCancelScope ident members = CancelScope (ScopeId ident) <$> newIORef NotCancelled <*> (newIORef $! members)

-- | Whether the scope was cancelled.
isCancelled :: CancelScope m -> IO Bool
isCancelled s = readIORef (scopeCancelled s) >>= \case NotCancelled -> pure False; _ -> pure True
{-# INLINE isCancelled #-}

-- | The scope is cancelled, if it was not: for an arrow whose parts run on one thread.
setCancelled :: CancelScope m -> IO ()
setCancelled s = readIORef (scopeCancelled s) >>= \case NotCancelled -> writeIORef (scopeCancelled s) (CancelledAt 0 []); _ -> pure ()
{-# INLINE setCancelled #-}

-- | The same from any thread, with a barrier, so that a part that enters the scope after the parts were read sees it.
setCancelledAtomic :: CancelScope m -> IO ()
setCancelledAtomic s = update (scopeCancelled s) (\case NotCancelled -> (CancelledAt 0 [], ()); cancelled -> (cancelled, ()))

-- | The instant that the scope was cancelled at, which is the given instant, the first time that it is asked for.
arrivalOf :: CancelScope m -> Word64 -> IO Word64
arrivalOf s now = update (scopeCancelled s) $ \case
  CancelledAt 0 armed -> (CancelledAt now armed, now)
  cancelled@(CancelledAt arrival _) -> (cancelled, arrival)
  NotCancelled -> (NotCancelled, 0)

-- | When a timer is to fire for this period of a grace that holds the scope's cancellation, at the given instant: if none is armed for
-- it yet, which the scope notes, and the period does not end at the end of time. The first time that the arrival is asked for, it is
-- the given instant.
armOnce :: CancelScope m -> Word64 -> Word64 -> IO (Maybe Word64)
armOnce s now period = update (scopeCancelled s) $ \case
  NotCancelled -> (NotCancelled, Nothing)
  CancelledAt known armed ->
    let arrival = if known == 0 then now else known
        end = heldUntil arrival period
     in if period `elem` armed || end == maxBound
          then (CancelledAt arrival armed, Nothing)
          else (CancelledAt arrival (period : armed), Just end)

-- | The instant that a period after an arrival ends at: the end of time, if it doesn't.
heldUntil :: Word64 -> Word64 -> Word64
heldUntil arrival period = let end = arrival + period in if end < arrival then maxBound else end

-- | How the cancellation of a scope is held from a part: whether a shield lies between, and the longest of the periods of the graces
-- between, which is 0 if there is none.
data Hold = Hold !Bool !Word64

-- | How the cancellation of the scope is held from a part with these frames: a part that the scope is not found in is held by a shield.
holdOf :: ScopeId -> [Frame m] -> Hold
holdOf ident = go False 0
  where
    go !shielded !period = \case
      [] -> Hold True period
      Shielded : outer -> go True period outer
      Graced limit : outer -> go shielded (max period limit) outer
      Inside s : outer
        | scopeIdent s == ident -> Hold shielded period
        | otherwise -> go shielded period outer
      _ : outer -> go shielded period outer

-- | The scopes that were cancelled that graces hold from a part with these frames, each with the longest period of the graces
-- between, shields or not: the periods are what a timer waits for.
heldIn :: [Frame m] -> IO [(CancelScope m, Word64)]
heldIn = go 0 []
  where
    go !period found = \case
      [] -> pure (reverse found)
      Graced limit : outer -> go (max period limit) found outer
      Inside s : outer -> isCancelled s >>= \cancelled -> go period (if cancelled && period > 0 then (s, period) : found else found) outer
      _ : outer -> go period found outer

-- | The scope whose cancellation reaches a part with these frames: the outermost cancelled scope above the
-- innermost shield, that the periods of the graces below it have let in. The clock is read when a grace holds a cancelled scope,
-- and the first time that it is asked for, the scope's arrival is its instant.
cancelledIn :: [Frame m] -> IO (Maybe ScopeId)
cancelledIn = go Nothing 0 0
  where
    go !found !period !now = \case
      [] -> pure found
      Shielded : _ -> pure found
      Graced limit : outer -> go found (max period limit) now outer
      Inside s : outer ->
        readIORef (scopeCancelled s) >>= \case
          NotCancelled -> go found period now outer
          _
            | period == 0 -> go (Just (scopeIdent s)) period now outer
            | otherwise -> do
                now' <- if now == 0 then monotonicNanoseconds else pure now
                arrival <- arrivalOf s now'
                go (if now' >= heldUntil arrival period then Just (scopeIdent s) else found) period now' outer
      _ : outer -> go found period now outer

-- | Whether any of these frames is a cancelled scope, shields or not.
cancelledAnywhere :: [Frame m] -> IO Bool
cancelledAnywhere = go
  where
    go [] = pure False
    go (Inside s : outer) = isCancelled s >>= \cancelled -> if cancelled then pure True else go outer
    go (_ : outer) = go outer

-- | 'cancelledIn' for a part that has a flag, which says whether a cancellation may have reached it: the frames are looked
-- at only if it is set.
cancelledFlagged :: IORef Bool -> [Frame m] -> IO (Maybe ScopeId)
cancelledFlagged flag frames = readIORef flag >>= \flagged -> if flagged then reachedBy flag frames else pure Nothing
{-# INLINE cancelledFlagged #-}

-- | The scope whose cancellation reaches a part with these frames and a flag that is set, as 'cancelledIn' says. The flag
-- is cleared when no frame at all is a cancelled scope: if one lies behind a shield, or a grace whose period has not passed, it
-- stays, so that the part finds it at its first point after the shield, or after the period. The flag is cleared before the frames are looked
-- at, with a barrier, so that a thread that cancels a scope meanwhile, which sets the scope's flag and then the part's, is not lost: the
-- part sees the scope's, or the thread's sets it again after.
reachedBy :: IORef Bool -> [Frame m] -> IO (Maybe ScopeId)
reachedBy flag frames = do
  atomicWriteIORef flag False
  found <- cancelledIn frames
  anywhere <- maybe (cancelledAnywhere frames) (const (pure True)) found
  when anywhere (atomicWriteIORef flag True)
  pure found
{-# NOINLINE reachedBy #-}

-- | Whether a cancellation of the scope is for a part with these frames at once: it lies above the innermost shield, and no grace is
-- between.
deliverable :: ScopeId -> [Frame m] -> Bool
deliverable ident frames = case holdOf ident frames of
  Hold shielded period -> not shielded && period == 0
