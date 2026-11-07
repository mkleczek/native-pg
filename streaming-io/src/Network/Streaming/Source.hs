{-# LANGUAGE LambdaCase #-}

-- | Bytes that are pulled when they're wanted, by a consumer that may find
-- that none are ready yet, and wait for them as it likes, together with its
-- other waits.
module Network.Streaming.Source
  ( Source (..),
    Pulled (..),
    AnySource (..),
    pull,
  )
where

import Data.ByteString (ByteString)
import Network.Streaming.Watch (Watch, awaitWatch)

-- | A source of chunks, with the consumer pulling each when it can use it.
--
-- Only one thread may use a source at a time, and the same one should pull
-- from it and wait for it, but ownership may pass to another between calls.
class Source source where
  -- | Takes the next chunk if it's available without waiting. The chunk stays
  -- valid until the next pull from the same source, or the end of the
  -- source's scope, so a consumer that has not finished with it must not pull
  -- again. A source that gives chunks of a size may hold bytes it has
  -- received, in its own space, until it has that many, so a 'Pending' pull
  -- may hold some, and keeps them for the pull that follows.
  pullReady :: source -> IO Pulled

  -- | Registers interest in a pull succeeding after 'pullReady' returned
  -- 'Pending', and fires once it may. The caller releases the watch. It
  -- doesn't pull, and doesn't guarantee that a pull succeeds.
  watchSource :: source -> IO Watch

-- | What a pull found.
data Pulled
  = -- | The next chunk, not empty. The end isn't an empty chunk.
    Pulled !ByteString
  | -- | Nothing is available yet. Watch the source, and pull again.
    Pending
  | -- | No more chunks.
    Ended

-- | A source of any kind, for values that must not give away the type:
-- one dictionary call per pull.
data AnySource = forall source. Source source => AnySource !source

-- | Pulls, waiting for the source as long as it takes: a pull, and if it
-- finds nothing, a wait on the source's watch and a pull again. 'Nothing' at
-- the end. The wait is interruptible and releases its watch.
pull :: Source source => source -> IO (Maybe ByteString)
pull source =
  pullReady source >>= \case
    Pulled chunk -> pure (Just chunk)
    Pending -> awaitWatch (watchSource source) *> pull source
    Ended -> pure Nothing
{-# INLINABLE pull #-}
