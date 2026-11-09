{-# LANGUAGE LambdaCase #-}

-- | Bytes that are pulled when they're wanted, by a consumer that decides what
-- to do when none are ready, and wait for them as it likes, together with its
-- other waits ('Source'), or by one that waits for them in the middle of the
-- pull, with a wait that it gives ('WaitingSource').
module Network.Streaming.Source
  ( Source (..),
    Pulled (..),
    AnySource (..),
    pull,
    retrying,
    WaitingSource (..),
    AnyWaitingSource (..),
    Waited (..),
  )
where

import Data.ByteString (ByteString)
import Network.Streaming.Watch (Watch, awaitWatch)

-- | A source of chunks, with the consumer pulling each when it can use it.
--
-- Only one thread may use a source at a time, and the same one should pull
-- from it, but ownership may pass to another between calls.
class Source source where
  -- | Gives the next chunk, or the end, to the first continuation. When none
  -- is ready it calls the stall with the registration of a watch for the pull
  -- to succeed, which the stall releases, and @again@, the same pull with the
  -- same continuations. The stall waits and runs @again@ before it returns,
  -- or doesn't and gives its own result, and the next pull starts from the
  -- top; it never keeps @again@, which would run outside the source's
  -- handlers. The first continuation only wraps what was found, and the
  -- consumer does the rest after the pull returns, so that its failures are
  -- not the source's.
  --
  -- The chunk stays valid until the next pull from the same source, or the end
  -- of the source's scope, so a consumer that has not finished with it must not
  -- pull again. A source that gives chunks of a size may hold bytes it has
  -- received, in its own space, until it has that many, so a pull that stalled
  -- may hold some, and keeps them for the pull that follows.
  pullWith :: source -> (Pulled -> IO a) -> (IO Watch -> IO a -> IO a) -> IO a

-- | What a pull found.
data Pulled
  = -- | The next chunk, not empty. The end isn't an empty chunk.
    Pulled !ByteString
  | -- | No more chunks.
    Ended

-- | A source of chunks that the consumer waits for in the middle of a pull,
-- on its own thread, with the wait that it gives, and not by stopping when
-- none is ready. It is for what a consumer can't go on without, such as the
-- body of a response, and it is not a 'Source': a consumer that has to be able
-- to stop, such as one that also reads from the server, can't be given one.
--
-- Only one thread may use a source at a time, and the same one should pull
-- from it. The chunk stays valid until the next pull from the same source, or
-- the end of the source's scope, so a consumer that has not finished with it
-- must not pull again.
class WaitingSource source where
  -- | Takes the next chunk, or 'Nothing' at the end. When none is ready, it
  -- calls the wait with an action that registers a watch for what the pull is
  -- stalled on, which the wait releases, and then tries again. An exception
  -- of the wait is an exception of the pull.
  pullWaiting :: source -> (IO Watch -> IO ()) -> IO (Maybe ByteString)

-- | A source of any kind, for values that must not give away the type:
-- one dictionary call per pull.
data AnySource = forall source. Source source => AnySource !source

-- | A waiting source of any kind, as 'AnySource' is for sources.
data AnyWaitingSource = forall source. WaitingSource source => AnyWaitingSource !source

-- | A source where a waiting source is wanted.
newtype Waited source = Waited source

instance Source source => WaitingSource (Waited source) where
  pullWaiting (Waited source) wait = pullWith source (pure . maybeChunk) (\watching again -> wait watching *> again)
  {-# INLINE pullWaiting #-}

-- | Pulls, waiting for the source as long as it takes: 'Nothing' at the end.
pull :: Source source => source -> IO (Maybe ByteString)
pull source = pullWith source (pure . maybeChunk) (\watching again -> awaitWatch watching *> again)
{-# INLINABLE pull #-}

maybeChunk :: Pulled -> Maybe ByteString
maybeChunk = \case
  Pulled chunk -> Just chunk
  Ended -> Nothing

-- | 'pullWith' of a pull that doesn't wait, which gives 'Nothing' when none is
-- ready, and a watch for it to succeed.
retrying :: IO (Maybe Pulled) -> IO Watch -> (Pulled -> IO a) -> (IO Watch -> IO a -> IO a) -> IO a
retrying pulling watching ready stall =
  pulling >>= \case
    Just found -> ready found
    Nothing -> stall watching (retrying pulling watching ready stall)
