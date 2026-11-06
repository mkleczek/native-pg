-- | Pull sources made of the actions a spec scripts, and a count of the
-- threads that are alive, for specs of uploads.
module PullSourceTestSupport
  ( PullSource (..),
    noWatch,
    liveThreads,
  )
where

import GHC.Conc (ThreadStatus (..), listThreads, threadStatus)
import Network.Streaming.Source (Pulled, Source (..))
import Network.Streaming.Watch (Watch)

-- | A source that pulls and watches as its actions say.
data PullSource = PullSource (IO Pulled) (IO Watch)

instance Source PullSource where
  pullReady (PullSource pulling _) = pulling
  watchSource (PullSource _ watching) = watching

-- | For a source that never has nothing ready.
noWatch :: IO Watch
noWatch = fail "unexpected source watch"

-- | The Haskell threads that haven't finished, the caller's among them.
liveThreads :: IO Int
liveThreads = do
  threads <- listThreads
  statuses <- mapM threadStatus threads
  pure (length (filter (`notElem` [ThreadFinished, ThreadDied]) statuses))
