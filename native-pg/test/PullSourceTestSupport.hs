-- | Pull sources made of the actions a spec scripts, and a count of the
-- threads that are alive, for specs of uploads.
module PullSourceTestSupport
  ( PullSource (..),
    noWatch,
    liveThreads,
  )
where

import GHC.Conc (ThreadStatus (..), listThreads, threadStatus)
import Network.Streaming.Source (Pulled, Source (..), retrying)
import Network.Streaming.Watch (Watch)

-- | A source that pulls and watches as its actions say: the pull gives
-- 'Nothing' when it has nothing ready.
data PullSource = PullSource (IO (Maybe Pulled)) (IO Watch)

instance Source PullSource where
  pullWith (PullSource pulling watching) = retrying pulling watching

-- | For a source that never has nothing ready.
noWatch :: IO Watch
noWatch = fail "unexpected source watch"

-- | The Haskell threads that haven't finished, the caller's among them.
liveThreads :: IO Int
liveThreads = do
  threads <- listThreads
  statuses <- mapM threadStatus threads
  pure (length (filter (`notElem` [ThreadFinished, ThreadDied]) statuses))
