{-# LANGUAGE DataKinds #-}

module OneCoreThreads where

import Data.Sieve
import Network.Streaming.Classes
import Network.Streaming.Threads (Threads)

-- | A cache of one core is not looked up by threads, which GHC preempts in the middle of a step that never waits.
onThreads :: Threads (Cache 'OneCore Int Int, Int) Int
onThreads = cached (now pure) (\_ v -> pure (Use v))
