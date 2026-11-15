{-# LANGUAGE DataKinds #-}

module OneCoreCooperative where

import Data.Sieve
import Network.Streaming.Classes
import Network.Streaming.Compiled (Compiled)
import Network.Streaming.Delimited (Delimited)

-- | A cache of one core is looked up by an arrow whose parts switch only at waits: the arrow of one thread, and the arrow of
-- a core.
onDelimited :: Delimited s (Cache 'OneCore Int Int, Int) Int
onDelimited = cached (now pure) (\_ v -> pure (Use v))

onCompiled :: Compiled (Cache 'OneCore Int Int, Int) Int
onCompiled = cached (now pure) (\_ v -> pure (Use v))
