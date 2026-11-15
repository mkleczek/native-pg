-- | What starts a graceful shutdown: an event of the scheduler's poller, which the settings of a server name.
module Network.Fulmen.Shutdown
  ( terminating,
  )
where

import Network.Streaming.Classes (Event)
import Network.Streaming.NotImplemented (notImplementedIO)

-- | The event that fires when the process is sent SIGTERM or SIGINT.
terminating :: IO Event
terminating = notImplementedIO "terminating"
