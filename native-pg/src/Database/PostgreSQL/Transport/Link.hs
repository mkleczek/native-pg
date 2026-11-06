-- | The byte-stream interface implemented by transport links.
module Database.PostgreSQL.Transport.Link
  ( Link (..),
    Readiness (..),
    StreamLengthError (..),
  )
where

import Network.Streaming.Link
