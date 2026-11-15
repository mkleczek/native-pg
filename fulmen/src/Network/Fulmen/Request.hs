-- | The request, and the body that it arrives with.
module Network.Fulmen.Request
  ( Request (..),
    Method (..),
    Version (..),
    Headers,
    Body,
    body,
  )
where

import Control.Arrow (Arrow, arr)
import Data.ByteString (ByteString)
import Network.Fulmen.Internal (Body (..), Call (..))
import Network.Socket (SockAddr)

-- | The head of a request, parsed whole into owned metadata.
data Request = Request
  { method :: Method,
    -- | The request target exactly as it was received.
    target :: ByteString,
    version :: Version,
    headers :: Headers,
    -- | The address that the connection came from, as the server saw it: that of the proxy in front of the server, if
    -- there is one.
    peer :: SockAddr
  }
  deriving (Eq, Show)

newtype Method = Method ByteString
  deriving (Eq, Ord, Show)

data Version = Http10 | Http11
  deriving (Eq, Ord, Show)

-- | The header field lines in the order they were received, one for each, with
-- the names as written and the values stripped of surrounding whitespace.
type Headers = [(ByteString, ByteString)]

-- | The body of the call.
body :: (Arrow arr) => arr Call Body
body = arr (\(Call _ requestBody _) -> requestBody)
