-- | Answering: the head, which says how long the body is, and the body, which
-- is sent as the views that it is given. The steps are stubs, which fail with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the server is there.
module Network.Fulmen.Response
  ( Status (..),
    ResponseHeaders,
    Length (..),
    Answering,
    respond,
    send,
    finish,
  )
where

import Data.ByteString (ByteString)
import Network.Fulmen.Internal (Answering, Call)
import Network.Streaming.Classes (Runtime)
import Network.Streaming.NotImplemented (notImplemented)

-- | A status code.
newtype Status = Status Int
  deriving (Eq, Ord, Show)

-- | The header fields of a response, in the order they are sent, besides those
-- that the server adds: @Date@, @Server@, @Connection@ and the framing.
type ResponseHeaders = [(ByteString, ByteString)]

-- | How long the response's body is: declared, or not known, which is sent
-- chunked.
data Length = Known Int | Unknown
  deriving (Eq, Show)

-- | Sends the head of the response. A response to a request whose method or
-- status forbids a body has none, whatever the length says.
respond :: (Runtime arr) => arr (Call, (Status, ResponseHeaders, Length)) Answering
respond = notImplemented "respond"

-- | Sends the pieces of the body, in order: views go straight to the write.
send :: (Runtime arr) => arr (Answering, [ByteString]) ()
send = notImplemented "send"

-- | Ends the response, after the last of its body.
finish :: (Runtime arr) => arr Answering ()
finish = notImplemented "finish"
