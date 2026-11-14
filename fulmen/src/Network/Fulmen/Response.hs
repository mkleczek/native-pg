-- | Answering: the head, which says how long the body is, and the body, which
-- is sent as the views that it is given. The head is held until the first of the body, so that they go out in one
-- write.
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

import Control.Arrow ((&&&), (***), (^>>))
import Data.ByteString (ByteString)
import Network.Fulmen.Internal (Answering (..), Call (..))
import Network.Fulmen.Internal.Response (Exchange, Length (..), ResponseHeaders, Status (..), begin, transmit)
import Network.Streaming.Classes (Effect (..), Runtime)

-- | Says how the request is answered: the head is sent with the first of the body, or at the end of the response. A
-- response to a request whose method or status forbids a body has none, whatever the length says, and a
-- 'Known' length that the body doesn't have ends the connection.
respond :: (Runtime arr) => arr (Call, (Status, ResponseHeaders, Length)) Answering
respond = now (\(Call _ _ exchange, response) -> Answering exchange <$ begin exchange response)

-- | Sends the pieces of the body, in order: views go straight to the write.
send :: (Runtime arr) => arr (Answering, [ByteString]) ()
send = (exchangeOf *** (, False)) ^>> transmit

-- | Ends the response, after the last of its body.
finish :: (Runtime arr) => arr Answering ()
finish = (exchangeOf &&& const ([], True)) ^>> transmit

-- | The exchange that the response is made in.
exchangeOf :: Answering -> Exchange
exchangeOf (Answering exchange) = exchange
