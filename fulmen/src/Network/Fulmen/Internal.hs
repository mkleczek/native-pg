-- | What a call is made of, which the server makes and the operations of
-- "Network.Fulmen.Request" and "Network.Fulmen.Response" take apart. The public
-- modules export these types without their constructors.
module Network.Fulmen.Internal
  ( Call (..),
    Body (..),
    Answering (..),
    Cores (..),
  )
where

import Network.Streaming.Classes (Effect, Stream (..))
import Network.Streaming.Loans (Loans)
import Network.Streaming.NotImplemented (notImplemented)

-- | One request in flight: its loans, its body and, once it has a head, its answer.
data Call = Call Loans Body

-- | The request body: its pieces are views of the request's buffer, valid until
-- the next 'next', and its end is the end of the body, whichever way it was
-- framed. Reading it is a stub, which fails with
-- 'Network.Streaming.NotImplemented.NotImplemented' until the server is there.
data Body = Body

instance (Effect arr) => Stream arr Body where
  next = notImplemented "next"

-- | A response whose head has been sent.
data Answering = Answering

-- | The cores that a server runs on, and what its parts share across them: the
-- free workers that the acceptor hands connections to.
newtype Cores = Cores Int
