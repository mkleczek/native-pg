-- | Where the server listens: a provider of the listening socket, which the acceptor opens when the server starts and
-- gives back when it ends, so that a program is built without a socket, and the socket is made where the program runs.
module Network.Streaming.Internal.Listen
  ( Listen (..),
    tcp,
    inherited,
    borrowed,
  )
where

import Control.Arrow (Arrow, arr)
import Control.Exception (onException)
import Network.Socket (AddrInfo (..), Socket)
import qualified Network.Socket as Socket
import Network.Streaming.Classes (Annotate, Effect)
import Network.Streaming.Combinators (box, done, step)
import Network.Streaming.Internal.Descriptors (fresh)
import System.Posix.Types (Fd (..))

-- | How the acceptor gets its listening socket when the server starts, and gives it back when it ends.
data Listen arr = Listen {openListener :: arr () Socket, closeListener :: arr Socket ()}

-- | A socket that is bound to the address and listens, made when the server starts and closed when it ends. The address
-- is as 'Socket.getAddrInfo' gives it, and the backlog is how many connections may wait past the workers that are free.
tcp :: (Effect arr, Annotate arr) => AddrInfo -> Int -> Listen arr
tcp address backlog = Listen (step "bind and listen" (const bound)) closing
  where
    bound = do
      socket <- Socket.openSocket address
      prepare socket `onException` Socket.close socket
      socket <$ fresh socket
    prepare socket = do
      Socket.setSocketOption socket Socket.ReuseAddr 1
      Socket.bind socket (addrAddress address)
      Socket.listen socket backlog

-- | The socket that the process was started with, as systemd's socket activation gives it, or as a restart hands it
-- over: it is made from the descriptor when the server starts, and closed when it ends.
inherited :: (Effect arr, Annotate arr) => Fd -> Listen arr
inherited (Fd descriptor) = Listen (step "adopt inherited listener" (const adopted)) closing
  where
    adopted = Socket.mkSocket descriptor >>= \socket -> socket <$ fresh socket

-- | A socket that the caller made, which listens already and which the caller closes: the server doesn't. A program
-- that is built with it is tied to that socket.
borrowed :: (Arrow arr, Annotate arr) => Socket -> Listen arr
borrowed socket = Listen (box "use borrowed listener" (arr (const socket))) (box "leave listener open" done)

-- | Closes the listener.
closing :: (Effect arr, Annotate arr) => arr Socket ()
closing = step "close listener" Socket.close
