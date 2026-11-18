-- | Where the server listens: a provider of the listening socket, which the acceptor opens when the server starts and
-- gives back when it ends, so that a program is built without a socket, and the socket is made where the program runs.
module Network.Fulmen.Listen
  ( Listen (..),
    tcp,
    inherited,
    borrowed,
  )
where

import Network.Streaming.Server (Listen (..), borrowed, inherited, tcp)
