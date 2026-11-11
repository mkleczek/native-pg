{-# LANGUAGE LambdaCase #-}

-- | The proxy of the design notes, written once over the classes, and what it
-- is tried with: an echo server, a client in two directions, and the same
-- client in one.
module Network.Directions.Proxy
  ( pump,
    pumpWith,
    proxy,
    proxyThrough,
    echo,
    together,
    receiveAll,
    exchange,
    sendThenReceive,
  )
where

import Control.Arrow (Arrow, ArrowChoice, returnA, (>>>), (>>^), (^>>), (|||))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef')
import Network.Directions.Classes

-- | One direction: what the source sends goes to the sink through the buffer,
-- until the source ends; then the sink's output ends.
pump :: (ArrowChoice arr, Link arr source, Link arr sink) => Buffer -> source -> sink -> arr () ()
pump = pumpWith id

-- | 'pump', with what goes through changed on its way.
pumpWith :: (ArrowChoice arr, Link arr source, Link arr sink) => (ByteString -> ByteString) -> Buffer -> source -> sink -> arr () ()
pumpWith change buffer source sink = loop
  where
    loop =
      const (roomOf buffer)
        ^>> receive source
        >>> (\count -> if count == 0 then Left () else Right [change (view buffer count)])
        ^>> (shutdownSend sink ||| (sendAll sink >>> loop))

-- | A proxy between a client's link and a server's: a direction each way.
proxy :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => Buffer -> Buffer -> client -> server -> arr () ()
proxy = proxyThrough id

-- | 'proxy', with the client's bytes changed on their way.
proxyThrough :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => (ByteString -> ByteString) -> Buffer -> Buffer -> client -> server -> arr () ()
proxyThrough change requests answers client server = together [pumpWith change requests client server, pump answers server client]

-- | A server that sends back what it receives, until its input ends: one
-- direction, from its link back to it.
echo :: (ArrowChoice arr, Link arr end) => Buffer -> end -> arr () ()
echo buffer end = pump buffer end end

-- | Runs the arrows at once, until all have ended.
together :: (Arrow arr, Concurrent arr) => [arr () ()] -> arr () ()
together = \case
  [] -> returnA
  [one] -> one
  one : others -> concurrently one (together others) >>^ const ()

-- | Keeps a copy of what arrives, the latest first, until the end of the
-- stream.
receiveAll :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> end -> IORef [ByteString] -> arr () ()
receiveAll buffer end kept = loop
  where
    loop =
      const (roomOf buffer)
        ^>> receive end
        >>> (\count -> if count == 0 then Left () else Right count)
        ^>> (returnA ||| (now keep >>> loop))
    keep count = do
      -- The view is of the buffer, which the next receive reuses, so it's
      -- copied now.
      let !copy = BS.copy (view buffer count)
      modifyIORef' kept (copy :)

-- | A client in two directions: one sends the request and ends it, the other
-- keeps the answer until it ends.
exchange :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () ()
exchange buffer end request kept = together [const [request] ^>> sendAll end >>> shutdownSend end, receiveAll buffer end kept]

-- | The same client in one direction: it sends all of the request, and only
-- then reads the answer. While it sends it reads nothing, so once the buffers
-- on the way back are full, it and the server wait for each other.
sendThenReceive :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () ()
sendThenReceive buffer end request kept =
  const [request] ^>> sendAll end >>> shutdownSend end >>> receiveAll buffer end kept
