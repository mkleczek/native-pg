{-# LANGUAGE LambdaCase #-}

-- | The proxy of the design notes, written once over the classes, and what it
-- is tried with: an echo server, a client in two directions, the same client
-- in one, and a transaction whose two directions exchange a decision.
module Network.Directions.Proxy
  ( pump,
    pumpWith,
    proxy,
    proxyThrough,
    echo,
    together,
    times,
    receiveAll,
    exchange,
    sendThenReceive,
    receiveBytes,
    transaction,
  )
where

import Control.Arrow (Arrow, ArrowChoice, arr, first, returnA, (>>>), (>>^), (^>>), (|||))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.IORef (IORef, modifyIORef')
import Network.Directions.Classes

-- | One direction: what the source sends goes to the sink through the buffer,
-- until the source ends; then the sink's output ends.
pump :: (ArrowChoice arr, Link arr source, Link arr sink) => Buffer -> source -> sink -> arr () ()
pump = pumpWith id
{-# INLINABLE pump #-}

-- | 'pump', with what goes through changed on its way.
pumpWith :: (ArrowChoice arr, Link arr source, Link arr sink) => (ByteString -> ByteString) -> Buffer -> source -> sink -> arr () ()
pumpWith change buffer source sink = loop
  where
    loop =
      const (roomOf buffer)
        ^>> receive source
        >>> (\count -> if count == 0 then Left () else Right [change (view buffer count)])
        ^>> (shutdownSend sink ||| (sendAll sink >>> loop))
{-# INLINABLE pumpWith #-}

-- | A proxy between a client's link and a server's: a direction each way.
proxy :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => Buffer -> Buffer -> client -> server -> arr () ()
proxy = proxyThrough id
{-# INLINABLE proxy #-}

-- | 'proxy', with the client's bytes changed on their way.
proxyThrough :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => (ByteString -> ByteString) -> Buffer -> Buffer -> client -> server -> arr () ()
proxyThrough change requests answers client server = together [pumpWith change requests client server, pump answers server client]
{-# INLINABLE proxyThrough #-}

-- | A server that sends back what it receives, until its input ends: one
-- direction, from its link back to it.
echo :: (ArrowChoice arr, Link arr end) => Buffer -> end -> arr () ()
echo buffer end = pump buffer end end
{-# INLINABLE echo #-}

-- | Runs the arrows at once on the same input, until all have ended.
together :: (Arrow arr, Concurrent arr) => [arr a ()] -> arr a ()
together = \case
  [] -> arr (const ())
  [one] -> one
  one : others -> concurrently one (together others) >>^ const ()
{-# INLINABLE together #-}

-- | Runs the arrow this many times.
times :: (ArrowChoice arr) => Int -> arr () () -> arr () ()
times count act = const count ^>> loop
  where
    loop = (\k -> if k == 0 then Left () else Right (k - 1)) ^>> (returnA ||| (((),) ^>> first act >>> arr snd >>> loop))
{-# INLINABLE times #-}

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
{-# INLINABLE receiveAll #-}

-- | A client in two directions: one sends the request and ends it, the other
-- keeps the answer until it ends.
exchange :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () ()
exchange buffer end request kept = together [const [request] ^>> sendAll end >>> shutdownSend end, receiveAll buffer end kept]
{-# INLINABLE exchange #-}

-- | The same client in one direction: it sends all of the request, and only
-- then reads the answer. While it sends it reads nothing, so once the buffers
-- on the way back are full, it and the server wait for each other.
sendThenReceive :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> end -> ByteString -> IORef [ByteString] -> arr () ()
sendThenReceive buffer end request kept =
  const [request] ^>> sendAll end >>> shutdownSend end >>> receiveAll buffer end kept
{-# INLINABLE sendThenReceive #-}

-- | Receives this many bytes, or what comes before the end of the stream, and
-- gives them.
receiveBytes :: (ArrowChoice arr, Link arr end) => Buffer -> end -> Int -> arr () ByteString
receiveBytes buffer end total = const BS.empty ^>> loop
  where
    loop = (roomOf buffer,) ^>> first (receive end) >>> collect ^>> (returnA ||| loop)
    collect (count, got)
      | count == 0 || BS.length next >= total = Left next
      | otherwise = Right next
      where
        -- The view is of the buffer, which the next receive reuses, so it's
        -- copied now.
        !next = got <> BS.copy (view buffer count)
{-# INLINABLE receiveBytes #-}

-- | A transaction's two directions, as a client of a server that answers each
-- line: the request side sends the batch, waits for the decision that the
-- answer side gives it, and sends the ending that was decided; the answer side
-- reads the batch's answer, decides from it, and keeps what follows. The
-- decision is a ticket that the transaction makes when it runs, and both
-- sides are given.
transaction :: (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr, Link arr end) => Buffer -> end -> ByteString -> (ByteString -> Bool) -> IORef [ByteString] -> arr () ()
transaction buffer end batch commits kept = newTicket >>> together [requests, answers]
  where
    requests =
      ([batch],)
        ^>> first (sendAll end)
        >>> snd
        ^>> await
        >>> (\commit -> [BS8.pack (if commit then "COMMIT\n" else "ROLLBACK\n")])
        ^>> sendAll end
        >>> shutdownSend end
    answers =
      ((),)
        ^>> first (receiveBytes buffer end (BS.length batch))
        >>> (\(answer, decision) -> (decision, commits answer))
        ^>> fulfil
        >>> receiveAll buffer end kept
{-# INLINABLE transaction #-}
