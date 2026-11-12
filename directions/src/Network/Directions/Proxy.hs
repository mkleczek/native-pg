-- | The proxy of the design notes, written once over the classes, and what it
-- is tried with: an echo server, a client in two directions, the same client
-- in one, and a transaction whose two directions exchange a decision.
module Network.Directions.Proxy
  ( pump,
    pumpWith,
    proxy,
    proxyThrough,
    echo,
    receiveAll,
    exchange,
    sendThenReceive,
    receiveBytes,
    transaction,
  )
where

import Control.Arrow (ArrowChoice, arr, first, returnA, (>>>), (^>>), (|||))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.IORef (IORef, modifyIORef')
import Data.Tuple (swap)
import Network.Streaming.Buffer (Buffer)
import Network.Streaming.Classes
import Network.Streaming.Combinators
import Network.Streaming.With

-- | One direction: what the source sends goes to the sink through the buffer,
-- until the source ends; then the sink's output ends. Its input is the source
-- and the sink.
pump :: (ArrowChoice arr, Link arr source, Link arr sink) => Buffer -> arr (source, sink) ()
pump = pumpWith id
{-# INLINABLE pump #-}

-- | 'pump', with what goes through changed on its way.
pumpWith :: (ArrowChoice arr, Link arr source, Link arr sink) => (ByteString -> ByteString) -> Buffer -> arr (source, sink) ()
pumpWith change buffer = loop
  where
    -- The links go through the loop beside what the steps work on, and not in an environment: a generic arrow runs each
    -- step of a loop as a call of its methods, and an environment adds some to every step.
    loop =
      (\links@(source, _) -> ((source, roomOf buffer), links))
        ^>> first receive
        >>> (\(count, links@(_, sink)) -> if count == 0 then Left sink else Right ((sink, [change (view buffer count)]), links))
        ^>> (shutdownSend ||| (first sendAll >>> snd ^>> loop))
{-# INLINABLE pumpWith #-}

-- | A proxy between a client's link and a server's: a direction each way.
proxy :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => Buffer -> Buffer -> arr (client, server) ()
proxy = proxyThrough id
{-# INLINABLE proxy #-}

-- | 'proxy', with the client's bytes changed on their way.
proxyThrough :: (ArrowChoice arr, Concurrent arr, Link arr client, Link arr server) => (ByteString -> ByteString) -> Buffer -> Buffer -> arr (client, server) ()
proxyThrough change requests answers = together [pumpWith change requests, swap ^>> pump answers]
{-# INLINABLE proxyThrough #-}

-- | A server that sends back what it receives, until its input ends: one
-- direction, from its link back to it.
echo :: (ArrowChoice arr, Link arr end) => Buffer -> arr end ()
echo buffer = (\end -> (end, end)) ^>> pump buffer
{-# INLINABLE echo #-}

-- | Keeps a copy of what arrives, the latest first, until the end of the
-- stream.
receiveAll :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> IORef [ByteString] -> arr end ()
receiveAll buffer kept = withOnly loop
  where
    loop =
      const (roomOf buffer)
        ^>> With receive
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
exchange :: (ArrowChoice arr, Concurrent arr, Effect arr, Link arr end) => Buffer -> ByteString -> IORef [ByteString] -> arr end ()
exchange buffer request kept = together [withOnly (const [request] ^>> With sendAll >>> asks shutdownSend), receiveAll buffer kept]
{-# INLINABLE exchange #-}

-- | The same client in one direction: it sends all of the request, and only
-- then reads the answer. While it sends it reads nothing, so once the buffers
-- on the way back are full, it and the server wait for each other.
sendThenReceive :: (ArrowChoice arr, Effect arr, Link arr end) => Buffer -> ByteString -> IORef [ByteString] -> arr end ()
sendThenReceive buffer request kept =
  withOnly (const [request] ^>> With sendAll >>> asks shutdownSend >>> asks (receiveAll buffer kept))
{-# INLINABLE sendThenReceive #-}

-- | Receives this many bytes, or what comes before the end of the stream, and
-- gives them.
receiveBytes :: (ArrowChoice arr, Link arr end) => Buffer -> Int -> arr end ByteString
receiveBytes buffer total = withOnly (const BS.empty ^>> loop)
  where
    loop = (roomOf buffer,) ^>> first (With receive) >>> collect ^>> (returnA ||| loop)
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
-- decision is a ticket of the arrow's own core that the transaction makes when
-- it runs, and the request side is given the end to wait at and the answer side
-- the end to give at.
transaction :: forall arr end. (ArrowChoice arr, Concurrent arr, Effect arr, Exchange arr (Local arr), Link arr end) => Buffer -> ByteString -> (ByteString -> Bool) -> IORef [ByteString] -> arr end ()
transaction buffer batch commits kept = withOnly (ticket @(With end arr) @(Local arr) >>> together [arr fst >>> requests, arr snd >>> answers])
  where
    requests =
      keeping (const [batch] ^>> With sendAll)
        >>> snd
        ^>> await
        >>> (\commit -> [BS8.pack (if commit then "COMMIT\n" else "ROLLBACK\n")])
        ^>> With sendAll
        >>> asks shutdownSend
    answers =
      keeping (asks (receiveBytes buffer (BS.length batch)))
        >>> (\(answer, decision) -> (decision, commits answer))
        ^>> give
        >>> arr (const ())
        >>> asks (receiveAll buffer kept)
{-# INLINABLE transaction #-}
