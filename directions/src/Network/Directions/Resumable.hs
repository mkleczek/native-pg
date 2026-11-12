{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}

-- | The other way to write a parser for a direction: a monad that never waits
-- and never does IO. A parser takes the bytes in hand and runs as far as they
-- go; when it needs more it comes to a 'More', and the arrow that drives it,
-- 'parse', receives and resumes it. A piece of a streamed field comes to a
-- 'Yield', for the driver to give to a consumer. Only a refill or a piece costs
-- the arrow anything.
module Network.Directions.Resumable
  ( Parser,
    takeBytes,
    takeWord,
    streamBytes,
    parse,
  )
where

import Control.Arrow (ArrowChoice, first, returnA, (>>>), (^>>), (|||))
import Control.Monad (ap, liftM)
import Data.Bits (shiftL, (.|.))
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import Foreign.Marshal.Utils (moveBytes)
import Foreign.Ptr (castPtr)
import GHC.ForeignPtr (unsafeWithForeignPtr)
import Network.Streaming.Buffer (Buffer (..))
import Network.Streaming.Classes hiding (Exit (..))

-- | Where a parser is: done, with the bytes it left in hand, or in need of
-- bytes, or with a piece of a field to give away.
data Parse a
  = Done a ByteString
  | -- | It needs this many bytes: what is in hand, and how to go on with the
    -- bytes in hand after they are received.
    More !Int ByteString (ByteString -> Parse a)
  | Yield ByteString (Parse a)

-- | A parser, in continuation-passing style: it is given the bytes in hand and
-- what to do with its result and what it leaves.
newtype Parser a = Parser (forall r. ByteString -> (a -> ByteString -> Parse r) -> Parse r)

instance Functor Parser where
  fmap = liftM
  {-# INLINE fmap #-}

instance Applicative Parser where
  pure a = Parser (\held done -> done a held)
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}

instance Monad Parser where
  Parser m >>= k = Parser (\held done -> m held (\a rest -> let Parser n = k a in n rest done))
  {-# INLINE (>>=) #-}

-- | The next n bytes, copied, so that they outlive the buffer's refills.
takeBytes :: Int -> Parser ByteString
takeBytes n = Parser go
  where
    go held done
      | BS.length held >= n = let (value, rest) = BS.splitAt n held; !copy = BS.copy value in done copy rest
      | otherwise = More n held (`go` done)
{-# INLINE takeBytes #-}

-- | The next n bytes, as a big-endian number.
takeWord :: Int -> Parser Int
takeWord n = Parser go
  where
    go held done
      | BS.length held >= n = let !value = BS.foldl' (\sofar byte -> sofar `shiftL` 8 .|. fromIntegral byte) 0 (BS.take n held) in done value (BS.drop n held)
      | otherwise = More n held (`go` done)
{-# INLINE takeWord #-}

-- | The next n bytes as pieces of what is in hand, given away as they come.
streamBytes :: Int -> Parser ()
streamBytes n = Parser (go n)
  where
    go 0 held done = done () held
    go remaining held done
      | BS.null held = More 1 held (\filled -> go remaining filled done)
      | otherwise =
          let (piece, rest) = BS.splitAt remaining held
           in Yield piece (go (remaining - BS.length piece) rest done)
{-# INLINE streamBytes #-}

-- | Runs the parser on the bytes in hand, which are in the buffer, receives
-- when it needs more, and gives each piece it yields to the consumer. It gives
-- the parser's result and the bytes it left in hand.
parse :: (ArrowChoice arr, Effect arr, Link arr link) => link -> Buffer -> arr ByteString () -> Parser a -> arr ByteString (a, ByteString)
parse link buffer consumer (Parser parser) = (`parser` Done) ^>> loop
  where
    loop = step ^>> ((more ||| chunk) ||| returnA)
    more = first (fill link buffer) >>> (\(received, go) -> go received) ^>> loop
    chunk = first consumer >>> snd ^>> loop
    step = \case
      Done value rest -> Right (value, rest)
      More count held go -> Left (Left ((count, held), go))
      Yield piece next -> Left (Right (piece, next))
{-# INLINABLE parse #-}

-- | Moves the bytes in hand to the buffer's start, and receives after them
-- until there are as many as the parser needs.
fill :: (ArrowChoice arr, Effect arr, Link arr link) => link -> Buffer -> arr (Int, ByteString) ByteString
fill link buffer = now compact >>> loop
  where
    Buffer memory size = buffer
    compact (count, held)
      | count > size = ioError (userError "an atom larger than the buffer")
      | otherwise = do
          unsafeWithForeignPtr memory (\target -> BSU.unsafeUseAsCStringLen held (\(from, n) -> moveBytes target (castPtr from) n))
          pure (count, BS.length held)
    loop = enough ^>> ((first (receive link) >>> added ^>> loop) ||| returnA)
    enough (count, filled)
      | filled >= count = Right (BSI.BS memory filled)
      | otherwise = Left (Room buffer filled (size - filled), (count, filled))
    added (received, (count, filled))
      | received <= 0 = error "the stream ended inside a value"
      | otherwise = (count, filled + received)
{-# INLINABLE fill #-}
