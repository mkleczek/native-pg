-- | Serialized bytes with their length known before they are written, so a
-- length prefix can be written ahead of them.
module Database.PostgreSQL.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
    outputLength,
    outputBuilder,
    outputStreams,
    outputByteString,
    word8,
    word16,
    int16,
    word32,
    int32,
    bytes,
    cstring,
    stream,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as LBS
import Data.Int (Int16, Int32)
import Data.Word (Word16, Word32, Word8)

-- | Bytes that arrive in chunks, with their total length known up front.
data ByteStream = ByteStream
  { byteStreamLength :: !Int,
    -- | The next chunk. An empty one means the end.
    byteStreamNext :: IO ByteString
  }

data Output = Output
  { outputLength :: {-# UNPACK #-} !Int,
    -- | The bytes before the first stream.
    outputBuilder :: Builder,
    -- | Each stream, followed by the bytes after it.
    outputStreams :: [(ByteStream, Builder)]
  }

instance Semigroup Output where
  Output leftLength left [] <> Output rightLength right rightStreams =
    Output (leftLength + rightLength) (left <> right) rightStreams
  Output leftLength left leftStreams <> Output rightLength right rightStreams =
    Output (leftLength + rightLength) left (appendAfter leftStreams)
    where
      appendAfter [(source, after)] = (source, after <> right) : rightStreams
      appendAfter (first : others) = first : appendAfter others
      appendAfter [] = rightStreams
  {-# INLINE (<>) #-}

instance Monoid Output where
  mempty = Output 0 mempty []
  {-# INLINE mempty #-}

-- | The bytes of an output that has no streams.
outputByteString :: Output -> ByteString
outputByteString (Output _ builder []) = LBS.toStrict (Builder.toLazyByteString builder)
outputByteString _ = error "an output with streams has no byte string"

word8 :: Word8 -> Output
word8 value = Output 1 (Builder.word8 value) []
{-# INLINE word8 #-}

word16 :: Word16 -> Output
word16 value = Output 2 (Builder.word16BE value) []
{-# INLINE word16 #-}

int16 :: Int16 -> Output
int16 value = Output 2 (Builder.int16BE value) []
{-# INLINE int16 #-}

word32 :: Word32 -> Output
word32 value = Output 4 (Builder.word32BE value) []
{-# INLINE word32 #-}

int32 :: Int32 -> Output
int32 value = Output 4 (Builder.int32BE value) []
{-# INLINE int32 #-}

-- | Large byte strings end up as their own chunks of the output, without
-- being copied.
bytes :: ByteString -> Output
bytes value = Output (BS.length value) (Builder.byteString value) []
{-# INLINE bytes #-}

-- | A zero byte inside the value would end the string early on the server,
-- so it is rejected.
cstring :: ByteString -> Output
cstring value
  | BS.elem 0 value = error "a C string sent to the server must not contain a zero byte"
  | otherwise = bytes value <> word8 0
{-# INLINE cstring #-}

-- | Bytes the client supplies as they're sent.
stream :: ByteStream -> Output
stream source = Output (byteStreamLength source) mempty [(source, mempty)]
{-# INLINE stream #-}
