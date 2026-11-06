-- | The PostgreSQL parts of serialized output, with the generic core of
-- "Network.Streaming.Output".
module Database.PostgreSQL.Internal.Protocol.Codec.Output
  ( ByteStream (..),
    Output,
    Writer,
    WriteResult (..),
    outputWriter,
    runWriter,
    outputLength,
    outputCopied,
    Uncopied (..),
    writeOutput,
    outputByteString,
    outputs,
    word8,
    word16,
    int16,
    word32,
    int32,
    bytes,
    cstring,
    stream,
    borrowedStream,
  )
where

import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Network.Streaming (Source)
import Network.Streaming.Output

-- | A zero byte inside the value would end the string early on the server,
-- so it is rejected.
cstring :: ByteString -> Output
cstring value
  | BS.elem 0 value = error "a C string sent to the server must not contain a zero byte"
  | otherwise = bytes value <> word8 0
{-# INLINE cstring #-}

-- | The source owns each chunk through its synchronous callback.
borrowedStream :: Int -> Source -> Output
borrowedStream count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedBorrowedStream count source) rest)
{-# INLINE borrowedStream #-}
