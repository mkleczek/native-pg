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
import Network.Streaming.Output
import Network.Streaming.Source (AnySource (..), Source)

-- | A zero byte inside the value would end the string early on the server,
-- so it is rejected.
cstring :: ByteString -> Output
cstring value
  | BS.elem 0 value = error "a C string sent to the server must not contain a zero byte"
  | otherwise = bytes value <> word8 0
{-# INLINE cstring #-}

-- | Bytes pulled from the source as they're sent, which declares how many
-- there are. A chunk stays valid until the source is pulled again.
borrowedStream :: Source s => Int -> s -> Output
borrowedStream count source = Output count 0 $ \rest ->
  Writer $ \_ _ used -> pure (Yield used (UncopiedSource count (AnySource source)) rest)
{-# INLINE borrowedStream #-}
