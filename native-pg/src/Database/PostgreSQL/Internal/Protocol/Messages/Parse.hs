module Database.PostgreSQL.Internal.Protocol.Messages.Parse
  ( parseEnvelope,
    serializeEnvelope,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.PostgreSQL.Internal.Protocol.Codec
  ( Output,
    Parsable (..),
    Serializable (..),
    runBodyParser,
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types
  ( Envelope (Envelope),
  )

parseEnvelope :: forall a s. Parsable a s => ByteString -> Either String (Envelope a s)
parseEnvelope input = do
  (message, rest) <- runBodyParser input (parser @a @s)
  if BS.null rest
    then pure (Envelope message)
    else
      Left $
        "parser left "
          <> show (BS.length rest)
          <> " unconsumed bytes"

serializeEnvelope :: forall a s. Serializable a s => Envelope a s -> Output
serializeEnvelope (Envelope message) = serialize @a @s message
