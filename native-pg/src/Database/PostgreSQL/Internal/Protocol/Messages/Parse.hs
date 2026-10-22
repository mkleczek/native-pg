module Database.PostgreSQL.Internal.Protocol.Messages.Parse
  ( parseMessage,
    parser,
    serialize,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.PostgreSQL.Internal.Protocol.Codec
  ( Input,
    Output,
    Parsable (..),
    Serializable (..),
    runBodyParser,
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types (Envelope)

parser :: forall message {payload} {schema}.
  (message ~ Envelope payload schema, Parsable message schema) => Input message
parser = fieldParser @message @schema
{-# INLINE parser #-}

parseMessage :: forall message {payload} {schema}.
  (message ~ Envelope payload schema, Parsable message schema) => ByteString -> Either String message
parseMessage input = do
  (message, rest) <- runBodyParser input (parser @message)
  if BS.null rest
    then pure message
    else
      Left $
        "parser left "
          <> show (BS.length rest)
          <> " unconsumed bytes"

serialize :: forall a s. Serializable (Envelope a s) s => Envelope a s -> Output
serialize = serializeField @(Envelope a s) @s
{-# INLINE serialize #-}
