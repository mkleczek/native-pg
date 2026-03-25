{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Database.PostgreSQL.Protocol.Messages.Parse
  ( parseEnvelope,
  )
where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Database.PostgreSQL.Protocol.Codec
  ( Parsable (..)
  )
import Database.PostgreSQL.Protocol.Codec.Input
  ( runBodyParser
  )
import Database.PostgreSQL.Protocol.Messages.Types
  ( Envelope (Envelope)
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
