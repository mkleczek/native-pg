module Database.PostgreSQL.Internal.Protocol.Messages.Parse
  ( serialize,
  )
where

import Database.PostgreSQL.Internal.Protocol.Codec
  ( Output,
    Serializable (..),
  )
import Database.PostgreSQL.Internal.Protocol.Messages.Types (Envelope)

serialize :: forall a s. Serializable (Envelope a s) s => Envelope a s -> Output
serialize = serializeField @(Envelope a s) @s
{-# INLINE serialize #-}
