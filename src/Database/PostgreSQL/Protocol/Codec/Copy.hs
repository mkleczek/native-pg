{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | A copy-message-specific codec that explores a more direct, data-first API.
--
-- The generic codec in [Codec.hs](/Users/michal/supabase/Development/haskell-native-pg/src/Database/PostgreSQL/Protocol/Codec.hs)
-- models message bodies with a reusable type-level payload DSL. That is useful
-- when we want one interpreter for many message families, but it also means the
-- schema shows up in class constraints and type families.
--
-- This module intentionally explores the opposite end of the design space for a
-- very small, regular family:
--
-- * each copy message still has a singleton discriminator in
--   [MessageType.hs](/Users/michal/supabase/Development/haskell-native-pg/src/Database/PostgreSQL/Protocol/Messages/MessageType.hs)
-- * and the class instances reuse the shared type-level 'Payload' DSL that is
--   already indexed into 'CopyMessageType'
-- * the copy-specific class therefore does not need its own payload type family
--   or dedicated payload GADT
--
-- This lets us inspect what a "data definitions first, class second" codec
-- looks like without disturbing the rest of the protocol surface.
module Database.PostgreSQL.Protocol.Codec.Copy
  ( CopyMessageCodec (..),
    -- serializeCopyMessage,
    -- parseCopyMessage,
  )
where

import Database.PostgreSQL.Protocol.Codec.Payload (PayloadValue)
import Database.PostgreSQL.Protocol.Messages
  ( CopyMessage (..), CopyMessageType (..)
  )
import Data.Kind (Type)

class HasPayload (a::k) where
  type PayloadOf a :: Type

-- | A payload-indexed singleton carries its schema in its /kind/ rather than
-- in the surface syntax of the promoted constructor. For example:
--
-- * @'CopyDoneType@ is /not/ syntactically the type application
--   @CopyMessageType 99 'PayloadEnd@
-- * but it /does/ have kind @CopyMessageType 99 'PayloadEnd@
--
-- Matching on the kind indices is therefore the generic way to recover the
-- attached schema from any copy-message singleton.
instance forall tag payload (messageType :: CopyMessageType tag payload). HasPayload messageType where
  type PayloadOf (messageType :: CopyMessageType tag payload) = PayloadValue payload

-- | The semantic message GADT simply reuses the payload attached to its
-- singleton index.
instance forall tag payload (messageType :: CopyMessageType tag payload). HasPayload (CopyMessage messageType) where
  type PayloadOf (CopyMessage (messageType :: CopyMessageType tag payload)) = PayloadOf messageType

-- | Copy-family-specific bridge between semantic messages and their payloads.
--
-- Compared with the generic 'MessageCodec' class, there is no extra payload
-- type family here. The payload schema is already part of the 'CopyMessageType'
-- index, so each instance works directly with 'PayloadValue payload'.
class HasPayload m => CopyMessageCodec m where
  toPayload :: m -> PayloadOf m
  -- fromPayload :: PayloadValue (PayloadOf m) -> Either String m
  -- encodePayload :: PayloadValue (PayloadOf m) -> Either String ByteString
  -- parsePayload :: ByteString -> Either String (PayloadValue (PayloadOf m))

instance CopyMessageCodec (CopyMessage 'CopyDoneType) where
  toPayload CopyDone = ()
  -- fromPayload () = Right (CopyData bytes)
  -- encodePayload (bytes, ()) = Right bytes
  -- parsePayload body = Right (body, ())

-- instance CopyMessageCodec 'CopyDoneType where
--   toPayload CopyDone = ()
--   fromPayload () = Right CopyDone
--   encodePayload () = Right BS.empty
--   parsePayload body = do
--     ensureEmptyCopyBody "CopyDone" body
--     Right ()

-- instance CopyMessageCodec 'CopyFailType where
--   toPayload (CopyFail message) = (message, ())
--   fromPayload (message, ()) = Right (CopyFail message)
--   encodePayload (message, ()) = Right (encodeUtf8 message <> BS.singleton 0)
--   parsePayload body = do
--     message <- parseSingleCStringBody "CopyFail" body
--     Right (message, ())

-- serializeCopyMessage ::
--   forall tag payload (messageType :: CopyMessageType tag payload).
--   ( KnownNat tag,
--     CopyMessageCodec messageType
--   ) =>
--   CopyMessage messageType ->
--   Either String WireFrame
-- serializeCopyMessage message =
--   TaggedFrame (natAsWord8 (Proxy @tag))
--     <$> encodePayload @_ @_ @messageType (toPayload message)

-- parseCopyMessage :: WireFrame -> Either String SomeCopyMessage
-- parseCopyMessage wireFrame =
--   case wireFrame of
--     TaggedFrame 100 body ->
--       SomeCopyMessage <$> parseKnownCopyMessage @'CopyDataType body
--     TaggedFrame 99 body ->
--       SomeCopyMessage <$> parseKnownCopyMessage @'CopyDoneType body
--     TaggedFrame 102 body ->
--       SomeCopyMessage <$> parseKnownCopyMessage @'CopyFailType body
--     TaggedFrame actualTag _ ->
--       Left ("unknown copy message tag: " <> show actualTag)
--     StartupFrame {} ->
--       Left "expected tagged copy message, got startup frame"

-- parseKnownCopyMessage ::
--   forall messageType.
--   CopyMessageCodec messageType =>
--   ByteString ->
--   Either String (CopyMessage messageType)
-- parseKnownCopyMessage body =
--   parsePayload @_ @_ @messageType body >>= fromPayload @_ @_ @messageType

-- ensureEmptyCopyBody :: String -> ByteString -> Either String ()
-- ensureEmptyCopyBody label body
--   | BS.null body = Right ()
--   | otherwise =
--       Left $
--         label
--           <> " must not contain a body, but got "
--           <> show (BS.length body)
--           <> " bytes"

-- parseSingleCStringBody :: String -> ByteString -> Either String Text
-- parseSingleCStringBody label body =
--   case BS.elemIndex 0 body of
--     Nothing ->
--       Left (label <> " body must contain one terminating zero byte")
--     Just terminatorIndex ->
--       let (rawValue, restWithTerminator) = BS.splitAt terminatorIndex body
--           rest = BS.drop 1 restWithTerminator
--        in if BS.null rest
--             then
--               case decodeUtf8' rawValue of
--                 Left err ->
--                   Left ("invalid UTF-8 in " <> label <> " body: " <> show err)
--                 Right value ->
--                   Right value
--             else
--               Left $
--                 label
--                   <> " body must contain exactly one C-string, but left "
--                   <> show (BS.length rest)
--                   <> " trailing bytes"

-- natAsWord8 :: forall n. KnownNat n => Proxy n -> Word8
-- natAsWord8 proxy =
--   let value = natVal proxy
--    in if value > toInteger (maxBound :: Word8)
--         then
--           error $
--             "type-level literal "
--               <> show value
--               <> " does not fit into Word8"
--         else
--           fromInteger value
