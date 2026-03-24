{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Shared type-level building blocks for the protocol codec DSL.
--
-- This module deliberately stays below both the message layer and the semantic
-- value layer. It only defines:
--
-- * the wire-oriented payload language itself
-- * the generic interpretation of that language into Haskell tuple types
-- * an open 'DenotationOf' family that lets nested payload fields refer to
--   singleton-indexed semantic values from higher layers
--
-- The important shift in the current design is that nested values no longer go
-- through a separate @ValueName / ValueType@ universe. A payload field may now
-- point directly at any payload-carrying singleton, and the semantic type for
-- that singleton is recovered through 'DenotationOf'.
module Database.PostgreSQL.Protocol.Codec.Payload
  ( Atom (..),
    Payload (..),
    DenotationOf,
    PayloadValue,
  )
where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)
import Data.Kind (Type)
import Data.Text (Text)
import Data.Word (Word32, Word8)
import GHC.TypeLits (Nat)

-- | Field-level atoms understood by the generic payload interpreter.
--
-- Most atoms correspond directly to wire fragments such as integers or
-- C-strings. Nested semantic values are represented directly by 'PayloadField'
-- rather than going through a dedicated wrapper atom.
data Atom where
  Int16Atom :: Atom
  Int32Atom :: Atom
  Word32Atom :: Atom
  ByteAtom :: Atom
  CStringAtom :: Atom
  CStringListAtom :: Atom
  FixedBytesAtom :: Nat -> Atom
  RemainingBytesAtom :: Atom
  MaybeBytes32Atom :: Atom
  CStringPairListZeroTerminatedAtom :: Atom
  ByteCStringPairListZeroTerminatedAtom :: Atom

data Payload where
  PayloadField :: k -> Payload
  PayloadCounted16 :: Payload -> Payload
  PayloadStruct :: [Payload] -> Payload

-- | Open mapping from a protocol-level singleton or atom to its denoted
-- Haskell type.
--
-- Message singletons and value singletons contribute instances from higher
-- layers, while atoms are defined here directly.
type family DenotationOf (a :: k) :: Type

type instance DenotationOf ('Int16Atom :: Atom) = Int16
type instance DenotationOf ('Int32Atom :: Atom) = Int32
type instance DenotationOf ('Word32Atom :: Atom) = Word32
type instance DenotationOf ('ByteAtom :: Atom) = Word8
type instance DenotationOf ('CStringAtom :: Atom) = Text
type instance DenotationOf ('CStringListAtom :: Atom) = [Text]
type instance DenotationOf ('FixedBytesAtom n :: Atom) = ByteString
type instance DenotationOf ('RemainingBytesAtom :: Atom) = ByteString
type instance DenotationOf ('MaybeBytes32Atom :: Atom) = Maybe ByteString
type instance DenotationOf ('CStringPairListZeroTerminatedAtom :: Atom) = [(Text, Text)]
type instance DenotationOf ('ByteCStringPairListZeroTerminatedAtom :: Atom) = [(Word8, Text)]

type family PayloadValue (payload :: Payload) :: Type where
  PayloadValue ('PayloadField atom) = DenotationOf atom
  PayloadValue ('PayloadCounted16 payload) = [PayloadValue payload]
  PayloadValue ('PayloadStruct '[]) = ()
  PayloadValue ('PayloadStruct '[payload]) = PayloadValue payload
  PayloadValue ('PayloadStruct (left ': right ': payloads)) =
    (PayloadValue left, PayloadValue ('PayloadStruct (right ': payloads)))
