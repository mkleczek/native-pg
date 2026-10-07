{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}

-- | Shared type-level building blocks for the protocol payload DSL.
--
-- This module deliberately stays below the message layer. It only defines the
-- wire-oriented payload language itself; semantic interpretation is deferred
-- until a later design pass.
module Database.PostgreSQL.Protocol.Codec.Payload
  ( Empty,
    Payload (..),
  )
where

import GHC.TypeLits (Nat)

data Payload where
  CStringAtom :: Payload
  FixedBytesAtom :: Nat -> Payload
  RemainingBytesAtom :: Payload
  MaybeBytes32Atom :: Payload
  PayloadField :: k -> Payload
  PayloadZeroTerminated :: Payload -> Payload
  PayloadCounted16 :: Payload -> Payload
  PayloadCounted32 :: Payload -> Payload
  PayloadStruct :: [Payload] -> Payload
  -- | Exactly one of the alternatives, told apart by their leading constants.
  -- The first alternative whose constants match the input is used.
  PayloadChoice :: [Payload] -> Payload
  PayloadConst :: Nat -> k -> Payload
  PayloadBounded :: Payload -> Payload

type Empty = 'PayloadStruct '[]
