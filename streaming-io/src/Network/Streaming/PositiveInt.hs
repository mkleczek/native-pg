{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | Positive values that fit the target platform's 'Int'.
module Network.Streaming.PositiveInt
  ( PositiveInt, positiveInt, mkPositiveInt, positiveIntValue ) where

#include "MachDeps.h"

import Data.Proxy (Proxy (..))
import GHC.TypeNats (KnownNat, natVal, type (<=))

-- The bound follows the compilation target, including cross compilation.
#if WORD_SIZE_IN_BITS == 64
type MaxInt = 9223372036854775807
#elif WORD_SIZE_IN_BITS == 32
type MaxInt = 2147483647
#else
#error Unsupported Int width
#endif

newtype PositiveInt = PositiveInt Int
  deriving (Eq, Ord)

instance Show PositiveInt where
  showsPrec precedence (PositiveInt value) = showsPrec precedence value

-- | Construct a constant whose positivity and range are checked at compile time.
positiveInt :: forall n. (KnownNat n, 1 <= n, n <= MaxInt) => PositiveInt
positiveInt = PositiveInt (fromIntegral (natVal (Proxy @n)))
{-# INLINE positiveInt #-}

-- | Check an arbitrary integer before converting it to the bounded representation.
mkPositiveInt :: Integer -> Maybe PositiveInt
mkPositiveInt value
  | value <= 0 || value > toInteger (maxBound :: Int) = Nothing
  | otherwise = Just (PositiveInt (fromInteger value))

positiveIntValue :: PositiveInt -> Int
positiveIntValue (PositiveInt value) = value
{-# INLINE positiveIntValue #-}
