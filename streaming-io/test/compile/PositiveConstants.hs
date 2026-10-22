{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

module PositiveConstants where

#include "MachDeps.h"

import Network.Streaming.PositiveInt

smallest :: PositiveInt
smallest = positiveInt @1

largest :: PositiveInt
#if WORD_SIZE_IN_BITS == 64
largest = positiveInt @9223372036854775807
#elif WORD_SIZE_IN_BITS == 32
largest = positiveInt @2147483647
#else
#error Unsupported Int width
#endif
